"""Record native quality Job and opt-in read-only SQL warehouse evidence.

Uses official CLI OAuth; never logs credentials, warehouse configuration, or raw errors.
Public data is bounded synthetic view output. No warehouse creation/configuration changes.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.platforms.managed_usage import UsageTables
from src.platforms.usage_portfolio import require_quality_namespace, serving_sql, VIEWS


def canonical_rows(rows):
    return sorted(rows, key=lambda r: json.dumps(r, sort_keys=True))


def validate_job(run, quality, serving, namespace):
    if run.get("state", {}).get("result_state") != "SUCCESS":
        raise ValueError("quality Job did not succeed")
    tasks = run.get("tasks", [])
    if (len(tasks) != 2 or {t["task_key"] for t in tasks} != {"quality_experiment", "serving_views"}
            or any(t.get("state", {}).get("result_state") != "SUCCESS" for t in tasks)):
        raise ValueError("both native tasks must succeed")
    for result in (quality, serving):
        if (result.get("execution_environment") != "databricks-serverless"
                or result.get("synthetic") is not True or result.get("verified") is not True
                or result.get("namespace") != namespace):
            raise ValueError("expected verified native synthetic evidence in the selected namespace")
    for flag in ("quality_failure_blocked", "published_versions_unchanged_on_failure",
                 "replay_business_match", "bronze_unchanged_on_replay"):
        if quality.get(flag) is not True:
            raise ValueError("quality/recovery evidence incomplete")
    if (quality.get("historical_totals") != {"20260128": 680, "20260129": 660}
            or quality.get("recovered_totals") != {"20260128": 700, "20260129": 660}):
        raise ValueError("unexpected historical/corrected fixture totals")
    if quality["final_versions"]["bronze"] != quality["recovered_versions"]["bronze"]:
        raise ValueError("Bronze version changed during replay")
    if quality["historical_station_snapshot"]["sha256"] == quality["business_snapshot"]["station"]["sha256"]:
        raise ValueError("time travel did not show the intended correction")
    if set(serving.get("view_rows", {})) != set(VIEWS):
        raise ValueError("missing SQL serving views")
    status = serving["view_rows"]["usage_quality_status_v2"]
    if ({r["use_ymd"] for r in status} != {"20260128", "20260129"} or len(status) != 2
            or any(r["quality_status"] != "PASS" or r["data_kind"] != "synthetic" for r in status)):
        raise ValueError("serving quality status failed")
    return {"verified": True, "synthetic": True, "job_id": run["job_id"], "run_id": run["run_id"],
            "elapsed_seconds": (run["end_time"] - run["start_time"]) / 1000,
            "tasks": [{"task_key": t["task_key"], "run_id": t["run_id"],
                       "result_state": t["state"]["result_state"],
                       "attempt_number": t.get("attempt_number", 0)} for t in tasks],
            "quality": quality, "serving": serving}


def validate_serving_update(run):
    if run.get("state", {}).get("result_state") != "SUCCESS":
        raise ValueError("serving update did not succeed")
    selected = [t for t in run.get("tasks", []) if t["task_key"] == "serving_views"]
    if len(selected) != 1 or selected[0].get("state", {}).get("result_state") != "SUCCESS":
        raise ValueError("selected serving task did not succeed")
    if any(t["task_key"] != "serving_views" and (
           t.get("state", {}).get("life_cycle_state") != "SKIPPED" or
           t.get("state", {}).get("result_state") not in {"DISABLED", "EXCLUDED", "SKIPPED"})
           for t in run.get("tasks", [])):
        raise ValueError("serving-only update unexpectedly executed another task")
    return selected[0]


def decode_statement(response):
    if response.get("status", {}).get("state") != "SUCCEEDED":
        raise ValueError("SQL statement did not succeed")
    manifest, result = response.get("manifest", {}), response.get("result", {})
    if (manifest.get("truncated") or manifest.get("total_chunk_count") != 1
            or result.get("next_chunk_index") is not None or result.get("next_chunk_internal_link")):
        raise ValueError("SQL evidence must be complete, bounded, and inline")
    columns = sorted(manifest.get("schema", {}).get("columns", []), key=lambda c: c["position"])
    if (not columns or len(columns) > 32 or [c["position"] for c in columns] != list(range(len(columns)))
            or len({c["name"] for c in columns}) != len(columns)):
        raise ValueError("invalid SQL result schema")
    values = result.get("data_array", [])
    if len(values) > 100 or len(values) != manifest.get("total_row_count"):
        raise ValueError("missing, oversized, or partial SQL rows")
    rows = []
    for values_row in values:
        if len(values_row) != len(columns):
            raise ValueError("SQL row shape differs from schema")
        row = {}
        for column, value in zip(columns, values_row):
            if value is not None and column["type_name"] in {"BIGINT", "INT", "SMALLINT", "TINYINT", "LONG", "INTEGER", "SHORT", "BYTE"}:
                value = int(value)
            row[column["name"]] = value
        rows.append(row)
    return {"statement_id": response["statement_id"], "state": "SUCCEEDED", "rows": canonical_rows(rows)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", default="databricks")
    parser.add_argument("--profile", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--serving-run-id", type=int)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--execute-sql", action="store_true")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    tables = UsageTables(args.namespace)
    require_quality_namespace(tables)
    if len(args.namespace.split(".")) != 2 or not re.fullmatch(r"[a-f0-9]{16}", args.warehouse_id):
        parser.error("select catalog.schema and an existing warehouse ID")
    if not args.execute_sql:
        parser.error("warehouse queries require explicit --execute-sql")
    if args.output_dir.exists():
        parser.error("output exists; preserve prior evidence")

    def cli(*command):
        process = subprocess.run([args.cli, *command, "--profile", args.profile, "--output", "json"],
                                 capture_output=True, text=True, timeout=90)
        if process.returncode:
            raise RuntimeError("Databricks API call failed; raw response is not logged")
        return json.loads(process.stdout)

    run = cli("jobs", "get-run", str(args.run_id))
    outputs = {}
    for task in run.get("tasks", []):
        output = cli("jobs", "get-run-output", str(task["run_id"])).get("notebook_output", {})
        if output.get("truncated") or not output.get("result"):
            raise ValueError("notebook evidence missing or truncated")
        outputs[task["task_key"]] = json.loads(output["result"])
    serving_update = None
    if args.serving_run_id:
        serving_run = cli("jobs", "get-run", str(args.serving_run_id))
        task = validate_serving_update(serving_run)
        output = cli("jobs", "get-run-output", str(task["run_id"])).get("notebook_output", {})
        if output.get("truncated") or not output.get("result"):
            raise ValueError("serving update output missing or truncated")
        outputs["serving_views"] = json.loads(output["result"])
        serving_update = {"job_id": serving_run["job_id"], "run_id": serving_run["run_id"],
                          "task_run_id": task["run_id"], "result_state": "SUCCESS",
                          "elapsed_seconds": (serving_run["end_time"] - serving_run["start_time"]) / 1000,
                          "scope": "serving_views selected; quality input tables preserved"}
    evidence = validate_job(run, outputs["quality_experiment"], outputs["serving_views"], args.namespace)
    if serving_update:
        evidence["serving_update"] = serving_update
    expected_hashes = {model: hashlib.sha256(serving_sql(tables, model).encode()).hexdigest()
                       for model in VIEWS}
    if outputs["serving_views"].get("view_definition_sha256") != expected_hashes:
        raise ValueError("native serving definitions differ from the reviewed local SQL models")
    evidence["serving_source_matches"] = True
    warehouse = cli("warehouses", "get", args.warehouse_id)
    if warehouse.get("enable_serverless_compute") is not True or warehouse.get("cluster_size") != "2X-Small":
        raise ValueError("select the existing Free Edition serverless 2X-Small warehouse")
    args.output_dir.mkdir(parents=True)
    catalog, schema = args.namespace.split(".")
    queries = {model: f"SELECT * FROM {args.namespace}.{model} ORDER BY use_ymd LIMIT 100"
               for model in VIEWS}
    queries["catalog_metadata"] = (
        f"SELECT table_name, table_type, comment FROM {catalog}.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name IN "
        "(" + ", ".join("'" + v + "'" for v in VIEWS) + ") ORDER BY table_name")
    statements = {}
    for key, sql in queries.items():
        request = {"warehouse_id": args.warehouse_id, "statement": sql, "wait_timeout": "10s",
                   "on_wait_timeout": "CONTINUE", "disposition": "INLINE", "format": "JSON_ARRAY",
                   "row_limit": 100, "byte_limit": 100000}
        path = args.output_dir / (key + "-request.json")
        path.write_text(json.dumps(request), encoding="utf-8")
        response = cli("api", "post", "/api/2.0/sql/statements", "--json", "@" + str(path))
        statement_id = response["statement_id"]
        deadline = time.monotonic() + 300
        while response.get("status", {}).get("state") in {"PENDING", "RUNNING"}:
            if time.monotonic() >= deadline:
                cli("api", "post", f"/api/2.0/sql/statements/{statement_id}/cancel")
                raise RuntimeError("own SQL statement timed out and was cancelled")
            time.sleep(2)
            response = cli("api", "get", f"/api/2.0/sql/statements/{statement_id}")
        decoded = decode_statement(response)
        if key in VIEWS and decoded["rows"] != canonical_rows(evidence["serving"]["view_rows"][key]):
            raise ValueError("warehouse and notebook view rows differ: " + key)
        if key == "catalog_metadata":
            comments = {model: re.search(r"COMMENT\s+'((?:''|[^'])*)'", serving_sql(tables, model),
                                         re.IGNORECASE).group(1).replace("''", "'") for model in VIEWS}
            if (len(decoded["rows"]) != 3 or {r["table_name"] for r in decoded["rows"]} != set(VIEWS)
                    or any(r["table_type"] != "VIEW" or r["comment"] != comments[r["table_name"]]
                           for r in decoded["rows"])):
                raise ValueError("Unity Catalog view metadata/comments differ from reviewed SQL source")
        statements[key] = decoded
        (args.output_dir / (key + "-result.json")).write_text(
            json.dumps(decoded, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print("Verified native warehouse query: " + key, flush=True)
    evidence["warehouse"] = {"id": args.warehouse_id, "serverless": True, "cluster_size": "2X-Small",
                             "auto_stop_mins": warehouse.get("auto_stop_mins")}
    evidence["sql_statements"] = statements
    evidence["warehouse_matches_notebook"] = True
    evidence["limitations"] = ["Synthetic bounded input; no production/S3 integration",
                               "No enterprise RBAC, managed quality monitoring, or cost benchmark"]
    (args.output_dir / "evidence.json").write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("Verified native quality Job, Delta time travel, UC views, and SQL warehouse", flush=True)


if __name__ == "__main__":
    main()
