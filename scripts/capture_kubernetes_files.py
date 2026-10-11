"""Read only synthetic PVC evidence: Delta roots and persisted worker logs."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

TABLES = ["bronze/subway_usage_delta", "bronze/subway_arrival_delta",
          "silver/fact_subway_usage_delta", "silver/fact_subway_arrival_delta",
          "gold/subway_usage/station_daily", "gold/subway_usage/line_daily",
          "gold/subway_arrival/station_daily_kpi"]


def compare_snapshots(before, after):
    if set(before) != set(after):
        raise ValueError("table sets differ")
    for name in before:
        if any(before[name][key] != after[name][key] for key in ("rows", "sha256")):
            raise ValueError("business rows changed: " + name)
        if name.startswith("bronze/") and before[name]["version"] != after[name]["version"]:
            raise ValueError("Bronze version changed during replay")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--compare", type=Path)
    args = parser.parse_args()
    if (os.getenv("SSDR_FIXTURE_MODE") != "true" or os.getenv("SSDR_DATA_KIND") != "synthetic"
            or not os.getenv("SSDR_DATA_ROOT", "").startswith("/opt/airflow/fixture-lake/")):
        parser.error("requires dedicated local synthetic PVC")
    if args.output.exists():
        parser.error("output exists")
    runtime = json.loads(args.runtime_evidence.read_text())
    if not runtime.get("actual_pod_execution") or not runtime.get("synthetic") or not runtime.get("verified"):
        raise ValueError("requires successful actual fixture Pod evidence")
    from deltalake import DeltaTable
    from src.common.storage import table_path

    snapshots = {}
    for prefix in TABLES:
        table = DeltaTable(table_path(prefix))
        # Bounded fixture only. Never use this collection on a production table.
        frame = table.to_pandas()
        if len(frame) > 1000:
            raise ValueError("fixture unexpectedly large")
        columns = sorted(frame.columns)
        payload = frame.sort_values(columns)[columns].to_json(
            orient="records", date_format="iso", date_unit="us", force_ascii=False, double_precision=15)
        snapshots[prefix] = {"version": table.version(), "rows": len(frame),
                             "sha256": hashlib.sha256(payload.encode()).hexdigest()}
    logs = []
    for worker in runtime["workers"]:
        dag, run, task = worker["dag_id"], worker["run_id"], worker["task_id"]
        if (dag not in {"usage_daily", "gold_daily", "report_daily"}
                or not re.fullmatch(r"(?:gold__)?(?:usage__)?manual__kubernetes_fixture_[A-Za-z0-9_]{1,24}", run)
                or not re.fullmatch(r"[a-z_]+", task)):
            raise ValueError("unexpected fixture log identity")
        path = (Path("/opt/airflow/logs") / ("dag_id=" + dag) / ("run_id=" + run)
                / ("task_id=" + task) / ("attempt=" + str(worker["try_number"]) + ".log"))
        content = path.read_bytes()
        if not content:
            raise ValueError("worker log is empty")
        logs.append({"dag_id": dag, "task_id": task, "try_number": worker["try_number"],
                     "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()})
    match = None
    if args.compare:
        before = json.loads(args.compare.read_text())["snapshots"]
        compare_snapshots(before, snapshots)
        match = True
    evidence = {"verified": True, "synthetic": True, "snapshots": snapshots,
                "worker_log_files_persisted": len(logs), "logs": logs,
                "business_rows_match_previous_run": match,
                "scope": "local synthetic PVC; committed Delta roots, no internal Parquet scan"}
    args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    print("Verified fixture Delta snapshots and persisted worker logs")


if __name__ == "__main__":
    main()
