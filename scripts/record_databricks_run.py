"""Read an actual successful Job run and record bounded synthetic evidence."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import subprocess

TASKS = {"seed_fixture", "silver", "gold", "verify_replay"}


def validate_run(run, result, local):
    if run.get("state", {}).get("result_state") != "SUCCESS":
        raise ValueError("Job did not finish successfully")
    tasks = run.get("tasks", [])
    if {t["task_key"] for t in tasks} != TASKS or any(
        t.get("state", {}).get("result_state") != "SUCCESS" for t in tasks
    ):
        raise ValueError("All four real Job tasks must succeed")
    if result.get("execution_environment") != "databricks-serverless":
        raise ValueError("result is not a Databricks execution")
    if not result.get("verified") or result.get("synthetic") is not True:
        raise ValueError("expected verified synthetic input")
    if result.get("totals") != {"20260128": 680, "20260129": 660}:
        raise ValueError("unexpected native totals")
    if not result.get("bronze_unchanged"):
        raise ValueError("Bronze changed during replay")
    if local.get("execution_environment") != "local-spark-managed-delta-contract":
        raise ValueError("comparison must use actual local managed-table evidence")
    if result["business_snapshot"] != local["business_snapshot"]:
        raise ValueError("local and Databricks business hashes differ")
    return {**result, "job_id": run["job_id"], "run_id": run["run_id"],
            "cross_engine_business_match": True, "start_time_ms": run["start_time"],
            "end_time_ms": run["end_time"],
            "elapsed_seconds": (run["end_time"] - run["start_time"]) / 1000,
            "tasks": [{"task_key": t["task_key"], "run_id": t["run_id"],
                       "result_state": t["state"]["result_state"],
                       "attempt_number": t.get("attempt_number", 0),
                       "setup_duration_ms": t.get("setup_duration"),
                       "execution_duration_ms": t.get("execution_duration")}
                      for t in sorted(tasks, key=lambda t: t["task_key"])]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", default="databricks")
    parser.add_argument("--profile", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--local-evidence", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists():
        parser.error("output already exists; preserve earlier results")

    def read_cli(*command):
        process = subprocess.run([args.cli, *command, "--profile", args.profile, "--output", "json"],
                                 capture_output=True, text=True)
        if process.returncode:
            raise RuntimeError("Databricks read failed; no credentials or raw response logged")
        return json.loads(process.stdout)

    run = read_cli("jobs", "get-run", str(args.run_id))
    target = next(t for t in run["tasks"] if t["task_key"] == "verify_replay")
    output = read_cli("jobs", "get-run-output", str(target["run_id"]))
    notebook = output.get("notebook_output", {})
    if notebook.get("truncated") or not notebook.get("result"):
        raise ValueError("Missing or truncated verification notebook output")
    result = json.loads(notebook["result"])
    local = json.loads(args.local_evidence.read_text())
    evidence = validate_run(run, result, local)
    args.output_dir.mkdir(parents=True)
    (args.output_dir / "evidence.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2)
                                                 + "\n", encoding="utf-8")
    print(f"Verified actual Databricks run {run['run_id']}; all business hashes match local execution")


if __name__ == "__main__":
    main()
