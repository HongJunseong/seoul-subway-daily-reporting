"""Opt-in synthetic scheduler/Pod recovery experiment, run inside the scheduler.

Never call against a production namespace. A deletion probe only removes the
specific worker UID belonging to this experiment after its task starts.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import os
import re
from pathlib import Path
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "dags")]


def worker_record(pod, namespace, run_ids):
    labels = pod.metadata.labels or {}
    if (pod.metadata.namespace != namespace or labels.get("kubernetes_executor") != "True"
            or labels.get("run_id") not in run_ids):
        return None
    base = next(c for c in pod.spec.containers if c.name == "base")
    image_id = next((s.image_id for s in (pod.status.container_statuses or []) if s.name == "base"), None)
    exits = [s.state.terminated.exit_code for s in (pod.status.container_statuses or [])
             if s.name == "base" and s.state and s.state.terminated]
    return {"uid": pod.metadata.uid, "name": pod.metadata.name,
            "dag_id": labels.get("dag_id"), "task_id": labels.get("task_id"),
            "run_id": labels.get("run_id"), "try_number": int(labels["try_number"]),
            "phase": pod.status.phase, "node": pod.spec.node_name,
            "image": base.image, "image_id": image_id, "profile": labels.get("ssdr-task-profile"),
            "resources": base.resources.to_dict(), "exit_codes": exits}


def validate_evidence(runs, workers, deleted=None):
    successful = [(r["dag_id"], t["task_id"]) for r in runs for t in r["tasks"]
                  if t["state"] == "success"]
    if len(runs) != 3 or any(r["state"] != "success" for r in runs) or len(successful) != 13:
        raise ValueError("expected three successful DAGs and thirteen actual tasks")
    skipped = [t for r in runs for t in r["tasks"] if t["state"] == "skipped"]
    if len(skipped) != 1 or skipped[0]["task_id"] != "bronze_usage":
        raise ValueError("live API task must be skipped")
    for dag_id, task_id in successful:
        complete = [p for p in workers if p["dag_id"] == dag_id and p["task_id"] == task_id
                    and p["phase"] == "Succeeded" and p["exit_codes"] == [0]]
        if not complete:
            raise ValueError("missing successful worker Pod evidence: " + task_id)
    if deleted:
        retries = [p for p in workers if p["dag_id"] == deleted["dag_id"]
                   and p["task_id"] == deleted["task_id"] and p["uid"] != deleted["uid"]
                   and p["try_number"] > deleted["try_number"] and p["phase"] == "Succeeded"
                   and p["exit_codes"] == [0]]
        if not retries:
            raise ValueError("deleted worker did not recover in a different successful Pod")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--delete-once-gold-worker", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    args = parser.parse_args()
    if (not args.namespace.startswith("ssdr-fixture-")
            or os.getenv("SSDR_FIXTURE_MODE") != "true"
            or os.getenv("SSDR_DATA_KIND") != "synthetic"):
        parser.error("only a dedicated synthetic fixture namespace is allowed")
    if not re.fullmatch(r"manual__kubernetes_fixture_[A-Za-z0-9_]{1,24}", args.run_id):
        parser.error("run_id must be dedicated to this experiment")
    if args.output_dir.exists():
        parser.error("output exists; preserve prior experiment")
    args.output_dir.mkdir(parents=True)
    from airflow.api.common.trigger_dag import trigger_dag
    from airflow.models import DagRun, Pool
    from airflow.utils.session import create_session
    from kubernetes import client, config, watch
    from scripts.verify_report import verify_report
    from src.report.publication import read_publication

    with create_session() as session:
        pool = session.query(Pool).filter_by(pool="ssdr_delta_writer").one()
        if pool.slots != 1:
            raise ValueError("fixture writer pool must have one slot")
        if session.query(DagRun).filter_by(run_id=args.run_id).count():
            raise ValueError("run already exists")
    expected = {"usage_daily": args.run_id, "gold_daily": "usage__" + args.run_id,
                "report_daily": "gold__usage__" + args.run_id}
    config.load_incluster_config()
    api = client.CoreV1Api()
    # Establish a resourceVersion before triggering so fast control Pods are not missed.
    initial = api.list_namespaced_pod(args.namespace, label_selector="kubernetes_executor=True")
    state = {"workers": {}, "watch_error": None}
    lock, stop = threading.Lock(), threading.Event()
    started_at = datetime.now(timezone.utc).isoformat()

    def observe():
        resource_version = initial.metadata.resource_version
        try:
            while not stop.is_set():
                stream = watch.Watch()
                for event in stream.stream(api.list_namespaced_pod, args.namespace,
                                           label_selector="kubernetes_executor=True",
                                           resource_version=resource_version, timeout_seconds=10):
                    pod = event["object"]
                    resource_version = pod.metadata.resource_version
                    row = worker_record(pod, args.namespace, set(expected.values()))
                    if row:
                        row["last_event_type"] = event["type"]
                        with lock:
                            state["workers"][row["uid"]] = row
                        with (args.output_dir / "pod-events.jsonl").open("a") as log:
                            log.write(json.dumps({"event": event["type"], **row}) + "\n")
                    if stop.is_set():
                        stream.stop()
                        break
        except Exception as exc:
            with lock:
                state["watch_error"] = type(exc).__name__

    thread = threading.Thread(target=observe, daemon=True)
    thread.start()
    started, deleted, runs = time.monotonic(), None, []
    trigger_dag("usage_daily", run_id=args.run_id,
                conf=json.dumps({"target_ymd": "20260201", "reprocess_only": True}))
    try:
        while time.monotonic() - started < args.timeout_seconds:
            with lock:
                workers = list(state["workers"].values())
                watch_error = state["watch_error"]
            if watch_error:
                raise RuntimeError("Pod watch failed: " + watch_error)
            runs = []
            with create_session() as session:
                for dag_id, run_id in expected.items():
                    dag_run = session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).one_or_none()
                    if dag_run:
                        tasks = [{"task_id": t.task_id, "state": t.state,
                                  "executed_attempts": t.prev_attempted_tries}
                                 for t in dag_run.get_task_instances(session=session)]
                        runs.append({"dag_id": dag_id, "run_id": run_id, "state": dag_run.state,
                                     "tasks": sorted(tasks, key=lambda t: t["task_id"])})
                if args.delete_once_gold_worker and deleted is None:
                    usage = next((r for r in runs if r["dag_id"] == "usage_daily"), None)
                    task_states = {t["task_id"]: t["state"] for t in usage["tasks"]} if usage else {}
                    candidate = next((p for p in workers if p["dag_id"] == "usage_daily"
                                      and p["task_id"] == "gold_usage" and p["phase"] == "Running"), None)
                    if (candidate and task_states.get("silver_usage") == "success"
                            and task_states.get("gold_usage") == "running"):
                        api.delete_namespaced_pod(candidate["name"], args.namespace,
                            body=client.V1DeleteOptions(grace_period_seconds=0,
                                 preconditions=client.V1Preconditions(uid=candidate["uid"])))
                        deleted = candidate
                        (args.output_dir / "deleted-worker.json").write_text(json.dumps(deleted, indent=2))
                        print("Deleted this fixture's running Gold worker; waiting for scheduler retry", flush=True)
            if any(r["state"] == "failed" for r in runs):
                raise RuntimeError("fixture DAG failed")
            if len(runs) == 3 and all(r["state"] == "success" for r in runs):
                # The DB may commit before the final worker termination event arrives.
                try:
                    validate_evidence(runs, workers, deleted)
                except ValueError:
                    time.sleep(1)
                    continue
                break
            time.sleep(1)
        else:
            raise RuntimeError("fixture scheduler/Pod experiment timed out")
        if args.delete_once_gold_worker and not deleted:
            raise RuntimeError("requested deletion probe was not executed")
        report = Path(os.environ["SSDR_REPORT_OUTPUT_DIR"]) / "subway_report_20260201.md"
        verify_report(report)
        verify_report(report.with_suffix(".publication.json"))
        context = read_publication(report.with_suffix(".publication.json"))["context"]
        if (context["data_kind"] != "synthetic" or context["usage"]["total_count"] != 680
                or context["arrival"]["total_observations"] != 4
                or context["arrival"]["valid_eta_observations"] != 4):
            raise ValueError("unexpected report result")
        evidence = {"verified": True, "execution_environment": "kubernetes-scheduler-executor",
                    "synthetic": True, "actual_pod_execution": True, "namespace": args.namespace,
                    "started_at_utc": started_at,
                    "elapsed_seconds": round(time.monotonic() - started, 6), "runs": runs,
                    "workers": sorted(workers, key=lambda p: (p["dag_id"], p["task_id"], p["try_number"])),
                    "deleted_worker": deleted, "worker_retry_verified": deleted is not None,
                    "report_hashes_verified": True, "usage_total": 680,
                    "daily_arrival_observations": 4, "daily_valid_eta_observations": 4,
                    "writer_pool_slots": 1,
                    "limitations": ["Single local node and synthetic input; no S3/API/Slack/LLM",
                                    "No peak memory/CPU or production SLA measurement"]}
        (args.output_dir / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
        print("Verified actual scheduler tasks, worker Pods, and report hashes", flush=True)
    finally:
        stop.set()
        thread.join(timeout=12)
        with lock:
            partial = {"runs": runs, "workers": list(state["workers"].values()),
                       "deleted_worker": deleted, "watch_error": state["watch_error"]}
        (args.output_dir / "observations.json").write_text(json.dumps(partial, indent=2) + "\n")


if __name__ == "__main__":
    main()
