"""Join allowlisted worker callback measurements with successful fixture Pod identities."""
from __future__ import annotations
import argparse
import json
import math
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.observability.task_events import PREFIX

NUMBERS = (
    "observed_seconds", "memory_container_peak_bytes", "sampled_memory_peak_bytes",
    "memory_limit_bytes", "cpu_limit_cores", "cpu_usage_seconds", "mean_cpu_cores",
    "sampled_peak_cpu_cores", "minimum_cpu_sample_interval_seconds", "cpu_throttled_periods",
    "cpu_periods", "cpu_throttled_seconds", "oom_kill_delta",
)


def quantity(value, kind):
    match = re.fullmatch(r"([0-9]+)(m|Ki|Mi|Gi)?", str(value))
    if not match:
        raise ValueError("unsupported fixture resource quantity")
    number, suffix = int(match[1]), match[2]
    if kind == "cpu":
        if suffix not in (None, "m"):
            raise ValueError("invalid CPU suffix")
        return number / 1000 if suffix == "m" else number
    if suffix not in (None, "Ki", "Mi", "Gi"):
        raise ValueError("invalid memory suffix")
    return number * {None: 1, "Ki": 1024, "Mi": 1024**2, "Gi": 1024**3}[suffix]


def validate_metrics(value, resources):
    if (not isinstance(value, dict) or value.get("status") != "observed"
            or value.get("source") != "task-worker-cgroup-v2"
            or type(value.get("samples")) is not int or value["samples"] < 2
            or value.get("sample_errors") != 0):
        raise ValueError("missing or partial worker resource measurements")
    for name in NUMBERS:
        number = value.get(name)
        if number is None and name in ("sampled_peak_cpu_cores", "minimum_cpu_sample_interval_seconds"):
            continue
        if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number) or number < 0:
            raise ValueError("invalid resource measurement: " + name)
    interval_count = value.get("cpu_interval_samples")
    if (type(interval_count) is not int or not 0 <= interval_count < value["samples"]
            or (interval_count == 0 and any(value[name] is not None for name in
                                           ("sampled_peak_cpu_cores", "minimum_cpu_sample_interval_seconds")))
            or (interval_count > 0 and (value["sampled_peak_cpu_cores"] is None
                                       or value["minimum_cpu_sample_interval_seconds"] is None
                                       or value["minimum_cpu_sample_interval_seconds"] < 0.25))):
        raise ValueError("invalid CPU interval coverage")
    if value["observed_seconds"] <= 0 or value["memory_container_peak_bytes"] < value["sampled_memory_peak_bytes"]:
        raise ValueError("invalid measurement duration or memory peak")
    if value["oom_kill_delta"] != 0:
        raise ValueError("worker experienced an OOM kill")
    if value["cpu_throttled_periods"] > value["cpu_periods"]:
        raise ValueError("invalid throttle counters")
    # A root/host cgroup must not be mistaken for this task container.
    if (value["memory_limit_bytes"] != quantity(resources["limits"]["memory"], "memory")
            or not math.isclose(value["cpu_limit_cores"], quantity(resources["limits"]["cpu"], "cpu"))):
        raise ValueError("cgroup scope does not match worker Pod limits")
    return {name: value.get(name) for name in NUMBERS} | {
        "samples": value["samples"], "cpu_interval_samples": value.get("cpu_interval_samples"),
        "cgroup_scope_matches_pod_limits": True,
    }


def summarize(runtime, logs):
    if (not runtime.get("verified") or not runtime.get("actual_pod_execution") or not runtime.get("synthetic")
            or not runtime.get("namespace", "").startswith("ssdr-fixture-")):
        raise ValueError("requires verified actual synthetic worker evidence")
    workers = runtime["workers"]
    events = {}
    for line in logs:
        if PREFIX not in line:
            continue
        try:
            event = json.loads(line.split(PREFIX, 1)[1])
        except (ValueError, TypeError):
            raise ValueError("malformed worker callback event") from None
        if not isinstance(event, dict) or event.get("schema_version") != 1:
            raise ValueError("invalid worker event schema")
        if event.get("event") != "succeeded":
            continue
        key = tuple(event.get(name) for name in ("dag_id", "task_id", "run_id", "try_number"))
        if key in events and event != events[key]:
            raise ValueError("conflicting callback measurements")
        events[key] = event
    tasks = []
    for worker in workers:
        if worker["phase"] != "Succeeded" or worker["exit_codes"] != [0]:
            raise ValueError("resource baseline requires successful workers without deletion")
        key = tuple(worker[name] for name in ("dag_id", "task_id", "run_id", "try_number"))
        event = events.get(key)
        if event is None:
            raise ValueError("missing callback for worker")
        metrics = validate_metrics(event.get("container_resources"), worker["resources"])
        for name in ("duration_seconds", "executor_queue_seconds"):
            number = event.get(name)
            if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number) or number < 0:
                raise ValueError("missing worker lifecycle timing")
        request_memory = quantity(worker["resources"]["requests"]["memory"], "memory")
        request_cpu = quantity(worker["resources"]["requests"]["cpu"], "cpu")
        if request_memory <= 0 or request_cpu <= 0:
            raise ValueError("fixture requests must be positive")
        tasks.append({
            "dag_id": worker["dag_id"], "task_id": worker["task_id"], "try_number": worker["try_number"],
            "pod_uid": worker["uid"], "profile": worker["profile"], "image_id": worker["image_id"],
            "resources": worker["resources"],
            "duration_seconds": event["duration_seconds"], "executor_queue_seconds": event["executor_queue_seconds"],
            "container": metrics,
            "memory_peak_to_request_ratio": metrics["memory_container_peak_bytes"] / request_memory,
            "memory_peak_to_limit_ratio": metrics["memory_container_peak_bytes"] / metrics["memory_limit_bytes"],
            "mean_cpu_to_request_ratio": metrics["mean_cpu_cores"] / request_cpu,
        })
    if len(tasks) != 12 or len({(t["dag_id"], t["task_id"]) for t in tasks}) != 12:
        raise ValueError("expected twelve distinct successful measured workers")
    profiles = []
    for profile in sorted({t["profile"] for t in tasks}):
        group = [t for t in tasks if t["profile"] == profile]
        peaks = [t["container"]["sampled_peak_cpu_cores"] for t in group
                 if t["container"]["sampled_peak_cpu_cores"] is not None]
        profiles.append({
            "profile": profile, "tasks": len(group),
            "maximum_container_memory_peak_bytes": max(t["container"]["memory_container_peak_bytes"] for t in group),
            "maximum_memory_peak_to_request_ratio": max(t["memory_peak_to_request_ratio"] for t in group),
            "maximum_sampled_cpu_cores": max(peaks) if peaks else None,
            "maximum_mean_cpu_cores": max(t["container"]["mean_cpu_cores"] for t in group),
            "maximum_executor_queue_seconds": max(t["executor_queue_seconds"] for t in group),
        })
    return {
        "schema_version": 1, "verified": True, "synthetic": True, "actual_pod_execution": True,
        "namespace": runtime["namespace"], "started_at_utc": runtime["started_at_utc"],
        "elapsed_seconds": runtime["elapsed_seconds"], "measured_workers": len(tasks),
        "writer_pool_slots": runtime["writer_pool_slots"], "profiles": profiles, "tasks": tasks,
        "limitations": [
            "Kernel memory peak includes bootstrap, child processes and cache through terminal callback",
            "CPU peaks are sampled interval averages of at least 0.25 seconds, not instantaneous peaks",
            "Executor queue includes Pod creation/scheduling/bootstrap; pure pool wait was not isolated",
            "Successful callbacks omit activity after the callback and workers killed without a terminal event",
            "Single local node and synthetic fixture; no production capacity or data-volume sizing claim",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-evidence", type=Path, required=True)
    parser.add_argument("--log-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; preserve prior measurements")
    runtime = json.loads(args.runtime_evidence.read_text())
    lines = []
    for worker in runtime["workers"]:
        dag, run, task, attempt = (worker[key] for key in ("dag_id", "run_id", "task_id", "try_number"))
        if (dag not in {"usage_daily", "gold_daily", "report_daily"}
                or not re.fullmatch(r"(?:gold__)?(?:usage__)?manual__kubernetes_fixture_[A-Za-z0-9_]{1,24}", run)
                or not re.fullmatch(r"[a-z_]+", task) or type(attempt) is not int or attempt < 1):
            raise ValueError("unexpected fixture log identity")
        file = args.log_root / f"dag_id={dag}" / f"run_id={run}" / f"task_id={task}" / f"attempt={attempt}.log"
        lines += file.read_text().splitlines()
    result = summarize(runtime, lines)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print("Verified twelve worker resource observations and cgroup scope")


if __name__ == "__main__":
    main()
