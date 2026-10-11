"""Opt-in, read-only cgroup v2 observations from the task worker process."""
from __future__ import annotations
import os
from pathlib import Path
import threading
import time

ROOT = Path("/sys/fs/cgroup")
MONITORS = {}
LOCK = threading.Lock()


def counters(text):
    values = {}
    for line in text.splitlines():
        name, value = line.split()
        number = int(value)
        if number < 0 or name in values:
            raise ValueError("invalid cgroup counter")
        values[name] = number
    return values


def read_snapshot(root=ROOT):
    """Never reset peaks or modify the container's cgroup."""
    root = Path(root)
    memory = int((root / "memory.current").read_text())
    limit_text = (root / "memory.max").read_text().strip()
    quota, period = (root / "cpu.max").read_text().split()
    cpu = counters((root / "cpu.stat").read_text())
    events = counters((root / "memory.events").read_text())
    peak_file = root / "memory.peak"
    peak = int(peak_file.read_text()) if peak_file.exists() else None
    limit = None if limit_text == "max" else int(limit_text)
    cpu_limit = None if quota == "max" else int(quota) / int(period)
    if memory < 0 or (peak is not None and peak < memory) or int(period) <= 0:
        raise ValueError("invalid resource values")
    if (limit is not None and limit <= 0) or (cpu_limit is not None and cpu_limit <= 0):
        raise ValueError("invalid resource limits")
    return {"memory_current_bytes": memory, "memory_peak_bytes": peak,
            "memory_limit_bytes": limit, "cpu_limit_cores": cpu_limit,
            "cpu_usage_usec": cpu["usage_usec"], "cpu_periods": cpu.get("nr_periods"),
            "cpu_throttled_periods": cpu.get("nr_throttled"),
            "cpu_throttled_usec": cpu.get("throttled_usec"), "oom_kill_count": events["oom_kill"]}


def summarize_samples(samples, sample_errors=0):
    if not samples:
        return {"status": "unavailable", "reason": "no_cgroup_samples", "sample_errors": sample_errors}
    first, last = samples[0], samples[-1]
    for _, row in samples:
        if any(row[key] != first[1][key] for key in ("memory_limit_bytes", "cpu_limit_cores")):
            return {"status": "unavailable", "reason": "resource_limits_changed", "sample_errors": sample_errors}
    elapsed = last[0] - first[0]
    intervals = []
    for (left_time, left), (right_time, right) in zip(samples, samples[1:]):
        seconds, used = right_time - left_time, right["cpu_usage_usec"] - left["cpu_usage_usec"]
        if seconds <= 0 or used < 0:
            return {"status": "unavailable", "reason": "clock_or_counter_regressed", "sample_errors": sample_errors}
        # Tiny terminal intervals amplify counter/read jitter; they do not describe a CPU burst.
        if seconds >= 0.25:
            intervals.append((seconds, used / 1_000_000 / seconds))
    def delta(key):
        left, right = first[1][key], last[1][key]
        return None if left is None or right is None else right - left
    changes = {key: delta(key) for key in ("cpu_usage_usec", "cpu_periods", "cpu_throttled_periods",
                                         "cpu_throttled_usec", "oom_kill_count")}
    if any(value is not None and value < 0 for value in changes.values()):
        return {"status": "unavailable", "reason": "counter_regressed", "sample_errors": sample_errors}
    peaks = [row["memory_peak_bytes"] for _, row in samples if row["memory_peak_bytes"] is not None]
    return {
        "status": "observed" if len(samples) >= 2 and not sample_errors else "partial",
        "source": "task-worker-cgroup-v2", "samples": len(samples), "sample_errors": sample_errors,
        "observed_seconds": round(elapsed, 6),
        "memory_container_peak_bytes": max(peaks) if peaks else None,
        "sampled_memory_peak_bytes": max(row["memory_current_bytes"] for _, row in samples),
        "memory_limit_bytes": last[1]["memory_limit_bytes"], "cpu_limit_cores": last[1]["cpu_limit_cores"],
        "cpu_usage_seconds": changes["cpu_usage_usec"] / 1_000_000,
        "mean_cpu_cores": changes["cpu_usage_usec"] / 1_000_000 / elapsed if elapsed > 0 else None,
        "sampled_peak_cpu_cores": max(value for _, value in intervals) if intervals else None,
        "cpu_interval_samples": len(intervals),
        "minimum_cpu_sample_interval_seconds": min(seconds for seconds, _ in intervals) if intervals else None,
        "cpu_throttled_periods": changes["cpu_throttled_periods"],
        "cpu_periods": changes["cpu_periods"],
        "cpu_throttled_seconds": changes["cpu_throttled_usec"] / 1_000_000
                                if changes["cpu_throttled_usec"] is not None else None,
        "oom_kill_delta": changes["oom_kill_count"],
        "limitations": [
            "Kernel memory peak includes container startup, descendants and file cache through the final sample",
            "CPU peak is the maximum interval average; shorter bursts and activity after the callback are unobserved",
            "Read-only sampler overhead is included; this is not application-only CPU or RSS",
        ],
    }


class Monitor:
    def __init__(self, read=read_snapshot, clock=time.monotonic, interval=1):
        self.read, self.clock, self.interval = read, clock, interval
        self.samples, self.errors = [], 0
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.observe, daemon=True)

    def sample(self):
        try:
            self.samples.append((self.clock(), self.read()))
        except (OSError, ValueError, KeyError, ZeroDivisionError):
            self.errors += 1

    def observe(self):
        while not self.stop.wait(self.interval):
            self.sample()

    def start(self):
        self.sample()
        self.thread.start()
        return self

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            return {"status": "unavailable", "reason": "sampler_did_not_stop"}
        self.sample()
        return summarize_samples(self.samples, self.errors)


def identity(context):
    task = context["task_instance"]
    return task.dag_id, task.task_id, task.run_id, getattr(task, "prev_attempted_tries", task.try_number)


def begin(context):
    if os.getenv("SSDR_TASK_RESOURCE_METRICS", "false").lower() != "true":
        return
    key = identity(context)
    with LOCK:
        if key not in MONITORS:
            MONITORS[key] = Monitor().start()


def finish(context):
    key = identity(context)
    with LOCK:
        monitor = MONITORS.pop(key, None)
    if monitor is None:
        if os.getenv("SSDR_TASK_RESOURCE_METRICS", "false").lower() == "true":
            # Scheduler-side callbacks must never be presented as a worker measurement.
            return {"status": "unavailable", "reason": "no_worker_start_monitor"}
        return None
    return monitor.finish()
