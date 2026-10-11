"""Allowlisted task lifecycle logs, without conf, URLs or exception messages."""
from __future__ import annotations
import json
import logging
import math
from datetime import datetime, timezone
from src.alerts.notify_pipeline import notify_failure
from src.observability import container_resources

PREFIX = "SSDR_TASK_EVENT "
log = logging.getLogger(__name__)


def utc(value):
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def elapsed(start, end):
    if start is None or end is None:
        return None
    seconds = (utc(end) - utc(start)).total_seconds()
    return round(seconds, 6) if seconds >= 0 else None


def task_event(context, event, now=None):
    if event not in ("started", "succeeded", "failed"):
        raise ValueError("invalid task event")
    task = context["task_instance"]
    now = utc(now or datetime.now(timezone.utc))
    start = getattr(task, "start_date", None)
    end = getattr(task, "end_date", None) or now
    run = context.get("dag_run")
    run_type = getattr(run, "run_type", None)
    scheduled = run_type == "scheduled"
    interval_end = context.get("data_interval_end") if scheduled else None
    error = context.get("exception")
    return {
        "schema_version": 1, "event": event, "observed_at_utc": now.isoformat(),
        "dag_id": task.dag_id, "task_id": task.task_id, "run_id": task.run_id,
        "try_number": getattr(task, "prev_attempted_tries", task.try_number), "pool": getattr(task, "pool", None),
        "duration_seconds": elapsed(start, end) if event != "started" else None,
        "executor_queue_seconds": elapsed(getattr(task, "queued_dttm", None), start),
        "scheduled_interval_to_start_seconds": elapsed(interval_end, start),
        "error_type": type(error).__name__ if error is not None and event == "failed" else None,
    }


def emit(context, event):
    value = task_event(context, event)
    if event != "started":
        resources = container_resources.finish(context)
        if resources is not None:
            value["container_resources"] = resources
    log.info("%s%s", PREFIX, json.dumps(value, ensure_ascii=False, allow_nan=False))
    return value


def on_task_start(context):
    container_resources.begin(context)
    emit(context, "started")


def on_task_success(context):
    emit(context, "succeeded")


def on_task_retry(context):
    emit(context, "failed")


def on_task_failure(context):
    emit(context, "failed")
    notify_failure(context)


def summarize_events(lines):
    events = {}
    malformed = 0
    for line in lines:
        if PREFIX not in line:
            continue
        try:
            value = json.loads(line.split(PREFIX, 1)[1])
            if not isinstance(value, dict):
                raise ValueError("task event must be an object")
            if not all(isinstance(value.get(field), str) and value[field] for field in ("dag_id", "task_id", "run_id")):
                raise ValueError("invalid task identity")
            if type(value.get("try_number")) is not int or value["try_number"] < 0:
                raise ValueError("invalid attempt")
            key = (value["dag_id"], value["task_id"], value["run_id"], int(value["try_number"]), value["event"])
            if value["schema_version"] != 1 or value["event"] not in ("started", "succeeded", "failed"):
                raise ValueError("invalid event schema")
            for field in ["duration_seconds", "executor_queue_seconds", "scheduled_interval_to_start_seconds"]:
                number = value.get(field)
                if number is not None and (isinstance(number, bool) or not isinstance(number, (int, float))
                                           or not math.isfinite(number) or number < 0):
                    raise ValueError("invalid timing")
            events[key] = value
        except (ValueError, KeyError, TypeError):
            malformed += 1
    attempts = {}
    for key, event in events.items():
        attempts.setdefault(key[:-1], {})[key[-1]] = event
    groups = {}
    for (dag, task, _, _), lifecycle in attempts.items():
        group = groups.setdefault((dag, task), {"dag_id": dag, "task_id": task, "attempts": 0,
                                               "succeeded": 0, "failed": 0, "incomplete": 0, "conflicting": 0,
                                               "_durations": [], "_queues": [], "_starts": []})
        group["attempts"] += 1
        terminal = [lifecycle[k] for k in ("succeeded", "failed") if k in lifecycle]
        if len(terminal) != 1:
            group["conflicting" if terminal else "incomplete"] += 1
            continue
        event = terminal[0]
        group[event["event"]] += 1
        for field, target in [("duration_seconds", "_durations"), ("executor_queue_seconds", "_queues"),
                              ("scheduled_interval_to_start_seconds", "_starts")]:
            if event.get(field) is not None:
                group[target].append(event[field])
    result = []
    for _, group in sorted(groups.items()):
        for key, target in [("_durations", "duration_seconds"), ("_queues", "executor_queue_seconds"),
                            ("_starts", "scheduled_interval_to_start_seconds")]:
            numbers = group.pop(key)
            group[target] = {"samples": len(numbers), "mean": sum(numbers) / len(numbers) if numbers else None,
                             "max": max(numbers) if numbers else None}
        result.append(group)
    return {"schema_version": 1, "unique_events": len(events), "malformed_events": malformed, "tasks": result,
            "limitations": ["Callbacks are observations, not authoritative scheduler state",
                            "An incomplete lifecycle can also indicate missing/truncated logs",
                            "Executor queue time excludes scheduler/pool waiting",
                            "Interval-to-start includes dependencies, pool and executor; it is not isolated pool wait"]}
