from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
import pytest
from src.observability import task_events as events


def context():
    start = datetime(2026, 2, 1, 6, tzinfo=timezone.utc)
    task = SimpleNamespace(dag_id="usage_daily", task_id="silver_usage", run_id="scheduled__test",
                           try_number=2, prev_attempted_tries=1, pool="ssdr_delta_writer",
                           queued_dttm=start-timedelta(seconds=10), start_date=start, end_date=start+timedelta(seconds=20))
    return dict(task_instance=task, dag_run=SimpleNamespace(run_type="scheduled"),
                data_interval_end=start-timedelta(seconds=40), exception=ValueError("SECRET_URL_AND_CONF"))


def line(event):
    return "airflow log prefix " + events.PREFIX + json.dumps(event)


def test_task_event_reports_distinct_timings_and_redacts_exception_message():
    event = events.task_event(context(), "failed")
    assert event["duration_seconds"] == 20
    assert event["executor_queue_seconds"] == 10
    assert event["scheduled_interval_to_start_seconds"] == 40
    assert event["try_number"] == 1
    assert event["error_type"] == "ValueError"
    assert "SECRET" not in json.dumps(event)


def test_manual_runs_do_not_claim_scheduled_start_delay():
    ctx = context()
    ctx["dag_run"].run_type = "manual"
    assert events.task_event(ctx, "started")["scheduled_interval_to_start_seconds"] is None


def test_log_duplicates_retries_and_incomplete_attempts_are_separate():
    ctx = context()
    started = events.task_event(ctx, "started")
    failed = events.task_event(ctx, "failed")
    ctx["task_instance"].prev_attempted_tries = 2
    succeeded = events.task_event(ctx, "succeeded")
    ctx["task_instance"].prev_attempted_tries = 3
    unfinished = events.task_event(ctx, "started")
    result = events.summarize_events([line(started), line(failed), line(failed), line(succeeded), line(unfinished)])
    task = result["tasks"][0]
    assert result["unique_events"] == 4
    assert task["attempts"] == 3
    assert (task["failed"], task["succeeded"], task["incomplete"]) == (1, 1, 1)
    assert task["duration_seconds"]["samples"] == 2


def test_conflicting_terminal_events_are_not_counted_as_success():
    ctx = context()
    result = events.summarize_events([line(events.task_event(ctx, "failed")),
                                      line(events.task_event(ctx, "succeeded"))])
    assert result["tasks"][0]["conflicting"] == 1
    assert result["tasks"][0]["succeeded"] == 0


@pytest.mark.parametrize("bad", ['{', '{"schema_version":1}', 'null'])
def test_malformed_logs_are_counted_without_exposing_input(bad):
    result = events.summarize_events(["plain log", events.PREFIX+bad])
    assert result["malformed_events"] == 1
    assert result["tasks"] == []


def test_failure_callback_keeps_notification_and_emits_event(monkeypatch, caplog):
    calls = []
    monkeypatch.setattr(events, "notify_failure", lambda ctx: calls.append(ctx))
    with caplog.at_level("INFO"):
        events.on_task_failure(context())
    assert len(calls) == 1
    assert events.PREFIX in caplog.text
    assert "SECRET" not in caplog.text

@pytest.mark.parametrize("kind,exit_code", [("valid", 0), ("empty", 2), ("malformed", 2)])
def test_log_summary_cli_records_evidence_and_rejects_missing_events(tmp_path, kind, exit_code):
    import subprocess
    import sys
    source = tmp_path / "task.log"
    output = tmp_path / "summary.json"
    content = line(events.task_event(context(), "succeeded")) if kind == "valid" else (
        events.PREFIX + "null" if kind == "malformed" else "plain unrelated log")
    source.write_text(content + "\n", encoding="utf-8")
    command = subprocess.run([sys.executable, "scripts/summarize_tasks.py", "--log-file", str(source),
                              "--output", str(output)], capture_output=True, text=True)
    assert command.returncode == exit_code, command.stderr
    result = json.loads(output.read_text())
    assert result["unique_events"] == (1 if kind == "valid" else 0)
    if kind == "valid":
        assert result["tasks"][0]["succeeded"] == 1


def test_retry_callback_records_failed_attempt_without_final_failure_notification(monkeypatch, caplog):
    calls = []
    monkeypatch.setattr(events, "notify_failure", lambda ctx: calls.append(ctx))
    with caplog.at_level("INFO"):
        events.on_task_retry(context())
    assert calls == []
    result = events.summarize_events(caplog.text.splitlines())
    assert result["tasks"][0]["failed"] == 1
    assert result["tasks"][0]["incomplete"] == 0
