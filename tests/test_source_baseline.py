from copy import deepcopy
from datetime import datetime, timedelta
import json
import subprocess
import sys

import pytest

from src.common.dates import KST
from src.observability.source_baseline import render_summary, summarize_captures


def capture(start="2026-01-31T23:55:00+09:00", *, states=("collected", "collected", "collected"),
            finished=True, usage=True):
    origin = datetime.fromisoformat(start)
    slots = []
    for i, state in enumerate(states):
        due = origin + timedelta(minutes=i * 5)
        item = {"slot": i, "planned_at_kst": due.isoformat(), "status": state, "logical_get_attempts": 0}
        if state != "missed_schedule":
            item.update(request_started_at_kst=due.isoformat(),
                        request_completed_at_kst=(due + timedelta(seconds=1)).isoformat(),
                        elapsed_seconds=1, logical_get_attempts=1)
            if state == "collected":
                item.update(rows=1000, captured_at_kst=(due + timedelta(seconds=.5)).isoformat())
            else:
                item["error_type"] = "HTTPError"
        slots.append(item)
    usage_request = None
    if usage:
        usage_request = {"status": "collected", "request_started_at_kst": origin.isoformat(),
                         "request_completed_at_kst": (origin + timedelta(seconds=1)).isoformat(),
                         "captured_at_kst": (origin + timedelta(seconds=.5)).isoformat(),
                         "elapsed_seconds": 1, "logical_get_attempts": 2, "rows": 617}
    return {"schema_version": 1, "data_kind": "observed",
            "status": ("collected" if all(s == "collected" for s in states) else "incomplete") if finished else "running",
            "started_at_kst": origin.isoformat(), "cadence_seconds": 300, "planned_snapshots": 3,
            "usage_day": (origin - timedelta(days=4)).strftime("%Y%m%d"), "arrival_slots": slots,
            "usage": usage_request, **({"completed_at_kst": (origin + timedelta(minutes=11)).isoformat(),
            "logical_get_attempts": sum(s["logical_get_attempts"] for s in slots) + (2 if usage else 0)}
            if finished else {})}


def summarize(manifests, as_of="2026-02-02T00:00:00+09:00"):
    return summarize_captures(manifests, start_day="20260131", end_day="20260202", as_of=as_of)


def test_midnight_slots_have_separate_kst_days_and_d4_is_not_capture_day():
    result = summarize([capture()])
    jan, feb, missing = result["daily"]
    assert jan["arrival"]["collected"] == 1
    assert feb["arrival"]["collected"] == 2
    assert jan["usage_observations"] == [{"usage_day": "20260127", "rows": 617}]
    assert feb["usage_observations"] == []
    assert jan["arrival_get_attempts"] + feb["arrival_get_attempts"] == 3
    assert jan["usage_get_attempts"] == 2
    assert feb["response_at_limit_snapshots"] == 2
    assert missing["request_outcome_success_rate"] is None
    assert missing["declared_due_slot_collection_rate"] is None
    assert result["days_without_declared_capture_slots"] == ["20260202"]
    assert result["days_with_collected_snapshots"] == ["20260131", "20260201"]
    assert feb["latest_snapshot_age_at_as_of_seconds"] == 86099.5


def test_failed_request_is_distinct_from_missed_schedule_and_unplanned_day():
    result = summarize([capture(states=("collected", "failed", "missed_schedule"))])
    jan, feb, _ = result["daily"]
    assert jan["request_outcome_success_rate"] == 1
    assert feb["request_outcome_success_rate"] == 0
    assert feb["declared_due_slot_collection_rate"] == 0
    assert feb["arrival"] == {"collected": 0, "failed": 1, "missed_schedule": 1,
                             "not_recorded_due": 0, "future": 0}
    assert feb["arrival_response_rows"] == 0
    assert feb["arrival_get_attempts"] == 1
    assert feb["arrival_request_elapsed_seconds"]["samples"] == 1


def test_running_capture_separates_due_unrecorded_slots_and_future_slots():
    manifest = capture(states=("collected",), finished=False, usage=False)
    result = summarize([manifest], as_of="2026-02-01T00:00:01+09:00")
    jan, feb, _ = result["daily"]
    assert jan["usage"]["not_recorded"] == 1
    assert feb["arrival"]["not_recorded_due"] == 1
    assert feb["arrival"]["future"] == 1
    assert feb["request_outcome_success_rate"] is None
    assert feb["declared_due_slot_collection_rate"] == 0
    assert feb["latest_snapshot_at_kst"] is None


def test_duplicate_copies_do_not_inflate_results_and_conflicting_revisions_require_choice():
    original = capture()
    result = summarize([original, deepcopy(original)])
    assert result["unique_capture_manifests"] == 1
    assert result["duplicate_manifests_ignored"] == 1
    assert result["daily"][0]["arrival_get_attempts"] == 1
    changed = deepcopy(original)
    changed["arrival_slots"][0]["rows"] = 999
    with pytest.raises(ValueError, match="conflicting"):
        summarize([original, changed])


def test_order_independence_with_repeated_usage_day_and_no_raw_fields_in_output():
    first = capture(start="2026-01-31T12:00:00+09:00")
    second = capture(start="2026-01-31T17:00:00+09:00")
    second["usage"]["rows"] = 777
    first["unexpected_raw_payload"] = "private-source-row"
    assert summarize([first, second]) == summarize([second, first])
    result = summarize([first, second])
    assert len(result["daily"][0]["usage_observations"]) == 2
    assert "private-source-row" not in json.dumps(result)
    assert "private-source-row" not in render_summary(result)


@pytest.mark.parametrize("change", ["naive", "utc", "nan", "negative", "bool", "over_limit", "zero_rows",
                                    "slot_order", "planned_time", "attempt_total", "budget", "status",
                                    "completion", "capture_time", "missing_terminal", "synthetic"])
def test_invalid_evidence_is_rejected_instead_of_published(change):
    item = capture()
    slot = item["arrival_slots"][0]
    if change == "naive":
        item["started_at_kst"] = item["started_at_kst"].removesuffix("+09:00")
    elif change == "utc":
        item["started_at_kst"] = "2026-01-31T14:55:00+00:00"
    elif change in ("nan", "negative"):
        slot["elapsed_seconds"] = float("nan") if change == "nan" else -1
    elif change in ("bool", "over_limit", "zero_rows"):
        slot["rows"] = {"bool": True, "over_limit": 1001, "zero_rows": 0}[change]
    elif change == "slot_order":
        item["arrival_slots"].reverse()
    elif change == "planned_time":
        slot["planned_at_kst"] = "2026-01-31T23:56:00+09:00"
    elif change == "attempt_total":
        item["logical_get_attempts"] += 1
    elif change == "budget":
        item["usage"]["logical_get_attempts"] = 11
    elif change == "status":
        item["status"] = "incomplete"
    elif change == "completion":
        item["completed_at_kst"] = item["started_at_kst"]
    elif change == "capture_time":
        slot["captured_at_kst"] = "2026-01-31T23:55:02+09:00"
    elif change == "missing_terminal":
        item["arrival_slots"].pop()
    else:
        item["data_kind"] = "synthetic"
    with pytest.raises(ValueError):
        summarize([item])


def test_explicit_asof_rejects_future_records_and_window_is_bounded():
    with pytest.raises(ValueError, match="analysis time"):
        summarize([capture()], as_of="2026-01-31T23:55:00+09:00")
    with pytest.raises(ValueError, match="1..31"):
        summarize_captures([], start_day="20260101", end_day="20260202", as_of=datetime(2026, 2, 3, tzinfo=KST))
    with pytest.raises(ValueError, match="1..31"):
        summarize_captures([], start_day="20260202", end_day="20260131", as_of=datetime(2026, 2, 3, tzinfo=KST))


def test_cli_reads_only_manifest_and_preserves_existing_output(tmp_path, monkeypatch):
    from scripts import summarize_source_observations as cli
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    from scripts import capture_sources
    monkeypatch.setattr(capture_sources, "ROOT", tmp_path)
    folder = tmp_path / ".work/input"
    folder.mkdir(parents=True)
    (folder / "capture.json").write_text(json.dumps(capture()), encoding="utf-8")
    output = tmp_path / ".work/result"
    args = ["summary", "--capture-dir", str(folder), "--start-day", "20260131",
            "--end-day", "20260202", "--as-of", "2026-02-02T00:00:00+09:00", "--output-dir", str(output)]
    monkeypatch.setattr(sys, "argv", args)
    assert cli.main() == 0
    assert (output / "summary.md").exists()
    saved = (output / "summary.json").read_bytes()
    assert json.loads(saved)["daily"][0]["usage_observations"][0]["rows"] == 617
    with pytest.raises(FileExistsError):
        cli.main()
    assert (output / "summary.json").read_bytes() == saved


def test_cli_help_needs_no_credentials():
    result = subprocess.run([sys.executable, "scripts/summarize_source_observations.py", "--help"],
                            capture_output=True, text=True)
    assert result.returncode == 0
    assert "--as-of" in result.stdout

def test_usage_failure_is_not_zero_rows_and_does_not_pollute_arrival_success():
    manifest = capture()
    manifest["usage"]["status"] = "failed"
    manifest["usage"].pop("rows")
    manifest["usage"].pop("captured_at_kst")
    manifest["status"] = "incomplete"
    result = summarize([manifest])
    jan = result["daily"][0]
    assert jan["usage"]["failed"] == 1
    assert jan["usage_observations"] == []
    assert jan["request_outcome_success_rate"] == 1
    assert jan["capture_status_counts"]["incomplete"] == 1


def test_no_records_means_unknown_coverage_and_no_success_rate():
    result = summarize([])
    assert result["days_with_collected_snapshots"] == []
    assert len(result["days_without_declared_capture_slots"]) == 3
    assert all(daily["request_outcome_success_rate"] is None for daily in result["daily"])


def test_failed_request_before_http_is_not_counted_as_a_get():
    manifest = capture(states=("failed", "collected", "collected"))
    manifest["arrival_slots"][0]["logical_get_attempts"] = 0
    manifest["logical_get_attempts"] -= 1
    daily = summarize([manifest])["daily"][0]
    assert daily["arrival"]["failed"] == 1
    assert daily["arrival_get_attempts"] == 0
    assert daily["request_outcome_success_rate"] == 0


def test_missed_slot_and_schema_boolean_are_rejected():
    manifest = capture(states=("collected", "missed_schedule", "collected"))
    manifest["arrival_slots"][1]["logical_get_attempts"] = False
    with pytest.raises(ValueError):
        summarize([manifest])
    manifest = capture()
    manifest["schema_version"] = True
    with pytest.raises(ValueError):
        summarize([manifest])
