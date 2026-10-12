from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import capture_sources
from scripts import run_source_observation_day as runner
from scripts.summarize_source_observations import read_campaign
from src.observability.source_campaign import render_campaign, summarize_campaign
from test_source_baseline import capture


@pytest.fixture
def campaign(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(capture_sources, "ROOT", tmp_path)
    folder = tmp_path / ".work/campaign"
    runner.initialize(folder, "20260131", days=3)
    return folder


def status(folder, as_of="2026-02-01T17:56:00+09:00"):
    return runner.campaign_status(folder, as_of=datetime.fromisoformat(as_of))


def record(folder, day, state, calls=None, manifest=None):
    target = folder / "days" / day
    target.mkdir(parents=True)
    item = {"status": state}
    if calls is not None:
        item["logical_get_attempts"] = calls
    (target / "state.json").write_text(json.dumps(item))
    if manifest is not None:
        (target / "capture").mkdir()
        (target / "capture/capture.json").write_text(json.dumps(manifest))


def sample(start="2026-01-31T17:50:00+09:00", **kwargs):
    return capture(start, **kwargs)


def test_absent_days_use_fixed_plan_without_inventing_failed_requests_or_zero_get(campaign):
    result = read_campaign(campaign, "2026-02-01T17:56:00+09:00")
    a, b, c = result["daily"]
    assert (a["due_slots"], b["due_slots"], c["due_slots"]) == (3, 2, 0)
    assert result["totals"]["planned_slots"] == 9
    assert result["totals"]["unconfirmed_due_slots"] == 5
    assert result["totals"]["failed_request_slots"] == 0
    assert result["totals"]["known_recorded_get_attempts"] == 0
    assert result["totals"]["days_with_unknown_get_count"] == ["20260131", "20260201"]
    assert a["recorded_get_attempts"] is None
    assert a["expected_usage_day"] == "20260127"
    assert b["expected_usage_day"] == "20260128"
    assert c["planned_due_slot_collection_rate"] is None
    assert "미확인" in render_campaign(result)


def test_missed_start_is_known_zero_but_crashed_reservation_is_unknown(campaign):
    record(campaign, "20260131", "missed_start_window", 0)
    (campaign / "days/20260201").mkdir()
    result = read_campaign(campaign, "2026-02-01T18:10:00+09:00")
    assert result["daily"][0]["missed_start_window_slots"] == 3
    assert result["daily"][0]["unconfirmed_due_slots"] == 0
    assert result["daily"][1]["run_status"] == "reserved_without_state"
    assert result["totals"]["days_with_unknown_get_count"] == ["20260201"]


def test_collected_day_reconciles_slots_get_count_and_d4(campaign):
    manifest = sample()
    record(campaign, "20260131", "collected", 5, manifest)
    result = read_campaign(campaign, "2026-02-01T18:10:00+09:00")
    assert result["daily"][0]["planned_due_slot_collection_rate"] == 1
    assert result["totals"]["planned_due_slot_collection_rate"] == .5
    assert result["totals"]["known_recorded_get_attempts"] == 5
    assert result["daily"][0]["response_at_limit_snapshots"] == 3
    assert result["capture_summary"]["daily"][0]["usage_observations"][0]["usage_day"] == "20260127"


def test_failed_request_and_skipped_slot_stay_distinct(campaign):
    manifest = sample(states=("collected", "failed", "missed_schedule"))
    record(campaign, "20260131", "incomplete", 4, manifest)
    result = read_campaign(campaign, "2026-01-31T18:10:00+09:00")
    row = result["daily"][0]
    assert (row["collected_slots"], row["failed_request_slots"], row["missed_capture_schedule_slots"]) == (1, 1, 1)
    assert row["unconfirmed_due_slots"] == 0


def test_interrupted_running_manifest_counts_only_recorded_requests(campaign):
    manifest = sample(states=("collected",), finished=False, usage=False)
    record(campaign, "20260131", "interrupted", 2, manifest)
    row = read_campaign(campaign, "2026-01-31T18:10:00+09:00")["daily"][0]
    assert row["collected_slots"] == 1 and row["unconfirmed_due_slots"] == 2
    assert row["recorded_get_attempts"] == 2 and row["manifest_recorded_get_attempts"] == 1


def test_actual_capture_start_is_preserved_separately_from_plan(campaign):
    manifest = sample("2026-01-31T17:54:00+09:00", states=("collected",), finished=False, usage=False)
    record(campaign, "20260131", "reserved", manifest=manifest)
    result = read_campaign(campaign, "2026-01-31T17:56:00+09:00")
    assert result["daily"][0]["due_slots"] == 2
    assert result["daily"][0]["unconfirmed_due_slots"] == 1
    assert result["capture_summary"]["daily"][0]["arrival"]["future"] == 2


@pytest.mark.parametrize("mutation", ["wrong_day", "wrong_usage", "wrong_calls", "wrong_state", "wrong_cadence"])
def test_contradictory_capture_and_reservation_are_rejected(campaign, mutation):
    manifest = sample()
    state, calls = "collected", 5
    if mutation == "wrong_day":
        manifest = sample("2026-02-01T17:50:00+09:00")
    if mutation == "wrong_usage":
        manifest["usage_day"] = "20260128"
    if mutation == "wrong_calls":
        calls = 4
    if mutation == "wrong_state":
        state, calls = "missed_start_window", 0
    if mutation == "wrong_cadence":
        manifest["cadence_seconds"] = 600
    record(campaign, "20260131", state, calls, manifest)
    with pytest.raises(ValueError):
        read_campaign(campaign, "2026-02-01T18:10:00+09:00")


@pytest.mark.parametrize("calls", [-1, True, 14])
def test_invalid_budget_counts_fail_closed(campaign, calls):
    record(campaign, "20260131", "interrupted", calls)
    with pytest.raises(ValueError):
        read_campaign(campaign, "2026-01-31T18:10:00+09:00")


def test_terminal_success_without_capture_is_not_published(campaign):
    record(campaign, "20260131", "collected", 5)
    with pytest.raises(ValueError, match="missing"):
        read_campaign(campaign, "2026-01-31T18:10:00+09:00")


def test_unknown_extra_manifest_fields_are_not_exported(campaign):
    manifest = sample()
    manifest["private_payload"] = "SECRET-ROW"
    record(campaign, "20260131", "collected", 5, manifest)
    result = read_campaign(campaign, "2026-01-31T18:10:00+09:00")
    assert "SECRET-ROW" not in json.dumps(result) + render_campaign(result)


def test_mutated_sealed_plan_is_rejected_before_reporting(campaign):
    path = campaign / "plan.json"
    plan = json.loads(path.read_text())
    plan["days"] = 4
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="sealed"):
        read_campaign(campaign, "2026-01-31T18:10:00+09:00")


def test_directory_escape_is_rejected_before_reading_state(campaign, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (campaign / "days").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="escaped"):
        read_campaign(campaign, "2026-01-31T18:10:00+09:00")


def test_calendar_duplicates_and_outside_captures_are_rejected(campaign):
    snapshot = status(campaign)
    snapshot["daily"][1] = deepcopy(snapshot["daily"][0])
    with pytest.raises(ValueError):
        summarize_campaign(snapshot, {})
    with pytest.raises(ValueError):
        summarize_campaign(status(campaign), {"20260130": sample()})


def test_cli_campaign_mode_preserves_plan_and_refuses_existing_output():
    root = Path(__file__).resolve().parents[1]
    import tempfile
    with tempfile.TemporaryDirectory(dir=root / ".work", prefix="campaign-report-test-") as temp:
        folder = Path(temp) / "campaign"
        runner.initialize(folder, "20260131", days=3)
        seal = (folder / "plan.sha256").read_bytes()
        output = Path(temp) / "summary"
        command = [sys.executable, str(root / "scripts/summarize_source_observations.py"),
                   "--campaign-dir", str(folder), "--as-of", "2026-02-01T17:56:00+09:00",
                   "--output-dir", str(output)]
        first = subprocess.run(command, capture_output=True, text=True)
        assert first.returncode == 0, first.stderr
        assert json.loads((output / "summary.json").read_text())["totals"]["unconfirmed_due_slots"] == 5
        assert (folder / "plan.sha256").read_bytes() == seal
        before = (output / "summary.json").read_bytes()
        assert subprocess.run(command, capture_output=True).returncode != 0
        assert (output / "summary.json").read_bytes() == before
        assert not (folder / "days").exists()

@pytest.mark.parametrize("state,calls", [("unrecorded_due", 0), ("interrupted", None)])
def test_unknown_and_interrupted_call_counts_cannot_be_conflated(campaign, state, calls):
    record(campaign, "20260131", state, calls)
    with pytest.raises(ValueError):
        read_campaign(campaign, "2026-01-31T18:10:00+09:00")


def test_calendar_shift_cannot_remove_an_expected_date(campaign):
    snapshot = status(campaign)
    snapshot["daily"] = snapshot["daily"][1:] + [deepcopy(snapshot["daily"][-1])]
    snapshot["daily"][-1]["capture_day_kst"] = "20260203"
    snapshot["daily"][-1]["planned_at_kst"] = "2026-02-03T17:50:00+09:00"
    with pytest.raises(ValueError, match="period"):
        summarize_campaign(snapshot, {})

def test_state_file_escape_is_rejected_before_reading_metadata(campaign, tmp_path):
    target = campaign / "days/20260131"
    target.mkdir(parents=True)
    outside = tmp_path / "external-state.json"
    outside.write_text('{"status":"collected","logical_get_attempts":5}')
    (target / "state.json").symlink_to(outside)
    with pytest.raises(ValueError, match="escaped"):
        read_campaign(campaign, "2026-01-31T18:10:00+09:00")
