from datetime import datetime, timedelta
import json
import pytest
from scripts import capture_sources as capture
from scripts import process_source_experiment as replay
from src.common.dates import KST


class Clock:
    def __init__(self):
        self.seconds = 0

    def now(self):
        return datetime(2026, 1, 31, 23, 55, tzinfo=KST) + timedelta(seconds=self.seconds)

    def tick(self):
        return self.seconds

    def sleep(self, seconds):
        self.seconds += seconds


class Session:
    attempts = 0

    def get(self):
        self.attempts += 1


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setattr(capture, "ROOT", tmp_path)
    monkeypatch.setattr(replay, "ROOT", tmp_path)
    (tmp_path / ".work").mkdir()
    return tmp_path


def collect(root, *, arrival=None, usage=None):
    timer, client = Clock(), Session()

    def arrivals(*, session):
        session.get()
        return [{"statnId": "1002000001", "barvlDt": "120"}]

    def usages(day, *, max_pages, session):
        session.get()
        assert max_pages == 10
        return [{"USE_YMD": day}]

    return capture.capture(root / ".work/capture", "20260128",
                           session=client, clock=timer.now, monotonic=timer.tick, sleep=timer.sleep,
                           arrival_fetch=arrival or arrivals, usage_fetch=usage or usages)


def test_capture_keeps_kst_source_dates_and_exact_cadence_across_midnight(root):
    result = collect(root)
    assert result["status"] == "collected"
    assert result["logical_get_attempts"] == 4
    assert [s["planned_at_kst"] for s in result["arrival_slots"]] == [
        "2026-01-31T23:55:00+09:00", "2026-02-01T00:00:00+09:00", "2026-02-01T00:05:00+09:00"]
    evidence, rows, hashes = replay.load_capture(root / ".work/capture")
    assert evidence["usage_day"] == "20260128"
    assert len(rows) == len(hashes) == 4
    assert (root / ".work/capture").stat().st_mode & 0o777 == 0o700


def test_failed_and_empty_slots_are_not_success_or_zero_observations(root):
    calls = 0

    def arrival(*, session):
        nonlocal calls
        session.get()
        calls += 1
        if calls == 1:
            raise RuntimeError("credential-in-a-private-url")
        return []

    result = collect(root, arrival=arrival)
    assert result["status"] == "incomplete"
    assert all(slot["status"] == "failed" and "rows" not in slot for slot in result["arrival_slots"])
    text = (root / ".work/capture/capture.json").read_text()
    assert "credential-in-a-private-url" not in text
    assert not list((root / ".work/capture").glob("arrival-*.json"))
    with pytest.raises(ValueError, match="incomplete"):
        replay.load_capture(root / ".work/capture")


def test_slow_fetch_records_missed_schedule_without_catchup_request(root):
    timer, client = Clock(), Session()

    def arrival(*, session):
        session.get()
        timer.seconds += 650
        return [{"id": "a"}]

    def usage(day, **kwargs):
        client.get()
        return [{"USE_YMD": day}]

    result = capture.capture(root / ".work/slow", "20260128", session=client,
                             clock=timer.now, monotonic=timer.tick, sleep=timer.sleep,
                             arrival_fetch=arrival, usage_fetch=usage)
    assert [s["status"] for s in result["arrival_slots"]] == ["collected", "missed_schedule", "collected"]
    assert result["logical_get_attempts"] == 3
    assert result["status"] == "incomplete"


@pytest.mark.parametrize("change", ["missing_slot", "changed_rows", "changed_stamp", "wrong_day", "reordered"])
def test_replay_rejects_incomplete_or_changed_capture_metadata(root, change):
    collect(root)
    folder = root / ".work/capture"
    manifest = json.loads((folder / "capture.json").read_text())
    if change == "missing_slot":
        manifest["arrival_slots"].pop()
    elif change == "reordered":
        manifest["arrival_slots"].reverse()
    else:
        file = folder / ("usage.json" if change == "wrong_day" else "arrival-00.json")
        payload = json.loads(file.read_text())
        if change == "changed_rows":
            payload["rows"].append(payload["rows"][0])
        elif change == "changed_stamp":
            payload["captured_at_kst"] = "2026-01-31T23:55:01+09:00"
        else:
            payload["rows"][0]["USE_YMD"] = "20260129"
        file.write_text(json.dumps(payload))
    (folder / "capture.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        replay.load_capture(folder)


def test_output_preserves_existing_files_and_requires_private_workspace(root):
    old = root / ".work/existing"
    old.mkdir()
    (old / "keep").write_text("user content")
    with pytest.raises(FileExistsError):
        capture.new_output(old)
    assert (old / "keep").read_text() == "user content"
    with pytest.raises(ValueError):
        capture.new_output(root / "production")
    with pytest.raises(ValueError):
        capture.new_output(root / ".work")


def test_replay_hashes_include_business_identity_and_preserve_bronze_version():
    before = {
        "bronze/a": {"rows": 1, "sha256": "x", "delta_version": 0},
        "gold/b": {"rows": 1, "sha256": "y", "delta_version": 0},
    }
    after = {key: dict(value) for key, value in before.items()}
    after["gold/b"]["delta_version"] = 1
    replay.verify_replay(before, after)
    after["gold/b"]["sha256"] = "different-identity-same-total"
    with pytest.raises(ValueError, match="content"):
        replay.verify_replay(before, after)
    after["gold/b"]["sha256"] = "y"
    after["bronze/a"]["delta_version"] = 1
    with pytest.raises(ValueError, match="Bronze"):
        replay.verify_replay(before, after)


def test_live_cli_requires_explicit_opt_in_before_output_or_credentials(tmp_path):
    import subprocess
    import sys
    target = tmp_path / "must-not-exist"
    result = subprocess.run([sys.executable, "scripts/capture_sources.py", "--output-dir", str(target)],
                            capture_output=True, text=True)
    assert result.returncode == 2
    assert "--fetch is required" in result.stderr
    assert not target.exists()


def test_replay_rejects_naive_capture_time_and_response_over_limit(root):
    collect(root)
    folder = root / ".work/capture"
    manifest = json.loads((folder / "capture.json").read_text())
    file = folder / "arrival-00.json"
    payload = json.loads(file.read_text())
    stamp = payload["captured_at_kst"]
    payload["captured_at_kst"] = stamp.removesuffix("+09:00")
    manifest["arrival_slots"][0]["captured_at_kst"] = payload["captured_at_kst"]
    file.write_text(json.dumps(payload))
    (folder / "capture.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="KST"):
        replay.load_capture(folder)
    payload["captured_at_kst"] = stamp
    manifest["arrival_slots"][0]["captured_at_kst"] = stamp
    payload["rows"] *= 1001
    manifest["arrival_slots"][0]["rows"] = 1001
    file.write_text(json.dumps(payload))
    (folder / "capture.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="limit"):
        replay.load_capture(folder)


@pytest.mark.parametrize("offset,accepted", [(-0.05, True), (-2, False)])
def test_schedule_wall_clock_tolerance_preserves_actual_observation(root, offset, accepted):
    collect(root)
    folder = root / ".work/capture"
    path = folder / "capture.json"
    manifest = json.loads(path.read_text())
    slot = manifest["arrival_slots"][1]
    original = slot["request_started_at_kst"]
    slot["request_started_at_kst"] = (datetime.fromisoformat(original) + timedelta(seconds=offset)).isoformat()
    path.write_text(json.dumps(manifest))
    if accepted:
        checked, _, _ = replay.load_capture(folder)
        assert checked["arrival_slots"][1]["request_started_at_kst"] == slot["request_started_at_kst"]
    else:
        with pytest.raises(ValueError, match="clock tolerance"):
            replay.load_capture(folder)
