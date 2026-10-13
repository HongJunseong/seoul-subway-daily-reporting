from datetime import datetime
import hashlib
import json
from pathlib import Path

import pytest

from scripts import capture_sources as capture
from scripts import process_source_experiment as replay
from src.observability.source_baseline import validate_manifest
from src.observability.source_integrity import integrity_summary
from test_source_experiment import collect


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setattr(capture, "ROOT", tmp_path)
    monkeypatch.setattr(replay, "ROOT", tmp_path)
    (tmp_path / ".work").mkdir()
    return tmp_path


def manifest(folder):
    return json.loads((folder / "capture.json").read_text())


def save_manifest(folder, value):
    (folder / "capture.json").write_text(json.dumps(value))


def test_collection_records_exact_utf8_bytes_and_replay_verifies_all_files(root):
    def arrival(*, session):
        session.get()
        return [{"statnNm": "서울역", "barvlDt": "120"}]
    result = collect(root, arrival=arrival)
    folder = root / ".work/capture"
    assert result["schema_version"] == 2
    items = [("usage.json", result["usage"])] + [
        (f"arrival-{i:02d}.json", item) for i, item in enumerate(result["arrival_slots"])]
    for filename, item in items:
        content = (folder / filename).read_bytes()
        assert item["raw_sha256"] == hashlib.sha256(content).hexdigest()
        assert (folder / filename).stat().st_mode & 0o777 == 0o600
    checked, _, hashes = replay.load_capture(folder)
    summary = integrity_summary(checked, hashes)
    assert summary["status"] == "verified_against_capture"
    assert summary["files_verified_against_capture"] == summary["files_hashed_at_processing"] == 4


@pytest.mark.parametrize("filename,field", [("arrival-00.json", "barvlDt"), ("usage.json", "USE_YMD")])
def test_same_count_and_timestamp_mutation_stops_before_output_or_spark(root, monkeypatch, filename, field):
    collect(root)
    folder = root / ".work/capture"
    path = folder / filename
    payload = json.loads(path.read_text())
    before = dict(payload)
    payload["rows"][0][field] = "121" if field == "barvlDt" else "20260129"
    assert len(payload["rows"]) == len(before["rows"])
    assert payload["captured_at_kst"] == before["captured_at_kst"]
    path.write_text(json.dumps(payload))
    monkeypatch.setattr(replay, "local_spark", lambda *_: pytest.fail("Spark started before integrity gate"))
    target = root / ".work/processing"
    with pytest.raises(ValueError, match="checksum differs"):
        replay.process(folder, target)
    assert not target.exists()


def test_byte_change_with_identical_parsed_json_is_detected(root):
    collect(root)
    folder = root / ".work/capture"
    path = folder / "arrival-00.json"
    original = json.loads(path.read_bytes())
    path.write_text(json.dumps(original, separators=(",", ":")) + "\n")
    assert json.loads(path.read_bytes()) == original
    with pytest.raises(ValueError, match="checksum differs"):
        replay.load_capture(folder)


@pytest.mark.parametrize("which", ["usage", "arrival"])
def test_new_capture_cannot_omit_a_successful_file_checksum(root, which):
    collect(root)
    folder = root / ".work/capture"
    value = manifest(folder)
    item = value["usage"] if which == "usage" else value["arrival_slots"][0]
    del item["raw_sha256"]
    save_manifest(folder, value)
    with pytest.raises(ValueError, match="checksum"):
        replay.load_capture(folder)


@pytest.mark.parametrize("digest", [None, True, 42, "a" * 63, "g" * 64, "A" * 64])
def test_bad_checksum_formats_are_not_accepted_as_integrity(root, digest):
    collect(root)
    folder = root / ".work/capture"
    value = manifest(folder)
    value["arrival_slots"][0]["raw_sha256"] = digest
    save_manifest(folder, value)
    with pytest.raises(ValueError, match="SHA-256"):
        replay.load_capture(folder)


def test_legacy_capture_is_preserved_but_not_claimed_verified_at_collection(root):
    collect(root)
    folder = root / ".work/capture"
    value = manifest(folder)
    value["schema_version"] = 1
    for item in [value["usage"], *value["arrival_slots"]]:
        del item["raw_sha256"]
    save_manifest(folder, value)
    before = (folder / "capture.json").read_bytes()
    checked, _, hashes = replay.load_capture(folder)
    summary = integrity_summary(checked, hashes)
    assert summary["status"] == "unverified_legacy"
    assert summary["files_hashed_at_processing"] == 4
    assert summary["files_verified_against_capture"] == 0
    assert (folder / "capture.json").read_bytes() == before


@pytest.mark.parametrize("version", [True, 0, 3, "2"])
def test_unknown_or_boolean_versions_fail_before_replay(root, version):
    collect(root)
    folder = root / ".work/capture"
    value = manifest(folder)
    value["schema_version"] = version
    save_manifest(folder, value)
    with pytest.raises(ValueError, match="schema"):
        replay.load_capture(folder)


def test_version_downgrade_cannot_keep_new_checksum_claim(root):
    collect(root)
    folder = root / ".work/capture"
    value = manifest(folder)
    value["schema_version"] = 1
    save_manifest(folder, value)
    with pytest.raises(ValueError, match="legacy"):
        replay.load_capture(folder)


@pytest.mark.parametrize("filename", ["arrival-00.json", "capture.json"])
def test_saved_source_cannot_escape_capture_folder_via_symlink(root, filename):
    collect(root)
    folder = root / ".work/capture"
    path = folder / filename
    content = path.read_bytes()
    outside = root / "outside.json"
    outside.write_bytes(content)
    path.unlink()
    path.symlink_to(outside)
    with pytest.raises(ValueError, match="escaped"):
        replay.load_capture(folder)


def test_metadata_only_validation_checks_digest_syntax_not_file_content(root):
    value = collect(root)
    value["arrival_slots"][0]["raw_sha256"] = "a" * 64
    validate_manifest(value, datetime.fromisoformat("2026-02-02T00:00:00+09:00"))
    save_manifest(root / ".work/capture", value)
    with pytest.raises(ValueError, match="checksum differs"):
        replay.load_capture(root / ".work/capture")


def test_failed_request_cannot_claim_saved_file_checksum(root):
    def failed(*, session):
        session.get()
        raise RuntimeError("failure")
    value = collect(root, arrival=failed)
    assert all("raw_sha256" not in item for item in value["arrival_slots"])
    value["arrival_slots"][0]["raw_sha256"] = "a" * 64
    with pytest.raises(ValueError, match="unsuccessful"):
        validate_manifest(value, datetime.fromisoformat("2026-02-02T00:00:00+09:00"))


def test_integrity_summary_cannot_use_different_processing_hashes(root):
    collect(root)
    checked, _, hashes = replay.load_capture(root / ".work/capture")
    hashes["arrival-00.json"] = "b" * 64
    with pytest.raises(ValueError, match="processing hashes"):
        integrity_summary(checked, hashes)
