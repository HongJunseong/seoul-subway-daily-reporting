import hashlib
import json
import os
from pathlib import Path

import pytest

from scripts.verify_report import verify_report
from src.report.generate_daily_report import render_report
from src.report.publication import (decode_publication, publication_outputs, read_publication,
                                    save_local_publication, save_s3_publication)

STEM = "subway_report_20260201"


def outputs(total=680):
    context = {
        "target_ymd": "20260201", "usage_ymd": "20260128", "arrival_ymd": "20260131",
        "data_kind": "synthetic",
        "usage": {"total_count": total, "top_stations_by_total": []},
        "arrival": {"total_observations": 4, "valid_eta_observations": 4,
                    "max_snapshots_per_station": 2, "line_wait_stats": []},
    }
    canonical = json.dumps(context, ensure_ascii=False, sort_keys=True, allow_nan=False)
    report = render_report(context)
    manifest = {"schema_version": 2, "report_mode": "template", "target_ymd": "20260201",
                "usage_ymd": "20260128", "arrival_ymd": "20260131",
                "context_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
                "report_sha256": hashlib.sha256(report.encode()).hexdigest()}
    return publication_outputs(report, canonical, manifest)


@pytest.mark.parametrize("stage", range(4))
def test_local_write_failure_keeps_prior_publication_readable_and_retry_repairs_exports(tmp_path, monkeypatch, stage):
    original, changed = outputs(), outputs(700)
    save_local_publication(tmp_path, original)
    bundle = tmp_path / f"{STEM}.publication.json"
    previous = bundle.read_bytes()
    replace = Path.replace
    def fail(path, target):
        if Path(target).name == list(changed)[stage]:
            raise OSError("injected local replacement failure")
        return replace(path, target)
    with monkeypatch.context() as patch:
        patch.setattr(Path, "replace", fail)
        with pytest.raises(OSError):
            save_local_publication(tmp_path, changed)
    assert bundle.read_bytes() == previous
    assert read_publication(bundle)["context"]["usage"]["total_count"] == 680
    verify_report(bundle)
    if stage > 0:
        with pytest.raises(ValueError):
            verify_report(tmp_path / f"{STEM}.md")
    assert not list(tmp_path.glob("*.tmp"))
    save_local_publication(tmp_path, changed)
    assert read_publication(bundle)["context"]["usage"]["total_count"] == 700
    verify_report(tmp_path / f"{STEM}.md")


@pytest.mark.parametrize("stage", range(4))
def test_first_publication_failure_has_no_committed_object(tmp_path, monkeypatch, stage):
    changed = outputs()
    replace = Path.replace
    def fail(path, target):
        if Path(target).name == list(changed)[stage]:
            raise OSError("first publication failed")
        return replace(path, target)
    monkeypatch.setattr(Path, "replace", fail)
    with pytest.raises(OSError):
        save_local_publication(tmp_path, changed)
    assert not (tmp_path / f"{STEM}.publication.json").exists()


def test_lost_ack_after_local_commit_serves_complete_new_value_and_retry_is_identical(tmp_path, monkeypatch):
    save_local_publication(tmp_path, outputs())
    changed = outputs(700)
    replace = Path.replace
    def fail(path, target):
        result = replace(path, target)
        if str(target).endswith(".publication.json"):
            raise OSError("commit acknowledgement lost")
        return result
    with monkeypatch.context() as patch:
        patch.setattr(Path, "replace", fail)
        with pytest.raises(OSError):
            save_local_publication(tmp_path, changed)
    bundle = tmp_path / f"{STEM}.publication.json"
    assert read_publication(bundle)["context"]["usage"]["total_count"] == 700
    before = bundle.read_bytes()
    save_local_publication(tmp_path, changed)
    assert bundle.read_bytes() == before
    verify_report(tmp_path / f"{STEM}.md")


def test_reader_pins_one_object_even_if_writer_replaces_it_during_read(tmp_path, monkeypatch):
    save_local_publication(tmp_path, outputs())
    bundle = tmp_path / f"{STEM}.publication.json"
    read = Path.read_bytes
    calls = []
    def change_after_read(path):
        value = read(path)
        calls.append(path)
        save_local_publication(tmp_path, outputs(700))
        return value
    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_bytes", change_after_read)
        snapshot = read_publication(bundle)
    assert calls == [bundle]
    assert snapshot["context"]["usage"]["total_count"] == 680
    assert "680회" in snapshot["report"]
    assert read_publication(bundle)["context"]["usage"]["total_count"] == 700


class MemoryS3:
    def __init__(self):
        self.objects, self.calls, self.fail_key, self.after_put = {}, [], None, False

    def put_object(self, **request):
        self.calls.append(request)
        if request["Key"] == self.fail_key and not self.after_put:
            raise OSError("mock S3 failure")
        self.objects[request["Key"]] = request["Body"]
        if request["Key"] == self.fail_key:
            raise OSError("mock S3 acknowledgement lost")


@pytest.mark.parametrize("stage", range(4))
def test_mock_s3_failure_keeps_prior_object_and_retry_commits_complete_new_payload(stage):
    client = MemoryS3()
    original, changed = outputs(), outputs(700)
    prefix = "v2/report_text/daily"
    key = f"{prefix}/{STEM}.publication.json"
    save_s3_publication(client, "fixture", prefix, original)
    previous = client.objects[key]
    client.fail_key = f"{prefix}/{list(changed)[stage]}"
    with pytest.raises(OSError):
        save_s3_publication(client, "fixture", prefix, changed)
    assert client.objects[key] == previous
    assert decode_publication(client.objects[key])["context"]["usage"]["total_count"] == 680
    client.fail_key = None
    save_s3_publication(client, "fixture", prefix, changed)
    assert decode_publication(client.objects[key])["context"]["usage"]["total_count"] == 700
    assert client.calls[-1]["Key"] == key
    assert all(call["Bucket"] == "fixture" for call in client.calls)


def test_mock_s3_lost_ack_can_commit_new_object_and_retry_is_idempotent():
    client = MemoryS3()
    prefix = "v2/report_text/daily"
    key = f"{prefix}/{STEM}.publication.json"
    save_s3_publication(client, "fixture", prefix, outputs())
    client.fail_key, client.after_put = key, True
    with pytest.raises(OSError):
        save_s3_publication(client, "fixture", prefix, outputs(700))
    assert decode_publication(client.objects[key])["context"]["usage"]["total_count"] == 700
    before = client.objects[key]
    client.fail_key = None
    save_s3_publication(client, "fixture", prefix, outputs(700))
    assert client.objects[key] == before


@pytest.mark.parametrize("field", ["report", "context"])
def test_single_object_reader_rejects_mutated_contents(field):
    value = json.loads(outputs()[f"{STEM}.publication.json"])
    value[field] += " "
    with pytest.raises(ValueError, match="hash mismatch"):
        decode_publication(json.dumps(value).encode())


@pytest.mark.parametrize("version", [True, 0, 2, "1"])
def test_unknown_bundle_schema_cannot_be_consumed(version):
    value = json.loads(outputs()[f"{STEM}.publication.json"])
    value["schema_version"] = version
    with pytest.raises(ValueError, match="schema"):
        decode_publication(json.dumps(value).encode())


def test_exports_cannot_look_successful_when_bundle_commit_failed(tmp_path):
    save_local_publication(tmp_path, outputs())
    changed = outputs(700)
    for name in list(changed)[:3]:
        (tmp_path / name).write_bytes(changed[name])
    # Legacy hashes agree with each other, but this version never committed.
    with pytest.raises(ValueError, match="committed bundle"):
        verify_report(tmp_path / f"{STEM}.md")
    assert read_publication(tmp_path / f"{STEM}.publication.json")["context"]["usage"]["total_count"] == 680


def test_corrupt_exports_are_rejected_before_any_local_or_mock_s3_write(tmp_path):
    changed = outputs()
    changed[f"{STEM}.md"] += b"changed"
    with pytest.raises(ValueError, match="exports"):
        save_local_publication(tmp_path, changed)
    assert not list(tmp_path.iterdir())
    client = MemoryS3()
    with pytest.raises(ValueError, match="exports"):
        save_s3_publication(client, "fixture", "reports", changed)
    assert not client.calls


def test_canonical_reader_never_falls_back_to_exports_when_bundle_is_corrupt(tmp_path):
    save_local_publication(tmp_path, outputs())
    bundle = tmp_path / f"{STEM}.publication.json"
    bundle.write_bytes(b'{"schema_version":1,')
    with pytest.raises(ValueError):
        verify_report(bundle)
    with pytest.raises(ValueError):
        verify_report(tmp_path / f"{STEM}.md")


@pytest.mark.parametrize("nonfinite", ["NaN", "1e309"])
def test_context_nonfinite_is_not_hidden_by_matching_checksums(nonfinite):
    changed = outputs()
    value = json.loads(changed[f"{STEM}.publication.json"])
    value["context"] = value["context"].replace('"total_count": 680', f'"total_count": {nonfinite}')
    value["manifest"]["context_sha256"] = hashlib.sha256(value["context"].encode()).hexdigest()
    with pytest.raises(ValueError, match="Non-finite"):
        decode_publication(json.dumps(value).encode())


@pytest.mark.parametrize("suffix", [".md", ".publication.json"])
def test_verification_cli_needs_no_inherited_pythonpath(tmp_path, suffix):
    import os
    import subprocess
    import sys
    save_local_publication(tmp_path, outputs())
    root = Path(__file__).resolve().parents[1]
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    result = subprocess.run([sys.executable, str(root / "scripts/verify_report.py"), str(tmp_path / f"{STEM}{suffix}")],
                            cwd=tmp_path, env=env, capture_output=True, text=True, check=True)
    assert json.loads(result.stdout)["target_ymd"] == "20260201"


@pytest.mark.skipif(os.name != "posix", reason="SIGKILL process-death test requires POSIX")
def test_real_process_death_between_exports_preserves_publication_and_retry(tmp_path):
    import os
    import signal
    import subprocess
    import sys
    save_local_publication(tmp_path, outputs())
    bundle = tmp_path / f"{STEM}.publication.json"
    previous = bundle.read_bytes()
    code = """
import os, signal, sys
from pathlib import Path
from src.report import publication as writer
from test_publication_commit import outputs
original = writer.atomic_write
def kill_before_manifest(path, content):
    if path.name.endswith(".manifest.json"):
        os.kill(os.getpid(), signal.SIGKILL)
    original(path, content)
writer.atomic_write = kill_before_manifest
writer.save_local_publication(Path(sys.argv[1]), outputs(700))
"""
    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(root), str(root / "tests")]))
    result = subprocess.run([sys.executable, "-c", code, str(tmp_path)], cwd=root, env=env,
                            capture_output=True, timeout=20)
    assert result.returncode == -signal.SIGKILL
    assert bundle.read_bytes() == previous
    assert read_publication(bundle)["context"]["usage"]["total_count"] == 680
    with pytest.raises(ValueError):
        verify_report(tmp_path / f"{STEM}.md")
    save_local_publication(tmp_path, outputs(700))
    assert read_publication(bundle)["context"]["usage"]["total_count"] == 700
    verify_report(tmp_path / f"{STEM}.md")
