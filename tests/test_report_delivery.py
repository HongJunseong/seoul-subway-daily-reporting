import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import sys
import traceback

import pytest
from src.report import generate_daily_report as generation
from src.report import delivery


@pytest.fixture
def job(tmp_path, monkeypatch):
    context = json.loads(Path("examples/subway_report_20260201.context.json").read_text())
    monkeypatch.setattr(generation, "build_report_context", Mock(return_value=context))
    response = SimpleNamespace(model="fixture", usage=None, choices=[SimpleNamespace(
        finish_reason="stop", message=SimpleNamespace(content="{{usage.total_count}}입니다."))])
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=Mock(return_value=response))))
    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=Mock(return_value=client)))
    monkeypatch.setenv("SSDR_REPORT_MODE", "llm")
    monkeypatch.setenv("SSDR_REPORT_OUTPUT_DIR", str(tmp_path))
    monkeypatch.delenv("SSDR_DATA_ROOT", raising=False)
    monkeypatch.setenv("SSDR_S3_BUCKET", "fixture-bucket")
    return client, tmp_path


def test_failed_upload_reuses_identical_run_snapshot_without_another_llm_call(job):
    llm, root = job
    receipt = delivery.generate_report_for_run("20260201", "manual__fixture")
    path = Path(receipt["publication_path"])
    expected = path.read_bytes()
    remote = Mock()
    remote.put_object.side_effect = RuntimeError("SECRET-WEBHOOK-OR-CREDENTIAL")
    with pytest.raises(RuntimeError, match="retry the saved publication") as caught:
        delivery.upload_report_for_run("20260201", "manual__fixture", receipt, client=remote)
    assert "SECRET" not in "".join(traceback.format_exception(caught.type, caught.value, caught.tb))
    # Date exports may be replaced by another execution; retry must use this run.
    (root / path.name).write_text("newer date export")
    generation.build_report_context.side_effect = AssertionError("Gold must not be reread")
    receipt_again = delivery.generate_report_for_run("20260201", "manual__fixture")
    assert receipt_again == receipt
    remote.reset_mock(side_effect=True)
    result = delivery.upload_report_for_run("20260201", "manual__fixture", receipt, client=remote)
    assert result["destination"] == "s3"
    assert remote.put_object.call_count == 4
    assert remote.put_object.call_args.kwargs["Body"] == expected
    assert path.read_bytes() == expected
    llm.chat.completions.create.assert_called_once()


def test_changed_snapshot_is_rejected_before_s3_and_does_not_regenerate(job):
    llm, _ = job
    receipt = delivery.generate_report_for_run("20260201", "fixture")
    Path(receipt["publication_path"]).write_bytes(b"corrupt")
    remote = Mock()
    with pytest.raises(ValueError, match="differs"):
        delivery.upload_report_for_run("20260201", "fixture", receipt, client=remote)
    with pytest.raises(ValueError):
        delivery.generate_report_for_run("20260201", "fixture")
    remote.put_object.assert_not_called()
    llm.chat.completions.create.assert_called_once()


def test_missing_snapshot_does_not_fall_back_to_latest_date_export(job):
    llm, root = job
    receipt = delivery.generate_report_for_run("20260201", "fixture")
    Path(receipt["publication_path"]).unlink()
    assert (root / "subway_report_20260201.publication.json").exists()
    remote = Mock()
    with pytest.raises(FileNotFoundError):
        delivery.upload_report_for_run("20260201", "fixture", receipt, client=remote)
    remote.put_object.assert_not_called()
    llm.chat.completions.create.assert_called_once()


def test_receipt_cannot_cross_run_or_target_date(job):
    _, root = job
    receipt = delivery.generate_report_for_run("20260201", "manual__/../../literal")
    assert Path(receipt["publication_path"]).is_relative_to(root / "runs")
    remote = Mock()
    with pytest.raises(ValueError, match="receipt"):
        delivery.upload_report_for_run("20260201", "another-run", receipt, client=remote)
    with pytest.raises(ValueError, match="receipt"):
        delivery.upload_report_for_run("20260202", "manual__/../../literal", receipt, client=remote)
    remote.put_object.assert_not_called()


def test_fixture_delivery_verifies_snapshot_but_never_creates_cloud_client(job, monkeypatch):
    _, root = job
    receipt = delivery.generate_report_for_run("20260201", "fixture")
    monkeypatch.setenv("SSDR_DATA_ROOT", str(root / "lake"))
    monkeypatch.delitem(sys.modules, "openai")
    remote = Mock()
    result = delivery.upload_report_for_run("20260201", "fixture", receipt, client=remote)
    assert result == {"destination": "local", "publication_sha256": receipt["publication_sha256"]}
    assert hashlib.sha256(Path(receipt["publication_path"]).read_bytes()).hexdigest() == result["publication_sha256"]
    remote.put_object.assert_not_called()
