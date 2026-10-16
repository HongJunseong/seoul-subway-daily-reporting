import json
import pytest
from scripts.verify_report import verify_report
from src.report import generate_daily_report as publication


@pytest.mark.parametrize("suffix", [".md", ".context.json"])
def test_publication_manifest_detects_changed_input_or_report(tmp_path, monkeypatch, suffix):
    context = {
        "target_ymd": "20260201", "usage_ymd": "20260128", "arrival_ymd": "20260131",
        "usage": {"total_count": 680, "top_stations_by_total": []},
        "arrival": {"total_observations": 4, "valid_eta_observations": 4,
                    "max_snapshots_per_station": 2, "line_wait_stats": []},
        "sources": {"usage": {"path": "v2/gold/usage", "delta_version": 2},
                    "arrival": {"path": "v2/gold/arrival", "delta_version": 0}},
    }
    monkeypatch.setattr(publication, "build_report_context", lambda date: context)
    monkeypatch.setenv("SSDR_REPORT_MODE", "template")
    monkeypatch.setenv("SSDR_REPORT_OUTPUT_DIR", str(tmp_path))
    report = publication.generate_daily_report("20260201", save_to_s3=False)
    manifest = verify_report(report)
    assert manifest["report_mode"] == "template"
    assert json.loads(report.with_suffix(".context.json").read_text())["sources"] == context["sources"]
    original = report.read_bytes()
    assert publication.generate_daily_report("20260201", save_to_s3=False).read_bytes() == original
    changed = report.with_suffix(suffix)
    changed.write_bytes(changed.read_bytes() + b" ")
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_report(report)

def llm_context():
    from pathlib import Path
    return json.loads(Path("examples/subway_report_20260201.context.json").read_text())


def response_client(content="입력 기준일과 관측 범위를 바탕으로 작성한 해설입니다.", finish="stop"):
    from types import SimpleNamespace
    from unittest.mock import Mock
    response = SimpleNamespace(model="gpt-5-mini-test", usage=SimpleNamespace(
        prompt_tokens=100, completion_tokens=20, total_tokens=120),
        choices=[SimpleNamespace(message=SimpleNamespace(content=content), finish_reason=finish)])
    return SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=Mock(return_value=response))))


def test_llm_prompt_excludes_storage_paths_and_station_records():
    context = llm_context()
    context["sources"] = {"private": "SECRET-STORAGE"}
    context["usage"]["top_stations_by_total"] = [{"raw_payload": "PRIVATE-ROWS"}]
    messages = publication.build_prompt(context)
    sent = json.dumps(messages, ensure_ascii=False)
    assert "SECRET-STORAGE" not in sent and "PRIVATE-ROWS" not in sent
    facts = json.loads(messages[1]["content"])
    assert facts["usage"]["total_count"] == context["usage"]["total_count"]
    assert facts["usage_ymd"] == "20260128" and facts["arrival_ymd"] == "20260131"


def test_llm_generation_is_bounded_and_records_prompt_model_usage(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock
    import sys
    client = response_client()
    factory = Mock(return_value=client)
    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=factory))
    monkeypatch.setattr(publication, "build_report_context", lambda date: llm_context())
    monkeypatch.setenv("SSDR_REPORT_MODE", "llm")
    monkeypatch.setenv("SSDR_LLM_MODEL", "gpt-5-mini")
    monkeypatch.setenv("SSDR_REPORT_OUTPUT_DIR", str(tmp_path))
    path = publication.generate_daily_report("20260201", save_to_s3=False)
    factory.assert_called_once_with(max_retries=0, timeout=45)
    client.chat.completions.create.assert_called_once()
    args = client.chat.completions.create.call_args.kwargs
    assert args["max_completion_tokens"] == 1000 and args["reasoning_effort"] == "minimal"
    manifest = verify_report(path)
    assert manifest["report_mode"] == "llm"
    assert manifest["llm"]["model"] == "gpt-5-mini-test"
    assert manifest["llm"]["tokens"]["total_tokens"] == 120
    assert len(manifest["llm"]["prompt_sha256"]) == 64


@pytest.mark.parametrize("content,finish", [("", "stop"), ("  ", "stop"), (None, "stop"),
                                          ("partial", "length"), ("blocked", "content_filter")])
def test_llm_incomplete_response_preserves_previous_publication(tmp_path, monkeypatch, content, finish):
    monkeypatch.setattr(publication, "build_report_context", lambda date: llm_context())
    monkeypatch.setenv("SSDR_REPORT_MODE", "template")
    monkeypatch.setenv("SSDR_REPORT_OUTPUT_DIR", str(tmp_path))
    publication.generate_daily_report("20260201", save_to_s3=False)
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    monkeypatch.setenv("SSDR_REPORT_MODE", "llm")
    client = response_client(content, finish)
    with pytest.raises(publication.ReportDataError, match="incomplete or empty"):
        publication.generate_daily_report("20260201", save_to_s3=False, llm_client=client)
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir()}

def test_llm_request_failure_does_not_publish_or_expose_provider_error(tmp_path, monkeypatch):
    import traceback
    monkeypatch.setattr(publication, "build_report_context", lambda date: llm_context())
    monkeypatch.setenv("SSDR_REPORT_MODE", "llm")
    monkeypatch.setenv("SSDR_REPORT_OUTPUT_DIR", str(tmp_path / "report"))
    client = response_client()
    client.chat.completions.create.side_effect = RuntimeError("SECRET-API-KEY")
    with pytest.raises(publication.ReportDataError, match="request failed") as caught:
        publication.generate_daily_report("20260201", save_to_s3=False, llm_client=client)
    assert "SECRET-API-KEY" not in "".join(traceback.format_exception(caught.type, caught.value, caught.tb))
    assert not (tmp_path / "report").exists()
