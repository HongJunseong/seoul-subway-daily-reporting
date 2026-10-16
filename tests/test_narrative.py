import json
from pathlib import Path
import pytest

from src.report.narrative import validate_narrative
from src.report.build_report_context_daily import ReportDataError
from src.report import generate_daily_report as generation


@pytest.fixture
def context():
    return json.loads(Path("examples/subway_report_20260201.context.json").read_text())


def test_named_fact_substitution_keeps_metric_date_unit_and_denominator(context):
    draft = "{{usage.date}}의 {{usage.total_count}}입니다. {{arrival.lines.0.long_eta_ratio_5m}}입니다."
    text, checks = validate_narrative(draft, context)
    assert "승하차 기준일 20260128" in text
    assert "승하차 합산 이용 횟수 680회" in text
    assert "유효 ETA 중 5분 이상 비율" in text
    assert "{{" not in text
    assert checks["free_text_semantics_verified"] is False


@pytest.mark.parametrize("draft", [
    "수치를 확인했습니다.", "{{usage.unknown}}입니다.", "{{usage.total_count}}이고 999명입니다.",
    "{{usage.total_count}}이고 기준일은 20260129입니다.", "{{usage.total_count}}이고 비율은 ９９％입니다.",
    "{{usage.total_count}}이고 전일 대비 증가했습니다.", "{{usage.total_count}}이고 실제 운행 지연입니다.",
    "{{arrival.valid_eta_observations}}이고 승객 대기시간입니다.",
    "{{usage.total_count}}이고 현재 혼잡합니다.", "{{usage.total_count}}이고 이용자는 고유 인원입니다.",
    "{{usage.total_count}}이고 {{malformed 입니다.", "{{usage.total_count}}명입니다.",
    "{{usage.total_count}} and passenger waiting time increased.",
    "{{usage.total_count}}초입니다.", "{{arrival.valid_eta_observations}}만 건입니다.",
])
def test_unverified_draft_is_rejected(context, draft):
    with pytest.raises(ReportDataError):
        validate_narrative(draft, context)


def test_failed_narrative_validation_preserves_previous_publication(context, tmp_path, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock
    monkeypatch.setattr(generation, "build_report_context", lambda date: context)
    monkeypatch.setenv("SSDR_REPORT_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("SSDR_REPORT_MODE", "template")
    generation.generate_daily_report("20260201", save_to_s3=False)
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    response = SimpleNamespace(choices=[SimpleNamespace(finish_reason="stop",
                               message=SimpleNamespace(content="{{usage.total_count}}이고 실제 지연입니다."))])
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=Mock(return_value=response))))
    monkeypatch.setenv("SSDR_REPORT_MODE", "llm")
    with pytest.raises(ReportDataError, match="interpretations"):
        generation.generate_daily_report("20260201", save_to_s3=False, llm_client=client)
    assert before == {p.name: p.read_bytes() for p in tmp_path.iterdir()}
