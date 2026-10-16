"""Ground numeric report claims through named fact references, not generated digits."""
from __future__ import annotations
import re

from src.report.build_report_context_daily import ReportDataError

REFERENCE = re.compile(r"\{\{([a-zA-Z0-9_.]+)\}\}")
# Conservative lexical checks; these do not prove arbitrary prose is factual.
UNSUPPORTED = re.compile(
    r"지연|혼잡|대기\s*시간|승객|고유\s*(?:인원|이용자)|명(?:입니다|이|의|을|으로|$)|"
    r"전일|전주|전년|증가|감소|증감|상승|하락|개선|악화|늘었|줄었|오늘|현재|"
    r"보다|가장|최고|최저|더\s*(?:높|낮|많|적)|"
    r"\b(?:delay|congestion|crowd|passenger|waiting|increase|decrease|today|yesterday|higher|lower)\w*\b", re.IGNORECASE)


def narrative_facts(ctx):
    facts = {
        "report.date": f"보고서 기준일 {ctx['target_ymd']}",
        "usage.date": f"승하차 기준일 {ctx['usage_ymd']}",
        "arrival.date": f"도착 관측 기준일 {ctx['arrival_ymd']}",
        "usage.total_count": f"승하차 합산 이용 횟수 {ctx['usage']['total_count']:,}회",
        "arrival.total_observations": f"도착 관측 {ctx['arrival']['total_observations']:,}건",
        "arrival.valid_eta_observations": f"유효 ETA 관측 {ctx['arrival']['valid_eta_observations']:,}건",
    }
    for index, row in enumerate(ctx['usage'].get('line_usage_stats', [])):
        facts[f"usage.lines.{index}.total_count"] = f"{row['line_name']}의 승하차 합산 이용 횟수 {row['total_count']:,}회"
    for index, row in enumerate(ctx['arrival']['line_wait_stats']):
        prefix = f"arrival.lines.{index}"
        facts[prefix + '.valid_eta_n'] = f"노선 ID {row['line_id']}의 유효 ETA 표본 {row['valid_eta_n']:,}건"
        facts[prefix + '.avg_eta_sec'] = f"노선 ID {row['line_id']}의 평균 ETA {row['avg_eta_sec']:.1f}초"
        facts[prefix + '.long_eta_ratio_5m'] = (
            f"노선 ID {row['line_id']}의 유효 ETA 중 5분 이상 비율 {row['long_eta_ratio_5m']:.1%}")
    return facts


def validate_narrative(text, ctx):
    facts = narrative_facts(ctx)
    references = REFERENCE.findall(text)
    prose = REFERENCE.sub('', text)
    if re.search(r"\}\}\s*(?:회|건|명|초|분|%|퍼센트|만|억|천|백|배)", text):
        raise ReportDataError("LLM narrative changes a referenced fact's unit")
    if not references or any(key not in facts for key in references):
        raise ReportDataError("LLM narrative contains missing or unknown fact references")
    if re.search(r"[{}]|\d", prose):
        raise ReportDataError("LLM narrative contains unsupported numeric claims")
    if UNSUPPORTED.search(prose):
        raise ReportDataError("LLM narrative contains unsupported comparisons or interpretations")
    rendered = REFERENCE.sub(lambda match: facts[match[1]], text)
    return rendered, {"policy": "fact-references-v1", "referenced_facts": sorted(set(references)),
                      "free_text_semantics_verified": False}
