"""Deterministic reports by default; optional LLM narrative uses the same validated context."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
from src.common.dates import now_kst, validate_ymd
from src.common.storage import storage_prefix
from src.common.config import load_s3_config
from src.report.build_report_context_daily import build_report_context, ReportDataError
from src.report.publication import publication_outputs, save_local_publication, save_s3_publication

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def render_report(ctx):
    usage, arrival = ctx["usage"], ctx["arrival"]
    lines = [f"# 서울 지하철 데이터 리포트 · {ctx['target_ymd']}", "",
             f"- 승하차 기준일: **{ctx['usage_ymd']}**",
             f"- 도착 관측 기준일: **{ctx['arrival_ymd']}**",
             "- 수집 방식: 공공 API 배치·주기적 스냅샷", "",
             "## 승하차 이용량", "",
             f"승차와 하차를 합산한 이용 횟수는 **{usage['total_count']:,}회**입니다.",
             "이 수치는 이용자 고유 인원이나 오늘의 혼잡도가 아닙니다.", "",
             "| 순위 | 역 | 노선 | 승하차 횟수 |", "|---:|---|---|---:|"]
    if ctx.get("data_kind") == "synthetic":
        lines[2:2] = ["> 합성 fixture 검증용 리포트입니다. 서울 실제 관측 결과가 아닙니다.", ""]
    for row in usage["top_stations_by_total"]:
        lines.append(f"| {row['rank']} | {row['station_name']} | {row['line_name']} | {row['total_count']:,} |")
    lines += ["", "## 도착 ETA 관측", "",
              f"관측 {arrival['total_observations']:,}건 중 유효 ETA는 {arrival['valid_eta_observations']:,}건입니다.",
              "열차·역별 도착예정시간 관측값을 요약합니다. 승객의 실제 대기시간이나 시간표 대비 지연을 측정한 값이 아닙니다.",
              "노선별 평균과 비율은 유효 ETA 표본 수로 가중합니다. 역별 분위수를 평균해 노선 분위수로 표시하지 않습니다.", "",
              "| 노선 ID | 유효 표본 | 평균 ETA(초) | ETA 5분 이상 비율 |", "|---:|---:|---:|---:|"]
    for row in arrival["line_wait_stats"]:
        lines.append(f"| {row['line_id']} | {row['valid_eta_n']} | {row['avg_eta_sec']:.1f} | {row['long_eta_ratio_5m']:.1%} |")
    observed = arrival.get("observation_range", {})
    if observed.get("first_snapshot_ts") and observed.get("last_snapshot_ts"):
        lines += ["", f"실제 관측 범위(KST): {observed['first_snapshot_ts']} ~ {observed['last_snapshot_ts']}"]
    if "max_source_age_sec" in observed:
        lines += [f"최대 원천 시각 차이: {observed['max_source_age_sec']:.0f}초 (snapshot - 원천 수신 시각)"]
    lines += ["", "## 데이터 범위와 해석 한계", "",
              f"- 역별 최대 수집 스냅샷 수: {arrival['max_snapshots_per_station']}",
              "- ALL 응답은 회당 최대 1,000건으로, 서울 전체 역·열차·시간대의 완전한 관측을 보장하지 않습니다.",
              "- 서로 다른 기준일의 이용량과 ETA를 합쳐 오늘의 혼잡도·실제 지연을 추정하지 않습니다.",
              "- 역명만을 이용한 승하차·도착 조인은 하지 않습니다.",
              "- LLM 사용 여부와 무관하게 수치·기준일은 검증된 Gold에서 결정합니다.", ""]
    return "\n".join(lines)


def build_prompt(ctx):
    # Only analytical aggregates and report dates cross the provider boundary.
    facts = {key: ctx[key] for key in ("target_ymd", "usage_ymd", "arrival_ymd")}
    facts["data_kind"] = ctx.get("data_kind", "api_observations")
    fields = {
        "usage": ("total_count", "station_line_count", "line_usage_stats"),
        "arrival": ("total_observations", "valid_eta_observations", "line_wait_stats",
                    "max_snapshots_per_station", "observation_range"),
    }
    for section, allowed in fields.items():
        facts[section] = {key: ctx[section][key] for key in allowed if key in ctx[section]}
    return [{"role": "system", "content":
             "검증된 지하철 데이터만 한국어로 설명하라. 기준 날짜·부분 수집·ETA의 한계를 명시하라. "
             "입력에 없는 수치·비교·운행 지연·현재 혼잡도를 추정하지 말라. "
             "한국어로 기준일, 주요 관측, 수집 한계를 간결하게 설명하라."},
            {"role": "user", "content": json.dumps(facts, ensure_ascii=False, allow_nan=False)}]


def generate_daily_report(target_ymd=None, save_to_s3=True, *, llm_client=None):
    target_ymd = validate_ymd(target_ymd or now_kst().strftime("%Y%m%d"))
    ctx = build_report_context(target_ymd)
    canonical = json.dumps(ctx, ensure_ascii=False, sort_keys=True, allow_nan=False)
    mode = os.getenv("SSDR_REPORT_MODE", "template")
    report_text = render_report(ctx)
    if mode == "llm":
        if llm_client is None:
            from openai import OpenAI
            llm_client = OpenAI(max_retries=0, timeout=45)
        model = os.getenv("SSDR_LLM_MODEL", "gpt-5-mini")
        options = {"reasoning_effort": "minimal"} if model == "gpt-5-mini" else {}
        messages = build_prompt(ctx)
        try:
            response = llm_client.chat.completions.create(model=model, messages=messages,
                                                          max_completion_tokens=1000, **options)
        except Exception:
            raise ReportDataError("LLM request failed; report not published") from None
        if not response.choices:
            raise ReportDataError("LLM returned no report choice")
        choice = response.choices[0]
        narrative = choice.message.content
        if choice.finish_reason != "stop" or not isinstance(narrative, str) or not narrative.strip():
            raise ReportDataError("LLM returned an incomplete or empty narrative")
        # Keep the deterministic evidence table; mark optional unverified interpretation separately.
        report_text += "\n## AI 해설 · 자동 생성, 추가 검증 필요\n\n" + narrative + "\n"
    elif mode != "template":
        raise ValueError("SSDR_REPORT_MODE must be template or llm")
    output_dir = Path(os.getenv("SSDR_REPORT_OUTPUT_DIR", str(PROJECT_ROOT / "data/report_text/daily")))
    stem = f"subway_report_{target_ymd}"
    out_path = output_dir / f"{stem}.md"
    provenance = {"schema_version": 2, "report_mode": mode,
                  "context_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
                  "report_sha256": hashlib.sha256(report_text.encode()).hexdigest(),
                  "target_ymd": target_ymd, "usage_ymd": ctx["usage_ymd"], "arrival_ymd": ctx["arrival_ymd"]}
    if mode == "llm":
        provenance["llm"] = {"model": response.model, "finish_reason": choice.finish_reason,
            "prompt_sha256": hashlib.sha256(json.dumps(messages, ensure_ascii=False, sort_keys=True).encode()).hexdigest()}
        if response.usage is not None:
            provenance["llm"]["tokens"] = {key: getattr(response.usage, key) for key in
                ("prompt_tokens", "completion_tokens", "total_tokens")}
    outputs = publication_outputs(report_text, canonical, provenance)
    save_local_publication(output_dir, outputs)
    if save_to_s3 and not os.getenv("SSDR_DATA_ROOT"):
        import boto3
        cfg = load_s3_config()
        client = boto3.client("s3", region_name=cfg.region)
        prefix = "/".join(p for p in [storage_prefix(), "report_text/daily"] if p)
        save_s3_publication(client, cfg.bucket, prefix, outputs)
    return out_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("target_ymd", type=validate_ymd)
    parser.add_argument("--local-only", action="store_true")
    args = parser.parse_args()
    print(generate_daily_report(args.target_ymd, save_to_s3=not args.local_only))


if __name__ == "__main__":
    main()
