"""Audit local Delta data; write machine-readable and human-readable results."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common.dates import parse_as_of, validate_ymd
from src.observability.audit import DeltaAuditReader, audit_lake


def render_audit(result):
    usage, arrival = result["usage"], result["arrival"]
    lines = ["# 데이터 품질·수집 현황", "", f"- 상태: {result['status']}",
             f"- 데이터 종류: {result.get('data_kind', 'unspecified')}",
             f"- 승하차 기준일: {result['usage_day']}",
             f"- 관측 구간(KST): [{result['window']['start_kst']}, {result['window']['end_kst']})", "",
             f"- 감사 기준 시각(KST): {result['window']['as_of_kst']}", "",
             "## 수집·처리", "",
             f"- 관측한 시간 구간: {arrival.get('observed_time_buckets', 'unknown')} / {arrival['expected_time_buckets']}",
             f"- 관측이 없는 시간 구간: {arrival.get('missing_time_buckets', 'unknown')}",
             f"- 유효 ETA / Silver 관측: {arrival.get('valid_eta_rows', 'unknown')} / {arrival.get('silver_rows', 'unknown')}",
             f"- 구간 내 최신 관측의 나이(초): {arrival.get('freshness_seconds')}",
             f"- 아직 Silver에 없는 snapshot: {arrival.get('unprocessed_snapshot_count', 'unknown')}",
             f"- 승하차 Silver/Gold 일치: {usage.get('silver_gold_business_match', 'unknown')}", "",
             "## 읽은 Delta snapshot", "",
             "| 테이블 | 상태 | 버전 | 조회 행 | 해당 날짜 후보 파일 | 후보 bytes |",
             "|---|---|---:|---:|---:|---:|"]
    for key, source in result["sources"].items():
        files = source.get("files", {})
        lines.append(f"| {key} | {source['status']} | {source.get('delta_version', '-')} | "
                     f"{source.get('rows', '-')} | {files.get('candidate_date_files', '-')} | "
                     f"{files.get('candidate_date_bytes', '-')} |")
    lines += ["", "## 판정", ""] + [f"- {i['severity']}: {i['code']}" for i in result["issues"]]
    lines += ["", "## 해석 한계", "",
              "- 관측이 없는 5분 구간이 API 실패를 증명하지는 않습니다.",
              "- 시간 구간 관측률은 전체 역·열차 커버리지가 아닙니다.",
              "- 파일 수·bytes는 Delta 활성 파일 메타데이터이며 실제 IO 비용이 아닙니다.",
              "- 최신 관측 나이는 이 조회 구간 안에서 계산합니다.",
              "- 처리 시간·pool 대기는 Delta 시각으로 추정하지 않습니다.", ""]
    return "\n".join(lines)


def save_audit(result, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    for name, content in [("quality.json", json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False)),
                          ("quality.md", render_audit(result))]:
        temp = output / (name + ".tmp")
        temp.write_text(content, encoding="utf-8")
        temp.replace(output / name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-root", required=True, type=Path)
    parser.add_argument("--prefix", default="v2")
    parser.add_argument("--usage-day", required=True, type=validate_ymd)
    for name in ("start", "end", "as-of"):
        parser.add_argument("--" + name, required=True, type=parse_as_of)
    parser.add_argument("--cadence-seconds", type=int, default=300)
    parser.add_argument("--stale-after-seconds", type=int, default=600)
    parser.add_argument("--data-kind", required=True, choices=["synthetic", "observed"])
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    result = audit_lake(DeltaAuditReader(args.local_root, args.prefix), args.usage_day, args.start, args.end,
                        args.as_of, args.cadence_seconds, args.stale_after_seconds)
    result["data_kind"] = args.data_kind
    save_audit(result, args.output_dir)
    print(json.dumps({"status": result["status"], "issues": result["issues"]}, ensure_ascii=False))
    return {"healthy": 0, "degraded": 1, "failed": 2}[result["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
