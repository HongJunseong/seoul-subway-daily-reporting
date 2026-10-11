"""Intentional local failure boundaries with committed Delta recovery evidence."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from src.common.storage import table_path
from src.ingestion.ingest_subway_usage_daily import save_to_bronze_delta as save_usage
from src.ingestion.ingest_subway_arrival_realtime import save_to_bronze_delta as save_arrival
from src.processing.build_fact_subway_usage_daily_spark import build_usage_silver
from src.processing.build_gold_subway_usage_daily_spark import build_usage_gold
from src.processing.build_fact_subway_arrival_spark import build_arrival_silver
from src.quality.checks import DataQualityError
from src.observability.audit import DeltaAuditReader, audit_lake
from scripts.audit_lake import save_audit
from scripts.local_spark import local_spark


class InjectedFailure(RuntimeError):
    pass


def check(condition, message):
    if not condition:
        raise RuntimeError("Recovery verification failed: " + message)


def run_usage(spark, day, fail_after_silver=False):
    build_usage_silver(spark, day)
    if fail_after_silver:
        raise InjectedFailure("intentional stop after Silver commit, before Gold")
    build_usage_gold(spark, day)


def table_state(spark, key, day, columns, date_column="use_ymd"):
    path = table_path(key)
    frame = spark.read.format("delta").load(path).where(F.col(date_column) == day)
    rows = [r.asDict() for r in frame.select(*columns).collect()]
    serialized = json.dumps(sorted(rows, key=lambda row: json.dumps(row, sort_keys=True)),
                            sort_keys=True, ensure_ascii=False, allow_nan=False)
    return {"delta_version": int(DeltaTable.forPath(spark, path).history(1).first().version),
            "rows": len(rows), "business_sha256": hashlib.sha256(serialized.encode()).hexdigest(),
            "total_count": sum(r["total_count"] for r in rows) if "total_count" in columns else None}


def state(spark, day):
    columns = ["use_ymd", "line_name", "station_name", "ride_count", "alight_count", "total_count"]
    return {
        "bronze": table_state(spark, "bronze/subway_usage_delta", day, ["raw_payload", "run_id"], "USE_YMD"),
        "silver": table_state(spark, "silver/fact_subway_usage_delta", day, columns),
        "gold": table_state(spark, "gold/subway_usage/station_daily", day, columns),
        "line_gold": table_state(spark, "gold/subway_usage/line_daily", day,
                                 ["use_ymd", "line_name", "ride_count", "alight_count", "total_count"])}


def run_experiment(spark):
    rows = json.loads((ROOT / "fixtures/usage.json").read_text(encoding="utf-8"))
    day = "20260128"
    for run in ["initial", "retry"]:
        save_usage(rows, day, spark=spark, captured_at=datetime(2026, 2, 1, 6), run_id=run)
    run_usage(spark, day)
    initial = state(spark, day)
    check(initial["gold"]["total_count"] == 660, "baseline")
    save_usage([dict(rows[0], REG_YMD="20260130", GTON_TNOPE="120")], day, spark=spark,
               captured_at=datetime(2026, 2, 1, 7), run_id="correction")
    source_before_failure = state(spark, day)["bronze"]
    started = time.perf_counter()
    try:
        run_usage(spark, day, fail_after_silver=True)
    except InjectedFailure:
        failure_seconds = time.perf_counter() - started
    else:
        raise RuntimeError("Failpoint did not fire")
    partial = state(spark, day)
    check(partial["silver"]["total_count"] == 680, "Silver committed correction")
    check(partial["gold"] == initial["gold"], "prior Gold retained after interruption")
    check(partial["line_gold"] == initial["line_gold"], "prior line Gold retained")
    started = time.perf_counter()
    run_usage(spark, day)
    recovery_seconds = time.perf_counter() - started
    recovered = state(spark, day)
    check(recovered["gold"]["rows"] == 3 and recovered["gold"]["total_count"] == 680, "recovered Gold")
    check(recovered["line_gold"]["total_count"] == 680, "recovered line total")
    check(recovered["silver"]["business_sha256"] == recovered["gold"]["business_sha256"], "business values")
    check(recovered["bronze"] == source_before_failure, "no API recollection or Bronze mutation")
    started = time.perf_counter()
    run_usage(spark, day)
    replay_seconds = time.perf_counter() - started
    replay = state(spark, day)
    for key in ["silver", "gold", "line_gold"]:
        check(replay[key]["business_sha256"] == recovered[key]["business_sha256"], "repeat " + key)
    check(replay["bronze"] == recovered["bronze"], "replay source unchanged")
    interrupted = {"failure_boundary": "after Silver commit, before Gold",
                   "before": initial, "after_failure": partial, "recovered": recovered, "replayed": replay,
                   "failed_attempt_seconds": round(failure_seconds, 6),
                   "recovery_processing_seconds": round(recovery_seconds, 6),
                   "replay_processing_seconds": round(replay_seconds, 6), "verified": True}

    # A source quality failure must leave both published layers untouched.
    other_day = "20260129"
    other_rows = [dict(row, USE_YMD=other_day) for row in rows]
    save_usage(other_rows, other_day, spark=spark, captured_at=datetime(2026, 2, 2, 6), run_id="quality-baseline")
    run_usage(spark, other_day)
    quality_before = state(spark, other_day)
    save_usage([dict(other_rows[0], REG_YMD="20260201", GTON_TNOPE="-1")], other_day, spark=spark,
               captured_at=datetime(2026, 2, 2, 7), run_id="bad-revision")
    try:
        run_usage(spark, other_day)
    except DataQualityError:
        pass
    else:
        raise RuntimeError("Invalid source revision was published")
    quality_failed = state(spark, other_day)
    for key in ["silver", "gold", "line_gold"]:
        check(quality_failed[key] == quality_before[key], "quality gate preserved " + key)
    save_usage([dict(other_rows[0], REG_YMD="20260202")], other_day, spark=spark,
               captured_at=datetime(2026, 2, 3, 6), run_id="fixed-revision")
    started = time.perf_counter()
    run_usage(spark, other_day)
    quality_recovery_seconds = time.perf_counter() - started
    quality_recovered = state(spark, other_day)
    for key in ["silver", "gold", "line_gold"]:
        check(quality_recovered[key]["business_sha256"] == quality_before[key]["business_sha256"],
              "source correction restored " + key)
    quality = {"failure_boundary": "latest source count is negative, rejected before Silver write",
               "before": quality_before, "after_failure": quality_failed, "recovered": quality_recovered,
               "recovery_processing_seconds": round(quality_recovery_seconds, 6), "verified": True}

    snapshots = json.loads((ROOT / "fixtures/arrival.json").read_text(encoding="utf-8"))
    for snapshot in snapshots:
        ts = datetime.fromisoformat(snapshot["snapshot_ts"])
        observations = [dict(r, recptnDt=ts.isoformat(sep=" ")) for r in snapshot["rows"]]
        for _ in range(2):
            save_arrival(observations, ts, spark=spark)
    build_arrival_silver(spark, datetime(2026, 2, 1, 0, 10))
    return {"schema_version": 1, "data_kind": "synthetic", "verified": True,
            "interrupted_processing": interrupted, "source_quality_failure": quality,
            "limitations": ["Injected exceptions, not a killed process or Kubernetes outage",
                            "Small local fixture timings exclude Spark startup and verification reads",
                            "No measured Airflow queue delay, S3 recovery or production SLA"]}



def render_recovery(result):
    lines = ["# 장애·복구 실험", "",
             "> 합성 fixture와 실제 로컬 Delta 테이블로 실행한 예외 주입 실험입니다.", "",
             "검증 성공: " + str(result["verified"]), ""]
    for key, title in [("interrupted_processing", "Silver 저장 직후 중단"),
                       ("source_quality_failure", "잘못된 최신 원천 값 차단")]:
        case = result[key]
        lines += [f"## {title}", "", f"중단 지점: {case['failure_boundary']}", "",
                  "| 단계 | Silver 합계 | Gold 합계 | Silver 버전 | Gold 버전 |",
                  "|---|---:|---:|---:|---:|"]
        for phase in ["before", "after_failure", "recovered", "replayed"]:
            if phase in case:
                state = case[phase]
                lines.append(f"| {phase} | {state['silver']['total_count']} | {state['gold']['total_count']} | "
                             f"{state['silver']['delta_version']} | {state['gold']['delta_version']} |")
        lines += ["", f"복구 처리 시간: {case['recovery_processing_seconds']:.3f}초", ""]
    lines += ["## 검증 의미", "",
              "- 중단된 처리의 기존 Gold를 유지하고 저장된 Bronze로 정정 결과를 복구했습니다.",
              "- 같은 원본의 반복 재처리에서 업무 컬럼 전체의 SHA-256이 유지됐습니다.",
              "- 품질 오류를 올바른 원천 정정으로 해결했고 이전 정상 결과가 복원됐습니다.",
              "- Delta 버전은 테이블 전체의 커밋 버전입니다. 파일명·버전은 재처리로 바뀔 수 있습니다.", "",
              "## 측정 한계", "",
              "- 실제 프로세스 종료·S3 장애·Kubernetes 복구를 시험한 결과는 아닙니다.",
              "- 시간은 소량 로컬 fixture 처리의 측정값이며 Spark 시작·검증 읽기를 제외합니다.",
              "- 실제 운영 RTO·SLA·pool 대기 시간으로 사용하지 않습니다.", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    output = args.output_dir.resolve()
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise ValueError("Recovery experiment requires a new/empty directory; existing data is preserved")
    output.mkdir(parents=True, exist_ok=True)
    os.environ["SSDR_DATA_ROOT"] = str(output / "lake")
    os.environ["SSDR_STORAGE_PREFIX"] = "v2"
    spark = local_spark("ssdr-recovery-experiment")
    try:
        result = run_experiment(spark)
    finally:
        spark.stop()
    audit = audit_lake(DeltaAuditReader(output / "lake"), "20260128",
                      datetime(2026, 1, 31, 23, 50), datetime(2026, 2, 1, 0, 10), datetime(2026, 2, 1, 0, 20))
    audit["data_kind"] = "synthetic"
    check(audit["usage"]["silver_gold_business_match"], "audit reconciliation")
    check(audit["arrival"]["missing_time_buckets"] == 1, "intentional unobserved bucket")
    check(audit["arrival"]["missing_eta_rows"] == 1 and audit["arrival"]["silver_rows"] == 6, "ETA denominator")
    check(audit["status"] == "degraded", "audit warning classification")
    save_audit(audit, output / "audit")
    (output / "recovery.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False),
                                         encoding="utf-8")
    (output / "recovery.md").write_text(render_recovery(result), encoding="utf-8")
    print(json.dumps({"verified": result["verified"], "audit_status": audit["status"],
                      "output": str(output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
