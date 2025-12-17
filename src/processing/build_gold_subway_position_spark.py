# src/gold/build_gold_subway_position_spark.py
from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime

from pyspark.sql import functions as F

# ---------------- 공통 경로 세팅 ----------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# ------------------------------------------------

from src.jobs.spark_common import build_spark, get_bucket, s3a


SILVER_PREFIX = "silver/fact_subway_position/"
GOLD_EVENTS_PREFIX = "gold/subway_position/events/"
GOLD_LINE_HOURLY_PREFIX = "gold/subway_position/line_hourly/"


def get_ymd(target_ymd: str) -> tuple[str, str, str]:
    return target_ymd[:4], target_ymd[4:6], target_ymd[6:8]


def main():
    # 기본: 오늘 날짜 / 필요하면 환경변수로 지정 가능
    target_ymd = os.getenv("TARGET_YMD") or datetime.today().strftime("%Y%m%d")
    y, m, d = get_ymd(target_ymd)

    bucket = get_bucket()

    # Silver 입력: hour 파티션 전체 읽기 (디렉토리 parquet)
    in_prefix = f"{SILVER_PREFIX}year={y}/month={m}/day={d}/hour=*/"
    in_path = s3a(bucket, in_prefix)

    # Gold 출력 경로
    out_events_prefix = f"{GOLD_EVENTS_PREFIX}year={y}/month={m}/day={d}"
    out_line_hourly_prefix = f"{GOLD_LINE_HOURLY_PREFIX}year={y}/month={m}/day={d}"

    out_events_path = s3a(bucket, out_events_prefix)
    out_line_hourly_path = s3a(bucket, out_line_hourly_prefix)

    print(f"[DEBUG] target_ymd={target_ymd}")
    print(f"[DEBUG] in_path={in_path}")
    print(f"[DEBUG] out_events_path={out_events_path}")
    print(f"[DEBUG] out_line_hourly_path={out_line_hourly_path}")

    spark = build_spark(f"ssdr_gold_position_{target_ymd}")

    df = spark.read.parquet(in_path)

    if df.rdd.isEmpty():
        print("[WARN] SILVER fact_subway_position is empty, skip.")
        spark.stop()
        return

    # ------------------------------------------------------------
    # 1) GOLD position_events
    # event_dt = last_recptn_dt 우선, 없으면 snapshot_ts
    # ------------------------------------------------------------
    events = (
        df
        .withColumn("snapshot_ts", F.to_timestamp("snapshot_ts"))
        .withColumn("last_recptn_dt", F.to_timestamp("last_recptn_dt"))
        .withColumn("event_dt", F.coalesce(F.col("last_recptn_dt"), F.col("snapshot_ts")))
        .withColumn("event_date", F.to_date("event_dt"))
        .withColumn("event_hour", F.hour("event_dt"))

        # 타입 정리
        .withColumn("line_id", F.col("line_id").cast("long"))
        .withColumn("station_id", F.col("station_id").cast("long"))
        .withColumn("dest_station_id", F.col("dest_station_id").cast("long"))
        .withColumn("updn_line", F.col("updn_line").cast("long"))
        .withColumn("train_status_code", F.col("train_status_code").cast("long"))

        .withColumn("line_name", F.col("line_name").cast("string"))
        .withColumn("station_name", F.col("station_name").cast("string"))
        .withColumn("dest_station_name", F.col("dest_station_name").cast("string"))
        .withColumn("train_no", F.col("train_no").cast("string"))

        # bool 정리 (null -> false)
        .withColumn("is_express", F.coalesce(F.col("is_express").cast("boolean"), F.lit(False)))
        .withColumn("is_last_train", F.coalesce(F.col("is_last_train").cast("boolean"), F.lit(False)))
    )

    # ------------------------------------------------------------
    # 2) GOLD line_hourly KPI
    # ------------------------------------------------------------
    group_cols = ["event_date", "event_hour", "line_id", "line_name", "updn_line"]

    line_hourly = (
        events
        .groupBy(*group_cols)
        .agg(
            F.countDistinct("train_no").alias("active_trains"),
            F.sum(F.when(F.col("is_express") == True, F.lit(1)).otherwise(F.lit(0))).alias("express_trains"),
            F.max("event_dt").alias("last_event_dt"),
            F.max(F.when(F.col("is_last_train") == True, F.lit(1)).otherwise(F.lit(0))).alias("last_train_flag"),
        )
    )

    # 저장 (작게 쌓이는 데이터면 1파일이 보기 편함)
    events.repartition(1).write.mode("overwrite").parquet(out_events_path)
    line_hourly.repartition(1).write.mode("overwrite").parquet(out_line_hourly_path)

    print(f"[OK] read silver: s3://{bucket}/{in_prefix}")
    print(f"[OK] wrote gold events: s3://{bucket}/{out_events_prefix}")
    print(f"[OK] wrote gold line_hourly: s3://{bucket}/{out_line_hourly_prefix}")

    spark.stop()


if __name__ == "__main__":
    main()
