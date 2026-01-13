from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------- 공통 경로 세팅 ----------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# ------------------------------------

from src.jobs.spark_common import build_spark, get_bucket, s3a

SILVER_PREFIX = "silver/fact_subway_arrival_delta/"
GOLD_PREFIX = "gold/subway_arrival/"


def get_date_parts(target_ymd: str) -> tuple[str, str, str]:
    return target_ymd[:4], target_ymd[4:6], target_ymd[6:8]


def parse_ymdh_from_key(key: str) -> tuple[str, str, str, str]:
    m = re.search(r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})/hour=(\d{1,2})", key)
    if not m:
        raise ValueError(f"Cannot parse year/month/day/hour from key: {key}")
    y = m.group(1)
    mo = m.group(2).zfill(2)
    d = m.group(3).zfill(2)
    h = m.group(4).zfill(2)
    return y, mo, d, h


def main():
    # 오늘 날짜(로컬 기준)
    target_ymd = datetime.today().strftime("%Y%m%d")
    y, m, d = get_date_parts(target_ymd)

    bucket = get_bucket()
    region = os.getenv("AWS_REGION")

    print(f"[INFO] Building GOLD subway_arrival for {target_ymd}")
    print(f"[DEBUG] bucket={bucket} region={region}")

    # SILVER 입력: hour=* 전체
    in_prefix = f"{SILVER_PREFIX}year={y}/month={m}/day={d}/"
    silver_root = s3a(bucket, in_prefix)

    # GOLD 출력
    events_prefix = f"{GOLD_PREFIX}events/year={y}/month={m}/day={d}"
    kpi_prefix = f"{GOLD_PREFIX}station_daily_kpi/year={y}/month={m}/day={d}"
    events_path = s3a(bucket, events_prefix)
    kpi_path = s3a(bucket, kpi_prefix)

    spark = build_spark(f"ssdr_gold_arrival_{target_ymd}")
    spark.sparkContext.setLogLevel("WARN")

    # 1) SILVER 로드 (해당 날짜의 hour 파티션 모두 읽기)
    df = (
        spark.read.format("delta").load(silver_root)
        .where((F.col("year")==int(y)) & (F.col("month")==int(m)) & (F.col("day")==int(d)))
    )

    # 2) arrival_events 만들기 (pandas build_arrival_events 이식)
    # event_dt = event_time(=recptn_dt) 우선, 없으면 recptn_dt, 없으면 snapshot_ts
    event_dt_col = F.coalesce(
        F.to_timestamp("event_time"),
        F.to_timestamp("recptn_dt"),
        F.to_timestamp("snapshot_ts"),
    )

    events = (
        df
        .withColumn("event_dt", event_dt_col)
        .withColumn("event_date", F.to_date("event_dt"))
        .withColumn("event_hour", F.hour("event_dt"))

        .withColumn("wait_sec", F.coalesce(F.col("arrival_seconds").cast("long"), F.lit(0)))
        .withColumn("is_long_wait_5", F.when(F.col("wait_sec") >= 300, F.lit(1)).otherwise(F.lit(0)))

        .withColumn("is_last_train", F.coalesce(F.col("is_last_train").cast("boolean"), F.lit(False)))
        .withColumn("is_last_train_flag", F.when(F.col("is_last_train") == True, F.lit(1)).otherwise(F.lit(0)))

        .withColumn("train_status", F.col("train_status").cast("string"))
        .withColumn(
            "is_express",
            F.when(F.col("train_status").isin(["급행", "ITX", "특급"]), F.lit(1)).otherwise(F.lit(0))
        )

        .withColumn("arrival_status_code", F.coalesce(F.col("arrival_status_code").cast("long"), F.lit(-1)))

        .withColumn(
            "arrival_phase",
            F.when(F.col("arrival_status_code").isin([0, 3, 4, 5]), F.lit("approach"))
             .when(F.col("arrival_status_code") == 1, F.lit("at_platform"))
             .when(F.col("arrival_status_code") == 2, F.lit("depart"))
             .otherwise(F.lit("other"))
        )

        .withColumn(
            "is_late_night",
            F.when(F.col("event_hour").isin([22, 23, 0, 1]), F.lit(1)).otherwise(F.lit(0))
        )

        # 타입 통일(원 코드와 맞추기)
        .withColumn("station_id", F.col("station_id").cast("long"))
        .withColumn("line_id", F.col("line_id").cast("long"))
        .withColumn("prev_station_id", F.col("prev_station_id").cast("long"))
        .withColumn("next_station_id", F.col("next_station_id").cast("long"))
        .withColumn("dest_station_id", F.col("dest_station_id").cast("long"))

        .withColumn("station_name", F.col("station_name").cast("string"))
        .withColumn("dest_station_name", F.col("dest_station_name").cast("string"))
        .withColumn("updn_line", F.col("updn_line").cast("string"))
        .withColumn("train_line_name", F.col("train_line_name").cast("string"))
        .withColumn("train_no", F.col("train_no").cast("string"))
    )

    # 3) station_daily_kpi 만들기 (pandas groupby/merge 이식)
    group_cols = ["event_date", "line_id", "station_id", "station_name"]

    # p95: percentile_approx 사용 (대용량에서 표준)
    agg = (
        events
        .groupBy(*group_cols)
        .agg(
            F.count(F.lit(1)).alias("total_events"),
            F.avg("wait_sec").alias("avg_wait_sec"),
            F.expr("percentile_approx(wait_sec, 0.95)").alias("p95_wait_sec"),
            F.avg("is_long_wait_5").alias("long_wait_ratio_5"),
            F.avg("is_express").alias("express_ratio"),
            F.sum("is_late_night").alias("late_night_events"),
        )
    )

    # 막차 정보: is_last_train=True인 것만
    last_train = (
        events
        .filter(F.col("is_last_train") == True)
        .groupBy(*group_cols)
        .agg(
            F.max("event_dt").alias("last_train_dt"),
            F.avg("wait_sec").alias("last_train_wait_sec"),
        )
    )

    kpi = (
        agg
        .join(last_train, on=group_cols, how="left")
    )

    # 4) 쓰기 (S3)
    # overwrite로 해당 day 파티션을 갱신
    events.write.mode("overwrite").parquet(events_path)
    kpi.write.mode("overwrite").parquet(kpi_path)

    print(f"[OK] read silver prefix: s3://{bucket}/{in_prefix}")
    print(f"[OK] wrote events:       s3://{bucket}/{events_prefix}")
    print(f"[OK] wrote station_kpi:  s3://{bucket}/{kpi_prefix}")

    spark.stop()


if __name__ == "__main__":
    main()
