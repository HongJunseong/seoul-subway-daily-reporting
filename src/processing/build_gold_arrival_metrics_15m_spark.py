# src/processing/build_gold_arrival_metrics_15m_spark.py (Gold)

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
import argparse

from pyspark.sql import functions as F
import pendulum

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs.spark_common import build_spark, get_bucket, s3a

SILVER_PREFIX = "silver/fact_subway_arrival_delta/"
GOLD_PREFIX   = "gold/arrival_metrics_15m_delta/"

WINDOW_MINUTES = 15
MIN_N = 20


def _floor_to_5min(ts: datetime) -> datetime:
    minute = (ts.minute // 5) * 5
    return ts.replace(minute=minute, second=0, microsecond=0)


def main():
    parser = argparse.ArgumentParser()
    # Airflow에서 data_interval_end 넘길 거야. 없으면 fallback으로 KST now 사용.
    parser.add_argument("--as_of", type=str, default=None, help="KST 기준 'YYYY-MM-DD HH:MM:SS'")
    args = parser.parse_args()

    bucket = get_bucket()

    if args.as_of:
        # KST naive datetime
        now = datetime.strptime(args.as_of, "%Y-%m-%d %H:%M:%S")
    else:
        now = pendulum.now("Asia/Seoul").naive()

    window_end = _floor_to_5min(now)
    window_start = window_end - timedelta(minutes=WINDOW_MINUTES)

    # 파티션 키
    yi, mi, di, hi = window_end.year, window_end.month, window_end.day, window_end.hour

    # Silver는 해당 hour 파티션만 로드 (정확 + 빠름)
    silver_hour_prefix = f"{SILVER_PREFIX}year={yi}/month={mi:02d}/day={di:02d}/hour={hi:02d}/"
    silver_hour_path = s3a(bucket, silver_hour_prefix)

    gold_path = s3a(bucket, GOLD_PREFIX)

    print(f"[DEBUG] as_of={args.as_of} now={now}")
    print(f"[DEBUG] window_start={window_start} window_end={window_end}")
    print(f"[DEBUG] silver_hour_path={silver_hour_path}")
    print(f"[DEBUG] gold_path={gold_path}")

    spark = build_spark(f"ssdr_gold_arrival_15m_{window_end.strftime('%Y%m%d_%H%M')}")
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(silver_hour_path)

    # window 비교는 문자열 → timestamp로 확정
    ws_str = window_start.strftime("%Y-%m-%d %H:%M:%S")
    we_str = window_end.strftime("%Y-%m-%d %H:%M:%S")
    ws = F.to_timestamp(F.lit(ws_str), "yyyy-MM-dd HH:mm:ss")
    we = F.to_timestamp(F.lit(we_str), "yyyy-MM-dd HH:mm:ss")

    df2 = (
        df
        .withColumn("event_ts", F.coalesce(F.col("event_time"), F.col("snapshot_ts")).cast("timestamp"))
        .filter((F.col("event_ts") >= ws) & (F.col("event_ts") < we))
    )

    # window에 데이터가 없으면 write 스킵
    if df2.rdd.isEmpty():
        print("[WARN] rows_in_window=0 -> skip gold write")
        spark.stop()
        return

    metrics = (
        df2
        .groupBy("station_id", "station_name", "line_id", "updn_line")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.avg(F.col("arrival_seconds").cast("double")).alias("avg_eta_sec"),
            F.expr("percentile_approx(arrival_seconds, 0.90, 2000)").cast("long").alias("p90_eta_sec"),
            F.avg(F.when(F.col("arrival_seconds") >= F.lit(600), F.lit(1.0)).otherwise(F.lit(0.0))).alias("long_ratio_10m"),
            F.max(F.when(F.col("is_last_train") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).alias("last_train_seen_int"),
        )
        .withColumn("last_train_seen", (F.col("last_train_seen_int") == F.lit(1)).cast("boolean"))
        .drop("last_train_seen_int")
    )

    out = (
        metrics
        .withColumn("window_start_ts", F.to_timestamp(F.lit(ws_str), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("window_end_ts",   F.to_timestamp(F.lit(we_str), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("window_minutes",  F.lit(WINDOW_MINUTES))
        .withColumn("min_n",           F.lit(MIN_N))
        .withColumn("year",  F.lit(yi))
        .withColumn("month", F.lit(mi))
        .withColumn("day",   F.lit(di))
        .withColumn("hour",  F.lit(hi))
        .withColumn("window_end_ymdhm", F.lit(window_end.strftime("%Y%m%d%H%M")).cast("string"))
    )

    (
        out.write
           .format("delta")
           .mode("overwrite")
           .option(
               "replaceWhere",
               f"year={yi} AND month={mi} AND day={di} AND hour={hi} AND window_end_ymdhm='{window_end.strftime('%Y%m%d%H%M')}'"
           )
           .partitionBy("year", "month", "day", "hour", "window_end_ymdhm")
           .save(gold_path)
    )

    total_groups = out.count()
    print(f"[OK] wrote gold groups={total_groups} -> s3://{bucket}/{GOLD_PREFIX}")
    print(f"[OK] partition: year={yi}, month={mi}, day={di}, hour={hi}, window_end_ymdhm={window_end.strftime('%Y%m%d%H%M')}")

    spark.stop()


if __name__ == "__main__":
    main()
