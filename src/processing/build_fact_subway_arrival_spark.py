# src/processing/build_fact_subway_arrival_spark.py (Silver)

from __future__ import annotations

import os
import sys
from pathlib import Path
from pyspark.sql import functions as F

from src.jobs.spark_common import build_spark, get_bucket, s3a
from src.common.s3_latest import find_latest_parquet_key, parse_partition_from_key

BRONZE_PREFIX = "bronze/subway_arrival_delta/"
SILVER_PREFIX = "silver/fact_subway_arrival_delta/"


def _parse_recptn_dt(col):
    """
    recptnDt 포맷이 환경마다 달라서(공공 API가 종종 그럼) 안전하게 여러 포맷을 시도.
    아래 3개는 실무에서 가장 흔한 형태.
    """
    return F.coalesce(
        F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(col, "yyyyMMddHHmmss"),
    )


def main():
    bucket = get_bucket()
    region = os.getenv("AWS_REGION")

    latest_key = find_latest_parquet_key(bucket, BRONZE_PREFIX, region=region)
    if not latest_key:
        raise RuntimeError(f"No parquet found under s3://{bucket}/{BRONZE_PREFIX}")

    y, m, d, h = parse_partition_from_key(latest_key, has_hour=True)
    yi, mi, di, hi = int(y), int(m), int(d), int(h)

    # prefix 만들 때는 2자리로 통일 추천
    in_prefix = f"{BRONZE_PREFIX}year={yi}/month={mi:02d}/day={di:02d}/hour={hi:02d}/"
    in_path = s3a(bucket, in_prefix)
    out_path = s3a(bucket, SILVER_PREFIX)

    print(f"[DEBUG] latest_key={latest_key}")
    print(f"[DEBUG] in_path(prefix)={in_path}")
    print(f"[DEBUG] out_path(table_root)={out_path}")

    spark = build_spark(f"ssdr_fact_arrival_{yi}{mi:02d}{di:02d}_{hi:02d}")
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(in_path)

    # snapshot_ts도 포맷이 애매하면 여러 포맷 시도 가능하지만,
    # 일단 기본 cast + fallback(파케에 이미 timestamp면 그냥 통과)
    snapshot_ts = F.col("snapshot_ts").cast("timestamp")
    recptn_dt = _parse_recptn_dt(F.col("recptnDt"))

    fact = (
        df
        .withColumn("snapshot_ts", snapshot_ts)
        .withColumn("recptn_dt", recptn_dt)
        # event_time은 무조건 timestamp가 되게 보장
        .withColumn("event_time", F.coalesce(F.col("recptn_dt"), F.col("snapshot_ts")).cast("timestamp"))

        .withColumn("station_id", F.col("statnId").cast("long"))
        .withColumn("station_name", F.col("statnNm").cast("string"))
        .withColumn("line_id", F.col("subwayId").cast("long"))
        .withColumn("updn_line", F.col("updnLine").cast("string"))
        .withColumn("train_line_name", F.col("trainLineNm").cast("string"))
        .withColumn("prev_station_id", F.col("statnFid").cast("long"))
        .withColumn("next_station_id", F.col("statnTid").cast("long"))
        .withColumn("dest_station_id", F.col("bstatnId").cast("long"))
        .withColumn("dest_station_name", F.col("bstatnNm").cast("string"))
        .withColumn("train_no", F.col("btrainNo").cast("string"))
        .withColumn("train_status", F.col("btrainSttus").cast("string"))
        .withColumn("arrival_seconds", F.col("barvlDt").cast("long"))
        .withColumn("arrival_status_code", F.col("arvlCd").cast("long"))
        .withColumn("arrival_msg_primary", F.col("arvlMsg2").cast("string"))
        .withColumn("arrival_msg_secondary", F.col("arvlMsg3").cast("string"))
        .withColumn(
            "is_last_train",
            F.when(F.col("lstcarAt").cast("string") == F.lit("1"), F.lit(True))
             .when(F.col("lstcarAt").cast("string") == F.lit("0"), F.lit(False))
             .otherwise(F.lit(None).cast("boolean"))
        )
        .withColumn("year", F.lit(yi))
        .withColumn("month", F.lit(mi))
        .withColumn("day", F.lit(di))
        .withColumn("hour", F.lit(hi))
        .select(
            "snapshot_ts","recptn_dt","event_time",
            "station_id","station_name","line_id","updn_line","train_line_name",
            "prev_station_id","next_station_id","dest_station_id","dest_station_name",
            "train_no","train_status",
            "arrival_seconds","arrival_status_code","arrival_msg_primary","arrival_msg_secondary",
            "is_last_train",
            "year","month","day","hour",
        )
    )

    (
        fact.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"year={yi} AND month={mi} AND day={di} AND hour={hi}")
            .partitionBy("year", "month", "day", "hour")
            .save(out_path)
    )

    print(f"[OK] read bronze hour prefix: s3://{bucket}/{in_prefix}")
    print(f"[OK] wrote silver delta (replaceWhere): s3://{bucket}/{SILVER_PREFIX} (year={yi}, month={mi}, day={di}, hour={hi})")

    spark.stop()


if __name__ == "__main__":
    main()
