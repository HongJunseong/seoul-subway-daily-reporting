from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import functions as F

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs.spark_common import build_spark, get_bucket, s3a
from src.common.s3_latest import find_latest_parquet_key, parse_partition_from_key

BRONZE_PREFIX = "bronze/subway_position/"
SILVER_PREFIX = "silver/fact_subway_position/"


def main():
    bucket = get_bucket()
    region = os.getenv("AWS_REGION")  # None 가능

    latest_key = find_latest_parquet_key(bucket, BRONZE_PREFIX, region=region)
    if not latest_key:
        raise RuntimeError(f"No parquet found under s3://{bucket}/{BRONZE_PREFIX}")

    y, m, d, h = parse_partition_from_key(latest_key, has_hour=True)

    # ✅ 변경 핵심: 최신 "단일 파일"이 아니라, 해당 hour 폴더 전체를 읽는다
    in_prefix = f"{BRONZE_PREFIX}year={y}/month={m}/day={d}/hour={h}/"
    in_path = s3a(bucket, in_prefix)

    out_prefix = f"{SILVER_PREFIX}year={y}/month={m}/day={d}/hour={h}"
    out_path = s3a(bucket, out_prefix)

    print(f"[DEBUG] bucket={bucket} region={region}")
    print(f"[DEBUG] latest_key={latest_key}")
    print(f"[DEBUG] in_path(prefix)={in_path}")
    print(f"[DEBUG] out_path={out_path}")

    spark = build_spark(f"ssdr_fact_position_{y}{m}{d}_{h}")
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(in_path)

    fact = (
        df
        .withColumn("snapshot_ts", F.to_timestamp("snapshot_ts"))
        .withColumn("line_id", F.col("subwayId").cast("long"))
        .withColumn("line_name", F.col("subwayNm").cast("string"))
        .withColumn("station_id", F.col("statnId").cast("long"))
        .withColumn("station_name", F.col("statnNm").cast("string"))
        .withColumn("updn_line", F.col("updnLine").cast("long"))
        .withColumn("train_no", F.col("trainNo").cast("string"))
        .withColumn("dest_station_id", F.col("statnTid").cast("long"))
        .withColumn("dest_station_name", F.col("statnTnm").cast("string"))
        .withColumn("train_status_code", F.col("trainSttus").cast("long"))
        .withColumn("is_express", F.col("directAt").cast("string").isin(["1", "7"]))
        .withColumn(
            "is_last_train",
            F.when(F.col("lstcarAt").cast("string") == F.lit("1"), F.lit(True))
             .when(F.col("lstcarAt").cast("string") == F.lit("0"), F.lit(False))
             .otherwise(F.lit(None).cast("boolean"))
        )
        .withColumn(
            "last_recptn_dt",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.coalesce(F.col("lastRecptnDt").cast("string"), F.lit("")),
                    F.coalesce(F.col("recptnDt").cast("string"), F.lit("")),
                )
            )
        )
        .select(
            "snapshot_ts",
            "line_id", "line_name",
            "station_id", "station_name",
            "updn_line",
            "train_no",
            "dest_station_id", "dest_station_name",
            "train_status_code",
            "is_express", "is_last_train",
            "last_recptn_dt",
        )
    )

    # ✅ hour fact를 overwrite로 확정(재실행 안정)
    fact.write.mode("overwrite").parquet(out_path)

    print(f"[OK] read bronze hour prefix: s3://{bucket}/{in_prefix}")
    print(f"[OK] wrote silver hour:       s3://{bucket}/{out_prefix}")

    spark.stop()


if __name__ == "__main__":
    main()
