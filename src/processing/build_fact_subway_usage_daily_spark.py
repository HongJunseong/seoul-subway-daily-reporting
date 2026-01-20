from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import functions as F
from src.jobs.spark_common import build_spark, get_bucket, s3a

BRONZE_DELTA_PREFIX = "bronze/subway_usage_delta"          # delta table root
SILVER_DELTA_PREFIX = "silver/fact_subway_usage_delta"     # delta table root


def ymd_parts(use_ymd: str) -> tuple[str, str, str]:
    y = use_ymd[0:4]
    m = use_ymd[4:6]
    d = use_ymd[6:8]
    return y, m, d


def main():
    if len(sys.argv) < 2:
        raise RuntimeError("Usage: spark-submit ... build_fact_subway_usage_daily_spark.py YYYYMMDD")

    use_ymd = sys.argv[1]
    y, m, d = ymd_parts(use_ymd)

    bucket = get_bucket()
    region = os.getenv("AWS_REGION")

    bronze_path = s3a(bucket, BRONZE_DELTA_PREFIX)
    silver_path = s3a(bucket, SILVER_DELTA_PREFIX)

    print(f"[DEBUG] bucket={bucket} region={region}")
    print(f"[DEBUG] use_ymd={use_ymd}")
    print(f"[DEBUG] bronze_delta={bronze_path}")
    print(f"[DEBUG] silver_delta={silver_path}")

    spark = build_spark(f"ssdr_fact_usage_{use_ymd}")
    spark.sparkContext.setLogLevel("WARN")

    # Bronze Delta 읽기 + 파티션만 필터링
    df = (
        spark.read.format("delta").load(bronze_path)
        .where((F.col("year") == y) & (F.col("month") == m) & (F.col("day") == d))
    )

    # 변환
    fact = (
        df
        .withColumn("use_ymd", F.lit(use_ymd))
        .withColumn("use_date", F.to_date(F.lit(use_ymd), "yyyyMMdd"))
        .withColumn("reg_date", F.to_date(F.col("REG_YMD").cast("string"), "yyyyMMdd"))

        .withColumn("line_name", F.col("SBWY_ROUT_LN_NM").cast("string"))
        .withColumn("station_name", F.col("SBWY_STNS_NM").cast("string"))

        .withColumn("ride_count", F.coalesce(F.col("GTON_TNOPE").cast("long"), F.lit(0)))
        .withColumn("alight_count", F.coalesce(F.col("GTOFF_TNOPE").cast("long"), F.lit(0)))
        .withColumn("total_count", (F.col("ride_count") + F.col("alight_count")).cast("long"))

        .select(
            "use_ymd", "use_date", "reg_date",
            "line_name", "station_name",
            "ride_count", "alight_count", "total_count",
            # 필요하면 lineage용
            F.lit(y).alias("year"), F.lit(m).alias("month"), F.lit(d).alias("day"),
        )
    )

    # Silver Delta에 파티션 단위 overwrite (해당 날짜만 교체)
    (
        fact.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"year = '{y}' AND month = '{m}' AND day = '{d}'")
        .partitionBy("year", "month", "day")
        .save(silver_path)
    )

    print(f"[OK] read bronze delta partition: s3://{bucket}/{BRONZE_DELTA_PREFIX}/year={y}/month={m}/day={d}")
    print(f"[OK] wrote silver delta partition: s3://{bucket}/{SILVER_DELTA_PREFIX}/year={y}/month={m}/day={d}")

    spark.stop()


if __name__ == "__main__":
    main()
