from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from pyspark.sql import functions as F

# ---------------- 공통 경로 세팅 ----------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs.spark_common import build_spark, get_bucket, s3a
from src.common.s3_latest import find_latest_parquet_key

BRONZE_PREFIX = "bronze/subway_usage/"
SILVER_PREFIX = "silver/fact_subway_usage/"


def parse_ymd_from_key(key: str) -> tuple[str, str, str]:
    """
    s3 key 문자열에서 year/month/day 파티션을 안전하게 파싱한다.
    예: bronze/subway_usage/year=2025/month=12/day=16/usage.parquet
    """
    m = re.search(r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})", key)
    if not m:
        raise ValueError(f"Cannot parse year/month/day from key: {key}")

    y = m.group(1)
    mo = m.group(2).zfill(2)
    d = m.group(3).zfill(2)
    return y, mo, d


def main():
    bucket = get_bucket()
    region = os.getenv("AWS_REGION")

    latest_key = find_latest_parquet_key(bucket, BRONZE_PREFIX, region=region)
    if not latest_key:
        raise RuntimeError(f"No parquet found under s3://{bucket}/{BRONZE_PREFIX}")

    y, m, d = parse_ymd_from_key(latest_key)

    in_path = s3a(bucket, latest_key)
    out_prefix = f"{SILVER_PREFIX}year={y}/month={m}/day={d}"
    out_path = s3a(bucket, out_prefix)

    print(f"[DEBUG] bucket={bucket} region={region}")
    print(f"[DEBUG] latest_key={latest_key}")
    print(f"[DEBUG] in_path={in_path}")
    print(f"[DEBUG] out_path={out_path}")

    spark = build_spark(f"ssdr_fact_usage_{y}{m}{d}")
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(in_path)

    # ---- 변환 (pandas 로직 Spark 이식)
    fact = (
        df
        .withColumn("use_ymd", F.col("USE_YMD").cast("string"))
        .withColumn("use_date", F.to_date(F.col("USE_YMD").cast("string"), "yyyyMMdd"))
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
        )
    )

    # (선택) 결과가 비었는지 빠르게 체크하고 싶으면 주석 해제
    # print("[DEBUG] fact count =", fact.count())

    fact.write.mode("overwrite").parquet(out_path)

    print(f"[OK] latest bronze: s3://{bucket}/{latest_key}")
    print(f"[OK] wrote silver:  s3://{bucket}/{out_prefix}")

    spark.stop()


if __name__ == "__main__":
    main()
