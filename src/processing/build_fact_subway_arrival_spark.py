from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import functions as F

# ---------------- 공통 경로 세팅 ----------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.jobs.spark_common import build_spark, get_bucket, s3a
from src.common.s3_latest import find_latest_parquet_key, parse_partition_from_key

BRONZE_PREFIX = "bronze/subway_arrival/"
SILVER_PREFIX = "silver/fact_subway_arrival/"


def main():
    bucket = get_bucket()
    region = os.getenv("AWS_REGION")  # None이면 내부에서 기본 체인 사용

    # 1) 최신 bronze key로부터 (y,m,d,h)만 얻는다
    latest_key = find_latest_parquet_key(bucket, BRONZE_PREFIX, region=region)
    if not latest_key:
        raise RuntimeError(f"No parquet found under s3://{bucket}/{BRONZE_PREFIX}")

    y, m, d, h = parse_partition_from_key(latest_key, has_hour=True)

    # 2) ✅ 변경 핵심: 최신 "단일 파일"이 아니라, 해당 hour 폴더 전체를 읽는다
    #    bronze가 minute/run_ts로 누적되면 hour 아래에 파일이 계속 쌓이므로, prefix 전체를 읽어야 hour 누적이 된다.
    in_prefix = f"{BRONZE_PREFIX}year={y}/month={m}/day={d}/hour={h}/"
    in_path = s3a(bucket, in_prefix)

    # 3) 출력은 hour 단위 fact를 overwrite로 확정
    out_prefix = f"{SILVER_PREFIX}year={y}/month={m}/day={d}/hour={h}"
    out_path = s3a(bucket, out_prefix)

    print(f"[DEBUG] bucket={bucket} region={region}")
    print(f"[DEBUG] latest_key={latest_key}")
    print(f"[DEBUG] in_path(prefix)={in_path}")
    print(f"[DEBUG] out_path={out_path}")

    spark = build_spark(f"ssdr_fact_arrival_{y}{m}{d}_{h}")
    spark.sparkContext.setLogLevel("WARN")

    # ---- read (hour prefix 전체)
    df = spark.read.parquet(in_path)

    # ---- 변환
    fact = (
        df
        .withColumn("snapshot_ts", F.to_timestamp("snapshot_ts"))
        .withColumn("recptn_dt", F.to_timestamp("recptnDt"))
        .withColumn("event_time", F.col("recptn_dt"))
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
        .select(
            "snapshot_ts","recptn_dt","event_time",
            "station_id","station_name","line_id","updn_line","train_line_name",
            "prev_station_id","next_station_id","dest_station_id","dest_station_name",
            "train_no","train_status",
            "arrival_seconds","arrival_status_code","arrival_msg_primary","arrival_msg_secondary",
            "is_last_train",
        )
    )

    # ---- write: hour fact를 overwrite로 확정(재실행해도 동일 결과 → 안정)
    fact.write.mode("overwrite").parquet(out_path)

    print(f"[OK] read bronze hour prefix: s3://{bucket}/{in_prefix}")
    print(f"[OK] wrote silver hour:        s3://{bucket}/{out_prefix}")

    spark.stop()


if __name__ == "__main__":
    main()
