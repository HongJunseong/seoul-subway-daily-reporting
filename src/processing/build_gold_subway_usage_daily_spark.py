# src/processing/build_gold_subway_usage_daily_spark.py
from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.common.config import load_s3_config


# Utils
def ymd_parts(ymd: str) -> tuple[str, str, str]:
    return ymd[:4], ymd[4:6], ymd[6:8]


def pick_first_existing(df: DataFrame, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


# Spark (local + S3A 안정화)
def build_spark(app_name: str, region: Optional[str]) -> SparkSession:
    region = (region or "").strip() or "ap-northeast-2"

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        # S3 연결 안정화 (멈춤/재시도 완화)
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "10000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "20000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "5")
        .config("spark.hadoop.fs.s3a.retry.limit", "5")
        .config("spark.sql.shuffle.partitions", "16")
    )

    ak = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
    sk = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
    token = os.getenv("AWS_SESSION_TOKEN")

    if ak and sk:
        builder = (
            builder.config("spark.hadoop.fs.s3a.access.key", ak)
            .config("spark.hadoop.fs.s3a.secret.key", sk)
            # 로컬에서 provider chain 때문에 멈추는 케이스 방지
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )
        if token:
            builder = builder.config("spark.hadoop.fs.s3a.session.token", token)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Load SILVER
def load_silver(spark: SparkSession, bucket: str, ymd: str) -> Optional[DataFrame]:
    y, m, d = ymd_parts(ymd)
    day_dir = f"s3a://{bucket}/silver/fact_subway_usage_delta/year={y}/month={m}/day={d}/"
    print(f"[INFO] Load SILVER from: {day_dir}")

    try:
        df = spark.read.parquet(day_dir)
        print("[INFO] SILVER loaded successfully")
        return df
    except AnalysisException as e:
        print(f"[WARN] SILVER not found: {day_dir}")
        print(f"[WARN] {e}")
        return None


# Transform: Station Daily (rank 컬럼 제거 버전)
def build_station_daily(df: DataFrame) -> DataFrame:
    date_col = pick_first_existing(df, ["pasngYmd", "use_ymd"])
    stn_nm_col = pick_first_existing(df, ["stnNm", "station_name"])
    line_nm_col = pick_first_existing(df, ["lineNm", "line_name"])
    stn_cd_col = pick_first_existing(df, ["stnCd"])
    line_no_col = pick_first_existing(df, ["lineNo"])

    ride_col = pick_first_existing(df, ["rideNope", "ride_count"])
    gff_col = pick_first_existing(df, ["gffNope", "alight_count"])
    total_col = pick_first_existing(df, ["totalNope", "total_count"])

    if not all([date_col, stn_nm_col, line_nm_col, total_col]):
        raise RuntimeError(
            f"Required columns missing for station_daily: "
            f"date={date_col}, stn={stn_nm_col}, line={line_nm_col}, total={total_col}"
        )

    group_cols = [date_col, stn_nm_col, line_nm_col]
    if stn_cd_col:
        group_cols.append(stn_cd_col)
    if line_no_col:
        group_cols.append(line_no_col)

    agg_exprs = [F.sum(F.col(total_col)).alias("totalNope")]
    if ride_col:
        agg_exprs.append(F.sum(F.col(ride_col)).alias("rideNope"))
    if gff_col:
        agg_exprs.append(F.sum(F.col(gff_col)).alias("gffNope"))

    out = df.groupBy(*group_cols).agg(*agg_exprs)

    out = (
        out.withColumnRenamed(date_col, "pasngYmd")
        .withColumnRenamed(stn_nm_col, "stnNm")
        .withColumnRenamed(line_nm_col, "lineNm")
    )

    # optional cols 표준화
    if stn_cd_col:
        out = out.withColumnRenamed(stn_cd_col, "stnCd")
    else:
        out = out.withColumn("stnCd", F.lit(None).cast("string"))

    if line_no_col:
        out = out.withColumnRenamed(line_no_col, "lineNo")
    else:
        out = out.withColumn("lineNo", F.lit(None).cast("string"))

    return out


# Transform: Line Daily (rank 컬럼 제거 버전)
def build_line_daily(df: DataFrame) -> DataFrame:
    date_col = pick_first_existing(df, ["pasngYmd", "use_ymd"])
    line_nm_col = pick_first_existing(df, ["lineNm", "line_name"])
    line_no_col = pick_first_existing(df, ["lineNo"])

    ride_col = pick_first_existing(df, ["rideNope", "ride_count"])
    gff_col = pick_first_existing(df, ["gffNope", "alight_count"])
    total_col = pick_first_existing(df, ["totalNope", "total_count"])

    if not all([date_col, line_nm_col, total_col]):
        raise RuntimeError(
            f"Required columns missing for line_daily: "
            f"date={date_col}, line={line_nm_col}, total={total_col}"
        )

    group_cols = [date_col, line_nm_col]
    if line_no_col:
        group_cols.append(line_no_col)

    agg_exprs = [F.sum(F.col(total_col)).alias("totalNope")]
    if ride_col:
        agg_exprs.append(F.sum(F.col(ride_col)).alias("rideNope"))
    if gff_col:
        agg_exprs.append(F.sum(F.col(gff_col)).alias("gffNope"))

    out = df.groupBy(*group_cols).agg(*agg_exprs)

    out = (
        out.withColumnRenamed(date_col, "pasngYmd")
        .withColumnRenamed(line_nm_col, "lineNm")
    )

    if line_no_col:
        out = out.withColumnRenamed(line_no_col, "lineNo")
    else:
        out = out.withColumn("lineNo", F.lit(None).cast("string"))

    return out


# Save GOLD
def save_gold(df: DataFrame, bucket: str, ymd: str, category: str) -> str:
    y, m, d = ymd_parts(ymd)
    out_dir = f"s3a://{bucket}/gold/subway_usage/{category}/year={y}/month={m}/day={d}/"
    print(f"[INFO] Write GOLD -> {out_dir}")

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(out_dir)
    )

    print("[OK] GOLD write completed")
    return out_dir


# Main
def main() -> None:
    # D+3 정도 늦게 확정되는 데이터라 D-4를 기본으로 처리
    # target_ymd = (datetime.today() - timedelta(days=4)).strftime("%Y%m%d")
    if len(sys.argv) > 1:
        target_ymd = sys.argv[1]   # DAG가 준 TARGET_YMD 사용
    else:
        target_ymd = pendulum.now("Asia/Seoul").subtract(days=4).format("YYYYMMDD")

    print(f"[INFO] Building GOLD subway_usage for {target_ymd}")

    s3cfg = load_s3_config()
    bucket = getattr(s3cfg, "bucket", None)
    region = getattr(s3cfg, "region", None)

    if not bucket:
        raise RuntimeError("SSDR_S3_BUCKET 환경변수가 비어있습니다. (.env 또는 시스템 env에 설정)")

    spark = build_spark(app_name=f"ssdr_gold_usage_{target_ymd}", region=region)

    try:
        silver = load_silver(spark, bucket, target_ymd)
        if silver is None:
            print(f"[WARN] No SILVER usage data for {target_ymd}. Skip GOLD build.")
            return

        station_daily = build_station_daily(silver)
        save_gold(station_daily, bucket, target_ymd, "station_daily")

        line_daily = build_line_daily(silver)
        save_gold(line_daily, bucket, target_ymd, "line_daily")

    finally:
        try:
            spark.stop()
            print("[INFO] Spark stopped.")
        except Exception as e:
            print(f"[WARN] spark.stop failed: {e}")


if __name__ == "__main__":
    main()
