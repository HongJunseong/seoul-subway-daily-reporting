"""Daily ETA observation summary; never label sampled ETA as actual service delay."""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F
from src.common.dates import date_parts, validate_ymd
from src.common.storage import table_path
from src.jobs.spark_common import build_spark
from src.quality.checks import require_nonempty, DataQualityError

SILVER_PREFIX = "silver/fact_subway_arrival_delta"
KPI_PREFIX = "gold/subway_arrival/station_daily_kpi"


def build_daily_kpi(df):
    require_nonempty(df, "daily arrival Silver")
    if df.where(F.col("arrival_seconds").isNotNull()).isEmpty():
        raise DataQualityError("daily arrival has no valid ETA")
    return (df.groupBy("line_id", "station_id", "station_name", "year", "month", "day")
            .agg(F.count("*").alias("total_events"), F.count("arrival_seconds").alias("valid_eta_n"),
                 F.countDistinct("snapshot_ts").alias("snapshot_count"),
                 F.avg("arrival_seconds").alias("avg_eta_sec"),
                 F.expr("percentile_approx(arrival_seconds, 0.95, 2000)").alias("p95_eta_sec"),
                 F.avg(F.when(F.col("arrival_seconds").isNotNull(),
                              (F.col("arrival_seconds") >= 300).cast("double"))).alias("long_eta_ratio_5m"),
                 F.max(F.when(F.col("is_last_train"), F.col("snapshot_ts"))).alias("last_train_observed_ts"),
                 F.max("source_age_sec").alias("max_source_age_sec"),
                 F.min("snapshot_ts").alias("first_snapshot_ts"),
                 F.max("snapshot_ts").alias("last_snapshot_ts")))


def build_arrival_gold(spark, target_ymd):
    y, m, d = date_parts(target_ymd)
    predicate = f"year='{y}' AND month='{m}' AND day='{d}'"
    df = spark.read.format("delta").load(table_path(SILVER_PREFIX)).where(predicate)
    kpi = build_daily_kpi(df)
    (kpi.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
     .partitionBy("year", "month", "day").save(table_path(KPI_PREFIX)))
    return kpi


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("target_ymd", type=validate_ymd)
    args = parser.parse_args()
    spark = build_spark("arrival-gold-daily")
    try:
        print(f"[OK] arrival daily groups={build_arrival_gold(spark, args.target_ymd).count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
