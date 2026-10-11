"""15-minute observation metrics, using all intersecting snapshot partitions."""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F
from src.common.dates import parse_as_of, window_bounds, hour_starts
from src.common.storage import table_path
from src.jobs.spark_common import build_spark
from src.processing.build_fact_subway_arrival_spark import partition_predicate
from src.quality.checks import require_nonempty, DataQualityError

SILVER_PREFIX = "silver/fact_subway_arrival_delta"
GOLD_PREFIX = "gold/arrival_metrics_15m_delta"


def aggregate_metrics(df, start, end):
    # Parse wall-clock boundaries in Spark's explicit KST session timezone.
    # F.lit(naive_datetime) instead uses the Python host timezone.
    start_ts = F.to_timestamp(F.lit(start.isoformat()))
    end_ts = F.to_timestamp(F.lit(end.isoformat()))
    observations = df.where((F.col("snapshot_ts") >= start_ts) & (F.col("snapshot_ts") < end_ts))
    require_nonempty(observations, "arrival window")
    if observations.where(F.col("arrival_seconds").isNotNull()).isEmpty():
        raise DataQualityError("arrival window has no valid ETA measurements")
    metrics = (observations.groupBy("station_id", "station_name", "line_id", "updn_line")
               .agg(F.count("*").alias("n"), F.count("arrival_seconds").alias("valid_eta_n"),
                    F.countDistinct("snapshot_ts").alias("snapshot_count"),
                    F.avg("arrival_seconds").alias("avg_eta_sec"),
                    F.expr("percentile_approx(arrival_seconds, 0.90, 2000)").alias("p90_eta_sec"),
                    F.avg(F.when(F.col("arrival_seconds").isNotNull(),
                                 (F.col("arrival_seconds") >= 600).cast("double"))).alias("long_ratio_10m"),
                    F.max("source_age_sec").alias("max_source_age_sec")))
    return (metrics.withColumn("window_start_ts", start_ts).withColumn("window_end_ts", end_ts)
            .withColumn("year", F.lit(end.strftime("%Y"))).withColumn("month", F.lit(end.strftime("%m")))
            .withColumn("day", F.lit(end.strftime("%d"))).withColumn("hour", F.lit(end.strftime("%H")))
            .withColumn("window_end_ymdhm", F.lit(end.strftime("%Y%m%d%H%M"))))


def build_arrival_metrics(spark, as_of):
    start, end = window_bounds(as_of)
    df = (spark.read.format("delta").load(table_path(SILVER_PREFIX))
          .where(partition_predicate(hour_starts(start, end))))
    metrics = aggregate_metrics(df, start, end)
    predicate = (f"year='{end:%Y}' AND month='{end:%m}' AND day='{end:%d}' "
                 f"AND hour='{end:%H}' AND window_end_ymdhm='{end:%Y%m%d%H%M}'")
    (metrics.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
     .partitionBy("year", "month", "day", "hour", "window_end_ymdhm").save(table_path(GOLD_PREFIX)))
    return metrics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--as_of", required=True, type=parse_as_of)
    args = parser.parse_args()
    spark = build_spark("arrival-gold-15m")
    try:
        print(f"[OK] arrival metrics groups={build_arrival_metrics(spark, args.as_of).count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
