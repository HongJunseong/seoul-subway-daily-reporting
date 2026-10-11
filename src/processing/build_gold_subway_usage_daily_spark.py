"""Publish date-scoped usage aggregates as atomic Delta partitions."""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F
from src.common.dates import date_parts, validate_ymd
from src.common.storage import table_path
from src.jobs.spark_common import build_spark
from src.quality.checks import require_nonempty, require_unique

STATION_PREFIX = "gold/subway_usage/station_daily"
LINE_PREFIX = "gold/subway_usage/line_daily"


def build_station_daily(df):
    require_unique(df, ["use_ymd", "line_name", "station_name"], "usage Gold input")
    return (df.groupBy("use_ymd", "line_name", "station_name", "year", "month", "day")
            .agg(F.sum("ride_count").alias("ride_count"), F.sum("alight_count").alias("alight_count"),
                 F.sum("total_count").alias("total_count")))


def build_line_daily(df):
    return (df.groupBy("use_ymd", "line_name", "year", "month", "day")
            .agg(F.sum("ride_count").alias("ride_count"), F.sum("alight_count").alias("alight_count"),
                 F.sum("total_count").alias("total_count"), F.count("*").alias("station_count")))


def build_usage_gold(spark, use_ymd):
    y, m, d = date_parts(use_ymd)
    predicate = f"year='{y}' AND month='{m}' AND day='{d}'"
    df = spark.read.format("delta").load(table_path("silver/fact_subway_usage_delta")).where(predicate)
    require_nonempty(df, "usage Silver")
    station, line = build_station_daily(df), build_line_daily(df)
    for frame, prefix in [(station, STATION_PREFIX), (line, LINE_PREFIX)]:
        (frame.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
         .partitionBy("year", "month", "day").save(table_path(prefix)))
    return station, line


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("use_ymd", type=validate_ymd)
    args = parser.parse_args()
    spark = build_spark("usage-gold")
    try:
        station, line = build_usage_gold(spark, args.use_ymd)
        print(f"[OK] usage Gold station_rows={station.count()} line_rows={line.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
