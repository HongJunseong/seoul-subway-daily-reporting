"""Latest revision per (usage date, line, station), replayed from immutable Bronze."""
from __future__ import annotations
import argparse
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.common.dates import date_parts, validate_ymd
from src.common.storage import table_path
from src.jobs.spark_common import build_spark
from src.quality.checks import require_columns, require_nonempty, reject_rows, require_unique

BRONZE_DELTA_PREFIX = "bronze/subway_usage_delta"
SILVER_DELTA_PREFIX = "silver/fact_subway_usage_delta"
KEYS = ["use_ymd", "line_name", "station_name"]


def transform_usage(df, use_ymd: str):
    validate_ymd(use_ymd)
    require_columns(df, ["USE_YMD", "REG_YMD", "SBWY_ROUT_LN_NM", "SBWY_STNS_NM",
                         "GTON_TNOPE", "GTOFF_TNOPE", "ingested_at", "record_hash"])
    require_nonempty(df, "usage Bronze")
    prepared = (df.withColumn("use_ymd", F.col("USE_YMD").cast("string"))
                .withColumn("line_name", F.trim("SBWY_ROUT_LN_NM"))
                .withColumn("station_name", F.trim("SBWY_STNS_NM"))
                .withColumn("ride_count", F.col("GTON_TNOPE").cast("long"))
                .withColumn("alight_count", F.col("GTOFF_TNOPE").cast("long")))
    reject_rows(prepared, F.col("use_ymd").isNull() | (F.col("use_ymd") != use_ymd) |
                F.col("line_name").isNull() | (F.col("line_name") == "") |
                F.col("station_name").isNull() | (F.col("station_name") == ""), "usage keys/date")
    reject_rows(prepared, F.col("REG_YMD").isNull() |
                (~F.col("REG_YMD").rlike("^[0-9]{8}$")) |
                F.to_date("REG_YMD", "yyyyMMdd").isNull(), "usage revision date")
    order = Window.partitionBy(*KEYS).orderBy(F.col("REG_YMD").desc_nulls_last(),
                                             F.col("ingested_at").desc_nulls_last(),
                                             F.col("record_hash").desc())
    latest = prepared.withColumn("_revision", F.row_number().over(order)).where("_revision = 1")
    reject_rows(latest, (~F.col("GTON_TNOPE").rlike("^[0-9]+$")) |
                (~F.col("GTOFF_TNOPE").rlike("^[0-9]+$")) |
                F.col("ride_count").isNull() | F.col("alight_count").isNull() |
                (F.col("ride_count") < 0) | (F.col("alight_count") < 0), "usage counts")
    y, m, d = date_parts(use_ymd)
    fact = (latest.withColumn("use_date", F.to_date("use_ymd", "yyyyMMdd"))
            .withColumn("reg_date", F.to_date("REG_YMD", "yyyyMMdd"))
            .withColumn("total_count", F.col("ride_count") + F.col("alight_count"))
            .withColumn("year", F.lit(y)).withColumn("month", F.lit(m)).withColumn("day", F.lit(d))
            .select(*KEYS, "use_date", "reg_date", "ride_count", "alight_count", "total_count",
                    "ingested_at", "record_hash", "year", "month", "day"))
    require_unique(fact, KEYS, "usage Silver")
    return fact


def build_usage_silver(spark, use_ymd: str):
    y, m, d = date_parts(use_ymd)
    df = (spark.read.format("delta").load(table_path(BRONZE_DELTA_PREFIX))
          .where((F.col("year") == y) & (F.col("month") == m) & (F.col("day") == d)))
    fact = transform_usage(df, use_ymd)
    (fact.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"year='{y}' AND month='{m}' AND day='{d}'")
     .partitionBy("year", "month", "day").save(table_path(SILVER_DELTA_PREFIX)))
    return fact


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("use_ymd", type=validate_ymd)
    args = parser.parse_args()
    spark = build_spark(f"usage-silver-{args.use_ymd}")
    try:
        fact = build_usage_silver(spark, args.use_ymd)
        print(f"[OK] usage Silver date={args.use_ymd} rows={fact.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
