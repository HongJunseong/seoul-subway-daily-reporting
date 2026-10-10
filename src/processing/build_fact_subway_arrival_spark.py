"""Replay complete snapshot hours from the Bronze Delta transaction log."""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.common.dates import parse_as_of, window_bounds, hour_starts, validate_ymd
from src.common.storage import table_path
from src.jobs.spark_common import build_spark
from src.quality.checks import require_nonempty, reject_rows, require_unique

BRONZE_PREFIX = "bronze/subway_arrival_delta"
SILVER_PREFIX = "silver/fact_subway_arrival_delta"
KEYS = ["snapshot_ts", "line_id", "station_id", "updn_line", "train_no", "dest_station_id"]


def transform_arrival(df):
    require_nonempty(df, "arrival Bronze")
    mapping = {"statnId": ("station_id", "long"), "statnNm": ("station_name", "string"),
               "subwayId": ("line_id", "long"), "updnLine": ("updn_line", "string"),
               "btrainNo": ("train_no", "string"), "bstatnId": ("dest_station_id", "long"),
               "bstatnNm": ("dest_station_name", "string"), "btrainSttus": ("train_status", "string"),
               "arvlCd": ("arrival_status_code", "long"), "arvlMsg2": ("arrival_msg_primary", "string")}
    out = df
    for source, (target, dtype) in mapping.items():
        out = out.withColumn(target, F.col(source).cast(dtype))
    out = (out.withColumn("recptn_dt", F.to_timestamp("recptnDt"))
           .withColumn("event_time", F.coalesce("recptn_dt", "snapshot_ts"))
           .withColumn("arrival_seconds_raw", F.col("barvlDt").cast("long"))
           .withColumn("arrival_seconds", F.when(F.col("barvlDt").rlike("^[0-9]+$") & (F.col("arrival_seconds_raw") >= 0),
                                                F.col("arrival_seconds_raw")))
           .withColumn("is_last_train", F.col("lstcarAt") == "1")
           .withColumn("source_age_sec", F.unix_timestamp("snapshot_ts") - F.unix_timestamp("event_time")))
    reject_rows(out, F.col("snapshot_ts").isNull() | F.col("station_id").isNull() |
                F.col("line_id").isNull() | F.col("station_name").isNull() |
                (F.trim("station_name") == "") | F.col("updn_line").isNull() |
                F.col("train_no").isNull(), "arrival natural keys")
    order = Window.partitionBy(*KEYS).orderBy(F.col("ingested_at").desc(), F.col("record_hash").desc())
    out = (out.withColumn("_revision", F.row_number().over(order)).where("_revision = 1")
           .select("snapshot_ts", "recptn_dt", "event_time", "station_id", "station_name", "line_id",
                   "updn_line", "train_no", "dest_station_id", "dest_station_name", "train_status",
                   "arrival_seconds", "arrival_status_code", "arrival_msg_primary", "is_last_train",
                   "source_age_sec", "record_hash", "ingested_at", "year", "month", "day", "hour"))
    require_unique(out, KEYS, "arrival Silver")
    return out


def partition_predicate(hours):
    return " OR ".join(
        f"(year='{h:%Y}' AND month='{h:%m}' AND day='{h:%d}' AND hour='{h:%H}')"
        for h in hours)


def build_arrival_silver(spark, as_of=None, *, day=None):
    if day:
        start = datetime.strptime(validate_ymd(day), "%Y%m%d")
        end = start + timedelta(days=1)
    else:
        start, end = window_bounds(as_of)
    hours = hour_starts(start, end)
    predicate = partition_predicate(hours)
    df = spark.read.format("delta").load(table_path(BRONZE_PREFIX)).where(predicate)
    # Rebuild each selected hour in full; an older replay must not erase later snapshots.
    fact = transform_arrival(df)
    (fact.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
     .partitionBy("year", "month", "day", "hour").save(table_path(SILVER_PREFIX)))
    return fact


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--as_of", type=parse_as_of)
    group.add_argument("--day", type=validate_ymd)
    args = parser.parse_args()
    spark = build_spark("arrival-silver")
    try:
        print(f"[OK] arrival Silver rows={build_arrival_silver(spark, args.as_of, day=args.day).count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
