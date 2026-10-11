"""Replay saved usage dates, or explicitly fetch historical API data first."""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta
from src.common.dates import validate_ymd
from src.jobs.spark_common import build_spark
from src.processing.build_fact_subway_usage_daily_spark import build_usage_silver
from src.processing.build_gold_subway_usage_daily_spark import build_usage_gold


def dates_between(start, end):
    first = datetime.strptime(validate_ymd(start), "%Y%m%d")
    last = datetime.strptime(validate_ymd(end), "%Y%m%d")
    if last < first or (last - first).days > 365:
        raise ValueError("backfill range must be ordered and no longer than 366 dates")
    while first <= last:
        yield first.strftime("%Y%m%d")
        first += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, type=validate_ymd)
    parser.add_argument("--end", required=True, type=validate_ymd)
    parser.add_argument("--fetch", action="store_true", help="Opt in to API collection; default replays Bronze")
    args = parser.parse_args()
    dates = list(dates_between(args.start, args.end))
    spark = build_spark("usage-backfill")
    try:
        for date in dates:
            if args.fetch:
                from src.ingestion.ingest_subway_usage_daily import fetch_subway_usage_daily, save_to_bronze_delta
                save_to_bronze_delta(fetch_subway_usage_daily(date), date, spark=spark)
            build_usage_silver(spark, date)
            build_usage_gold(spark, date)
            print(f"[OK] usage replay date={date}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
