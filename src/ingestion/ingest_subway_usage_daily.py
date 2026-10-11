"""Paginated usage collection: fail on incomplete input, preserve immutable revisions."""
from __future__ import annotations
import argparse
import os
import uuid
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from src.common.dates import date_parts, now_kst, validate_ymd
from src.common.get_api import get_passenger_api_key
from src.common.http import get_json, SourceError
from src.common.storage import table_path, public_uri
from src.ingestion.bronze import bronze_frame, USAGE_COLUMNS
from src.jobs.spark_common import build_spark

BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE_NAME = "CardSubwayStatsNew"


def fetch_subway_usage_daily(use_ymd: str, page_size: int = 1000, max_pages: int = 10, *, session=None):
    validate_ymd(use_ymd)
    if not 1 <= page_size <= 1000 or max_pages < 1:
        raise ValueError("invalid pagination limits")
    key = get_passenger_api_key()
    rows = []
    total = None
    for page in range(max_pages):
        start = page * page_size + 1
        url = f"{BASE_URL}/{key}/json/{SERVICE_NAME}/{start}/{start + page_size - 1}/{use_ymd}"
        data = get_json(url, session=session)
        service = data.get(SERVICE_NAME)
        if not isinstance(service, dict):
            raise SourceError("Usage API returned an error or unavailable date")
        if service.get("RESULT", {}).get("CODE") != "INFO-000":
            raise SourceError("Usage API did not report success")
        try:
            current_total = int(service["list_total_count"])
        except (KeyError, TypeError, ValueError):
            raise SourceError("Usage API missing valid total count") from None
        if total is not None and current_total != total:
            raise SourceError("Usage total changed during pagination; retry the complete collection")
        total = current_total
        batch = service.get("row", [])
        if not isinstance(batch, list):
            raise SourceError("Usage API row is not a list")
        rows.extend(batch)
        print(f"[INFO] usage date={use_ymd} page={page + 1} rows={len(batch)}")
        if len(rows) >= total or not batch:
            break
    if not rows or len(rows) != total:
        raise SourceError(f"Usage collection incomplete: expected={total}, received={len(rows)}")
    if any(str(row.get("USE_YMD")) != use_ymd for row in rows):
        raise SourceError("Usage API returned a different usage date")
    return rows


def save_to_bronze_delta(rows, use_ymd: str, *, spark=None, captured_at=None, run_id=None) -> str:
    y, m, d = date_parts(use_ymd)
    owned = spark is None
    spark = spark or build_spark("bronze-usage")
    try:
        df = bronze_frame(spark, rows, USAGE_COLUMNS, captured_at or now_kst(),
                          run_id or os.getenv("RUN_ID") or str(uuid.uuid4()))
        df = df.withColumn("year", F.lit(y)).withColumn("month", F.lit(m)).withColumn("day", F.lit(d))
        path = table_path("bronze/subway_usage_delta")
        df.write.format("delta").mode("append").partitionBy("year", "month", "day").save(path)
        return public_uri(path)
    finally:
        if owned:
            spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("use_ymd", nargs="?", type=validate_ymd,
                        default=(now_kst() - timedelta(days=4)).strftime("%Y%m%d"))
    args = parser.parse_args()
    rows = fetch_subway_usage_daily(args.use_ymd)
    print(f"[OK] usage Bronze rows={len(rows)} path={save_to_bronze_delta(rows, args.use_ymd)}")


if __name__ == "__main__":
    main()
