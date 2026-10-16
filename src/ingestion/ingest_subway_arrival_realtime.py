"""Capture observed arrival snapshots; historical API backfill is not available."""
from __future__ import annotations
import os
import uuid
from pyspark.sql import functions as F
from src.common.dates import now_kst
from src.common.get_api import get_arrival_api_key
from src.common.http import get_json, SourceError
from src.common.deadline import collection_deadline
from src.common.storage import table_path, public_uri
from src.ingestion.bronze import bronze_frame, ARRIVAL_COLUMNS
from src.jobs.spark_common import build_spark

BASE_URL = "http://swopenapi.seoul.go.kr/api/subway"
SERVICE_NAME = "realtimeStationArrival"


@collection_deadline(45)
def fetch_realtime_arrival_all_json(start_index=1, end_index=1000, *, session=None):
    if not 1 <= start_index <= end_index <= 1000:
        raise ValueError("invalid arrival response range")
    key = get_arrival_api_key()
    url = f"{BASE_URL}/{key}/json/{SERVICE_NAME}/{start_index}/{end_index}/ALL"
    data = get_json(url, session=session, timeout=10)
    rows = data.get("realtimeArrivalList")
    if not isinstance(rows, list) or not rows:
        raise SourceError("Arrival API returned no observations or an error")
    # ALL is the API query scope, not proof of complete network coverage.
    return rows


def save_to_bronze_delta(rows, snapshot_time, *, spark=None, run_id=None) -> str:
    owned = spark is None
    spark = spark or build_spark("bronze-arrival")
    try:
        df = bronze_frame(spark, rows, ARRIVAL_COLUMNS, snapshot_time,
                          run_id or os.getenv("RUN_ID") or str(uuid.uuid4()))
        df = (df.withColumn("snapshot_ts", F.to_timestamp(F.lit(snapshot_time.isoformat())))
              .withColumn("collection_scope", F.lit("ALL response, maximum 1000 observations; partial coverage"))
              .withColumn("response_at_limit", F.lit(len(rows) >= 1000))
              .withColumn("year", F.lit(snapshot_time.strftime("%Y")))
              .withColumn("month", F.lit(snapshot_time.strftime("%m")))
              .withColumn("day", F.lit(snapshot_time.strftime("%d")))
              .withColumn("hour", F.lit(snapshot_time.strftime("%H"))))
        path = table_path("bronze/subway_arrival_delta")
        df.write.format("delta").mode("append").partitionBy("year", "month", "day", "hour").save(path)
        return public_uri(path)
    finally:
        if owned:
            spark.stop()


def main():
    # Capture time is actual observation time, never a fabricated scheduler timestamp.
    rows = fetch_realtime_arrival_all_json()
    snapshot_time = now_kst()
    print(f"[OK] arrival Bronze rows={len(rows)} path={save_to_bronze_delta(rows, snapshot_time)}")


if __name__ == "__main__":
    main()
