import json
from datetime import datetime
from pathlib import Path
import pytest
from pyspark.sql import functions as F
from src.common.storage import table_path
from src.ingestion.ingest_subway_usage_daily import save_to_bronze_delta as save_usage
from src.ingestion.ingest_subway_arrival_realtime import save_to_bronze_delta as save_arrival
from src.processing.build_fact_subway_usage_daily_spark import build_usage_silver
from src.processing.build_gold_subway_usage_daily_spark import build_usage_gold
from src.processing.build_fact_subway_arrival_spark import build_arrival_silver
from src.processing.build_gold_arrival_metrics_15m_spark import build_arrival_metrics
from src.quality.checks import DataQualityError

pytestmark = pytest.mark.spark


def usage_rows():
    return json.loads(Path("fixtures/usage.json").read_text())


def totals(spark):
    return spark.read.format("delta").load(table_path("gold/subway_usage/station_daily")).agg(
        F.sum("total_count")).first()[0]


def test_duplicate_collection_replay_and_revision_are_idempotent(spark, storage):
    rows = usage_rows()
    for run in ["first", "retry"]:
        save_usage(rows, "20260128", spark=spark, captured_at=datetime(2026, 2, 1, 6), run_id=run)
    build_usage_silver(spark, "20260128")
    build_usage_gold(spark, "20260128")
    assert totals(spark) == 660
    build_usage_silver(spark, "20260128")
    build_usage_gold(spark, "20260128")
    assert totals(spark) == 660
    revised = dict(rows[0], REG_YMD="20260130", GTON_TNOPE="120")
    save_usage([revised], "20260128", spark=spark, captured_at=datetime(2026, 2, 1, 7))
    fact = build_usage_silver(spark, "20260128")
    build_usage_gold(spark, "20260128")
    assert fact.count() == 3
    assert totals(spark) == 680
    # Adjacent date survives the date-scoped replacement.
    other = [dict(r, USE_YMD="20260129") for r in rows]
    save_usage(other, "20260129", spark=spark, captured_at=datetime(2026, 2, 2, 6))
    build_usage_silver(spark, "20260129")
    build_usage_gold(spark, "20260129")
    build_usage_silver(spark, "20260128")
    build_usage_gold(spark, "20260128")
    assert totals(spark) == 1340


@pytest.mark.parametrize("invalid_count", ["-1", "1.5"])
def test_invalid_revision_does_not_replace_published_data(spark, storage, invalid_count):
    rows = usage_rows()
    save_usage(rows, "20260128", spark=spark, captured_at=datetime(2026, 2, 1, 6))
    build_usage_silver(spark, "20260128")
    save_usage([dict(rows[0], REG_YMD="20260130", GTON_TNOPE=invalid_count)],
               "20260128", spark=spark, captured_at=datetime(2026, 2, 1, 7))
    with pytest.raises(DataQualityError, match="counts"):
        build_usage_silver(spark, "20260128")
    fact = spark.read.format("delta").load(table_path("silver/fact_subway_usage_delta"))
    assert fact.agg(F.sum("total_count")).first()[0] == 660


def test_window_crosses_midnight_and_null_eta_does_not_become_zero(spark, storage):
    snapshots = json.loads(Path("fixtures/arrival.json").read_text())
    for snapshot in snapshots:
        ts = datetime.fromisoformat(snapshot["snapshot_ts"])
        rows = [dict(r, recptnDt=ts.isoformat(sep=" ")) for r in snapshot["rows"]]
        save_arrival(rows, ts, spark=spark)
        save_arrival(rows, ts, spark=spark)  # replayed API result
    end = datetime(2026, 2, 1, 0, 5)
    build_arrival_silver(spark, end)
    metrics = build_arrival_metrics(spark, end).collect()
    assert sum(r.n for r in metrics) == 6
    assert sum(r.valid_eta_n for r in metrics) == 5
    line2 = next(r for r in metrics if r.line_id == 1002)
    assert line2.avg_eta_sec == 360
    assert line2.long_ratio_10m == 0.5
    build_arrival_silver(spark, end)
    build_arrival_metrics(spark, end)
    persisted = spark.read.format("delta").load(table_path("gold/arrival_metrics_15m_delta"))
    assert persisted.count() == 2
    boundaries = persisted.select(
        F.date_format("window_start_ts", "yyyy-MM-dd HH:mm:ss").alias("start_kst"),
        F.date_format("window_end_ts", "yyyy-MM-dd HH:mm:ss").alias("end_kst"),
        "window_end_ymdhm").distinct().collect()
    assert [(r.start_kst, r.end_kst, r.window_end_ymdhm) for r in boundaries] == [
        ("2026-01-31 23:50:00", "2026-02-01 00:05:00", "202602010005")]


def test_arrival_same_capture_prefers_latest_source_generation_over_hash(spark, storage):
    from src.processing.build_fact_subway_arrival_spark import transform_arrival
    row = json.loads(Path("fixtures/arrival.json").read_text())[0]["rows"][0]
    stamp = datetime(2026, 2, 1, 0, 0, 5)
    rows = [
        dict(row, recptnDt="2026-01-31 23:59:50", barvlDt="180"),
        dict(row, recptnDt="2026-02-01 00:00:01", barvlDt="60"),
        dict(row, recptnDt=None, barvlDt="45"),
    ]
    save_arrival(rows, stamp, spark=spark, run_id="mixed-source-generation")
    bronze = spark.read.format("delta").load(table_path("bronze/subway_arrival_delta"))
    # Force the older generation to win a hash-only tie, so this verifies source-time semantics.
    bronze = bronze.withColumn("record_hash", F.when(F.col("barvlDt") == "180", F.lit("z"))
                               .when(F.col("barvlDt") == "45", F.lit("y")).otherwise(F.lit("a")))
    for frame in [bronze.orderBy("record_hash"), bronze.orderBy(F.col("record_hash").desc())]:
        facts = transform_arrival(frame).withColumn(
            "recptn_kst", F.date_format("recptn_dt", "yyyy-MM-dd HH:mm:ss")).collect()
        assert len(facts) == 1
        assert facts[0].arrival_seconds == 60
        assert facts[0].recptn_kst == "2026-02-01 00:00:01"
        assert facts[0].source_age_sec == 4
