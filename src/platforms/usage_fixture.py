"""Explicit synthetic managed-table input and replay verification."""
from datetime import datetime
import json
from pathlib import Path
from pyspark.sql import functions as F
from src.ingestion.bronze import bronze_frame, USAGE_COLUMNS
from src.platforms.managed_usage import (
    publish_silver, publish_gold, version, fixture_snapshot,
)

DATES = ("20260128", "20260129")


def seed_fixture(spark, tables, fixture_path):
    if not tables.namespace.split(".")[-1].startswith("ssdr_v2_fixture"):
        raise ValueError("synthetic writes require a dedicated ssdr_v2_fixture* schema")
    table = tables.name("bronze")
    if spark.catalog.tableExists(table):
        source = spark.table(table)
        if source.where(F.col("run_id").isNull() | ~F.col("run_id").startswith("fixture-v2")).limit(1).count():
            raise ValueError("existing Bronze contains non-fixture input")
        return
    rows = json.loads(Path(fixture_path).read_text(encoding="utf-8"))
    revised = dict(rows[0], REG_YMD="20260130", GTON_TNOPE="120")
    all_rows = rows + rows + [revised] + [dict(row, USE_YMD=DATES[1]) for row in rows]
    frame = bronze_frame(spark, all_rows, USAGE_COLUMNS, datetime(2026, 2, 1, 6), "fixture-v2")
    frame.write.format("delta").mode("append").saveAsTable(table)


def verify_fixture_replay(spark, tables, execution_environment):
    if not tables.namespace.split(".")[-1].startswith("ssdr_v2_fixture"):
        raise ValueError("replay experiment requires a dedicated fixture schema")
    before_bronze = version(spark, tables.name("bronze"))
    for date in DATES:
        publish_silver(spark, tables, date)
        publish_gold(spark, tables, date)
    first = fixture_snapshot(spark, tables)
    publish_silver(spark, tables, DATES[0])
    publish_gold(spark, tables, DATES[0])
    second = fixture_snapshot(spark, tables)
    if first != second:
        raise AssertionError("business rows changed during replay")
    if version(spark, tables.name("bronze")) != before_bronze:
        raise AssertionError("Bronze changed during replay")
    totals = {row["use_ymd"]: int(row["total"]) for row in
              spark.table(tables.name("station")).groupBy("use_ymd")
              .agg(F.sum("total_count").alias("total")).collect()}
    if totals != {DATES[0]: 680, DATES[1]: 660}:
        raise AssertionError(f"unexpected synthetic totals: {totals}")
    return {"synthetic": True, "execution_environment": execution_environment,
            "spark_version": spark.version, "verified": True,
            "dates": list(DATES), "totals": totals, "business_snapshot": second,
            "bronze_unchanged": True,
            "table_versions": {layer: version(spark, tables.name(layer))
                               for layer in ("bronze", "silver", "station", "line")}}
