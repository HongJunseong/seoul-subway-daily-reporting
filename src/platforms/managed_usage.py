"""Unity Catalog/managed Delta adapter using the production usage transforms.

No SparkSession, JVM, SparkContext, S3 credentials, or local paths are created here.
Callers supply the Databricks session and serialize writers for this namespace.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from pyspark.sql import functions as F
from src.common.dates import validate_ymd
from src.processing.build_fact_subway_usage_daily_spark import transform_usage
from src.processing.build_gold_subway_usage_daily_spark import build_station_daily, build_line_daily
from src.quality.checks import require_nonempty, require_unique, reject_rows


@dataclass(frozen=True)
class UsageTables:
    namespace: str

    def __post_init__(self):
        parts = self.namespace.split(".")
        if len(parts) not in (1, 2) or any(
            not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in parts
        ):
            raise ValueError("namespace must be schema or catalog.schema with simple identifiers")

    def name(self, layer):
        names = {
            "bronze": "bronze_subway_usage_v2",
            "silver": "silver_subway_usage_v2",
            "station": "gold_subway_usage_station_v2",
            "line": "gold_subway_usage_line_v2",
        }
        return f"{self.namespace}.{names[layer]}"


def version(spark, table):
    # Names only come from validated UsageTables, never notebook SQL input.
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])


def read_version(spark, table, at_version):
    return spark.read.format("delta").option("versionAsOf", at_version).table(table)


def replace_date(frame, table, use_ymd, keys):
    validate_ymd(use_ymd)
    require_nonempty(frame, "managed usage publication")
    reject_rows(frame, F.col("use_ymd").isNull() | (F.col("use_ymd") != use_ymd),
                "managed publication date")
    writer = frame.write.format("delta")
    if frame.sparkSession.catalog.tableExists(table):
        from delta.tables import DeltaTable
        condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
        (DeltaTable.forName(frame.sparkSession, table).alias("target")
         .merge(frame.alias("source"), condition)
         .whenMatchedUpdateAll().whenNotMatchedInsertAll()
         .whenNotMatchedBySourceDelete(condition=f"target.use_ymd = '{use_ymd}'")
         .execute())
    else:
        writer.mode("error").saveAsTable(table)


def publish_silver(spark, tables, use_ymd):
    validate_ymd(use_ymd)
    bronze_table = tables.name("bronze")
    at_version = version(spark, bronze_table)
    source = read_version(spark, bronze_table, at_version).where(F.col("USE_YMD") == use_ymd)
    fact = transform_usage(source, use_ymd)
    replace_date(fact, tables.name("silver"), use_ymd, ["use_ymd", "line_name", "station_name"])
    return {"bronze_version": at_version, "silver_version": version(spark, tables.name("silver"))}


def publish_gold(spark, tables, use_ymd):
    validate_ymd(use_ymd)
    table = tables.name("silver")
    at_version = version(spark, table)
    fact = read_version(spark, table, at_version).where(F.col("use_ymd") == use_ymd)
    require_nonempty(fact, "managed usage Silver")
    require_unique(fact, ["use_ymd", "line_name", "station_name"], "managed usage Silver")
    station, line = build_station_daily(fact), build_line_daily(fact)
    # These are separate Delta transactions. Replay repairs a partial Gold write.
    replace_date(station, tables.name("station"), use_ymd, ["use_ymd", "line_name", "station_name"])
    replace_date(line, tables.name("line"), use_ymd, ["use_ymd", "line_name"])
    return {"silver_version": at_version,
            "station_version": version(spark, tables.name("station")),
            "line_version": version(spark, tables.name("line"))}


def fingerprint(frame, columns):
    # Bounded fixture verification only; do not collect an unbounded production table.
    rows = [row.asDict() for row in frame.select(*columns).orderBy(*columns).collect()]
    payload = json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return {"rows": len(rows), "sha256": hashlib.sha256(payload.encode()).hexdigest()}


BUSINESS_COLUMNS = {
    "silver": ["use_ymd", "line_name", "station_name", "reg_date",
               "ride_count", "alight_count", "total_count"],
    "station": ["use_ymd", "line_name", "station_name", "ride_count", "alight_count", "total_count"],
    "line": ["use_ymd", "line_name", "ride_count", "alight_count", "total_count", "station_count"],
}


def fixture_snapshot(spark, tables):
    return {layer: fingerprint(spark.table(tables.name(layer)), columns)
            for layer, columns in BUSINESS_COLUMNS.items()}
