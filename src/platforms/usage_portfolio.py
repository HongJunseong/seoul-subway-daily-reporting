"""Bounded Free Edition experiment: quality gate, Delta history, and SQL serving.

Business transforms stay in managed_usage. Only a new synthetic schema is written.
No automatic schedule, S3 connection, enterprise monitoring, or production benchmark.
"""
from __future__ import annotations
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re

from pyspark.sql import functions as F
from src.ingestion.bronze import bronze_frame, USAGE_COLUMNS
from src.platforms.managed_usage import (
    UsageTables, publish_silver, publish_gold, version, read_version,
    fixture_snapshot, fingerprint, BUSINESS_COLUMNS,
)
from src.platforms.usage_fixture import seed_fixture, DATES
from src.quality.checks import DataQualityError


def require_quality_namespace(tables):
    if not re.fullmatch(r"ssdr_v2_fixture_quality_[A-Za-z0-9_]{1,32}",
                        tables.namespace.split(".")[-1]):
        raise ValueError("experiment requires a dedicated ssdr_v2_fixture_quality_* schema")


def totals(frame):
    return {r["use_ymd"]: int(r["total"]) for r in
            frame.groupBy("use_ymd").agg(F.sum("total_count").alias("total")).collect()}


def quality_experiment(spark, tables, fixture_path, execution_environment):
    """One new schema per experiment; preserve failed experiments instead of resetting."""
    require_quality_namespace(tables)
    if any(spark.catalog.tableExists(tables.name(layer))
           for layer in ("bronze", "silver", "station", "line")):
        raise ValueError("experiment tables already exist; choose a new dedicated schema")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {tables.namespace}")
    seed_fixture(spark, tables, fixture_path)
    for date in DATES:
        publish_silver(spark, tables, date)
        publish_gold(spark, tables, date)
    initial = fixture_snapshot(spark, tables)
    initial_versions = {layer: version(spark, tables.name(layer))
                        for layer in ("bronze", "silver", "station", "line")}
    if totals(spark.table(tables.name("station"))) != {DATES[0]: 680, DATES[1]: 660}:
        raise AssertionError("unexpected starting fixture totals")

    row = json.loads(Path(fixture_path).read_text(encoding="utf-8"))[0]
    for name, count, revision in [("invalid", "-1", "20260201"), ("repair", "140", "20260202")]:
        patch = dict(row, GTON_TNOPE=count, REG_YMD=revision)
        frame = bronze_frame(spark, [patch], USAGE_COLUMNS, datetime(2026, 2, 3),
                             "fixture-v2-quality-" + name)
        frame.write.format("delta").mode("append").saveAsTable(tables.name("bronze"))
        if name == "invalid":
            rejected = False
            try:
                publish_silver(spark, tables, DATES[0])
            except DataQualityError:
                rejected = True
            if not rejected:
                raise AssertionError("invalid latest correction was accepted")
            if fixture_snapshot(spark, tables) != initial or any(
                version(spark, tables.name(layer)) != initial_versions[layer]
                for layer in BUSINESS_COLUMNS
            ):
                raise AssertionError("quality failure changed published business rows or versions")
            invalid_bronze_version = version(spark, tables.name("bronze"))
        else:
            publish_silver(spark, tables, DATES[0])
            publish_gold(spark, tables, DATES[0])

    recovered = fixture_snapshot(spark, tables)
    recovered_versions = {layer: version(spark, tables.name(layer))
                          for layer in ("bronze", "silver", "station", "line")}
    recovered_totals = totals(spark.table(tables.name("station")))
    if recovered_totals != {DATES[0]: 700, DATES[1]: 660}:
        raise AssertionError("repair did not change the intended date only")
    historical = read_version(spark, tables.name("station"), initial_versions["station"])
    historical_totals = totals(historical)
    if historical_totals != {DATES[0]: 680, DATES[1]: 660}:
        raise AssertionError("time travel did not preserve the prior publication")
    if fingerprint(historical, BUSINESS_COLUMNS["station"]) != initial["station"]:
        raise AssertionError("historical business hash changed")

    # Replay uses the corrected Bronze, preserving every business column.
    publish_silver(spark, tables, DATES[0])
    publish_gold(spark, tables, DATES[0])
    if fixture_snapshot(spark, tables) != recovered:
        raise AssertionError("repair replay changed business rows")
    if version(spark, tables.name("bronze")) != recovered_versions["bronze"]:
        raise AssertionError("replay modified Bronze")
    history = {}
    for layer in ("bronze", "silver", "station", "line"):
        # Never publish userName, notebook paths, job identities, or raw commit metadata.
        history[layer] = [
            {"version": int(r["version"]), "operation": r["operation"]}
            for r in spark.sql(f"DESCRIBE HISTORY {tables.name(layer)}")
            .select("version", "operation").orderBy("version").limit(16).collect()
        ]
    return {
        "verified": True, "synthetic": True, "execution_environment": execution_environment,
        "spark_version": spark.version, "namespace": tables.namespace,
        "quality_failure_blocked": True, "published_versions_unchanged_on_failure": True,
        "replay_business_match": True, "bronze_unchanged_on_replay": True,
        "historical_totals": historical_totals, "recovered_totals": recovered_totals,
        "initial_versions": initial_versions, "invalid_bronze_version": invalid_bronze_version,
        "recovered_versions": recovered_versions,
        "final_versions": {layer: version(spark, tables.name(layer)) for layer in history},
        "historical_station_snapshot": initial["station"], "business_snapshot": recovered,
        "history": history,
    }


VIEWS = ("usage_quality_status_v2", "daily_usage_overview_v2", "station_usage_rank_v2")


def serving_sql(tables, model):
    """SQL source also serves as a reviewable semantic contract for warehouse consumers."""
    # UsageTables validates SQL identifiers before they enter these static SQL files.
    if model not in VIEWS:
        raise ValueError("unknown serving model")
    path = Path(__file__).resolve().parents[2] / "databricks/sql" / (model + ".sql")
    return path.read_text(encoding="utf-8").replace("__NAMESPACE__", tables.namespace)


def create_serving_views(spark, tables, execution_environment):
    require_quality_namespace(tables)
    for model in VIEWS:
        spark.sql(serving_sql(tables, model))
    rows = [r.asDict() for r in spark.table(tables.namespace + ".usage_quality_status_v2")
            .orderBy("use_ymd").collect()]
    if len(rows) != 2 or any(r["quality_status"] != "PASS" for r in rows):
        raise DataQualityError("serving contract failed; SQL consumers must require PASS")
    view_rows = {}
    for model in VIEWS:
        frame = spark.table(tables.namespace + "." + model)
        values = [r.asDict() for r in frame.limit(101).collect()]
        if len(values) > 100:
            raise ValueError("serving evidence is limited to synthetic fixture rows")
        view_rows[model] = sorted(values, key=lambda r: json.dumps(r, sort_keys=True))
    return {"verified": True, "synthetic": True, "execution_environment": execution_environment,
            "namespace": tables.namespace, "views": list(VIEWS), "quality_rows": rows,
            "view_rows": view_rows,
            "view_definition_sha256": {model: hashlib.sha256(
                serving_sql(tables, model).encode()).hexdigest() for model in VIEWS}}
