from pathlib import Path
import uuid
import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from src.platforms.managed_usage import UsageTables
from src.platforms.usage_portfolio import (
    quality_experiment, create_serving_views, require_quality_namespace, serving_sql,
)

pytestmark = pytest.mark.spark


def test_quality_failure_time_travel_and_sql_semantic_drift(spark, tmp_path):
    tables = UsageTables("ssdr_v2_fixture_quality_" + uuid.uuid4().hex[:12])
    spark.sql(f"CREATE DATABASE {tables.namespace} LOCATION '{(tmp_path / 'managed').as_uri()}'")
    old_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        result = quality_experiment(spark, tables, Path("fixtures/usage.json"), "local-test")
        assert result["historical_totals"] == {"20260128": 680, "20260129": 660}
        assert result["recovered_totals"] == {"20260128": 700, "20260129": 660}
        assert result["published_versions_unchanged_on_failure"]
        assert result["final_versions"]["bronze"] == result["recovered_versions"]["bronze"]
        with pytest.raises(ValueError, match="already exist"):
            quality_experiment(spark, tables, Path("fixtures/usage.json"), "local-test")
        serving = create_serving_views(spark, tables, "local-test")
        assert {r["quality_status"] for r in serving["quality_rows"]} == {"PASS"}
        assert spark.table(tables.namespace + ".station_usage_rank_v2").count() == 6
        # Wrong station identity preserves totals and row counts; expose semantic drift.
        DeltaTable.forName(spark, tables.name("station")).update(
            condition="use_ymd = '20260128' AND line_name = '2호선' AND station_name = '가상환승역'",
            set={"station_name": F.lit("잘못된역")})
        rows = {r["use_ymd"]: r.asDict() for r in
                spark.table(tables.namespace + ".usage_quality_status_v2").collect()}
        assert rows["20260128"]["silver_total_count"] == rows["20260128"]["station_total_count"] == 700
        assert rows["20260128"]["quality_status"] == "FAIL"
        assert rows["20260128"]["station_different_rows"] == 2
        assert {r["use_ymd"] for r in spark.table(
            tables.namespace + ".daily_usage_overview_v2").collect()} == {"20260129"}
        assert spark.table(tables.namespace + ".station_usage_rank_v2").count() == 3
        # Missing Gold date remains visible as FAIL instead of silently disappearing.
        DeltaTable.forName(spark, tables.name("station")).delete("use_ymd = '20260129'")
        rows = {r["use_ymd"]: r.asDict() for r in
                spark.table(tables.namespace + ".usage_quality_status_v2").collect()}
        assert rows["20260129"]["quality_status"] == "FAIL"
        assert rows["20260129"]["station_total_count"] is None
        assert spark.table(tables.namespace + ".daily_usage_overview_v2").isEmpty()
        assert spark.table(tables.namespace + ".station_usage_rank_v2").isEmpty()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", old_ansi)


def test_experiment_scope_and_sql_models_are_restricted():
    with pytest.raises(ValueError, match="dedicated"):
        require_quality_namespace(UsageTables("workspace.default"))
    with pytest.raises(ValueError, match="unknown"):
        serving_sql(UsageTables("workspace.ssdr_v2_fixture_quality_test"), "not_a_model")
