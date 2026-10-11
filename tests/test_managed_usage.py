from datetime import datetime
from pathlib import Path
import uuid
import pytest
from pyspark.sql import functions as F
from src.ingestion.bronze import bronze_frame, USAGE_COLUMNS
from src.platforms.managed_usage import UsageTables, publish_silver, version, fixture_snapshot
from src.platforms.usage_fixture import seed_fixture, verify_fixture_replay
from src.quality.checks import DataQualityError
from scripts.export_usage_snapshot import export_snapshot

pytestmark = pytest.mark.spark


@pytest.mark.parametrize("namespace", ["x;DROP TABLE y", "catalog.schema.table", "../data", "`raw`", ""])
def test_identifier_input_cannot_become_sql(namespace):
    with pytest.raises(ValueError):
        UsageTables(namespace)


def test_managed_replay_bad_input_and_versioned_export(spark, tmp_path):
    tables = UsageTables("ssdr_v2_fixture_" + uuid.uuid4().hex[:12])
    spark.sql(f"CREATE DATABASE {tables.namespace} LOCATION '{(tmp_path / 'managed').as_uri()}'")
    old_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        seed_fixture(spark, tables, Path("fixtures/usage.json"))
        evidence = verify_fixture_replay(spark, tables, "local-test")
        assert evidence["verified"] and evidence["totals"] == {"20260128": 680, "20260129": 660}
        before = fixture_snapshot(spark, tables)
        before_version = version(spark, tables.name("silver"))
        import json
        row = json.loads(Path("fixtures/usage.json").read_text())[0]
        for count, revision in [("bad", "20260201"), ("9223372036854775808", "20260202"),
                                ("120", "20261340")]:
            bad = dict(row, GTON_TNOPE=count, REG_YMD=revision)
            frame = bronze_frame(spark, [bad], USAGE_COLUMNS, datetime(2026, 2, 3), "fixture-v2-bad")
            frame.write.format("delta").mode("append").saveAsTable(tables.name("bronze"))
            with pytest.raises(DataQualityError):
                publish_silver(spark, tables, "20260128")
            assert version(spark, tables.name("silver")) == before_version
        assert fixture_snapshot(spark, tables) == before
        with pytest.raises(DataQualityError, match="empty"):
            publish_silver(spark, tables, "20260130")
        assert version(spark, tables.name("silver")) == before_version

        root = spark.sql(f"DESCRIBE DETAIL {tables.name('bronze')}").first()["location"]
        result = export_snapshot(spark, root, "20260128", tmp_path / "export", at_version=0)
        assert result["source_version"] == 0 and result["row_count"] == 7
        exported = spark.read.parquet(str(tmp_path / "export/data"))
        assert exported.count() == 7
        assert exported.where(F.col("GTON_TNOPE") == "bad").count() == 0
        with pytest.raises(ValueError, match="exists"):
            export_snapshot(spark, root, "20260128", tmp_path / "export", at_version=0)
    finally:
        spark.conf.set("spark.sql.ansi.enabled", old_ansi)
