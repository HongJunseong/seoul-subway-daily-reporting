"""Run the managed-table contract locally; this is not a Databricks deployment."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
import uuid
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.local_spark import local_spark
from src.platforms.managed_usage import UsageTables
from src.platforms.usage_fixture import seed_fixture, verify_fixture_replay


def run(output):
    output = Path(output).absolute()
    if output.exists():
        raise ValueError("output already exists; preserve earlier evidence")
    output.mkdir(parents=True)
    spark = local_spark("ssdr-managed-usage-contract")
    namespace = "ssdr_v2_fixture_" + uuid.uuid4().hex[:12]
    tables = UsageTables(namespace)
    try:
        spark.conf.set("spark.sql.ansi.enabled", "true")
        spark.sql(f"CREATE DATABASE {namespace} LOCATION '{(output / 'tables').as_uri()}'")
        seed_fixture(spark, tables, Path(__file__).resolve().parents[1] / "fixtures/usage.json")
        evidence = verify_fixture_replay(spark, tables, "local-spark-managed-delta-contract")
        (output / "evidence.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n",
                                            encoding="utf-8")
        return evidence
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), ensure_ascii=False, indent=2))
