"""Offline end-to-end experiment using explicitly synthetic API-shaped observations."""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path,
                        default=ROOT / ".work/demo" / datetime.now().strftime("%Y%m%dT%H%M%S"))
    args = parser.parse_args()
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        raise ValueError("Demo requires a new/empty output directory; existing data is preserved")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    os.environ["SSDR_DATA_ROOT"] = str(args.output_dir.resolve() / "lake")
    os.environ["SSDR_REPORT_OUTPUT_DIR"] = str(args.output_dir.resolve() / "reports")
    os.environ["SSDR_REPORT_MODE"] = "template"
    os.environ["SSDR_DATA_KIND"] = "synthetic"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession, functions as F
    from src.common.storage import table_path
    from src.ingestion.ingest_subway_usage_daily import save_to_bronze_delta as save_usage
    from src.ingestion.ingest_subway_arrival_realtime import save_to_bronze_delta as save_arrival
    from src.processing.build_fact_subway_usage_daily_spark import build_usage_silver
    from src.processing.build_gold_subway_usage_daily_spark import build_usage_gold
    from src.processing.build_fact_subway_arrival_spark import build_arrival_silver
    from src.processing.build_gold_arrival_metrics_15m_spark import build_arrival_metrics
    from src.processing.build_gold_subway_arrival_spark import build_arrival_gold
    from src.report.generate_daily_report import generate_daily_report
    from scripts.verify_report import verify_report
    builder = (SparkSession.builder.master("local[2]").appName("ssdr-offline-demo")
               .config("spark.sql.session.timeZone", "Asia/Seoul")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
               .config("spark.databricks.delta.snapshotPartitions", "2")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.jars.ivy", str(ROOT / ".work/ivy"))
               .config("spark.jars.ivySettings", str(ROOT / "tests/ivysettings.xml")))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    started = time.perf_counter()
    try:
        rows = json.loads((ROOT / "fixtures/usage.json").read_text())
        for run in ["first", "retry"]:
            save_usage(rows, "20260128", spark=spark, captured_at=datetime(2026, 2, 1, 6), run_id=run)
        fact = build_usage_silver(spark, "20260128")
        build_usage_gold(spark, "20260128")
        baseline = int(fact.agg(F.sum("total_count")).first()[0])
        assert baseline == 660 and fact.count() == 3
        build_usage_silver(spark, "20260128")
        station, _ = build_usage_gold(spark, "20260128")
        assert station.agg(F.sum("total_count")).first()[0] == baseline
        save_usage([dict(rows[0], REG_YMD="20260130", GTON_TNOPE="120")], "20260128",
                   spark=spark, captured_at=datetime(2026, 2, 1, 7), run_id="correction")
        build_usage_silver(spark, "20260128")
        station, _ = build_usage_gold(spark, "20260128")
        corrected = int(station.agg(F.sum("total_count")).first()[0])
        assert corrected == 680
        for snapshot in json.loads((ROOT / "fixtures/arrival.json").read_text()):
            ts = datetime.fromisoformat(snapshot["snapshot_ts"])
            observations = [dict(r, recptnDt=ts.isoformat(sep=" ")) for r in snapshot["rows"]]
            save_arrival(observations, ts, spark=spark)
        end = datetime(2026, 2, 1, 0, 5)
        build_arrival_silver(spark, end)
        metrics = build_arrival_metrics(spark, end).collect()
        assert sum(r.n for r in metrics) == 6
        assert sum(r.valid_eta_n for r in metrics) == 5
        build_arrival_silver(spark, day="20260131")
        build_arrival_gold(spark, "20260131")
        report = generate_daily_report("20260201", save_to_s3=False)
        manifest = verify_report(report)
        evidence = {"data_kind": "synthetic fixture; not live Seoul observations",
                    "publication_hashes_verified": True, "report_schema_version": manifest["schema_version"],
                    "usage_bronze_initial_rows": 6, "usage_silver_rows": 3,
                    "usage_total_before_correction": baseline, "usage_total_after_correction": corrected,
                    "cross_midnight_observations": 6, "cross_midnight_valid_eta": 5,
                    "elapsed_seconds": round(time.perf_counter() - started, 3),
                    "report": str(report.relative_to(args.output_dir.resolve())),
                    "limitations": "Small local fixture demonstrates correctness, not throughput or cloud availability"}
        (args.output_dir / "evidence.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
        print(json.dumps(evidence, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
