"""Seed a new local lake with synthetic Bronze; perform no API collection."""
from __future__ import annotations
import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def fixture_expectations():
    snapshots = json.loads((ROOT / "fixtures/arrival.json").read_text())
    daily = [row for snapshot in snapshots if snapshot["snapshot_ts"][:10] == "2026-01-31"
             for row in snapshot["rows"]]
    return {"data_kind": "synthetic", "usage_bronze_rows": 7, "arrival_bronze_rows": 12,
            "expected_usage_total": 680, "expected_daily_arrival_rows": len(daily),
            "expected_daily_valid_eta_rows": sum(row["barvlDt"] is not None for row in daily)}


def seed(output):
    output = Path(output).resolve()
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise ValueError("Fixture seeding requires a new/empty output directory")
    output.mkdir(parents=True, exist_ok=True)
    os.environ["SSDR_DATA_ROOT"] = str(output / "lake")
    os.environ["SSDR_STORAGE_PREFIX"] = "v2"
    from scripts.local_spark import local_spark
    from src.ingestion.ingest_subway_usage_daily import save_to_bronze_delta as save_usage
    from src.ingestion.ingest_subway_arrival_realtime import save_to_bronze_delta as save_arrival
    spark = local_spark("ssdr-bronze-fixture")
    try:
        rows = json.loads((ROOT / "fixtures/usage.json").read_text())
        for run in ["first", "retry"]:
            save_usage(rows, "20260128", spark=spark, captured_at=datetime(2026, 2, 1, 6), run_id=run)
        save_usage([dict(rows[0], REG_YMD="20260130", GTON_TNOPE="120")], "20260128",
                   spark=spark, captured_at=datetime(2026, 2, 1, 7), run_id="correction")
        for snapshot in json.loads((ROOT / "fixtures/arrival.json").read_text()):
            stamp = datetime.fromisoformat(snapshot["snapshot_ts"])
            rows = [dict(row, recptnDt=stamp.isoformat(sep=" ")) for row in snapshot["rows"]]
            for _ in range(2):
                save_arrival(rows, stamp, spark=spark)
    finally:
        spark.stop()
    evidence = fixture_expectations()
    (output / "seed.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True, type=Path)
    print(json.dumps(seed(parser.parse_args().output_dir)))


if __name__ == "__main__":
    main()
