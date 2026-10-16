"""Replay saved live source observations into a fresh local Delta lake; never calls an API."""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.capture_sources import new_output, write_json
from scripts.local_spark import local_spark
from src.observability.fingerprints import delta_fingerprint
from src.observability.source_baseline import validate_manifest
from src.observability.source_integrity import capture_version, integrity_summary, verify_payload
from src.common.dates import KST, validate_ymd
from src.observability.audit import DeltaAuditReader, audit_lake
from scripts.audit_lake import save_audit
from src.observability.health import pipeline_health, render_health

TABLES = [
    "bronze/subway_usage_delta", "bronze/subway_arrival_delta",
    "silver/fact_subway_usage_delta", "silver/fact_subway_arrival_delta",
    "gold/subway_usage/station_daily", "gold/subway_usage/line_daily",
    "gold/subway_arrival/station_daily_kpi",
]


def kst_stamp(value):
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None or stamp.utcoffset() != timedelta(hours=9):
        raise ValueError("source evidence timestamps must explicitly identify KST")
    return stamp


def load_capture(source):
    source = Path(source).resolve()
    if not source.is_relative_to((ROOT / ".work").resolve()):
        raise ValueError("saved observations must be in the private repository .work directory")
    manifest_path = source / "capture.json"
    if not manifest_path.resolve().is_relative_to(source):
        raise ValueError("capture manifest escaped capture directory")
    evidence = json.loads(manifest_path.read_text(encoding="utf-8"))
    if evidence.get("data_kind") != "observed" or evidence.get("status") != "collected":
        raise ValueError("incomplete capture cannot be published as a successful source experiment")
    version = capture_version(evidence)
    validate_manifest(evidence, datetime.now(KST))
    day = validate_ymd(evidence["usage_day"])
    slots = evidence["arrival_slots"]
    payloads, hashes = [], {}
    for name, item in [("usage.json", evidence["usage"])] + [
            (f"arrival-{i:02d}.json", slot) for i, slot in enumerate(slots)]:
        if item.get("status") != "collected":
            raise ValueError("failed source request cannot be published")
        path = source / name
        if not path.resolve().is_relative_to(source):
            raise ValueError("saved payload escaped capture directory")
        content = path.read_bytes()
        digest = verify_payload(content, item, version)
        payload = json.loads(content)
        rows = payload["rows"]
        if (not isinstance(rows, list) or not rows or not all(isinstance(r, dict) for r in rows)
                or len(rows) != item["rows"]):
            raise ValueError("saved row count differs from capture evidence")
        stamp = kst_stamp(payload["captured_at_kst"])
        if payload["captured_at_kst"] != item["captured_at_kst"]:
            raise ValueError("saved capture timestamp differs")
        if kst_stamp(item["request_started_at_kst"]) > stamp or stamp > kst_stamp(item["request_completed_at_kst"]):
            raise ValueError("capture timestamp outside request boundaries")
        if name == "usage.json":
            if len(rows) > 10000 or any(str(row.get("USE_YMD")) != day for row in rows):
                raise ValueError("usage payload outside bounded requested date")
        elif len(rows) > 1000:
            raise ValueError("arrival payload exceeded request limit")
        payloads.append(payload)
        hashes[name] = digest
    return evidence, payloads, hashes


def fingerprints(spark, lake):
    from delta.tables import DeltaTable
    result = {}
    for key in TABLES:
        path = str(lake / "v2" / key)
        version = int(DeltaTable.forPath(spark, path).history(1).select("version").first()[0])
        rows = delta_fingerprint(spark, path, version, allow_empty=True)
        result[key] = rows | {"delta_version": version}
    return result


def verify_replay(before, after):
    if set(before) != set(after):
        raise ValueError("replay table set differs")
    for key in before:
        if any(before[key][field] != after[key][field] for field in ("rows", "sha256")):
            raise ValueError("saved-source replay changed table content: " + key)
        if key.startswith("bronze/") and before[key]["delta_version"] != after[key]["delta_version"]:
            raise ValueError("saved-source replay changed Bronze version")


def process(source, output):
    capture, payloads, hashes = load_capture(source)
    output = new_output(output)
    lake = output / "lake"
    os.environ.update(SSDR_DATA_ROOT=str(lake), SSDR_STORAGE_PREFIX="v2", SSDR_DATA_KIND="observed")
    from src.ingestion.ingest_subway_usage_daily import save_to_bronze_delta as save_usage
    from src.ingestion.ingest_subway_arrival_realtime import save_to_bronze_delta as save_arrival
    from src.processing.build_fact_subway_usage_daily_spark import build_usage_silver
    from src.processing.build_gold_subway_usage_daily_spark import build_usage_gold
    from src.processing.build_fact_subway_arrival_spark import build_arrival_silver
    from src.processing.build_gold_subway_arrival_spark import build_arrival_gold

    spark, started = local_spark("ssdr-saved-live-source"), time.perf_counter()
    try:
        usage_day = capture["usage_day"]
        save_usage(payloads[0]["rows"], usage_day, spark=spark,
                   captured_at=kst_stamp(payloads[0]["captured_at_kst"]).replace(tzinfo=None),
                   run_id="observed-usage")
        arrival_days = set()
        for i, payload in enumerate(payloads[1:]):
            stamp = kst_stamp(payload["captured_at_kst"]).replace(tzinfo=None)
            arrival_days.add(stamp.strftime("%Y%m%d"))
            save_arrival(payload["rows"], stamp, spark=spark, run_id=f"observed-arrival-{i}")

        def rebuild():
            build_usage_silver(spark, usage_day)
            build_usage_gold(spark, usage_day)
            for day in sorted(arrival_days):
                build_arrival_silver(spark, day=day)
                build_arrival_gold(spark, day)

        rebuild()
        before = fingerprints(spark, lake)
        rebuild()
        after = fingerprints(spark, lake)
        verify_replay(before, after)
    finally:
        spark.stop()
    processing_seconds = round(time.perf_counter() - started, 6)
    start = kst_stamp(capture["started_at_kst"])
    end = kst_stamp(capture["arrival_slots"][-1]["captured_at_kst"]) + timedelta(microseconds=1)
    quality = audit_lake(DeltaAuditReader(lake), usage_day, start, end, datetime.now(KST),
                         capture["cadence_seconds"])
    quality["data_kind"] = "observed"
    save_audit(quality, output / "audit")
    health = pipeline_health(quality)
    write_json(output / "health.json", health)
    (output / "health.md").write_text(render_health(health), encoding="utf-8")
    # Only capture metadata, content hashes and aggregate metrics are suitable for public evidence.
    evidence = {
        "schema_version": 1, "data_kind": "observed", "actual_api_collection": True,
        "saved_source_replay_verified": True, "capture": capture,
        "source_file_sha256": hashes, "source_integrity": integrity_summary(capture, hashes),
        "before_replay": before, "after_replay": after,
        "quality": quality, "local_processing_and_replay_seconds": processing_seconds,
        "scope": "bounded private local experiment; no S3, Databricks, Kubernetes or notification execution",
    }
    write_json(output / "evidence.json", evidence)
    print(json.dumps({"status": quality["status"], "replay_verified": True,
                      "usage_rows": after["silver/fact_subway_usage_delta"]["rows"],
                      "arrival_rows": after["silver/fact_subway_arrival_delta"]["rows"],
                      "issues": quality["issues"]}, ensure_ascii=False))
    return 2 if quality["status"] == "failed" else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    return process(args.capture_dir, args.output_dir)


if __name__ == "__main__":
    raise SystemExit(main())
