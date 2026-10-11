"""Bounded local synthetic small-file experiment; no production maintenance or API collection."""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta
import hashlib
from importlib.metadata import version as package_version
import json
import os
from pathlib import Path
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.capture_sources import new_output, write_json
from scripts.local_spark import local_spark
from src.common.dates import KST
from src.common.storage import table_path

BRONZE = "bronze/subway_arrival_delta"
SILVER = "silver/fact_subway_arrival_delta"
GOLD = "gold/arrival_metrics_15m_delta"
MAX_ROWS = 48000


def closed_hour(start, as_of):
    for stamp in (start, as_of):
        if stamp.tzinfo is None or stamp.utcoffset() != timedelta(hours=9):
            raise ValueError("hour scope requires explicit KST")
    if (start.minute, start.second, start.microsecond) != (0, 0, 0):
        raise ValueError("scope must start at an exact hour")
    if start + timedelta(hours=1) > as_of:
        raise ValueError("cannot compact an open or future hour")
    return dict(year=f"{start:%Y}", month=f"{start:%m}", day=f"{start:%d}", hour=f"{start:%H}")


def predicate(partition):
    if set(partition) != {"year", "month", "day", "hour"}:
        raise ValueError("expected one arrival hour partition")
    stamp = datetime.strptime("".join(partition[k] for k in ("year", "month", "day", "hour")), "%Y%m%d%H")
    if partition != dict(year=f"{stamp:%Y}", month=f"{stamp:%m}", day=f"{stamp:%d}", hour=f"{stamp:%H}"):
        raise ValueError("noncanonical hour partition")
    return " AND ".join(f"{key}='{partition[key]}'" for key in ("year", "month", "day", "hour"))


def layout(actions, partition):
    selected, outside = [], []
    for row in actions:
        if type(row["size_bytes"]) is not int or row["size_bytes"] <= 0:
            raise ValueError("invalid active file size")
        target = all(str(row[f"partition.{key}"]) == value for key, value in partition.items())
        (selected if target else outside).append(row)
    sizes = [row["size_bytes"] for row in selected]
    # Paths are hashed only; neither Parquet contents nor private file names are exposed.
    outside_digest = hashlib.sha256(json.dumps(
        sorted((row["path"], row["size_bytes"]) for row in outside), separators=(",", ":")).encode()).hexdigest()
    return dict(active_table_files=len(actions), selected_hour_files=len(selected),
                selected_hour_bytes=sum(sizes), selected_min_file_bytes=min(sizes) if sizes else None,
                selected_max_file_bytes=max(sizes) if sizes else None,
                outside_files=len(outside), outside_paths_and_sizes_sha256=outside_digest)


def inventory(path, version, partition):
    from deltalake import DeltaTable
    import pyarrow as pa
    return layout(pa.table(DeltaTable(str(path), version=version).get_add_actions(flatten=True)).to_pylist(), partition)


def version(spark, path):
    from delta.tables import DeltaTable
    return int(DeltaTable.forPath(spark, str(path)).history(1).select("version").first()[0])


def fingerprint(spark, path, revision):
    from pyspark.sql import functions as F
    frame = spark.read.format("delta").option("versionAsOf", revision).load(str(path))
    payloads = frame.select(F.to_json(F.struct(*sorted(frame.columns)), {
        "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX", "ignoreNullFields": "false",
    }).alias("payload")).limit(MAX_ROWS + 1).collect()
    if not payloads or len(payloads) > MAX_ROWS:
        raise ValueError("empty or oversized bounded fixture")
    rows = sorted(row.payload for row in payloads)
    return {"rows": len(rows), "sha256": hashlib.sha256(("\n".join(rows) + "\n").encode()).hexdigest()}


def sample_query(spark, path, revision, partition, start):
    from pyspark.sql import functions as F
    began = time.perf_counter()
    frame = (spark.read.format("delta").option("versionAsOf", revision).load(str(path))
             .where(predicate(partition)).where(F.col("snapshot_ts") >= F.lit(start)))
    result = frame.agg(F.count("*").alias("rows"), F.sum(F.col("barvlDt").cast("long")).alias("eta_sum"),
                       F.sum(F.length("raw_payload")).alias("payload_chars")).first().asDict()
    return result, round(time.perf_counter() - began, 6)


def paired_reads(spark, path, before, after, partition, hour):
    result = {}
    for label, lower in (("hour", hour), ("last_15_minutes", hour + timedelta(minutes=45))):
        observations = {before: [], after: []}
        expected = None
        # Both revisions get a warm-up; sample order alternates, with no persisted Spark cache.
        for revision in (before, after):
            values, _ = sample_query(spark, path, revision, partition, lower)
            if expected is not None and values != expected:
                raise ValueError("query result changed during compaction")
            expected = values
        for pair in range(5):
            for revision in ((before, after) if pair % 2 == 0 else (after, before)):
                values, seconds = sample_query(spark, path, revision, partition, lower)
                if values != expected:
                    raise ValueError("pinned query result changed")
                observations[revision].append(seconds)
        result[label] = {"query_result": expected, "before_seconds": observations[before],
                         "after_seconds": observations[after],
                         "before_median_seconds": statistics.median(observations[before]),
                         "after_median_seconds": statistics.median(observations[after])}
    return result


def verify_change(before_rows, after_rows, before_files, after_files, actions):
    if before_rows != after_rows:
        raise ValueError("compaction changed full logical rows")
    if (before_files["outside_files"] != after_files["outside_files"]
            or before_files["outside_paths_and_sizes_sha256"] != after_files["outside_paths_and_sizes_sha256"]):
        raise ValueError("compaction rewrote an adjacent partition")
    changes = [row[key] for row in actions for key in ("add", "remove") if key in row]
    if not changes or any(row.get("dataChange") is not False for row in changes):
        raise ValueError("maintenance must use dataChange=false")
    if after_files["selected_hour_files"] >= before_files["selected_hour_files"]:
        raise ValueError("experiment did not reduce selected hour files")


def seed(spark, snapshots, rows_per_snapshot):
    from src.ingestion.ingest_subway_arrival_realtime import save_to_bronze_delta
    began = time.perf_counter()
    start = datetime(2026, 1, 31, 22, tzinfo=KST)
    for slot in range(snapshots):
        stamp = start + timedelta(minutes=5 * slot)
        rows = [dict(statnId=str(10020000+i), statnNm=f"가상역{i}", subwayId="1002",
                     updnLine="상행", btrainNo=str(20000+i), bstatnId="10020002",
                     bstatnNm="가상종착역", barvlDt=str((i*37 + slot*13) % 1200),
                     recptnDt=(stamp-timedelta(seconds=30)).isoformat(sep=" "), lstcarAt="0")
                for i in range(rows_per_snapshot)]
        save_to_bronze_delta(rows, stamp, spark=spark, run_id=f"synthetic-layout-{slot:02d}")
    return round(time.perf_counter()-began, 6)


def rebuild(spark):
    from src.processing.build_fact_subway_arrival_spark import build_arrival_silver
    from src.processing.build_gold_arrival_metrics_15m_spark import build_arrival_metrics
    for end in (datetime(2026, 2, 1), datetime(2026, 2, 1, 0, 15)):
        build_arrival_silver(spark, end)
        build_arrival_metrics(spark, end)
    return {key: fingerprint(spark, table_path(key), version(spark, table_path(key))) for key in (SILVER, GOLD)}


def experiment(output, *, snapshots=36, rows_per_snapshot=1000):
    if (type(snapshots) is not int or not 25 <= snapshots <= 48
            or type(rows_per_snapshot) is not int or not 1 <= rows_per_snapshot <= 1000):
        raise ValueError("bounded synthetic input requires 25..48 snapshots and 1..1000 rows each")
    output = new_output(output)
    os.environ["SSDR_DATA_ROOT"] = str(output / "lake")
    os.environ["SSDR_STORAGE_PREFIX"] = "v2"
    hour = datetime(2026, 1, 31, 23, tzinfo=KST)
    partition = closed_hour(hour, datetime(2026, 2, 1, 2, tzinfo=KST))
    spark = local_spark("ssdr-small-files")
    started = time.perf_counter()
    try:
        seeded_seconds = seed(spark, snapshots, rows_per_snapshot)
        path = Path(table_path(BRONZE))
        before = version(spark, path)
        before_rows, before_files = fingerprint(spark, path, before), inventory(path, before, partition)
        downstream_before = rebuild(spark)
        # A dedicated fixture is the sole writer; there is no general CLI for existing tables.
        from delta.tables import DeltaTable
        table = DeltaTable.forPath(spark, str(path))
        compact_start = time.perf_counter()
        metrics = table.optimize().where(predicate(partition)).executeCompaction().first()["metrics"]
        compact_seconds = round(time.perf_counter()-compact_start, 6)
        after = version(spark, path)
        if after != before + 1:
            raise ValueError("unexpected concurrent commit in dedicated fixture")
        after_rows, after_files = fingerprint(spark, path, after), inventory(path, after, partition)
        log = path / "_delta_log" / f"{after:020d}.json"
        actions = [json.loads(line) for line in log.read_text().splitlines()]
        verify_change(before_rows, after_rows, before_files, after_files, actions)
        if fingerprint(spark, path, before) != before_rows:
            raise ValueError("old full snapshot was not preserved after compaction")
        read_samples = paired_reads(spark, path, before, after, partition, hour)
        downstream_after = rebuild(spark)
        if downstream_before != downstream_after or version(spark, path) != after:
            raise ValueError("downstream replay changed results or Bronze version")
        noop = table.optimize().where(predicate(partition)).executeCompaction().first()["metrics"]
        if noop["numFilesAdded"] != 0 or noop["numFilesRemoved"] != 0 or version(spark, path) != after:
            raise ValueError("second compaction was not a physical no-op")
        result = {
            "schema_version": 1, "verified": True, "data_kind": "synthetic",
            "spark_version": spark.version, "delta_version": package_version("delta-spark"),
            "input": {"snapshots": snapshots, "rows_per_snapshot": rows_per_snapshot,
                      "rows": snapshots*rows_per_snapshot, "cadence_seconds": 300,
                      "first_snapshot_kst": "2026-01-31T22:00:00+09:00",
                      "seeding_seconds": seeded_seconds},
            "selected_hour_kst": hour.isoformat(), "partition": partition,
            "before": {"version": before, "logical_rows": before_rows, "files": before_files},
            "after": {"version": after, "logical_rows": after_rows, "files": after_files},
            "optimize": {"seconds": compact_seconds, "num_files_added": metrics["numFilesAdded"],
                         "num_files_removed": metrics["numFilesRemoved"], "data_change_false": True},
            "adjacent_partition_files_unchanged": True, "time_travel_rows_match": True,
            "downstream_replay": {"full_rows_before": downstream_before, "full_rows_after": downstream_after,
                                  "bronze_version_unchanged": True},
            "second_compaction_noop": True, "read_samples": read_samples,
            "elapsed_seconds": round(time.perf_counter()-started, 6),
            "limitations": ["Dedicated local[2] synthetic lake; no API, cloud, Kubernetes or production workload",
                            "Five paired warm reads per query/version; OS/JVM/cache effects remain",
                            "Candidate active file bytes are not measured IO bytes or S3 request cost",
                            "Compaction creates a dataChange=false Bronze commit; source replay does not",
                            "No VACUUM or automatic maintenance schedule; old snapshots remain readable"],
        }
        write_json(output / "evidence.json", result)
        return result
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--snapshots", type=int, default=36)
    parser.add_argument("--rows-per-snapshot", type=int, default=1000)
    args = parser.parse_args()
    result = experiment(args.output_dir, snapshots=args.snapshots, rows_per_snapshot=args.rows_per_snapshot)
    print(json.dumps({"verified": result["verified"], "rows": result["input"]["rows"],
                      "files_before": result["before"]["files"]["selected_hour_files"],
                      "files_after": result["after"]["files"]["selected_hour_files"]}))


if __name__ == "__main__":
    main()
