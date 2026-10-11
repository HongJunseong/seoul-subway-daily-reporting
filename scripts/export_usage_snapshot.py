"""Export a committed v2 Bronze snapshot, never enumerate internal Parquet.

S3 is opt-in. A local Delta root can be used without AWS access.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pyspark.sql import functions as F
from src.common.dates import validate_ymd
from src.jobs.spark_common import build_spark


def export_snapshot(spark, root, use_ymd, output, at_version=None):
    validate_ymd(use_ymd)
    output = Path(output).absolute()
    if output.exists():
        raise ValueError("export destination already exists; use a new directory")
    if at_version is None:
        at_version = int(spark.sql(
            "DESCRIBE HISTORY delta.`" + root.replace("`", "``") + "` LIMIT 1"
        ).first()["version"])
    frame = (spark.read.format("delta").option("versionAsOf", at_version).load(root)
             .where(F.col("USE_YMD") == use_ymd))
    # Preserve the v2 lineage schema. The upload is a snapshot, not a new observation.
    from src.quality.checks import require_columns, require_nonempty
    require_columns(frame, ["raw_payload", "record_hash", "ingested_at", "run_id", "USE_YMD"])
    require_nonempty(frame, "export Bronze snapshot")
    count = frame.count()
    frame.coalesce(1).write.mode("errorifexists").parquet(str(output / "data"))
    manifest = {"source_format": "delta", "source_version": at_version,
                "use_ymd": use_ymd, "row_count": count, "export_format": "parquet_snapshot"}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--delta-root", required=True)
    parser.add_argument("--use-ymd", required=True, type=validate_ymd)
    parser.add_argument("--output", required=True)
    parser.add_argument("--version", type=int)
    parser.add_argument("--allow-s3", action="store_true")
    args = parser.parse_args()
    if args.delta_root.startswith(("s3:", "s3a:")) and not args.allow_s3:
        parser.error("S3 export requires --allow-s3")
    spark = build_spark("databricks-bronze-snapshot-export")
    try:
        print(json.dumps(export_snapshot(spark, args.delta_root, args.use_ymd,
                                        args.output, args.version), ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
