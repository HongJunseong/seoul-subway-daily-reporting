"""Persist each DAG run separately so delivery retries never generate a new report."""
from __future__ import annotations
import hashlib
import os
from pathlib import Path

from src.common.config import load_s3_config
from src.common.dates import validate_ymd
from src.common.storage import storage_prefix
from src.report.publication import (decode_publication, publication_outputs, save_local_publication,
                                    save_s3_publication, strict_json)


def report_directory():
    return Path(os.getenv("SSDR_REPORT_OUTPUT_DIR", str(Path(__file__).resolve().parents[2] / "data/report_text/daily")))


def run_publication_path(target_ymd, run_id):
    target_ymd = validate_ymd(target_ymd)
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("Report run_id must not be empty")
    identity = hashlib.sha256(run_id.encode()).hexdigest()
    return report_directory() / "runs" / identity / f"subway_report_{target_ymd}.publication.json"


def saved_outputs(path, expected_sha256=None):
    content = Path(path).read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if expected_sha256 is not None and digest != expected_sha256:
        raise ValueError("Saved report differs from generated publication")
    decoded = decode_publication(content)
    bundle = strict_json(content)
    outputs = publication_outputs(decoded["report"], bundle["context"], decoded["manifest"])
    return outputs, decoded["manifest"], digest


def generate_report_for_run(target_ymd, run_id):
    path = run_publication_path(target_ymd, run_id)
    if not path.exists():
        from src.report.generate_daily_report import generate_daily_report
        generate_daily_report(target_ymd, save_to_s3=False, output_dir=path.parent)
    outputs, manifest, digest = saved_outputs(path)
    if (manifest["target_ymd"] != target_ymd
            or manifest["report_mode"] != os.getenv("SSDR_REPORT_MODE", "template")):
        raise ValueError("Saved report does not match this run's date or mode")
    # Keep the existing human-facing date exports; delivery uses only the run snapshot.
    save_local_publication(report_directory(), outputs)
    return {"target_ymd": target_ymd, "publication_path": str(path), "publication_sha256": digest}


def upload_saved_publication(path, *, expected_sha256=None, client=None):
    outputs, manifest, digest = saved_outputs(path, expected_sha256)
    if os.getenv("SSDR_DATA_ROOT"):
        return {"destination": "local", "publication_sha256": digest}
    cfg = load_s3_config()
    prefix = "/".join(p for p in [storage_prefix(), "report_text/daily"] if p)
    try:
        if client is None:
            import boto3
            from botocore.config import Config
            client = boto3.client("s3", region_name=cfg.region,
                                  config=Config(retries={"total_max_attempts": 1}, connect_timeout=10, read_timeout=30))
        save_s3_publication(client, cfg.bucket, prefix, outputs)
    except Exception:
        raise RuntimeError("S3 report upload failed; retry the saved publication") from None
    return {"destination": "s3", "target_ymd": manifest["target_ymd"], "publication_sha256": digest}


def upload_report_for_run(target_ymd, run_id, receipt, *, client=None):
    path = run_publication_path(target_ymd, run_id)
    if (not isinstance(receipt, dict) or receipt.get("target_ymd") != target_ymd
            or receipt.get("publication_path") != str(path)
            or not isinstance(receipt.get("publication_sha256"), str)
            or len(receipt["publication_sha256"]) != 64):
        raise ValueError("Report receipt does not match this DAG run")
    return upload_saved_publication(path, expected_sha256=receipt["publication_sha256"], client=client)


def main():
    import argparse
    import json
    parser = argparse.ArgumentParser(description="Upload a saved publication without invoking the LLM")
    parser.add_argument("publication", type=Path)
    parser.add_argument("--sha256", required=True, help="Expected publication SHA-256 from the generating task")
    args = parser.parse_args()
    print(json.dumps(upload_saved_publication(args.publication, expected_sha256=args.sha256)))


if __name__ == "__main__":
    main()
