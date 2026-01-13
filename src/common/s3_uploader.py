# src/common/s3_uploader.py
from __future__ import annotations

from pathlib import Path
import boto3

def upload_file_to_s3(local_path: Path, bucket: str, key: str, *, region: str | None = None) -> None:
    cli = boto3.client("s3", region_name=region) if region else boto3.client("s3")
    cli.upload_file(str(local_path), bucket, key)
