from __future__ import annotations
import os
from typing import Optional
import boto3

def find_latest_parquet_key(bucket: str, prefix: str, *, region: str | None = None) -> str:
    """
    prefix 아래의 .parquet 객체 중 LastModified 기준 최신 key 반환
    """
    s3 = boto3.client("s3", region_name=region) if region else boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    latest_key: Optional[str] = None
    latest_ts = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            ts = obj["LastModified"]
            if latest_ts is None or ts > latest_ts:
                latest_ts = ts
                latest_key = key

    if latest_key is None:
        raise FileNotFoundError(f"No parquet found: s3://{bucket}/{prefix}")

    return latest_key

def parse_partition_from_key(key: str, *, has_hour: bool) -> tuple[str, str, str, str | None]:
    """
    key 예)
      bronze/subway_arrival/year=2025/month=12/day=11/hour=08/arrivals.parquet
      bronze/subway_usage/year=2025/month=12/day=11/usage.parquet
    """
    parts = key.split("/")
    y = [p for p in parts if p.startswith("year=")][0].split("=", 1)[1]
    m = [p for p in parts if p.startswith("month=")][0].split("=", 1)[1]
    d = [p for p in parts if p.startswith("day=")][0].split("=", 1)[1]
    h = None
    if has_hour:
        h = [p for p in parts if p.startswith("hour=")][0].split("=", 1)[1]
    return y, m, d, h
