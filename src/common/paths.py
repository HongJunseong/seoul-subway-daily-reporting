# src/common/paths.py
from __future__ import annotations

from datetime import datetime

def s3_bronze_arrival_key(snapshot_time: datetime) -> str:
    y = snapshot_time.strftime("%Y")
    m = snapshot_time.strftime("%m")
    d = snapshot_time.strftime("%d")
    h = snapshot_time.strftime("%H")
    return f"bronze/subway_arrival/year={y}/month={m}/day={d}/hour={h}/arrivals.parquet"

def s3_bronze_position_key(snapshot_time: datetime) -> str:
    y = snapshot_time.strftime("%Y")
    m = snapshot_time.strftime("%m")
    d = snapshot_time.strftime("%d")
    h = snapshot_time.strftime("%H")
    return f"bronze/subway_position/year={y}/month={m}/day={d}/hour={h}/positions.parquet"

def s3_bronze_usage_key(target_ymd: str) -> str:
    # target_ymd: "20251211"
    y = target_ymd[0:4]
    m = target_ymd[4:6]
    d = target_ymd[6:8]
    return f"bronze/subway_usage/year={y}/month={m}/day={d}/usage.parquet"