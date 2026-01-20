# src/report/build_report_context_daily.py
from __future__ import annotations

import os
import sys
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional

import pandas as pd

from src.common.config import load_s3_config

# Lag days (지연 반영)
USAGE_LAG_DAYS = 4
ARRIVAL_LAG_DAYS = 0


# S3 Parquet Loader (prefix 아래 part-*.parquet 전부 읽기)
#   - s3fs 없이 boto3 + pyarrow로 읽음
def _require(pkg: str) -> None:
    try:
        __import__(pkg)
    except Exception as e:
        raise RuntimeError(f"Missing dependency: {pkg}. Install: pip install {pkg}") from e


def _list_s3_keys(bucket: str, prefix: str, region: str) -> list[str]:
    import boto3

    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")

    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            k = obj.get("Key")
            if k:
                keys.append(k)
    return keys


def _read_parquet_prefix_s3(bucket: str, prefix: str, region: str) -> pd.DataFrame:
    """
    bucket/prefix 아래의 *.parquet(보통 part-....parquet)들을 모두 읽어 pandas로 합친다.
    prefix 예: "gold/subway_usage/station_daily/year=2025/month=12/day=13/"
    """
    _require("boto3")
    _require("pyarrow")

    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq

    s3 = boto3.client("s3", region_name=region)

    keys = _list_s3_keys(bucket=bucket, prefix=prefix, region=region)
    parquet_keys = [
        k for k in keys
        if k.lower().endswith(".parquet")
        and not Path(k).name.startswith("_")   # _SUCCESS 같은 거 제외
        and "/_" not in k
    ]

    if not parquet_keys:
        print(f"[WARN] No parquet files under s3://{bucket}/{prefix}")
        return pd.DataFrame()

    tables: list[pa.Table] = []
    for k in parquet_keys:
        try:
            obj = s3.get_object(Bucket=bucket, Key=k)
            raw = obj["Body"].read()
            table = pq.read_table(BytesIO(raw))
            tables.append(table)
        except Exception as e:
            print(f"[WARN] Failed reading parquet: s3://{bucket}/{k} ({e})")

    if not tables:
        return pd.DataFrame()

    try:
        combined = pa.concat_tables(tables, promote=True)
        return combined.to_pandas()
    except Exception:
        # fallback
        dfs = [t.to_pandas() for t in tables]
        return pd.concat(dfs, ignore_index=True)


# 공통: 컬럼 이름 유연하게 찾기 (pandas)
def _find_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def get_date_parts(target_ymd: str) -> Tuple[str, str, str]:
    return target_ymd[:4], target_ymd[4:6], target_ymd[6:8]


# 1) GOLD 로더들 (S3에서 로드)
def load_usage_gold(target_ymd: str) -> pd.DataFrame:
    cfg = load_s3_config()
    bucket = cfg.bucket
    region = getattr(cfg, "region", None) or os.getenv("AWS_REGION", "ap-northeast-2")

    target_date = datetime.strptime(target_ymd, "%Y%m%d").date()
    usage_date = target_date - timedelta(days=USAGE_LAG_DAYS)

    y = usage_date.strftime("%Y")
    m = usage_date.strftime("%m")
    d = usage_date.strftime("%d")

    prefix = f"gold/subway_usage/station_daily/year={y}/month={m}/day={d}/"
    print(f"[INFO] Load usage GOLD (D-{USAGE_LAG_DAYS}) from s3://{bucket}/{prefix}")

    df = _read_parquet_prefix_s3(bucket=bucket, prefix=prefix, region=region)
    print(f"[INFO] Loaded usage GOLD rows: {len(df)}")
    return df


def load_arrival_kpi_gold(target_ymd: str) -> pd.DataFrame:
    cfg = load_s3_config()
    bucket = cfg.bucket
    region = getattr(cfg, "region", None) or os.getenv("AWS_REGION", "ap-northeast-2")

    target_date = datetime.strptime(target_ymd, "%Y%m%d").date()
    arr_date = target_date - timedelta(days=ARRIVAL_LAG_DAYS)

    y = arr_date.strftime("%Y")
    m = arr_date.strftime("%m")
    d = arr_date.strftime("%d")

    prefix = f"gold/subway_arrival/station_daily_kpi/year={y}/month={m}/day={d}/"
    print(f"[INFO] Load arrival GOLD from s3://{bucket}/{prefix}")

    df = _read_parquet_prefix_s3(bucket=bucket, prefix=prefix, region=region)
    print(f"[INFO] Loaded arrival station_daily_kpi GOLD rows: {len(df)}")
    return df


# 2) usage 요약
def summarize_usage(df_usage: pd.DataFrame) -> Dict[str, Any]:
    if df_usage.empty:
        return {"has_data": False}

    station_col = _find_first_existing_column(df_usage, ["station_name", "stnNm", "STN_NM", "역명"])
    line_col = _find_first_existing_column(df_usage, ["line_name", "lineNm", "LINE_NM", "호선명"])
    total_col = _find_first_existing_column(df_usage, ["totalNope", "TOTALNOPE", "tot_nope", "승하차총승객수", "total"])
    rank_col = _find_first_existing_column(df_usage, ["rank_daily", "RANK_DAILY", "rank", "순위"])

    if not station_col or not total_col:
        print("[ERROR] usage GOLD missing required columns for summarization")
        print("  columns:", list(df_usage.columns))
        return {"has_data": False}

    select_cols = [station_col, total_col]
    if line_col:
        select_cols.append(line_col)
    if rank_col:
        select_cols.append(rank_col)

    tmp_top = (
        df_usage.sort_values(total_col, ascending=False)
        .head(10)[select_cols]
        .copy()
    )

    rename_map = {station_col: "station_name", total_col: "totalNope"}
    if line_col:
        rename_map[line_col] = "line_name"
    if rank_col:
        rename_map[rank_col] = "rank_daily"

    tmp_top.rename(columns=rename_map, inplace=True)

    if "line_name" not in tmp_top.columns:
        tmp_top["line_name"] = None
    if "rank_daily" not in tmp_top.columns:
        tmp_top["rank_daily"] = None

    top_stations = tmp_top[["station_name", "line_name", "totalNope", "rank_daily"]].to_dict(orient="records")

    line_usage_stats: list[dict] = []
    if line_col:
        tmp_line = (
            df_usage.groupby(line_col)[total_col]
            .agg(["mean", "max"])
            .reset_index()
            .rename(columns={line_col: "line_name", "mean": "avg_totalNope", "max": "max_totalNope"})
        )
        line_usage_stats = tmp_line.to_dict(orient="records")

    return {
        "has_data": True,
        "top_stations_by_total": top_stations,
        "line_usage_stats": line_usage_stats,
    }


# 3) arrival 요약 (station_daily_kpi)
def summarize_arrival(df_arr: pd.DataFrame, df_usage: pd.DataFrame) -> Dict[str, Any]:
    if df_arr.empty:
        return {"has_data": False}

    by_line = (
        df_arr.groupby("line_id")
        .agg(
            avg_wait_sec=("avg_wait_sec", "mean"),
            p95_wait_sec=("p95_wait_sec", "mean"),
            long_wait_ratio_5=("long_wait_ratio_5", "mean"),
            express_ratio=("express_ratio", "mean"),
        )
        .reset_index()
        .to_dict(orient="records")
    )

    painful_stations: list[dict] = []

    if not df_usage.empty:
        usage_station_col = _find_first_existing_column(df_usage, ["station_name", "stnNm", "STN_NM", "역명"])
        usage_total_col = _find_first_existing_column(df_usage, ["totalNope", "TOTALNOPE", "tot_nope", "승하차총승객수", "total"])

        if usage_station_col and usage_total_col and "station_name" in df_arr.columns:
            usage_for_merge = df_usage[[usage_station_col, usage_total_col]].copy()
            usage_for_merge.rename(columns={usage_station_col: "station_name", usage_total_col: "totalNope"}, inplace=True)

            merged = pd.merge(df_arr, usage_for_merge, on="station_name", how="inner")

            if not merged.empty and "avg_wait_sec" in merged.columns and "totalNope" in merged.columns:
                merged["avg_wait_norm"] = merged["avg_wait_sec"] / (merged["avg_wait_sec"].max() + 1e-6)
                merged["totalNope_norm"] = merged["totalNope"] / (merged["totalNope"].max() + 1e-6)
                merged["pain_score"] = merged["avg_wait_norm"] * merged["totalNope_norm"]

                select_cols = [
                    "station_name",
                    "line_id",
                    "avg_wait_sec",
                    "p95_wait_sec",
                    "long_wait_ratio_5",
                    "totalNope",
                    "pain_score",
                ]
                select_cols = [c for c in select_cols if c in merged.columns]

                painful_stations = (
                    merged.sort_values("pain_score", ascending=False)
                    .head(10)[select_cols]
                    .to_dict(orient="records")
                )

    last_train_info: list[dict] = []
    if "last_train_dt" in df_arr.columns:
        tmp = df_arr[~df_arr["last_train_dt"].isna()].copy()
        if not tmp.empty:
            tmp["last_train_time_str"] = tmp["last_train_dt"].astype(str)
            cols = ["station_name", "line_id", "last_train_time_str", "last_train_wait_sec"]
            cols = [c for c in cols if c in tmp.columns]

            last_train_info = (
                tmp.sort_values("last_train_dt", ascending=False)
                .head(10)[cols]
                .to_dict(orient="records")
            )

    late_night_hotspots: list[dict] = []
    if "late_night_events" in df_arr.columns:
        cols = ["station_name", "line_id", "late_night_events"]
        cols = [c for c in cols if c in df_arr.columns]

        late_night_hotspots = (
            df_arr.sort_values("late_night_events", ascending=False)
            .head(10)[cols]
            .to_dict(orient="records")
        )

    return {
        "has_data": True,
        "line_wait_stats": by_line,
        "painful_stations": painful_stations,
        "last_train_info": last_train_info,
        "late_night_hotspots": late_night_hotspots,
    }

# 4) 전체 Report Context
def build_report_context(target_ymd: str) -> Dict[str, Any]:
    print(f"[INFO] Building report context for {target_ymd}")
    print(f"[INFO] LAG: usage={USAGE_LAG_DAYS}, arrival={ARRIVAL_LAG_DAYS}")

    usage = load_usage_gold(target_ymd)
    arrival_kpi = load_arrival_kpi_gold(target_ymd)

    usage_ctx = summarize_usage(usage)
    arrival_ctx = summarize_arrival(arrival_kpi, usage)

    ctx = {
        "target_ymd": target_ymd,
        "usage": usage_ctx,
        "arrival": arrival_ctx,
    }
    return ctx


def main():
    target_ymd = datetime.today().strftime("%Y%m%d")
    ctx = build_report_context(target_ymd)

    print("\n=== REPORT CONTEXT (SUMMARY) ===")
    print("target_ymd:", ctx["target_ymd"])
    print("\n[usage]")
    print(ctx["usage"])
    print("\n[arrival]")
    print(ctx["arrival"])

if __name__ == "__main__":
    main()
