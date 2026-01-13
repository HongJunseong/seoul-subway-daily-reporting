from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime
import json

import requests
import pandas as pd
from dotenv import load_dotenv
import pendulum

# --- 프로젝트 루트 경로 세팅 ---
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from src.configs.settings import get_passenger_api_key
from src.common.config import load_s3_config
from pyspark.sql import SparkSession, functions as F

# 서울시 열린데이터광장 지하철 호선별 역별 승하차 인원 API
BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE_NAME = "CardSubwayStatsNew"


def fetch_subway_usage_daily(use_ymd: str, page_size: int = 1000, max_pages: int = 10) -> pd.DataFrame:
    api_key = get_passenger_api_key()

    all_pages: list[pd.DataFrame] = []
    start = 1
    page_no = 1

    while page_no <= max_pages:
        end = start + page_size - 1
        url = f"{BASE_URL}/{api_key}/json/{SERVICE_NAME}/{start}/{end}/{use_ymd}"
        print(f"[INFO] Request page {page_no}: {url}")

        resp = requests.get(url, timeout=15)
        print("[DEBUG] status:", resp.status_code)
        resp.raise_for_status()

        data = resp.json()
        if SERVICE_NAME not in data:
            print("[ERROR] Unexpected response structure:", list(data.keys()))
            raise RuntimeError(f"Unexpected response: {data}")

        svc = data[SERVICE_NAME]
        total_count = svc.get("list_total_count")
        result = svc.get("RESULT", {})
        code = result.get("CODE")
        msg = result.get("MESSAGE")
        print(f"[DEBUG] RESULT: CODE={code}, MESSAGE={msg}, total_count={total_count}")

        if code not in ("INFO-000", None):
            raise RuntimeError(f"API 오류: CODE={code}, MESSAGE={msg}")

        rows = svc.get("row", [])
        print(f"[DEBUG] page {page_no} rows:", len(rows))
        if not rows:
            break

        all_pages.append(pd.DataFrame(rows))

        if len(rows) < page_size:
            break

        try:
            tc_int = int(total_count) if total_count is not None else None
        except Exception:
            tc_int = None

        if tc_int is not None and end >= tc_int:
            break

        start += page_size
        page_no += 1

    if not all_pages:
        print("[WARN] No data returned from API (all_pages empty).")
        return pd.DataFrame()

    df = pd.concat(all_pages, ignore_index=True)
    print("[DEBUG] df.columns:", list(df.columns))
    return df


def _normalize_for_spark(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1) dict/list 같은 복잡 타입은 JSON 문자열로
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
            )

    # 2) 전부 null인 컬럼 제거
    all_null_cols = [c for c in df.columns if df[c].isna().all()]
    if all_null_cols:
        df = df.drop(columns=all_null_cols)

    # 3) Bronze 안정성: 전부 string 통일
    for c in df.columns:
        df[c] = df[c].astype("string")

    return df


def save_to_bronze_delta(df: pd.DataFrame, use_ymd: str) -> str:
    """
    Spark가 S3에 직접 Delta append
    path: s3a://{bucket}/bronze/subway_usage_delta
    partition: year, month, day  (use_ymd 기준)
    """
    s3cfg = load_s3_config()
    delta_path = f"s3a://{s3cfg.bucket}/bronze/subway_usage_delta"

    y, m, d = use_ymd[0:4], use_ymd[4:6], use_ymd[6:8]
    ingested_at = pendulum.now("Asia/Seoul").to_iso8601_string()
    run_ts = pendulum.now("Asia/Seoul").format("YYYYMMDDTHHmmss")

    df2 = df.copy()
    df2["ingested_at"] = ingested_at
    df2["run_ts"] = run_ts
    df2["year"] = y
    df2["month"] = m
    df2["day"] = d

    df2 = _normalize_for_spark(df2)

    spark = (
        SparkSession.builder
        .appName("bronze-usage-delta")
        .getOrCreate()
    )

    sdf = spark.createDataFrame(df2)
    sdf = sdf.withColumn("ingested_at", F.to_timestamp("ingested_at"))

    (sdf.write
        .format("delta")
        .mode("append")
        .partitionBy("year", "month", "day")
        .save(delta_path)
    )

    s3_uri = delta_path.replace("s3a://", "s3://")
    print(f"[OK] Saved {sdf.count()} rows to {s3_uri}")
    return s3_uri


def main():
    if len(sys.argv) > 1:
        use_ymd = sys.argv[1]
    else:
        # KST 기준 today - 4일
        use_ymd = pendulum.now("Asia/Seoul").subtract(days=4).format("YYYYMMDD")

    print("[INFO] Fetching CardSubwayStatsNew for use_ymd:", use_ymd)

    df = fetch_subway_usage_daily(use_ymd)
    print("[INFO] Rows fetched:", len(df))

    if len(df) > 0:
        print(df.head())
        save_to_bronze_delta(df, use_ymd)
    else:
        print("[WARN] Empty DataFrame. Check API key / date(use_ymd) / service status.")


if __name__ == "__main__":
    main()