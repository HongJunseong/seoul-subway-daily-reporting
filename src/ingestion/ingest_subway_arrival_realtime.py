from __future__ import annotations

import sys
from datetime import datetime
import json

import requests
import pandas as pd
from src.utils.time import now_kst

from src.common.get_api import get_arrival_api_key
from src.common.config import load_s3_config
from pyspark.sql import SparkSession, functions as F


BASE_URL = "http://swopenapi.seoul.go.kr/api/subway"
SERVICE_NAME = "realtimeStationArrival"


def fetch_realtime_arrival_all_json(start_index: int = 1, end_index: int = 1000) -> pd.DataFrame:
    """
    ALL 일괄 호출 (JSON)
    - start/end는 1부터 시작하는 게 안전함
    - end를 크게 주면 호출 횟수 줄어듦 (API가 허용하는 범위 내에서)
    """
    api_key = get_arrival_api_key()
    url = f"{BASE_URL}/{api_key}/json/{SERVICE_NAME}/{start_index}/{end_index}/ALL"

    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if "realtimeArrivalList" not in data:
        # 에러 응답이면 상세 출력
        print(f"[DEBUG] raw response = {data}")
        if "errorMessage" in data:
            em = data["errorMessage"]
            raise RuntimeError(
                f"Arrival API error: status={em.get('status')} code={em.get('code')} msg={em.get('message')}"
            )
        raise RuntimeError(f"Unknown response keys={list(data.keys())}")

    rows = data["realtimeArrivalList"]
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["query_station_name"] = "ALL"
    return df


def _normalize_for_spark(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1) dict/list 같은 복잡 타입은 JSON 문자열로 변환 (Spark 스키마 추론 실패 방지)
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
            )

    # 2) 전부 NaN/None인 컬럼은 제거 (CANNOT_DETERMINE_TYPE 최빈 원인)
    all_null_cols = [c for c in df.columns if df[c].isna().all()]
    if all_null_cols:
        df = df.drop(columns=all_null_cols)

    # 3) Bronze는 안정성 위해 문자열로 통일
    for c in df.columns:
        df[c] = df[c].astype("string")

    return df


def save_to_bronze_delta(df: pd.DataFrame, snapshot_time: datetime) -> str:
    s3cfg = load_s3_config()
    delta_path = f"s3a://{s3cfg.bucket}/bronze/subway_arrival_delta"

    y = snapshot_time.strftime("%Y")
    m = snapshot_time.strftime("%m")
    d = snapshot_time.strftime("%d")
    h = snapshot_time.strftime("%H")
    mm = snapshot_time.strftime("%M")
    run_ts = snapshot_time.strftime("%Y%m%dT%H%M%S")

    df2 = df.copy()
    df2["snapshot_ts"] = snapshot_time.isoformat()
    df2["run_ts"] = run_ts
    df2["year"] = y
    df2["month"] = m
    df2["day"] = d
    df2["hour"] = h
    df2["minute"] = mm

    df2 = _normalize_for_spark(df2)

    spark = (
        SparkSession.builder
        .appName("bronze-arrival-delta")
        .getOrCreate()
    )

    sdf = spark.createDataFrame(df2)
    sdf = sdf.withColumn("snapshot_ts", F.to_timestamp("snapshot_ts"))

    (sdf.write
        .format("delta")
        .mode("append")
        .partitionBy("year", "month", "day", "hour", "minute")
        .save(delta_path)
    )

    return delta_path.replace("s3a://", "s3://")


def main():
    snapshot_time = now_kst()
    print(f"[INFO] Fetching realtime arrivals (ALL) at snapshot: {snapshot_time}")

    # 전체 역 정보 호출
    df_all = fetch_realtime_arrival_all_json(start_index=1, end_index=1000)
    print("[INFO] Total arrival rows:", len(df_all))

    sample_cols = [c for c in ["subwayId", "statnNm", "trainLineNm", "arvlMsg2", "arvlMsg3"] if c in df_all.columns]
    if sample_cols:
        print(df_all[sample_cols].head())

    save_to_bronze_delta(df_all, snapshot_time)
    print("[OK] Saved to bronze delta.")


if __name__ == "__main__":
    main()
