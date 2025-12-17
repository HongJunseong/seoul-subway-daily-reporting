from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime
from typing import Sequence
import tempfile

import requests
import pandas as pd
from dotenv import load_dotenv
from src.utils.time import now_kst

# --------- 경로 & .env 세팅 (Run 버튼에서 실행 가능하게) ----------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")
# ---------------------------------------------------------------

from src.configs.settings import get_arrival_api_key
from src.utils.station_loader import load_station_names_from_realtime_ref

from src.common.config import load_s3_config
from src.common.s3_uploader import upload_file_to_s3

BASE_URL = "http://swopenapi.seoul.go.kr/api/subway"
SERVICE_NAME = "realtimeStationArrival"


def fetch_realtime_arrival_for_station(station_name: str) -> pd.DataFrame:
    api_key = get_arrival_api_key()
    start_index = 0
    end_index = 50

    url = f"{BASE_URL}/{api_key}/json/{SERVICE_NAME}/{start_index}/{end_index}/{station_name}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[WARN] Failed to request arrival for {station_name}: {e}")
        return pd.DataFrame()

    if "realtimeArrivalList" not in data:
        print(f"[DEBUG] raw response for {station_name} = {data}")

        if "errorMessage" in data:
            em = data["errorMessage"]
            print(
                f"[WARN] Arrival API error for {station_name}: "
                f"status={em.get('status')} code={em.get('code')} msg={em.get('message')}"
            )
        else:
            print(f"[WARN] Unknown response for {station_name}: keys={list(data.keys())}")
        return pd.DataFrame()

    rows = data["realtimeArrivalList"]
    if not rows:
        print(f"[INFO] No arrival rows for station: {station_name}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["query_station_name"] = station_name

    if "statnNm" not in df.columns:
        df["statnNm"] = station_name

    return df


def fetch_realtime_arrival_for_stations(stations: Sequence[str]) -> pd.DataFrame:
    dfs: list[pd.DataFrame] = []
    for s in stations:
        try:
            df_s = fetch_realtime_arrival_for_station(s)
            if not df_s.empty:
                dfs.append(df_s)
            print(f"[INFO] Fetched {len(df_s)} rows for station: {s}")
        except Exception as e:
            print(f"[WARN] Failed to fetch for station {s}: {e}")

    if not dfs:
        raise RuntimeError("어느 역에서도 도착 데이터를 가져오지 못했습니다.")

    return pd.concat(dfs, ignore_index=True)


def bronze_arrival_snapshot_key(snapshot_time: datetime) -> str:
    """
    ✅ 누적 스냅샷 저장용 S3 key 생성

    - hour 폴더 아래에 minute 파티션을 두고,
    - 파일명에 run_ts(초 단위)까지 넣어서 같은 minute에 여러 번 돌려도 덮어쓰지 않게 함.

    예)
    bronze/subway_arrival/year=2025/month=12/day=10/hour=14/minute=05/arrivals_20251210T140512.parquet
    """
    y = snapshot_time.strftime("%Y")
    m = snapshot_time.strftime("%m")
    d = snapshot_time.strftime("%d")
    h = snapshot_time.strftime("%H")
    mm = snapshot_time.strftime("%M")
    run_ts = snapshot_time.strftime("%Y%m%dT%H%M%S")

    return (
        f"bronze/subway_arrival/"
        f"year={y}/month={m}/day={d}/hour={h}/minute={mm}/"
        f"arrivals_{run_ts}.parquet"
    )


def save_to_bronze_s3(df: pd.DataFrame, snapshot_time: datetime) -> str:
    """
    snapshot_time 기준으로 S3에 '누적 스냅샷' Parquet 업로드.
    """
    s3cfg = load_s3_config()
    key = bronze_arrival_snapshot_key(snapshot_time)

    df = df.copy()
    df["snapshot_ts"] = snapshot_time.isoformat()

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "arrivals.parquet"
        df.to_parquet(local_path, index=False)
        upload_file_to_s3(local_path, s3cfg.bucket, key, region=s3cfg.region)

    s3_uri = f"s3://{s3cfg.bucket}/{key}"
    print(f"[OK] Uploaded {len(df)} rows to {s3_uri}")
    return s3_uri


def main():
    station_names = load_station_names_from_realtime_ref()
    print(f"[INFO] Total stations from realtime reference (STATN_NM): {len(station_names)}")

    snapshot_time = now_kst()
    print(f"[INFO] Fetching realtime arrivals at snapshot: {snapshot_time}")

    df_all = fetch_realtime_arrival_for_stations(station_names)
    print("[INFO] Total arrival rows:", len(df_all))

    sample_cols = [
        c
        for c in ["subwayId", "statnNm", "trainLineNm", "arvlMsg2", "arvlMsg3", "query_station_name"]
        if c in df_all.columns
    ]
    if sample_cols:
        print(df_all[sample_cols].head())

    save_to_bronze_s3(df_all, snapshot_time)


if __name__ == "__main__":
    main()
