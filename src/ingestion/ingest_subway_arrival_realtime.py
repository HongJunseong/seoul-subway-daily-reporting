# src/ingestion/ingest_subway_arrival_realtime.py

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime
from typing import Sequence

import requests
import pandas as pd
from dotenv import load_dotenv

# --------- 경로 & .env 세팅 (Run 버튼에서 실행 가능하게) ----------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")
# ---------------------------------------------------------------

from src.configs.settings import get_arrival_api_key, BRONZE_DIR
# ✅ 이제 usage가 아니라, '실시간도착_역정보(20251103).xlsx' 기준 역 리스트 사용
from src.utils.station_loader import load_station_names_from_realtime_ref

BASE_URL = "http://swopenapi.seoul.go.kr/api/subway"
SERVICE_NAME = "realtimeStationArrival"


def fetch_realtime_arrival_for_station(station_name: str) -> pd.DataFrame:
    """
    특정 역(station_name)에 대한 실시간 도착정보 조회.

    station_name 은 '실시간도착_역정보(20251103).xlsx' 의 STATN_NM 기준.
    (이미 서울시가 제공한 공식 역명이므로 별도 정규화 없이 그대로 사용)
    """
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

    # 정상 응답 구조: { "errorMessage": {...}, "realtimeArrivalList": [...] }
    if "realtimeArrivalList" not in data:
        # 디버깅용으로 원본 응답도 한 번 찍어두기
        print(f"[DEBUG] raw response for {station_name} = {data}")

        # 오류 응답이면 경고만 찍고 해당 역은 스킵
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
        # 데이터가 아예 없을 수도 있으니 빈 DF 반환
        print(f"[INFO] No arrival rows for station: {station_name}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # 쿼리 기준 역명 기록 (디버깅/매핑용)
    df["query_station_name"] = station_name  # reference(STATN_NM) 기준 이름

    # 역 이름 컬럼이 혹시 없으면 보강
    if "statnNm" not in df.columns:
        df["statnNm"] = station_name

    return df


def fetch_realtime_arrival_for_stations(stations: Sequence[str]) -> pd.DataFrame:
    """
    여러 역 리스트에 대해 조회 → 하나의 DataFrame으로 concat.

    stations: 실시간 레퍼런스(STATN_NM) 기준 역 이름 리스트.
    """
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

    df_all = pd.concat(dfs, ignore_index=True)
    return df_all


def save_to_bronze_partition(df: pd.DataFrame, snapshot_time: datetime) -> Path:
    """
    snapshot_time 기준으로 파티션 디렉터리 구성 후 Parquet 저장.
    예) data/bronze/subway_arrival/year=2025/month=12/day=10/hour=14/arrivals.parquet
    """
    y = snapshot_time.strftime("%Y")
    m = snapshot_time.strftime("%m")
    d = snapshot_time.strftime("%d")
    h = snapshot_time.strftime("%H")

    out_dir = BRONZE_DIR / "subway_arrival" / f"year={y}" / f"month={m}" / f"day={d}" / f"hour={h}"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["snapshot_ts"] = snapshot_time.isoformat()

    out_path = out_dir / "arrivals.parquet"
    df.to_parquet(out_path, index=False)
    print(f"[INFO] Saved {len(df)} rows to {out_path}")
    return out_path


def main():
    # ✅ usage가 아니라, '실시간도착_역정보(20251103).xlsx' 에서 STATN_NM 기준 역명 로딩
    station_names = load_station_names_from_realtime_ref()
    print(f"[INFO] Total stations from realtime reference (STATN_NM): {len(station_names)}")

    # 🔹 개발/테스트 시 쿼터 관리용으로 일부만 잘라서 쓰고 싶으면, 아래처럼 제한 두면 됨
    # MAX_STATIONS = 200
    # station_names = station_names[:MAX_STATIONS]
    # print(f"[INFO] Limiting to first {len(station_names)} stations for this run")

    snapshot_time = datetime.now()
    print(f"[INFO] Fetching realtime arrivals at snapshot: {snapshot_time}")

    df_all = fetch_realtime_arrival_for_stations(station_names)
    print("[INFO] Total arrival rows:", len(df_all))

    # 디버깅용 샘플 출력
    sample_cols = [
        c
        for c in ["subwayId", "statnNm", "trainLineNm", "arvlMsg2", "arvlMsg3", "query_station_name"]
        if c in df_all.columns
    ]
    if sample_cols:
        print(df_all[sample_cols].head())

    save_to_bronze_partition(df_all, snapshot_time)


if __name__ == "__main__":
    main()
