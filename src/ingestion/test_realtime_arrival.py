# src/ingestion/test_realtime_arrival.py

from __future__ import annotations

import requests
import pandas as pd

import sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# 현재 파일 기준으로 src 경로를 자동으로 PYTHONPATH에 추가
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
SRC_DIR = PROJECT_ROOT / "src"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.configs.settings import get_seoul_api_key, BRONZE_DIR

def fetch_realtime_arrival_all() -> pd.DataFrame:
    """
    서울교통공사 지하철 실시간 도착정보 호출 테스트.
    서비스명: realtimeStationArrival
    """

    api_key = get_seoul_api_key()

    base_url = "http://swopenAPI.seoul.go.kr/api/subway"
    service_name = "realtimeStationArrival"
    start_index = 0
    end_index = 100

    # 일단 역 하나(강남)만 테스트해보자
    station_name = "강남"

    # 🚩 xml → json 으로 바꾸기!!
    url = f"{base_url}/{api_key}/json/{service_name}/{start_index}/{end_index}/{station_name}"
    print("[DEBUG] Request URL:", url)

    resp = requests.get(url, timeout=10)
    resp.raise_for_status()

    # 응답 내용 먼저 한 번 찍어보자 (디버그용)
    # print(resp.text[:300])

    data = resp.json()

    # 에러 응답인 경우 (RESULT만 있을 때)
    if "RESULT" in data and "realtimeStationArrival" not in data:
        raise RuntimeError(f"API 오류 응답: {data['RESULT']}")

    if "realtimeStationArrival" not in data:
        raise RuntimeError(f"응답에 realtimeStationArrival 키가 없습니다: {data.keys()}")

    body = data["realtimeStationArrival"]
    if "row" not in body:
        raise RuntimeError(f"응답에 row 필드가 없습니다: {body.keys()}")

    rows = body["row"]
    df = pd.DataFrame(rows)
    return df



def save_to_bronze(df: pd.DataFrame) -> None:
    out_path = BRONZE_DIR / "subway_arrival_sample.parquet"
    df.to_parquet(out_path, index=False)
    print(f"[INFO] Saved sample to {out_path}")


def main():
    print("[INFO] Fetching realtime subway arrival (all stations)...")
    df = fetch_realtime_arrival_all()
    print("[INFO] Rows:", len(df))
    print(df.head(10))

    save_to_bronze(df)


if __name__ == "__main__":
    main()
