# src/ingestion/test_realtime_arrival.py

from __future__ import annotations

import sys
from pathlib import Path

import requests
import pandas as pd
from dotenv import load_dotenv

# =========================
#  경로 & .env 세팅 (Run 버튼용)
# =========================
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 루트의 .env 로드
load_dotenv(PROJECT_ROOT / ".env")

from src.configs.settings import get_seoul_api_key, BRONZE_DIR


def fetch_realtime_arrival(station_name: str = "강남") -> pd.DataFrame:
    """
    swopenapi 기반 서울 지하철 실시간 도착 정보 (역 단위)
    SERVICE : realtimeStationArrival
    응답 키  : realtimeArrivalList
    """

    api_key = get_seoul_api_key()

    base_url = "http://swopenapi.seoul.go.kr/api/subway"
    service_name = "realtimeStationArrival"
    start_index = 0
    end_index = 50  # 최대 50개 정도만

    url = f"{base_url}/{api_key}/json/{service_name}/{start_index}/{end_index}/{station_name}"
    print("[DEBUG] Request URL:", url)

    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    print("[DEBUG] Top-level keys:", data.keys())

    # 에러/상태만 온 경우
    # 예: {"status": ..., "code": ..., "message": ..., ...}
    if "realtimeArrivalList" not in data:
        # swopenapi 정상 응답 형식: { "errorMessage": {...}, "realtimeArrivalList": [...] }
        if "errorMessage" in data:
            em = data["errorMessage"]
            raise RuntimeError(
                f"API 오류: status={em.get('status')} code={em.get('code')} "
                f"message={em.get('message')}"
            )
        # data.go.kr 스타일 메타 응답만 온 경우
        if {"status", "code", "message"} <= set(data.keys()):
            raise RuntimeError(
                f"API 메타 응답만 수신: status={data.get('status')} "
                f"code={data.get('code')} message={data.get('message')}"
            )

        # 그 외 알 수 없는 구조
        raise RuntimeError(f"알 수 없는 응답 구조: {data}")

    rows = data["realtimeArrivalList"]
    df = pd.DataFrame(rows)
    return df


def save_to_bronze(df: pd.DataFrame) -> None:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = BRONZE_DIR / "subway_arrival_sample.parquet"
    df.to_parquet(out_path, index=False)
    print(f"[INFO] Saved sample to {out_path}")


def main():
    print("[INFO] Fetching realtime subway arrival for station '강남' ...")
    df = fetch_realtime_arrival("강남")
    print("[INFO] Rows:", len(df))
    print(df.head(10))

    save_to_bronze(df)


if __name__ == "__main__":
    main()
