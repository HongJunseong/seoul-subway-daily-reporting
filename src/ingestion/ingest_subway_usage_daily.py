# src/ingestion/ingest_subway_usage_daily.py

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta

import requests
import pandas as pd

# --- 프로젝트 루트 경로 세팅 ---
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.configs.settings import BRONZE_DIR, get_passenger_api_key

# 서울시 열린데이터광장 지하철 호선별 역별 승하차 인원 API
BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE_NAME = "CardSubwayStatsNew"


def fetch_subway_usage_daily(
    use_ymd: str,
    page_size: int = 1000,
    max_pages: int = 10,
) -> pd.DataFrame:
    """
    서울시 지하철호선별 역별 승하차 인원 (CardSubwayStatsNew)
    - 특정 사용일자(use_ymd, 'YYYYMMDD') 기준 전체 역/호선 데이터를 DataFrame으로 반환
    - JSON 포맷 사용

    use_ymd: 'YYYYMMDD' (예: '20251201')
    """

    api_key = get_passenger_api_key()  # 서울시 openapi.seoul.go.kr 인증키

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
            # 에러 응답 처리
            print("[ERROR] Unexpected response structure:", list(data.keys()))
            raise RuntimeError(f"Unexpected response: {data}")

        svc = data[SERVICE_NAME]

        # 총 건수 / 결과 코드 확인
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

        df_page = pd.DataFrame(rows)
        all_pages.append(df_page)

        # 페이지네이션 종료 조건
        if len(rows) < page_size:
            # 마지막 페이지로 판단
            break

        try:
            tc_int = int(total_count) if total_count is not None else None
        except Exception:
            tc_int = None

        if tc_int is not None and end >= tc_int:
            # 이미 total_count 이상 조회했으면 종료
            break

        # 다음 페이지로
        start += page_size
        page_no += 1

    if not all_pages:
        print("[WARN] No data returned from API (all_pages empty).")
        return pd.DataFrame()

    df = pd.concat(all_pages, ignore_index=True)

    # 디버그: 실제 컬럼 확인
    print("[DEBUG] df.columns:", list(df.columns))

    # 여기서는 원본 컬럼을 그대로 유지 (USE_YMD, SBWY_ROUT_LN_NM, SBWY_STNS_NM, GTON_TNOPE, GTOFF_TNOPE, REG_YMD 등)
    # 타입 정리/집계는 SILVER 단계에서 처리

    return df


def save_to_bronze(df: pd.DataFrame, use_ymd: str) -> Path:
    """
    /bronze/subway_usage/year=YYYY/month=MM/day=DD/usage.parquet 로 저장
    (partition 기준은 사용일자 USE_YMD 가 아니라, '이 사용일자의 통계를 적재하는 파티션 날짜' 개념으로 봐도 됨.
     여기서는 편의상 use_ymd 기준으로 파티션을 구성.)
    """
    y = use_ymd[0:4]
    m = use_ymd[4:6]
    d = use_ymd[6:8]

    out_dir = BRONZE_DIR / "subway_usage" / f"year={y}" / f"month={m}" / f"day={d}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "usage.parquet"
    df.to_parquet(out_path, index=False)
    print(f"[INFO] Saved Bronze subway_usage: {out_path}")
    return out_path


def main():
    """
    기본 동작:
    - 인자가 없으면 '오늘 - 3일' 의 사용일자를 대상으로 호출
      (CardSubwayStatsNew: 3일 전 데이터를 매일 갱신)
    - 인자로 YYYYMMDD 가 들어오면 그 날짜를 사용일자로 간주
    """
    if len(sys.argv) > 1:
        use_ymd = sys.argv[1]
    else:
        use_ymd = (datetime.today() - timedelta(days=4)).strftime("%Y%m%d")

    print("[INFO] Fetching CardSubwayStatsNew for use_ymd:", use_ymd)

    df = fetch_subway_usage_daily(use_ymd)
    print("[INFO] Rows fetched:", len(df))
    if len(df) > 0:
        print("[INFO] Head:")
        print(df.head())

        save_to_bronze(df, use_ymd)
    else:
        print("[WARN] Empty DataFrame. Check API key / date(use_ymd) / service status.")


if __name__ == "__main__":
    main()