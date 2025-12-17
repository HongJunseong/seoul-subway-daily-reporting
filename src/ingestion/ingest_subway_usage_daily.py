from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
import tempfile

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
from src.common.s3_uploader import upload_file_to_s3
from src.common.paths import s3_bronze_usage_key

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

        df_page = pd.DataFrame(rows)
        all_pages.append(df_page)

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


def save_to_bronze_s3(df: pd.DataFrame, use_ymd: str) -> str:
    s3cfg = load_s3_config()
    key = s3_bronze_usage_key(use_ymd)

    df = df.copy()
    # ✅ KST 고정
    df["ingested_at"] = pendulum.now("Asia/Seoul").to_iso8601_string()

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "usage.parquet"
        df.to_parquet(local_path, index=False)
        upload_file_to_s3(local_path, s3cfg.bucket, key, region=s3cfg.region)

    s3_uri = f"s3://{s3cfg.bucket}/{key}"
    print(f"[OK] Uploaded {len(df)} rows to {s3_uri}")
    return s3_uri


def main():
    if len(sys.argv) > 1:
        use_ymd = sys.argv[1]
    else:
        # ✅ KST 기준 today - 4일
        use_ymd = pendulum.now("Asia/Seoul").subtract(days=4).format("YYYYMMDD")

    print("[INFO] Fetching CardSubwayStatsNew for use_ymd:", use_ymd)

    df = fetch_subway_usage_daily(use_ymd)
    print("[INFO] Rows fetched:", len(df))
    if len(df) > 0:
        print(df.head())
        save_to_bronze_s3(df, use_ymd)
    else:
        print("[WARN] Empty DataFrame. Check API key / date(use_ymd) / service status.")


if __name__ == "__main__":
    main()
