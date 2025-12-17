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

# ---------- 경로 & .env 세팅 ----------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")
# -------------------------------------

from src.configs.settings import get_position_api_key
from src.utils.station_loader import load_lines_from_realtime_ref

from src.common.config import load_s3_config
from src.common.s3_uploader import upload_file_to_s3

BASE_URL = "http://swopenapi.seoul.go.kr/api/subway"
SERVICE_NAME = "realtimePosition"


def fetch_realtime_position_for_line(line_name: str) -> pd.DataFrame:
    """
    특정 호선(예: '2호선', '경의중앙선')에 대한 실시간 열차 위치 조회.
    line_name 은 레퍼런스 엑셀의 '호선이름' 기준.
    """
    api_key = get_position_api_key()

    start_index = 0
    end_index = 100

    url = f"{BASE_URL}/{api_key}/json/{SERVICE_NAME}/{start_index}/{end_index}/{line_name}"

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"[WARN] Failed request for {line_name}: {e}")
        return pd.DataFrame()

    if "realtimePositionList" not in data:
        if "errorMessage" in data:
            em = data["errorMessage"]
            print(
                f"[WARN] API error at {line_name}: "
                f"status={em.get('status')}, code={em.get('code')}, message={em.get('message')}"
            )
        else:
            print(f"[WARN] Unknown response for {line_name}: keys={list(data.keys())}")
        return pd.DataFrame()

    rows = data["realtimePositionList"]
    if not rows:
        print(f"[INFO] No position rows for line: {line_name}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # 조회 기준 호선 컬럼 추가
    df["query_line_name"] = line_name
    if "subwayNm" not in df.columns:
        df["subwayNm"] = line_name

    return df


def fetch_realtime_position_for_lines(lines: Sequence[str]) -> pd.DataFrame:
    dfs: list[pd.DataFrame] = []
    for ln in lines:
        df_line = fetch_realtime_position_for_line(ln)
        print(f"[INFO] Fetched {len(df_line)} rows for line: {ln}")
        if not df_line.empty:
            dfs.append(df_line)

    if not dfs:
        raise RuntimeError("호선 어느 곳에서도 데이터를 가져오지 못했습니다.")

    return pd.concat(dfs, ignore_index=True)


def bronze_position_snapshot_key(snapshot_time: datetime) -> str:
    """
    ✅ 누적 스냅샷 저장용 S3 key 생성

    예)
    bronze/subway_position/year=2025/month=12/day=10/hour=14/minute=05/positions_20251210T140512.parquet
    """
    y = snapshot_time.strftime("%Y")
    m = snapshot_time.strftime("%m")
    d = snapshot_time.strftime("%d")
    h = snapshot_time.strftime("%H")
    mm = snapshot_time.strftime("%M")
    run_ts = snapshot_time.strftime("%Y%m%dT%H%M%S")

    return (
        f"bronze/subway_position/"
        f"year={y}/month={m}/day={d}/hour={h}/minute={mm}/"
        f"positions_{run_ts}.parquet"
    )


def save_position_to_bronze_s3(df: pd.DataFrame, snapshot_time: datetime) -> str:
    """
    snapshot_time 기준으로 S3 bronze 파티션 경로에 Parquet 업로드 (누적)
    """
    s3cfg = load_s3_config()
    key = bronze_position_snapshot_key(snapshot_time)

    df = df.copy()
    df["snapshot_ts"] = snapshot_time.isoformat()

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "positions.parquet"
        df.to_parquet(local_path, index=False)
        upload_file_to_s3(local_path, s3cfg.bucket, key, region=s3cfg.region)

    s3_uri = f"s3://{s3cfg.bucket}/{key}"
    print(f"[OK] Uploaded {len(df)} rows to {s3_uri}")
    return s3_uri


def main():
    lines = load_lines_from_realtime_ref()
    print(f"[INFO] Total lines from realtime reference (호선이름): {len(lines)}")
    print("[INFO] Lines used for realtime position:", lines)

    snapshot_time = now_kst()
    print(f"[INFO] Fetching realtime subway positions at snapshot: {snapshot_time}")

    df_all = fetch_realtime_position_for_lines(lines)
    print("[INFO] Total position rows:", len(df_all))

    sample_cols = [c for c in ["subwayNm", "trainNo", "statnNm", "trainSttus", "statnId"] if c in df_all.columns]
    if sample_cols:
        print(df_all[sample_cols].head())

    save_position_to_bronze_s3(df_all, snapshot_time)


if __name__ == "__main__":
    main()