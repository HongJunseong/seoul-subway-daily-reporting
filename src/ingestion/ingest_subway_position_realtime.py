# src/ingestion/ingest_subway_position_realtime.py

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime
from typing import Sequence

import requests
import pandas as pd
from dotenv import load_dotenv

# ---------- 경로 & .env 세팅 ----------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")
# -------------------------------------

from src.configs.settings import get_position_api_key, BRONZE_DIR
from src.utils.station_loader import load_lines_from_usage   # ★ 신규 함수 사용


BASE_URL = "http://swopenapi.seoul.go.kr/api/subway"
SERVICE_NAME = "realtimePosition"


def fetch_realtime_position_for_line(line_name: str) -> pd.DataFrame:
    """
    특정 호선(예: '2호선', '4호선')에 대한 실시간 열차 위치 조회
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
        # 오류 메시지 포맷
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
    """
    모든 호선에 대해 위치정보를 조회하여 하나의 DF로 합쳐 반환
    """
    dfs = []
    for ln in lines:
        df_line = fetch_realtime_position_for_line(ln)
        print(f"[INFO] Fetched {len(df_line)} rows for line: {ln}")
        if not df_line.empty:
            dfs.append(df_line)

    if not dfs:
        raise RuntimeError("호선 어느 곳에서도 데이터를 가져오지 못했습니다.")

    return pd.concat(dfs, ignore_index=True)


def save_position_to_bronze(df: pd.DataFrame, snapshot_time: datetime) -> Path:
    """
    snapshot_time 기준 파티션 디렉터리에 저장
    예) bronze/subway_position/year=YYYY/month=MM/day=DD/hour=HH/positions.parquet
    """
    y, m, d, h = (
        snapshot_time.strftime("%Y"),
        snapshot_time.strftime("%m"),
        snapshot_time.strftime("%d"),
        snapshot_time.strftime("%H"),
    )

    out_dir = BRONZE_DIR / "subway_position" / f"year={y}" / f"month={m}" / f"day={d}" / f"hour={h}"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["snapshot_ts"] = snapshot_time.isoformat()

    out_path = out_dir / "positions.parquet"
    df.to_parquet(out_path, index=False)

    print(f"[INFO] Saved {len(df)} rows to {out_path}")
    return out_path


def main():
    # ⭐ usage → Silver에서 호선 목록 자동 로딩
    lines = load_lines_from_usage()
    print(f"[INFO] Total lines detected from usage: {len(lines)}")
    print("Lines:", lines)

    snapshot_time = datetime.now()
    print(f"[INFO] Fetching realtime subway positions at snapshot: {snapshot_time}")

    df_all = fetch_realtime_position_for_lines(lines)
    print("[INFO] Total position rows:", len(df_all))

    # 샘플 출력
    sample_cols = [
        c for c in ["subwayNm", "trainNo", "statnNm", "trainSttus", "statnId"]
        if c in df_all.columns
    ]
    if sample_cols:
        print(df_all[sample_cols].head())

    save_position_to_bronze(df_all, snapshot_time)


if __name__ == "__main__":
    main()
