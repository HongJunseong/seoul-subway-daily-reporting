# src/processing/build_gold_subway_arrival.py

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime
from glob import glob
from typing import List
import pandas as pd

# ---------------- 공통 경로 세팅 ----------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# ------------------------------------------------

from src.configs.settings import SILVER_DIR, GOLD_DIR


# ------------------------------------------------
# 1) 특정 날짜(day=DD)에 해당하는 모든 SILVER arrival 파일 찾기
# ------------------------------------------------
def list_silver_arrival_files_for_date(target_ymd: str) -> List[Path]:
    y, m, d = target_ymd[:4], target_ymd[4:6], target_ymd[6:8]

    base = SILVER_DIR / "fact_subway_arrival" / f"year={y}" / f"month={m}" / f"day={d}"
    pattern = str(base / "hour=*" / "fact_subway_arrival.parquet")

    paths = glob(pattern)
    files = [Path(p) for p in paths]

    if not files:
        print(f"[WARN] No SILVER arrival files found for {target_ymd}")
    else:
        print(f"[INFO] Found {len(files)} SILVER arrival snapshot files for {target_ymd}")

    return files


def load_silver_arrival_daily(target_ymd: str) -> pd.DataFrame:
    files = list_silver_arrival_files_for_date(target_ymd)
    if not files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    print(f"[INFO] Loaded {len(df)} rows from SILVER arrival for {target_ymd}")
    return df


# ------------------------------------------------
# 2) 이벤트 시각(event_time) 정제
#    recptnDt → primary
#    없으면 snapshot_ts 사용
# ------------------------------------------------
def normalize_event_time(df: pd.DataFrame) -> pd.DataFrame:
    if "recptn_dt" in df.columns:
        event_time = pd.to_datetime(df["recptn_dt"], errors="coerce")
    else:
        event_time = pd.to_datetime(df["snapshot_ts"], errors="coerce")

    df["event_time"] = event_time
    df["event_date"] = df["event_time"].dt.date
    df["event_hour"] = df["event_time"].dt.hour

    return df


# ------------------------------------------------
# 3) GOLD 요약 1: 역 × 호선 × 방향 기준 일 집계
# ------------------------------------------------
def build_arrival_station_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # 마지막열차 여부 처리
    if "is_last_train" in df.columns:
        df["is_last_train_flag"] = (
            df["is_last_train"]
            .replace({True: 1, False: 0, "1": 1, "0": 0})
            .fillna(0)
            .astype(int)
        )
    else:
        df["is_last_train_flag"] = 0

    # arrival_seconds 정제
    if "arrival_seconds" in df.columns:
        df["arrival_seconds"] = pd.to_numeric(df["arrival_seconds"], errors="coerce")

    group_cols = [
        "event_date",
        "station_id",
        "station_name",
        "line_id",
        "updn_line",
    ]

    summary = (
        df.groupby(group_cols, dropna=False)
          .agg(
              event_count=("train_no", "count"),
              last_train_count=("is_last_train_flag", "sum"),
              avg_arrival_seconds=("arrival_seconds", "mean"),
          )
          .reset_index()
    )

    # 역당 이벤트 수로 랭킹
    summary["rank_by_event"] = summary["event_count"].rank(
        ascending=False, method="dense"
    )

    return summary


# ------------------------------------------------
# 4) GOLD 요약 2: 역 × 호선 × 방향 × 시간대(hour) 집계
# ------------------------------------------------
def build_arrival_station_hourly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = [
        "event_date",
        "event_hour",
        "station_id",
        "station_name",
        "line_id",
        "updn_line",
    ]

    hourly = (
        df.groupby(group_cols, dropna=False)
          .agg(
              event_count=("train_no", "count"),
          )
          .reset_index()
    )

    return hourly


# ------------------------------------------------
# 5) 저장 유틸
# ------------------------------------------------
def save_gold(df: pd.DataFrame, category: str, filename: str, target_ymd: str) -> Path:
    y, m, d = target_ymd[:4], target_ymd[4:6], target_ymd[6:8]
    out_dir = GOLD_DIR / "subway_arrival" / category / f"year={y}" / f"month={m}" / f"day={d}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / filename
    df.to_parquet(out_path, index=False)

    print(f"[INFO] Saved GOLD {category}: {out_path}")
    return out_path


# ------------------------------------------------
# 6) MAIN
# ------------------------------------------------
def main():
    # arrival은 “실제 수집한 날짜(day)” 기준으로 분석함
    target_ymd = datetime.today().strftime("%Y%m%d")
    print(f"[INFO] Building GOLD arrival for {target_ymd}")

    df = load_silver_arrival_daily(target_ymd)
    if df.empty:
        print("[WARN] No data, skipping.")
        return

    df = normalize_event_time(df)

    daily = build_arrival_station_daily(df)
    hourly = build_arrival_station_hourly(df)

    save_gold(daily, "station_daily", "fact_arrival_station_daily.parquet", target_ymd)
    save_gold(hourly, "station_hourly", "fact_arrival_station_hourly.parquet", target_ymd)


if __name__ == "__main__":
    main()
