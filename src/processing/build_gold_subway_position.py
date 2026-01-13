# src/gold/build_gold_subway_position.py

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Tuple
from glob import glob
from datetime import datetime

import pandas as pd

# ---------- 공통 경로 세팅 ----------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# ------------------------------------

from src.configs.settings import SILVER_DIR, GOLD_DIR


def get_date_parts(target_ymd: str) -> Tuple[str, str, str]:
    return target_ymd[:4], target_ymd[4:6], target_ymd[6:8]


def list_silver_position_files(target_ymd: str) -> List[Path]:
    """
    silver/fact_subway_position/year=YYYY/month=MM/day=DD/hour=*/fact_subway_position.parquet
    """
    y, m, d = get_date_parts(target_ymd)
    base = SILVER_DIR / "fact_subway_position"
    pattern = str(base / f"year={y}" / f"month={m}" / f"day={d}" / "hour=*" / "fact_subway_position.parquet")

    paths = glob(pattern)
    files = [Path(p) for p in paths]

    if not files:
        print(f"[WARN] No SILVER fact_subway_position files for {target_ymd} under {base}")
    else:
        print(f"[INFO] Found {len(files)} SILVER position fact files for {target_ymd}")

    return files


def load_silver_position(target_ymd: str) -> pd.DataFrame:
    files = list_silver_position_files(target_ymd)
    if not files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    print(f"[INFO] Loaded SILVER position fact rows: {len(df)}")
    return df


def build_position_events(df_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Silver fact_subway_position → GOLD position_events

    Silver 컬럼(네 코드 기준):
      snapshot_ts,
      line_id, line_name,
      station_id, station_name,
      updn_line,
      train_no,
      dest_station_id, dest_station_name,
      train_status_code,
      is_express (bool),
      is_last_train (bool),
      last_recptn_dt (datetime)
    """
    if df_silver.empty:
        return df_silver

    df = df_silver.copy()

    # 이벤트 시각: last_recptn_dt 우선, 없으면 snapshot_ts
    if "last_recptn_dt" in df.columns:
        df["event_dt"] = pd.to_datetime(df["last_recptn_dt"], errors="coerce")
    else:
        df["event_dt"] = pd.to_datetime(df.get("snapshot_ts"), errors="coerce")

    df["event_date"] = df["event_dt"].dt.date
    df["event_hour"] = df["event_dt"].dt.hour

    # 타입 정리
    for col in ["line_id", "station_id", "dest_station_id", "updn_line", "train_status_code"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["line_name", "station_name", "dest_station_name", "train_no"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    df["is_express"] = df.get("is_express").fillna(False).astype(bool)
    df["is_last_train"] = df.get("is_last_train").fillna(False).astype(bool)

    return df


def build_line_hourly_kpi(df_events: pd.DataFrame) -> pd.DataFrame:
    """
    GOLD position_events → GOLD line_hourly KPI

    group 기준:
      event_date, event_hour, line_id, line_name, updn_line

    지표:
      - active_trains : 해당 시간대 서로 다른 열차 수 (nunique(train_no))
      - express_trains : 급행/특급 열차 수
      - last_event_dt : 그 시간대 내 마지막 관측 시각
      - last_train_flag : 그 시간대 막차가 있었는지 여부(있으면 1)
    """
    if df_events.empty:
        return pd.DataFrame()

    df = df_events.copy()

    group_cols = ["event_date", "event_hour", "line_id", "line_name", "updn_line"]

    agg_df = (
        df.groupby(group_cols, dropna=False)
        .agg(
            active_trains=("train_no", pd.Series.nunique),
            express_trains=("is_express", lambda x: int(x.sum())),
            last_event_dt=("event_dt", "max"),
            last_train_flag=("is_last_train", lambda x: int(x.any())),
        )
        .reset_index()
    )

    return agg_df


def save_events(df_events: pd.DataFrame, target_ymd: str) -> Path:
    y, m, d = get_date_parts(target_ymd)
    out_dir = GOLD_DIR / "subway_position" / "events" / f"year={y}" / f"month={m}" / f"day={d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "fact_position_events.parquet"
    df_events.to_parquet(out_path, index=False)
    print(f"[INFO] Saved position events: {out_path}")
    return out_path


def save_line_hourly_kpi(df_kpi: pd.DataFrame, target_ymd: str) -> Path:
    y, m, d = get_date_parts(target_ymd)
    out_dir = GOLD_DIR / "subway_position" / "line_hourly" / f"year={y}" / f"month={m}" / f"day={d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "fact_position_line_hourly.parquet"
    df_kpi.to_parquet(out_path, index=False)
    print(f"[INFO] Saved position line_hourly: {out_path}")
    return out_path


def main():
    target_ymd = datetime.today().strftime("%Y%m%d")
    print(f"[INFO] Building GOLD subway_position for {target_ymd}")

    silver = load_silver_position(target_ymd)
    if silver.empty:
        print("[WARN] SILVER fact_subway_position empty, skip.")
        return

    events = build_position_events(silver)
    print(f"[INFO] Position events rows: {len(events)}")

    line_hourly = build_line_hourly_kpi(events)
    print(f"[INFO] Position line_hourly rows: {len(line_hourly)}")

    save_events(events, target_ymd)
    save_line_hourly_kpi(line_hourly, target_ymd)


if __name__ == "__main__":
    main()
