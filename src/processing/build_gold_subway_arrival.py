# src/gold/build_gold_subway_arrival.py

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


def list_silver_arrival_files(target_ymd: str) -> List[Path]:
    """
    silver/fact_subway_arrival/year=YYYY/month=MM/day=DD/hour=*/fact_subway_arrival.parquet
    """
    y, m, d = get_date_parts(target_ymd)
    base = SILVER_DIR / "fact_subway_arrival"
    pattern = str(base / f"year={y}" / f"month={m}" / f"day={d}" / "hour=*" / "fact_subway_arrival.parquet")

    paths = glob(pattern)
    files = [Path(p) for p in paths]

    if not files:
        print(f"[WARN] No SILVER fact_subway_arrival files for {target_ymd} under {base}")
    else:
        print(f"[INFO] Found {len(files)} SILVER arrival fact files for {target_ymd}")

    return files


def load_silver_arrival(target_ymd: str) -> pd.DataFrame:
    files = list_silver_arrival_files(target_ymd)
    if not files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    print(f"[INFO] Loaded SILVER arrival fact rows: {len(df)}")
    return df


def build_arrival_events(df_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Silver fact_subway_arrival → GOLD arrival_events
    Silver 컬럼(네 코드 기준):

      snapshot_ts, recptn_dt, event_time,
      station_id, station_name, line_id, updn_line, train_line_name,
      prev_station_id, next_station_id, dest_station_id, dest_station_name,
      train_no, train_status,
      arrival_seconds, arrival_status_code,
      arrival_msg_primary, arrival_msg_secondary,
      is_last_train (bool)
    """
    if df_silver.empty:
        return df_silver

    df = df_silver.copy()

    # 이벤트 시각: event_time(=recptn_dt) 기준 사용
    if "event_time" in df.columns:
        df["event_dt"] = pd.to_datetime(df["event_time"], errors="coerce")
    elif "recptn_dt" in df.columns:
        df["event_dt"] = pd.to_datetime(df["recptn_dt"], errors="coerce")
    else:
        df["event_dt"] = pd.to_datetime(df.get("snapshot_ts"), errors="coerce")

    df["event_date"] = df["event_dt"].dt.date
    df["event_hour"] = df["event_dt"].dt.hour

    # 대기 시간(초)
    df["wait_sec"] = pd.to_numeric(df.get("arrival_seconds"), errors="coerce")
    df["wait_sec"] = df["wait_sec"].fillna(0).astype(int)

    # 5분 이상 대기 여부
    df["is_long_wait_5"] = (df["wait_sec"] >= 300).astype(int)

    # 막차 여부 (bool → int flag도 함께)
    df["is_last_train"] = df.get("is_last_train").fillna(False).astype(bool)
    df["is_last_train_flag"] = df["is_last_train"].astype(int)

    # 급행/ITX/특급 여부 (train_status)
    df["train_status"] = df.get("train_status").astype("string")
    df["is_express"] = df["train_status"].isin(["급행", "ITX", "특급"]).astype(int)

    # 도착 상태(코드 → phase)
    df["arrival_status_code"] = pd.to_numeric(df.get("arrival_status_code"), errors="coerce").fillna(-1).astype(int)

    def _phase(code: int) -> str:
        if code in {0, 3, 4, 5}:
            return "approach"      # 진입/전역출발/전역진입/전역도착
        elif code == 1:
            return "at_platform"   # 도착
        elif code == 2:
            return "depart"        # 출발
        else:
            return "other"

    df["arrival_phase"] = df["arrival_status_code"].apply(_phase)

    # 야간 여부 (대략 22~익일 1시)
    df["is_late_night"] = df["event_hour"].isin([22, 23, 0, 1]).astype(int)

    # 타입 통일
    for col in ["station_id", "line_id", "prev_station_id", "next_station_id", "dest_station_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["station_name", "dest_station_name", "updn_line", "train_line_name", "train_no"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


def build_arrival_station_daily_kpi(df_events: pd.DataFrame) -> pd.DataFrame:
    """
    GOLD arrival_events → GOLD station_daily_kpi

    group 기준: event_date, line_id, station_id, station_name

    지표:
      - total_events
      - avg_wait_sec
      - p95_wait_sec
      - long_wait_ratio_5
      - express_ratio
      - late_night_events
      - last_train_dt
      - last_train_wait_sec
    """
    if df_events.empty:
        return pd.DataFrame()

    df = df_events.copy()

    group_cols = ["event_date", "line_id", "station_id", "station_name"]

    def p95(x: pd.Series) -> float:
        return float(x.quantile(0.95)) if len(x) > 0 else 0.0

    agg_df = (
        df.groupby(group_cols, dropna=False)
        .agg(
            total_events=("wait_sec", "size"),
            avg_wait_sec=("wait_sec", "mean"),
            p95_wait_sec=("wait_sec", p95),
            long_wait_ratio_5=("is_long_wait_5", "mean"),
            express_ratio=("is_express", "mean"),
            late_night_events=("is_late_night", "sum"),
        )
        .reset_index()
    )

    # 막차 정보 추가
    last_train = (
        df[df["is_last_train"]]
        .groupby(group_cols, dropna=False)
        .agg(
            last_train_dt=("event_dt", "max"),
            last_train_wait_sec=("wait_sec", "mean"),
        )
        .reset_index()
    )

    kpi = pd.merge(
        agg_df,
        last_train,
        on=group_cols,
        how="left",
    )

    return kpi


def save_events(df_events: pd.DataFrame, target_ymd: str) -> Path:
    y, m, d = get_date_parts(target_ymd)
    out_dir = GOLD_DIR / "subway_arrival" / "events" / f"year={y}" / f"month={m}" / f"day={d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "fact_arrival_events.parquet"
    df_events.to_parquet(out_path, index=False)
    print(f"[INFO] Saved arrival events: {out_path}")
    return out_path


def save_station_daily_kpi(df_kpi: pd.DataFrame, target_ymd: str) -> Path:
    y, m, d = get_date_parts(target_ymd)
    out_dir = GOLD_DIR / "subway_arrival" / "station_daily_kpi" / f"year={y}" / f"month={m}" / f"day={d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "fact_arrival_station_daily_kpi.parquet"
    df_kpi.to_parquet(out_path, index=False)
    print(f"[INFO] Saved arrival station_daily_kpi: {out_path}")
    return out_path


def main():
    target_ymd = datetime.today().strftime("%Y%m%d")
    print(f"[INFO] Building GOLD subway_arrival for {target_ymd}")

    silver = load_silver_arrival(target_ymd)
    if silver.empty:
        print("[WARN] SILVER fact_subway_arrival empty, skip.")
        return

    events = build_arrival_events(silver)
    print(f"[INFO] Arrival events rows: {len(events)}")

    kpi = build_arrival_station_daily_kpi(events)
    print(f"[INFO] Arrival station_daily_kpi rows: {len(kpi)}")

    save_events(events, target_ymd)
    save_station_daily_kpi(kpi, target_ymd)


if __name__ == "__main__":
    main()
