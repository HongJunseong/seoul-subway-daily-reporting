# src/processing/build_gold_subway_position.py

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
# 1) 특정 날짜(day=DD)에 해당하는 SILVER position 파일 찾기
#    silver/fact_subway_position/year=Y/month=M/day=D/hour=H/fact_subway_position.parquet
# ------------------------------------------------
def list_silver_position_files_for_date(target_ymd: str) -> List[Path]:
    y, m, d = target_ymd[:4], target_ymd[4:6], target_ymd[6:8]

    base = SILVER_DIR / "fact_subway_position" / f"year={y}" / f"month={m}" / f"day={d}"
    pattern = str(base / "hour=*" / "fact_subway_position.parquet")

    paths = glob(pattern)
    files = [Path(p) for p in paths]

    if not files:
        print(f"[WARN] No SILVER position files found for {target_ymd} under {base}")
    else:
        print(f"[INFO] Found {len(files)} SILVER position snapshot files for {target_ymd}")

    return files


def load_silver_position_daily(target_ymd: str) -> pd.DataFrame:
    files = list_silver_position_files_for_date(target_ymd)
    if not files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    print(f"[INFO] Loaded {len(df)} rows from SILVER position for {target_ymd}")
    return df


# ------------------------------------------------
# 2) 이벤트 시각(event_time) 정제
#    - lastRecptnDt + recptnDt 가 있으면 그걸 우선 사용
#    - 없으면 recptn_dt 사용
#    - 그것도 없으면 snapshot_ts 사용
# ------------------------------------------------
def normalize_event_time(df: pd.DataFrame) -> pd.DataFrame:
    event_time = None

    # 1) lastRecptnDt + recptnDt 조합 (API 원형 그대로인 경우)
    if "lastRecptnDt" in df.columns and "recptnDt" in df.columns:
        # 문자열 붙여서 "YYYY-MM-DD HH:MM:SS" 형태로 만들기
        date_str = df["lastRecptnDt"].astype("string").fillna("")
        time_str = df["recptnDt"].astype("string").fillna("")
        combined = (date_str + " " + time_str).str.strip()
        event_time = pd.to_datetime(combined, errors="coerce")

    # 2) 이미 silver 단계에서 recptn_dt 로 변환해둔 경우
    if event_time is None or event_time.isna().all():
        if "recptn_dt" in df.columns:
            event_time = pd.to_datetime(df["recptn_dt"], errors="coerce")

    # 3) 최후 fallback: snapshot_ts
    if (event_time is None or event_time.isna().all()) and "snapshot_ts" in df.columns:
        event_time = pd.to_datetime(df["snapshot_ts"], errors="coerce")

    if event_time is None:
        # 이벤트 시각을 만들 수 없으면 전부 NaT
        df["event_time"] = pd.NaT
        df["event_date"] = pd.NaT
        df["event_hour"] = pd.NA
        return df

    df["event_time"] = event_time
    df["event_date"] = df["event_time"].dt.date
    df["event_hour"] = df["event_time"].dt.hour

    return df


# ------------------------------------------------
# 3) 열차 번호/역/호선 컬럼 이름 유연하게 찾기
# ------------------------------------------------
def pick_first_existing(df: pd.DataFrame, candidates: list[str], default: str | None = None) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return default


# ------------------------------------------------
# 4) GOLD 요약 1: 역 × 호선 × 시간대별 "열차 수" (밀집도)
# ------------------------------------------------
def build_position_station_hourly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # 열차/역/호선 컬럼 이름 추론
    train_col = pick_first_existing(df, ["train_no", "trainNo", "btrainNo"])
    station_id_col = pick_first_existing(df, ["station_id", "statnId"])
    station_name_col = pick_first_existing(df, ["station_name", "statnNm"])
    line_id_col = pick_first_existing(df, ["line_id", "subwayId"])

    if train_col is None or station_id_col is None or station_name_col is None or line_id_col is None:
        print("[WARN] 필수 컬럼(train/station/line)이 없어 station_hourly 요약을 만들 수 없습니다.")
        return pd.DataFrame()

    group_cols = [
        "event_date",
        "event_hour",
        station_id_col,
        station_name_col,
        line_id_col,
    ]

    hourly = (
        df.groupby(group_cols, dropna=False)[train_col]
          .nunique()
          .reset_index(name="train_count")
    )

    # 컬럼 이름을 일관된 형태로 정리
    hourly = hourly.rename(
        columns={
            station_id_col: "station_id",
            station_name_col: "station_name",
            line_id_col: "line_id",
        }
    )

    return hourly


# ------------------------------------------------
# 5) GOLD 요약 2: 열차별 일일 이동 요약
# ------------------------------------------------
def build_position_train_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    train_col = pick_first_existing(df, ["train_no", "trainNo", "btrainNo"])
    station_id_col = pick_first_existing(df, ["station_id", "statnId"])
    station_name_col = pick_first_existing(df, ["station_name", "statnNm"])
    line_id_col = pick_first_existing(df, ["line_id", "subwayId"])

    if train_col is None or line_id_col is None:
        print("[WARN] 필수 컬럼(train/line)이 없어 train_daily 요약을 만들 수 없습니다.")
        return pd.DataFrame()

    # event_time 기준으로 처음/마지막 위치 요약
    df_sorted = df.sort_values(["event_time"])

    agg = (
        df_sorted.groupby(["event_date", train_col, line_id_col], dropna=False)
        .agg(
            first_time=("event_time", "min"),
            last_time=("event_time", "max"),
            event_count=("event_time", "count"),
        )
        .reset_index()
    )

    agg = agg.rename(
        columns={
            train_col: "train_no",
            line_id_col: "line_id",
        }
    )

    return agg


# ------------------------------------------------
# 6) 저장 유틸
# ------------------------------------------------
def save_gold(df: pd.DataFrame, category: str, filename: str, target_ymd: str) -> Path:
    y, m, d = target_ymd[:4], target_ymd[4:6], target_ymd[6:8]
    out_dir = GOLD_DIR / "subway_position" / category / f"year={y}" / f"month={m}" / f"day={d}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / filename
    df.to_parquet(out_path, index=False)

    print(f"[INFO] Saved GOLD position {category}: {out_path}")
    return out_path


# ------------------------------------------------
# 7) MAIN
# ------------------------------------------------
def main():
    # 위치 정보도 arrival과 마찬가지로 "실제 수집한 날짜(day)" 기준
    target_ymd = datetime.today().strftime("%Y%m%d")
    print(f"[INFO] Building GOLD subway_position for {target_ymd}")

    df = load_silver_position_daily(target_ymd)
    if df.empty:
        print("[WARN] No SILVER position rows. Skip GOLD build.")
        return

    df = normalize_event_time(df)

    station_hourly = build_position_station_hourly(df)
    train_daily = build_position_train_daily(df)

    if not station_hourly.empty:
        save_gold(station_hourly, "station_hourly", "fact_position_station_hourly.parquet", target_ymd)
    else:
        print("[WARN] station_hourly summary is empty.")

    if not train_daily.empty:
        save_gold(train_daily, "train_daily", "fact_position_train_daily.parquet", target_ymd)
    else:
        print("[WARN] train_daily summary is empty.")


if __name__ == "__main__":
    main()
