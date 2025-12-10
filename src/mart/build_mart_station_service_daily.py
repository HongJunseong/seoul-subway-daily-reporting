# src/mart/build_mart_station_service_daily.py

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.configs.settings import GOLD_DIR


# -----------------------------
# 1) GOLD 로더들
# -----------------------------
def load_usage_daily(target_ymd: str) -> pd.DataFrame:
    """
    usage는 '전날(D-1)' 승하차 통계를 가져와서
    '어제 기준 혼잡도(규모)'로 쓰도록 한다.
    """
    target_date = datetime.strptime(target_ymd, "%Y%m%d").date()
    usage_date = target_date - timedelta(days=1)
    y = usage_date.strftime("%Y")
    m = usage_date.strftime("%m")
    d = usage_date.strftime("%d")

    path = GOLD_DIR / "subway_usage" / "station_daily" / f"year={y}" / f"month={m}" / f"day={d}" / "fact_usage_station_daily.parquet"
    if not path.exists():
        print(f"[WARN] usage GOLD not found for D-1: {path}")
        return pd.DataFrame()

    df = pd.read_parquet(path)

    # 실제 골드 컬럼명에 맞게 필요하면 수정
    df = df.rename(
        columns={
            "stnCd": "station_id",
            "stnNm": "station_name",
            "lineNm": "line_name",
        }
    )

    df["usage_ref_date"] = usage_date
    return df


def load_arrival_daily(target_ymd: str) -> pd.DataFrame:
    y, m, d = target_ymd[:4], target_ymd[4:6], target_ymd[6:8]
    path = GOLD_DIR / "subway_arrival" / "station_daily" / f"year={y}" / f"month={m}" / f"day={d}" / "fact_arrival_station_daily.parquet"
    if not path.exists():
        print(f"[WARN] arrival GOLD not found: {path}")
        return pd.DataFrame()
    df = pd.read_parquet(path)
    # 여기에는 이미 station_id, station_name, line_id, event_date 등이 있다고 가정
    return df


def load_position_hourly(target_ymd: str) -> pd.DataFrame:
    """GOLD subway_position station_hourly 로드 후 하루 단위로 다시 집계."""
    y, m, d = target_ymd[:4], target_ymd[4:6], target_ymd[6:8]
    path = GOLD_DIR / "subway_position" / "station_hourly" / f"year={y}" / f"month={m}" / f"day={d}" / "fact_position_station_hourly.parquet"
    if not path.exists():
        print(f"[WARN] position GOLD not found: {path}")
        return pd.DataFrame()

    df = pd.read_parquet(path)

    # event_date, event_hour, station_id, station_name, line_id, train_count 라고 가정
    daily = (
        df.groupby(["event_date", "station_id", "station_name", "line_id"], dropna=False)
          .agg(
              avg_train_count=("train_count", "mean"),
              max_train_count=("train_count", "max"),
          )
          .reset_index()
    )
    return daily


# -----------------------------
# 2) 키 타입 정규화 (중요!)
# -----------------------------
def normalize_key_types(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    for col in ["station_id", "station_name", "line_id", "line_name"]:
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


# -----------------------------
# 3) MART 빌드
# -----------------------------
def build_mart_station_service_daily(target_ymd: str) -> pd.DataFrame:
    usage = load_usage_daily(target_ymd)
    arrival = load_arrival_daily(target_ymd)
    position_daily = load_position_hourly(target_ymd)

    # 타입 먼저 통일 (여기서 에러가 나던 것 방지)
    usage = normalize_key_types(usage)
    arrival = normalize_key_types(arrival)
    position_daily = normalize_key_types(position_daily)

    if usage.empty and arrival.empty and position_daily.empty:
        print("[WARN] 모든 소스 GOLD가 비어 있습니다.")
        return pd.DataFrame()

    mart = None

    # 1) usage + arrival
    if not usage.empty and not arrival.empty:
        mart = pd.merge(
            arrival,
            usage,
            on=["station_id", "station_name"],
            how="outer",
            suffixes=("_arr", "_usage"),
        )
    elif not usage.empty:
        mart = usage.copy()
    elif not arrival.empty:
        mart = arrival.copy()

    # 2) position_daily 도 붙이기
    if not position_daily.empty:
        if mart is None:
            mart = position_daily.copy()
        else:
            # line_id 기준까지 맞추어서 붙이되, usage에는 line_id 없을 수 있으므로 left join
            if "line_id" in mart.columns:
                mart = pd.merge(
                    mart,
                    position_daily,
                    on=["station_id", "station_name", "line_id"],
                    how="left",
                )
            else:
                mart = pd.merge(
                    mart,
                    position_daily,
                    on=["station_id", "station_name"],
                    how="left",
                    suffixes=("", "_pos"),
                )

    # event_date_x / event_date_y 통합 정리
    if "event_date_x" in mart.columns:
        mart = mart.rename(columns={"event_date_x": "event_date"})
    if "event_date_y" in mart.columns:
        if "event_date" in mart.columns:
            mart["event_date"] = mart["event_date"].fillna(mart["event_date_y"])
        else:
            mart["event_date"] = mart["event_date_y"]
        mart = mart.drop(columns=["event_date_y"])

    # 최종 target_ymd 컬럼
    mart["target_ymd"] = target_ymd

    return mart


def save_mart(mart: pd.DataFrame, target_ymd: str) -> Path:
    y, m, d = target_ymd[:4], target_ymd[4:6], target_ymd[6:8]
    out_dir = GOLD_DIR / "mart" / "station_service_daily" / f"year={y}" / f"month={m}" / f"day={d}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "mart_station_service_daily.parquet"
    mart.to_parquet(out_path, index=False)
    print(f"[INFO] Saved mart_station_service_daily: {out_path}")
    return out_path


def main():
    target_ymd = datetime.today().strftime("%Y%m%d")
    print(f"[INFO] Building MART station_service_daily for {target_ymd}")

    mart = build_mart_station_service_daily(target_ymd)
    if mart.empty:
        print("[WARN] Mart is empty. Skip saving.")
        return

    print("[INFO] Mart rows:", len(mart))
    print(mart.head(10))

    save_mart(mart, target_ymd)


if __name__ == "__main__":
    main()
