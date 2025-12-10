# src/processing/build_gold_subway_usage_daily.py

from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.configs.settings import SILVER_DIR, GOLD_DIR


def load_silver(pasng_ymd: str) -> pd.DataFrame:
    y, m, d = pasng_ymd[:4], pasng_ymd[4:6], pasng_ymd[6:8]
    path = SILVER_DIR / "fact_subway_usage" / f"year={y}" / f"month={m}" / f"day={d}" / "fact_subway_usage_daily.parquet"
    if not path.exists():
        raise FileNotFoundError(f"[ERROR] SILVER not found: {path}")
    return pd.read_parquet(path)


# ---------------------------------------------------------
# 1) 역별 일일 요약 테이블 (Daily Station Summary)
# ---------------------------------------------------------
def build_station_daily(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby(["pasngYmd", "stnCd", "stnNm", "lineNo", "lineNm"], dropna=False)
          [["rideNope", "gffNope", "totalNope"]]
          .sum()
          .reset_index()
    )
    summary["rank_daily"] = summary["totalNope"].rank(ascending=False, method="dense")
    return summary


# ---------------------------------------------------------
# 2) 역×시간대 패턴 (Hourly Station Usage)
# ---------------------------------------------------------
def build_station_hourly(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby(["pasngYmd", "pasngHr", "stnCd", "stnNm", "lineNo", "lineNm"])
          [["rideNope", "gffNope", "totalNope"]]
          .sum()
          .reset_index()
    )
    return summary


# ---------------------------------------------------------
# 3) 호선별 일일 요약 테이블 (Line Daily Summary)
# ---------------------------------------------------------
def build_line_daily(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby(["pasngYmd", "lineNo", "lineNm"])
          [["rideNope", "gffNope", "totalNope"]]
          .sum()
          .reset_index()
    )
    summary["rank_daily"] = summary["totalNope"].rank(ascending=False, method="dense")
    return summary


# ---------------------------------------------------------
# 저장 헬퍼
# ---------------------------------------------------------
def save_gold(df: pd.DataFrame, pasng_ymd: str, category: str, filename: str):
    y, m, d = pasng_ymd[:4], pasng_ymd[4:6], pasng_ymd[6:8]
    out_dir = GOLD_DIR / "subway_usage" / category / f"year={y}" / f"month={m}" / f"day={d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / filename
    df.to_parquet(out_path, index=False)
    print(f"[INFO] Saved GOLD {category}: {out_path}")
    return out_path


# ---------------------------------------------------------
# 메인 실행
# ---------------------------------------------------------
def build_gold(pasng_ymd: str):
    df = load_silver(pasng_ymd)

    station_daily = build_station_daily(df)
    station_hourly = build_station_hourly(df)
    line_daily = build_line_daily(df)

    save_gold(station_daily, pasng_ymd, "station_daily", "fact_usage_station_daily.parquet")
    save_gold(station_hourly, pasng_ymd, "station_hourly", "fact_usage_station_hourly.parquet")
    save_gold(line_daily, pasng_ymd, "line_daily", "fact_usage_line_daily.parquet")


def main():
    target_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    print(f"[INFO] Building GOLD for {target_date}")
    build_gold(target_date)


if __name__ == "__main__":
    main()
