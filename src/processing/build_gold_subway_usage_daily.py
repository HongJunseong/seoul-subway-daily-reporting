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


# ---------------------------------------------------------
# 공통: 컬럼 이름 유연하게 찾기
# ---------------------------------------------------------
def pick_first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ---------------------------------------------------------
# SILVER 로딩 (하루치)
# ---------------------------------------------------------
def load_silver(pasng_ymd: str) -> pd.DataFrame:
    y, m, d = pasng_ymd[:4], pasng_ymd[4:6], pasng_ymd[6:8]
    path = SILVER_DIR / "fact_subway_usage" / f"year={y}" / f"month={m}" / f"day={d}" / "fact_subway_usage_daily.parquet"

    if not path.exists():
        print(f"[WARN] SILVER usage not found for {pasng_ymd}: {path}")
        return pd.DataFrame()

    df = pd.read_parquet(path)
    print(f"[INFO] Loaded SILVER usage rows: {len(df)} from {path}")
    return df


# ---------------------------------------------------------
# 1) 역별 일일 요약 (Daily Station Summary)
# ---------------------------------------------------------
def build_station_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    date_col = pick_first_existing(df, ["pasngYmd", "use_ymd"])
    stn_nm_col = pick_first_existing(df, ["stnNm", "station_name"])
    line_nm_col = pick_first_existing(df, ["lineNm", "line_name"])

    stn_cd_col = pick_first_existing(df, ["stnCd"])
    line_no_col = pick_first_existing(df, ["lineNo"])

    ride_col = pick_first_existing(df, ["rideNope", "ride_count"])
    gff_col = pick_first_existing(df, ["gffNope", "alight_count"])
    total_col = pick_first_existing(df, ["totalNope", "total_count"])

    if date_col is None or stn_nm_col is None or line_nm_col is None or total_col is None:
        print("[WARN] 필수 컬럼이 부족하여 station_daily를 생성할 수 없습니다.")
        return pd.DataFrame()

    group_cols = [date_col]
    if stn_cd_col:
        group_cols.append(stn_cd_col)
    group_cols.append(stn_nm_col)
    if line_no_col:
        group_cols.append(line_no_col)
    group_cols.append(line_nm_col)

    agg_cols = {}
    if ride_col:
        agg_cols["rideNope"] = (ride_col, "sum")
    if gff_col:
        agg_cols["gffNope"] = (gff_col, "sum")
    agg_cols["totalNope"] = (total_col, "sum")

    summary = df.groupby(group_cols, dropna=False).agg(**agg_cols).reset_index()

    rename_map = {
        date_col: "pasngYmd",
        stn_cd_col or "": "stnCd",
        stn_nm_col: "stnNm",
        line_no_col or "": "lineNo",
        line_nm_col: "lineNm"
    }
    rename_map = {k: v for k, v in rename_map.items() if k}

    summary = summary.rename(columns=rename_map)

    summary["rank_daily"] = summary["totalNope"].rank(ascending=False, method="dense")

    return summary


# ---------------------------------------------------------
# 2) 호선별 일일 요약 (Line Daily Summary)
# ---------------------------------------------------------
def build_line_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    date_col = pick_first_existing(df, ["pasngYmd", "use_ymd"])
    line_nm_col = pick_first_existing(df, ["lineNm", "line_name"])
    line_no_col = pick_first_existing(df, ["lineNo"])

    ride_col = pick_first_existing(df, ["rideNope", "ride_count"])
    gff_col = pick_first_existing(df, ["gffNope", "alight_count"])
    total_col = pick_first_existing(df, ["totalNope", "total_count"])

    if date_col is None or line_nm_col is None or total_col is None:
        print("[WARN] 필수 컬럼이 부족하여 line_daily를 생성할 수 없습니다.")
        return pd.DataFrame()

    group_cols = [date_col]
    if line_no_col:
        group_cols.append(line_no_col)
    group_cols.append(line_nm_col)

    agg_cols = {}
    if ride_col:
        agg_cols["rideNope"] = (ride_col, "sum")
    if gff_col:
        agg_cols["gffNope"] = (gff_col, "sum")
    agg_cols["totalNope"] = (total_col, "sum")

    summary = df.groupby(group_cols).agg(**agg_cols).reset_index()

    rename_map = {
        date_col: "pasngYmd",
        line_no_col or "": "lineNo",
        line_nm_col: "lineNm",
    }
    rename_map = {k: v for k, v in rename_map.items() if k}

    summary = summary.rename(columns=rename_map)

    summary["rank_daily"] = summary["totalNope"].rank(ascending=False, method="dense")

    return summary


# ---------------------------------------------------------
# 저장 헬퍼
# ---------------------------------------------------------
def save_gold(df: pd.DataFrame, pasng_ymd: str, category: str, filename: str):
    if df.empty:
        print(f"[INFO] GOLD {category} is empty. Skip saving.")
        return None

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
    if df.empty:
        print(f"[WARN] No SILVER usage data for {pasng_ymd}. Skip GOLD build.")
        return

    # 1) 역별 일일 요약
    station_daily = build_station_daily(df)
    save_gold(station_daily, pasng_ymd, "station_daily", "fact_usage_station_daily.parquet")

    # 2) 호선별 일일 요약
    line_daily = build_line_daily(df)
    save_gold(line_daily, pasng_ymd, "line_daily", "fact_usage_line_daily.parquet")


def main():
    # D+3 업데이트라 4일 전 데이터가 안정적임
    target_date = (datetime.today() - timedelta(days=4)).strftime("%Y%m%d")
    print(f"[INFO] Building GOLD subway_usage for {target_date}")
    build_gold(target_date)


if __name__ == "__main__":
    main()
