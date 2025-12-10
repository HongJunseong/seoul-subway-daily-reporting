# src/report/build_report_context_daily.py

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple

import pandas as pd

# ---------- 공통 경로 세팅 ----------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# ------------------------------------

from src.configs.settings import GOLD_DIR

USAGE_LAG_DAYS = 4
ARRIVAL_LAG_DAYS = 0
POSITION_LAG_DAYS = 0

def _find_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """
    주어진 후보 컬럼명 리스트 중, DataFrame에 실제로 존재하는 첫 번째 컬럼명을 반환.
    없으면 None.
    """
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

def get_date_parts(target_ymd: str) -> Tuple[str, str, str]:
    return target_ymd[:4], target_ymd[4:6], target_ymd[6:8]


# ============================================================
# 1) GOLD 로더들
# ============================================================
def load_usage_gold(target_ymd: str) -> pd.DataFrame:
    target_date = datetime.strptime(target_ymd, "%Y%m%d").date()
    usage_date = target_date - timedelta(days=USAGE_LAG_DAYS)

    y = usage_date.strftime("%Y")
    m = usage_date.strftime("%m")
    d = usage_date.strftime("%d")

    path = GOLD_DIR / "subway_usage" / "station_daily" / f"year={y}" / f"month={m}" / f"day={d}" / "fact_usage_station_daily.parquet"
    if not path.exists():
        print(f"[WARN] usage GOLD not found: {path}")
        return pd.DataFrame()

    df = pd.read_parquet(path)
    print(f"[INFO] Loaded usage GOLD rows: {len(df)}")
    return df


def load_arrival_kpi_gold(target_ymd: str) -> pd.DataFrame:
    target_date = datetime.strptime(target_ymd, "%Y%m%d").date()
    arr_date = target_date - timedelta(days=ARRIVAL_LAG_DAYS)

    y = arr_date.strftime("%Y")
    m = arr_date.strftime("%m")
    d = arr_date.strftime("%d")

    path = GOLD_DIR / "subway_arrival" / "station_daily_kpi" / f"year={y}" / f"month={m}" / f"day={d}" / "fact_arrival_station_daily_kpi.parquet"
    if not path.exists():
        print(f"[WARN] arrival station_daily_kpi GOLD not found: {path}")
        return pd.DataFrame()

    df = pd.read_parquet(path)
    print(f"[INFO] Loaded arrival station_daily_kpi GOLD rows: {len(df)}")
    return df


def load_position_line_hourly_gold(target_ymd: str) -> pd.DataFrame:
    target_date = datetime.strptime(target_ymd, "%Y%m%d").date()
    pos_date = target_date - timedelta(days=POSITION_LAG_DAYS)

    y = pos_date.strftime("%Y")
    m = pos_date.strftime("%m")
    d = pos_date.strftime("%d")

    path = GOLD_DIR / "subway_position" / "line_hourly" / f"year={y}" / f"month={m}" / f"day={d}" / "fact_position_line_hourly.parquet"
    if not path.exists():
        print(f"[WARN] position line_hourly GOLD not found: {path}")
        return pd.DataFrame()

    df = pd.read_parquet(path)
    print(f"[INFO] Loaded position line_hourly GOLD rows: {len(df)}")
    return df


# ============================================================
# 2) usage 요약
# ============================================================
def summarize_usage(df_usage: pd.DataFrame) -> Dict[str, Any]:
    if df_usage.empty:
        return {"has_data": False}

    # ---- 컬럼 후보 세팅 ----
    station_col = _find_first_existing_column(
        df_usage,
        ["station_name", "stnNm", "STN_NM", "역명"]
    )
    line_col = _find_first_existing_column(
        df_usage,
        ["line_name", "lineNm", "LINE_NM", "호선명"]
    )
    total_col = _find_first_existing_column(
        df_usage,
        ["totalNope", "TOTALNOPE", "tot_nope", "승하차총승객수", "total"]
    )
    rank_col = _find_first_existing_column(
        df_usage,
        ["rank_daily", "RANK_DAILY", "rank", "순위"]
    )

    # 아무 것도 못 찾으면 그냥 has_data=False로
    if not station_col or not total_col:
        print("[ERROR] usage GOLD missing required columns for summarization")
        print("  columns:", list(df_usage.columns))
        return {"has_data": False}

    # ===== 1) 상위 혼잡역 10개 =====
    select_cols = [station_col, total_col]
    if line_col:
        select_cols.append(line_col)
    if rank_col:
        select_cols.append(rank_col)

    tmp_top = (
        df_usage
        .sort_values(total_col, ascending=False)
        .head(10)[select_cols]
        .copy()
    )

    # 표준 키 이름으로 맞춰두기
    rename_map = {
        station_col: "station_name",
        total_col: "totalNope",
    }
    if line_col:
        rename_map[line_col] = "line_name"
    if rank_col:
        rename_map[rank_col] = "rank_daily"

    tmp_top.rename(columns=rename_map, inplace=True)

    # 없는 컬럼은 None으로 채워서 키는 고정
    if "line_name" not in tmp_top.columns:
        tmp_top["line_name"] = None
    if "rank_daily" not in tmp_top.columns:
        tmp_top["rank_daily"] = None

    top_stations = tmp_top[["station_name", "line_name", "totalNope", "rank_daily"]].to_dict(
        orient="records"
    )

    # ===== 2) 노선별 평균/최대 이용량 =====
    line_usage_stats: list[dict] = []
    if line_col:
        tmp_line = (
            df_usage
            .groupby(line_col)[total_col]
            .agg(["mean", "max"])
            .reset_index()
            .rename(
                columns={
                    line_col: "line_name",
                    "mean": "avg_totalNope",
                    "max": "max_totalNope",
                }
            )
        )
        line_usage_stats = tmp_line.to_dict(orient="records")

    return {
        "has_data": True,
        "top_stations_by_total": top_stations,
        "line_usage_stats": line_usage_stats,
    }


# ============================================================
# 3) arrival 요약 (station_daily_kpi)
# ============================================================
def summarize_arrival(df_arr: pd.DataFrame, df_usage: pd.DataFrame) -> Dict[str, Any]:
    if df_arr.empty:
        return {"has_data": False}

    # -------------------------------
    # 1) 노선(line_id) 기준 대기/급행 지표
    # -------------------------------
    by_line = (
        df_arr
        .groupby("line_id")
        .agg(
            avg_wait_sec=("avg_wait_sec", "mean"),
            p95_wait_sec=("p95_wait_sec", "mean"),
            long_wait_ratio_5=("long_wait_ratio_5", "mean"),
            express_ratio=("express_ratio", "mean"),
        )
        .reset_index()
        .to_dict(orient="records")
    )

    # -------------------------------
    # 2) usage와 결합해서 "고통역" TOP10
    #    (대기 길고 이용량도 많은 역)
    # -------------------------------
    painful_stations: list[dict] = []

    if not df_usage.empty:
        # usage에서 역 이름 / 이용량 컬럼 찾기
        usage_station_col = _find_first_existing_column(
            df_usage,
            ["station_name", "stnNm", "STN_NM", "역명"]
        )
        usage_total_col = _find_first_existing_column(
            df_usage,
            ["totalNope", "TOTALNOPE", "tot_nope", "승하차총승객수", "total"]
        )

        if usage_station_col and usage_total_col and "station_name" in df_arr.columns:
            usage_for_merge = df_usage[[usage_station_col, usage_total_col]].copy()
            usage_for_merge.rename(
                columns={
                    usage_station_col: "station_name",
                    usage_total_col: "totalNope",
                },
                inplace=True,
            )

            merged = pd.merge(
                df_arr,
                usage_for_merge,
                on="station_name",
                how="inner",
            )

            if not merged.empty and "avg_wait_sec" in merged.columns and "totalNope" in merged.columns:
                merged["avg_wait_norm"] = merged["avg_wait_sec"] / (merged["avg_wait_sec"].max() + 1e-6)
                merged["totalNope_norm"] = merged["totalNope"] / (merged["totalNope"].max() + 1e-6)
                merged["pain_score"] = merged["avg_wait_norm"] * merged["totalNope_norm"]

                # line_name은 arrival_kpi에는 없을 수 있으니, 있으면 같이 포함
                select_cols = [
                    "station_name",
                    "line_id",
                    "avg_wait_sec",
                    "p95_wait_sec",
                    "long_wait_ratio_5",
                    "totalNope",
                    "pain_score",
                ]
                select_cols = [c for c in select_cols if c in merged.columns]

                painful_stations = (
                    merged
                    .sort_values("pain_score", ascending=False)
                    .head(10)[select_cols]
                    .to_dict(orient="records")
                )

    # -------------------------------
    # 3) 막차 정보: 막차가 가장 늦게 있었던 역 TOP10
    # -------------------------------
    last_train_info: list[dict] = []
    if "last_train_dt" in df_arr.columns:
        tmp = df_arr[~df_arr["last_train_dt"].isna()].copy()
        if not tmp.empty:
            tmp["last_train_time_str"] = tmp["last_train_dt"].astype(str)
            cols = ["station_name", "line_id", "last_train_time_str", "last_train_wait_sec"]
            cols = [c for c in cols if c in tmp.columns]

            last_train_info = (
                tmp
                .sort_values("last_train_dt", ascending=False)
                .head(10)[cols]
                .to_dict(orient="records")
            )

    # -------------------------------
    # 4) 야간 이벤트 많은 역 TOP10
    # -------------------------------
    late_night_hotspots: list[dict] = []
    if "late_night_events" in df_arr.columns:
        cols = ["station_name", "line_id", "late_night_events"]
        cols = [c for c in cols if c in df_arr.columns]

        late_night_hotspots = (
            df_arr
            .sort_values("late_night_events", ascending=False)
            .head(10)[cols]
            .to_dict(orient="records")
        )

    return {
        "has_data": True,
        "line_wait_stats": by_line,
        "painful_stations": painful_stations,
        "last_train_info": last_train_info,
        "late_night_hotspots": late_night_hotspots,
    }



# ============================================================
# 4) position 요약 (line_hourly)
# ============================================================
def summarize_position(df_pos: pd.DataFrame) -> Dict[str, Any]:
    if df_pos.empty:
        return {"has_data": False}

    df = df_pos.copy()

    # 노선별 전체 active_trains 평균/최대
    line_overall = (
        df
        .groupby(["line_id", "line_name"])
        .agg(
            avg_active_trains=("active_trains", "mean"),
            max_active_trains=("active_trains", "max"),
        )
        .reset_index()
        .to_dict(orient="records")
    )

    # 출근/퇴근 시간대 (7–9, 18–20) 집중도
    df_peak = df[df["event_hour"].isin([7, 8, 9, 18, 19, 20])]
    peak_stats = []
    if not df_peak.empty:
        peak_stats = (
            df_peak
            .groupby(["line_id", "line_name"])
            .agg(
                avg_active_trains_peak=("active_trains", "mean"),
                max_active_trains_peak=("active_trains", "max"),
            )
            .reset_index()
            .to_dict(orient="records")
        )

    # 상·하행 비대칭성
    asymmetry = []
    if "updn_line" in df.columns:
        asym = (
            df
            .groupby(["line_id", "line_name", "updn_line"])["active_trains"]
            .mean()
            .reset_index()
        )
        pivot = asym.pivot_table(
            index=["line_id", "line_name"],
            columns="updn_line",
            values="active_trains",
            fill_value=0.0,
        ).reset_index()

        pivot["up_active"] = pivot.get(0, 0.0)
        pivot["down_active"] = pivot.get(1, 0.0)
        pivot["direction_gap"] = (pivot["up_active"] - pivot["down_active"]).abs()

        asymmetry = pivot[
            ["line_id", "line_name", "up_active", "down_active", "direction_gap"]
        ].to_dict(orient="records")

    return {
        "has_data": True,
        "line_overall": line_overall,
        "peak_stats": peak_stats,
        "direction_asymmetry": asymmetry,
    }


# ============================================================
# 5) 전체 리포트 컨텍스트
# ============================================================
def build_report_context(target_ymd: str) -> Dict[str, Any]:
    print(f"[INFO] Building report context for {target_ymd}")

    usage = load_usage_gold(target_ymd)
    arrival_kpi = load_arrival_kpi_gold(target_ymd)
    position_hourly = load_position_line_hourly_gold(target_ymd)

    usage_ctx = summarize_usage(usage)
    arrival_ctx = summarize_arrival(arrival_kpi, usage)
    position_ctx = summarize_position(position_hourly)

    ctx = {
        "target_ymd": target_ymd,
        "usage": usage_ctx,
        "arrival": arrival_ctx,
        "position": position_ctx,
    }
    return ctx


def main():
    target_ymd = datetime.today().strftime("%Y%m%d")
    ctx = build_report_context(target_ymd)

    print("\n=== REPORT CONTEXT (SUMMARY) ===")
    print("target_ymd:", ctx["target_ymd"])
    print("\n[usage]")
    print(ctx["usage"])
    print("\n[arrival]")
    print(ctx["arrival"])
    print("\n[position]")
    print(ctx["position"])


if __name__ == "__main__":
    main()