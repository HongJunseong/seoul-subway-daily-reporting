# src/processing/build_fact_subway_usage_daily.py

from __future__ import annotations

import sys
from pathlib import Path
from glob import glob
from typing import List

import pandas as pd

# ---------------- 공통 경로 세팅 ----------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# ------------------------------------------------

from src.configs.settings import BRONZE_DIR, SILVER_DIR


def list_bronze_usage_files() -> List[Path]:
    """
    bronze/subway_usage/year=*/month=*/day=*/usage.parquet
    패턴에 매칭되는 모든 BRONZE 파일 리스트 반환
    """
    base = BRONZE_DIR / "subway_usage"
    pattern = str(base / "year=*" / "month=*" / "day=*" / "usage.parquet")

    paths = glob(pattern)
    files = [Path(p) for p in paths]

    if not files:
        print(f"[WARN] No bronze subway_usage files found in {base}")
    else:
        print(f"[INFO] Found {len(files)} bronze usage files")

    return files


def transform_to_fact_subway_usage_daily(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """
    CardSubwayStatsNew BRONZE → SILVER FactSubwayUsageDaily 변환

    입력 컬럼(주요):
      - USE_YMD: 사용일자 (YYYYMMDD)
      - SBWY_ROUT_LN_NM: 호선명
      - SBWY_STNS_NM: 역명
      - GTON_TNOPE: 승차총승객수
      - GTOFF_TNOPE: 하차총승객수
      - REG_YMD: 등록일자
    """

    fact = pd.DataFrame()

    # 1) 날짜 정보
    if "USE_YMD" in df_bronze.columns:
        fact["use_ymd"] = df_bronze["USE_YMD"].astype("string")
        fact["use_date"] = pd.to_datetime(df_bronze["USE_YMD"], format="%Y%m%d", errors="coerce")
    else:
        fact["use_ymd"] = pd.Series(dtype="string")
        fact["use_date"] = pd.NaT

    if "REG_YMD" in df_bronze.columns:
        fact["reg_date"] = pd.to_datetime(df_bronze["REG_YMD"], format="%Y%m%d", errors="coerce")
    else:
        fact["reg_date"] = pd.NaT

    # 2) 호선/역 이름
    fact["line_name"] = df_bronze.get("SBWY_ROUT_LN_NM", pd.Series(dtype="string")).astype("string")
    fact["station_name"] = df_bronze.get("SBWY_STNS_NM", pd.Series(dtype="string")).astype("string")

    # 3) 승차/하차/합계 인원
    ride = pd.to_numeric(df_bronze.get("GTON_TNOPE"), errors="coerce")
    alight = pd.to_numeric(df_bronze.get("GTOFF_TNOPE"), errors="coerce")

    ride = ride.fillna(0)
    alight = alight.fillna(0)

    fact["ride_count"] = ride.astype("Int64")
    fact["alight_count"] = alight.astype("Int64")
    fact["total_count"] = (ride + alight).astype("Int64")

    return fact


def save_silver_fact(df_fact: pd.DataFrame, bronze_file: Path) -> Path:
    """
    bronze 경로를 기준으로 silver 경로를 구성해서 저장.

    bronze:
      .../bronze/subway_usage/year=YYYY/month=MM/day=DD/usage.parquet

    silver:
      .../silver/fact_subway_usage/year=YYYY/month=MM/day=DD/fact_subway_usage_daily.parquet
    """
    # subway_usage 이후의 파티션 경로(year=../month=../day=..)만 추출
    rel_partition = bronze_file.parent.relative_to(BRONZE_DIR / "subway_usage")

    out_dir = SILVER_DIR / "fact_subway_usage" / rel_partition
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "fact_subway_usage_daily.parquet"
    df_fact.to_parquet(out_path, index=False)

    print(f"[INFO] Saved Silver fact usage: {out_path}")
    return out_path


def main():
    files = list_bronze_usage_files()
    if not files:
        return

    # ⭐ 모든 파일을 도는 대신, 가장 최근(mtime 기준) BRONZE 파일 1개만 처리
    latest_file = max(files, key=lambda p: p.stat().st_mtime)
    print(f"[INFO] Processing ONLY latest bronze usage file: {latest_file}")

    df_bronze = pd.read_parquet(latest_file)
    print(f"[INFO] Bronze rows: {len(df_bronze)}")

    df_fact = transform_to_fact_subway_usage_daily(df_bronze)
    print(f"[INFO] Fact rows: {len(df_fact)}")

    save_silver_fact(df_fact, latest_file)

    print("[DONE] FactSubwayUsageDaily build finished (latest snapshot only).")


if __name__ == "__main__":
    main()
