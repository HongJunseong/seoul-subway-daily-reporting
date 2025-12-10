# src/utils/station_loader.py

from pathlib import Path
import pandas as pd
import sys

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.configs.settings import SILVER_DIR, BRONZE_DIR


def _load_latest_parquet(base: Path, pattern: str) -> pd.DataFrame | None:
    """
    base 아래에서 pattern에 맞는 parquet 중 가장 최근 파일 하나를 읽어서 반환.
    없으면 None.
    """
    files = sorted(base.glob(pattern))
    if not files:
        return None

    latest = files[-1]
    print(f"[INFO] Using usage file: {latest}")
    return pd.read_parquet(latest)


# -----------------------------------------------------
#  1) STATION NAME LOADER
# -----------------------------------------------------
def load_station_names_from_usage() -> list[str]:
    """
    1순위: SILVER usage(fact_subway_usage_daily)에서 station_name 추출
    2순위: BRONZE usage(subway_usage)에서 SBWY_STNS_NM 추출
    """
    # 1) SILVER 검색
    silver_base = SILVER_DIR / "fact_subway_usage_daily"
    df = _load_latest_parquet(
        silver_base,
        "year=*/month=*/day=*/fact_subway_usage_daily.parquet"
    )

    # 2) SILVER 없으면 BRONZE 사용
    if df is None:
        bronze_base = BRONZE_DIR / "subway_usage"
        df = _load_latest_parquet(
            bronze_base,
            "year=*/month=*/day=*/usage.parquet"
        )

    if df is None:
        raise RuntimeError("Usage 데이터가 없습니다. 역 목록을 생성할 수 없습니다.")

    # NEW 컬럼 우선순위
    candidate_cols = ["station_name", "SBWY_STNS_NM"]
    col = None
    for c in candidate_cols:
        if c in df.columns:
            col = c
            break

    if col is None:
        raise RuntimeError(f"역 이름 컬럼(station_name/SBWY_STNS_NM)이 없습니다. columns={df.columns}")

    stations = sorted(df[col].dropna().unique().tolist())
    print(f"[INFO] Loaded {len(stations)} station names.")
    return stations


# -----------------------------------------------------
#  2) LINE NAME LOADER
# -----------------------------------------------------
def load_lines_from_usage() -> list[str]:
    """
    SILVER usage 데이터에서 호선명(line_name) 가져오기.
    """
    silver_root = SILVER_DIR / "fact_subway_usage"
    files = list(silver_root.glob("year=*/month=*/day=*/fact_subway_usage_daily.parquet"))
    if not files:
        raise RuntimeError("Usage SILVER 데이터 없음. 호선 목록을 생성할 수 없습니다.")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    # NEW 호선 컬럼명
    candidate_cols = ["line_name", "SBWY_ROUT_LN_NM"]

    col = None
    for c in candidate_cols:
        if c in df.columns:
            col = c
            break

    if col is None:
        raise RuntimeError(f"Usage 데이터에서 호선 컬럼을 찾을 수 없습니다. columns={df.columns}")

    return sorted(df[col].dropna().unique().tolist())
