# src/utils/station_loader.py

from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# usage용 SILVER_DIR, BRONZE_DIR은 이 파일에선 안 써도 되긴 하지만
# 나중에 dim_station/dim_line 만들 때 쓸 수 있으니 남겨둬도 무방
from src.configs.settings import SILVER_DIR, BRONZE_DIR  # 필요 없으면 삭제해도 됨

# 📌 서울시 실시간 도착 역정보 엑셀(공식 기준)
#    파일 위치: <프로젝트 루트>/data/reference/실시간도착_역정보(20251103).xlsx
REFERENCE_DIR = PROJECT_ROOT / "data" / "reference"
REALTIME_STATION_INFO_PATH = REFERENCE_DIR / "실시간도착_역정보(20251103).xlsx"


# -----------------------------------------------------
#  🔥 서울시 '실시간 도착 역정보' 기반 로더들
#     (SUBWAY_ID / STATN_ID / STATN_NM / 호선이름)
# -----------------------------------------------------
def _load_realtime_station_reference() -> pd.DataFrame:
    """
    서울시에서 제공한 '실시간도착_역정보(20251103).xlsx'를 읽어서 반환.

    expected columns:
      - SUBWAY_ID : 지하철 호선 ID (예: 1001, 1002, 1063 ...)
      - STATN_ID  : 역 ID
      - STATN_NM  : 역명 (실시간 API에서 사용하는 공식 역명)
      - 호선이름   : 호선 이름 (예: 1호선, 경의중앙선 ...)
    """
    path = REALTIME_STATION_INFO_PATH
    if not path.exists():
        raise FileNotFoundError(f"실시간 역 정보 파일을 찾을 수 없습니다: {path}")

    df = pd.read_excel(path)

    required_cols = {"SUBWAY_ID", "STATN_ID", "STATN_NM", "호선이름"}
    missing = required_cols.difference(df.columns)
    if missing:
        raise RuntimeError(
            f"실시간 역 정보 파일에 필요한 컬럼이 없습니다. "
            f"missing={missing}, columns={df.columns}"
        )

    print(f"[INFO] Loaded realtime station reference from: {path}")
    return df


def load_station_names_from_realtime_ref() -> list[str]:
    """
    실시간 도착/위치 API에서 인식하는 공식 역 이름 리스트.
    → '실시간도착_역정보(20251103).xlsx'의 STATN_NM 사용.
    arrival ingest 에서 STATN_NM 그대로 써서 호출하면 됨.
    """
    df = _load_realtime_station_reference()
    stations = sorted(df["STATN_NM"].dropna().unique().tolist())
    print(f"[INFO] Loaded {len(stations)} station names from REALTIME reference (STATN_NM).")
    return stations


def load_lines_from_realtime_ref() -> list[str]:
    """
    실시간 도착/위치 API에서 사용할 호선 이름 리스트.
    → '실시간도착_역정보(20251103).xlsx'의 '호선이름' 사용.

    position ingest 에서는 이 리스트를 사용해서
    realtimePosition(line_name) 호출에 쓰면 됨.
    """
    df = _load_realtime_station_reference()
    lines = sorted(df["호선이름"].dropna().unique().tolist())
    print(f"[INFO] Loaded {len(lines)} line names from REALTIME reference (호선이름).")
    return lines


def load_subway_id_mapping() -> pd.DataFrame:
    """
    SUBWAY_ID / STATN_ID / STATN_NM / 호선이름 전체를 그대로 반환.
    dim_station / dim_line 설계, 조인 키 만들 때 사용.
    """
    return _load_realtime_station_reference().copy()
