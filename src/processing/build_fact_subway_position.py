# src/processing/build_fact_subway_position.py

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

import pandas as pd
from glob import glob

# ---------- 공통 경로 세팅 ----------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# ------------------------------------

from src.configs.settings import BRONZE_DIR, SILVER_DIR


def list_bronze_position_files() -> List[Path]:
    """
    data/bronze/subway_position/year=*/month=*/day=*/hour=*/positions.parquet
    패턴에 매칭되는 모든 파일 리스트 반환
    """
    base = BRONZE_DIR / "subway_position"
    pattern = str(base / "year=*" / "month=*" / "day=*" / "hour=*" / "positions.parquet")

    paths = glob(pattern)
    files = [Path(p) for p in paths]

    if not files:
        print(f"[WARN] No bronze subway_position files found in {base}")
    else:
        print(f"[INFO] Found {len(files)} bronze position files")

    return files


def transform_to_fact_subway_position(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """
    Bronze subway_position → Silver FactSubwayPositionRealtime 변환
    (필요한 컬럼만 정제)
    """

    fact = pd.DataFrame()

    # 1) 스냅샷 시각
    fact["snapshot_ts"] = pd.to_datetime(df_bronze.get("snapshot_ts"), errors="coerce")

    # 2) 호선/역 정보
    fact["line_id"] = pd.to_numeric(df_bronze.get("subwayId"), errors="coerce").astype("Int64")
    fact["line_name"] = df_bronze.get("subwayNm").astype("string")

    fact["station_id"] = pd.to_numeric(df_bronze.get("statnId"), errors="coerce").astype("Int64")
    fact["station_name"] = df_bronze.get("statnNm").astype("string")

    # 3) 상/하행
    fact["updn_line"] = pd.to_numeric(df_bronze.get("updnLine"), errors="coerce").astype("Int64")

    # 4) 열차 번호
    fact["train_no"] = df_bronze.get("trainNo").astype("string")

    # 5) 종착역 정보
    fact["dest_station_id"] = pd.to_numeric(df_bronze.get("statnTid"), errors="coerce").astype("Int64")
    fact["dest_station_name"] = df_bronze.get("statnTnm").astype("string")

    # 6) 열차 상태 코드
    fact["train_status_code"] = pd.to_numeric(df_bronze.get("trainSttus"), errors="coerce").astype("Int64")

    # 7) 급행 여부
    direct_raw = df_bronze.get("directAt").astype("string")
    fact["is_express"] = direct_raw.isin(["1", "7"])

    # 8) 막차 여부
    last_raw = df_bronze.get("lstcarAt").astype("string")
    fact["is_last_train"] = last_raw == "1"

    # 9) 최종 수신 일시 조합 (날짜 + 시간)
    date_raw = df_bronze.get("lastRecptnDt").astype("string")
    time_raw = df_bronze.get("recptnDt").astype("string")

    # "YYYY-MM-DD HH:MM:SS" 형태로 합치기 (형식이 애매하면 개별 파싱으로 fallback)
    combined = (date_raw.fillna("") + " " + time_raw.fillna("")).str.strip()
    fact["last_recptn_dt"] = pd.to_datetime(combined, errors="coerce")

    return fact


def save_silver_fact(df_fact: pd.DataFrame, bronze_file: Path) -> Path:
    """
    bronze 파티션(year=..../hour=...) 구조를 그대로 silver에 복사해서 저장.
    """
    rel_partition = bronze_file.parent.relative_to(BRONZE_DIR / "subway_position")

    out_dir = SILVER_DIR / "fact_subway_position" / rel_partition
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "fact_subway_position.parquet"
    df_fact.to_parquet(out_path, index=False)

    print(f"[INFO] Saved Silver position fact: {out_path}")
    return out_path


def main():
    files = list_bronze_position_files()
    if not files:
        return

    # ⭐ 여기서부터가 핵심 변경 부분
    # - 모든 파일을 도는 대신, 가장 최근(마지막으로 수정된) 브론즈 파일 하나만 처리
    latest_file = max(files, key=lambda p: p.stat().st_mtime)
    print(f"[INFO] Processing ONLY latest bronze position file: {latest_file}")

    df_bronze = pd.read_parquet(latest_file)
    df_fact = transform_to_fact_subway_position(df_bronze)
    print(f"[INFO] Fact rows: {len(df_fact)}")

    save_silver_fact(df_fact, latest_file)

    print("[DONE] FactSubwayPositionRealtime build finished (latest snapshot only).")


if __name__ == "__main__":
    main()
