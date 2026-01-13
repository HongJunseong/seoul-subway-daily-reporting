# src/processing/build_fact_subway_arrival.py

from __future__ import annotations

import sys
from pathlib import Path
from typing import List
from glob import glob

import pandas as pd

# ---------------- 공통 경로 세팅 ----------------
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# ------------------------------------------------

from src.configs.settings import BRONZE_DIR, SILVER_DIR


def list_bronze_arrival_files() -> List[Path]:
    """
    bronze/subway_arrival/year=*/month=*/day=*/hour=*/arrivals.parquet
    패턴에 매칭되는 모든 파일 리스트 반환
    """
    base = BRONZE_DIR / "subway_arrival"
    pattern = str(base / "year=*" / "month=*" / "day=*" / "hour=*" / "arrivals.parquet")

    paths = glob(pattern)
    files = [Path(p) for p in paths]

    if not files:
        print(f"[WARN] No bronze subway_arrival files found in {base}")
    else:
        print(f"[INFO] Found {len(files)} bronze arrivals files")

    return files


def transform_to_fact_subway_arrival(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """
    Bronze subway arrival → Silver FactSubwayArrivalRealtime 변환
    (필요한 컬럼만 추출 + 타입 정리)
    """

    fact = pd.DataFrame()

    # 1) 시간 정보
    if "snapshot_ts" in df_bronze.columns:
        fact["snapshot_ts"] = pd.to_datetime(df_bronze["snapshot_ts"], errors="coerce")
    else:
        fact["snapshot_ts"] = pd.NaT

    fact["recptn_dt"] = pd.to_datetime(df_bronze.get("recptnDt"), errors="coerce")
    fact["event_time"] = fact["recptn_dt"]

    # 2) 역/호선/방향
    fact["station_id"] = pd.to_numeric(df_bronze.get("statnId"), errors="coerce").astype("Int64")
    fact["station_name"] = df_bronze.get("statnNm").astype("string")
    fact["line_id"] = pd.to_numeric(df_bronze.get("subwayId"), errors="coerce").astype("Int64")
    fact["updn_line"] = df_bronze.get("updnLine").astype("string")
    fact["train_line_name"] = df_bronze.get("trainLineNm").astype("string")

    # 3) 이전/다음/종착역 정보
    fact["prev_station_id"] = pd.to_numeric(df_bronze.get("statnFid"), errors="coerce").astype("Int64")
    fact["next_station_id"] = pd.to_numeric(df_bronze.get("statnTid"), errors="coerce").astype("Int64")
    fact["dest_station_id"] = pd.to_numeric(df_bronze.get("bstatnId"), errors="coerce").astype("Int64")
    fact["dest_station_name"] = df_bronze.get("bstatnNm").astype("string")

    # 4) 열차 정보
    fact["train_no"] = df_bronze.get("btrainNo").astype("string")
    fact["train_status"] = df_bronze.get("btrainSttus").astype("string")

    # 5) 도착 시간/상태
    fact["arrival_seconds"] = pd.to_numeric(df_bronze.get("barvlDt"), errors="coerce").astype("Int64")
    fact["arrival_status_code"] = pd.to_numeric(df_bronze.get("arvlCd"), errors="coerce").astype("Int64")
    fact["arrival_msg_primary"] = df_bronze.get("arvlMsg2").astype("string")
    fact["arrival_msg_secondary"] = df_bronze.get("arvlMsg3").astype("string")
    
    # 6) 막차 여부 (0/1 → bool 비슷하게)
    lst = df_bronze.get("lstcarAt").astype("string")
    fact["is_last_train"] = lst.replace({"1": True, "0": False})

    return fact


def save_silver_fact(df_fact: pd.DataFrame, bronze_file: Path) -> Path:
    """
    bronze 파일 경로를 기준으로 silver 경로를 구성해서 저장
    bronze:  .../bronze/subway_arrival/year=YYYY/month=MM/day=DD/hour=HH/arrivals.parquet
    silver:  .../silver/fact_subway_arrival/year=YYYY/month=MM/day=DD/hour=HH/fact_subway_arrival.parquet
    """
    # bronze/subway_arrival/ 이후의 파티션 경로(year=../month=../...)만 따오기
    rel_partition = bronze_file.parent.relative_to(BRONZE_DIR / "subway_arrival")

    out_dir = SILVER_DIR / "fact_subway_arrival" / rel_partition
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "fact_subway_arrival.parquet"
    df_fact.to_parquet(out_path, index=False)

    print(f"[INFO] Saved Silver fact: {out_path}")
    return out_path


def main():
    files = list_bronze_arrival_files()
    if not files:
        return

    # ⭐ 모든 파일을 도는 대신, 가장 최근(mtime 기준) 브론즈 파일만 처리
    latest_file = max(files, key=lambda p: p.stat().st_mtime)
    print(f"[INFO] Processing ONLY latest bronze arrival file: {latest_file}")

    df_bronze = pd.read_parquet(latest_file)
    df_fact = transform_to_fact_subway_arrival(df_bronze)
    print(f"[INFO] Fact rows: {len(df_fact)}")

    save_silver_fact(df_fact, latest_file)

    print("[DONE] FactSubwayArrivalRealtime build finished (latest snapshot only).")


if __name__ == "__main__":
    main()
