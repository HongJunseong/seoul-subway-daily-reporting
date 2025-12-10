# src/debug/inspect_fact_subway_arrival.py

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# 프로젝트 루트 & src 경로 추가
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.configs.settings import SILVER_DIR


def main():
    base = SILVER_DIR / "fact_subway_arrival"

    # fact_subway_arrival.parquet 전부 찾기
    files = list(base.rglob("fact_subway_arrival.parquet"))
    if not files:
        print(f"[ERROR] No silver files found under {base}")
        return

    # 가장 최근 수정된 파일 하나 선택
    latest = max(files, key=lambda p: p.stat().st_mtime)
    print(f"[INFO] Latest silver file: {latest}")

    df = pd.read_parquet(latest)

    print("\n[INFO] Row count:", len(df))
    print("\n[INFO] Head:")
    print(df.head(10))

    print("\n[INFO] dtypes:")
    print(df.dtypes)

    # 간단한 sanity check 몇 개
    print("\n[INFO] line_id counts:")
    print(df["line_id"].value_counts().head())

    print("\n[INFO] arrival_status_code counts:")
    print(df["arrival_status_code"].value_counts().head())

    print("\n[INFO] is_last_train true ratio:",
          df["is_last_train"].value_counts(normalize=True))


if __name__ == "__main__":
    main()
