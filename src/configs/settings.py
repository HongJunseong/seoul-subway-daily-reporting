# src/config/settings.py

from __future__ import annotations
from pathlib import Path
import os

# 프로젝트 루트: 이 파일 기준으로 상위 3단계 (상황에 따라 조정 가능)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

BRONZE_SUBWAY_PASSENGER_DIR = BRONZE_DIR / "subway_passenger"
SILVER_FACT_SUBWAY_PASSENGER_DIR = SILVER_DIR / "fact_subway_passenger"

# 디렉터리 없으면 생성
for p in [
    DATA_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
    BRONZE_SUBWAY_PASSENGER_DIR,
    SILVER_FACT_SUBWAY_PASSENGER_DIR,
]:
    p.mkdir(parents=True, exist_ok=True)


def get_env(key: str, default: str | None = None) -> str:
    """
    환경변수에서 key를 가져오고, 없으면 default 사용.
    필수 값인데 없으면 에러 발생.
    """
    value = os.getenv(key, default)
    if value is None:
        raise RuntimeError(f"환경변수 {key}가 설정되어 있지 않습니다.")
    return value

def get_seoul_api_key() -> str:
    key = os.getenv("SEOUL_OPENAPI_KEY")
    if not key:
        raise RuntimeError("환경변수 SEOUL_OPENAPI_KEY 가 설정되어 있지 않습니다. (.env 확인)")
    return key