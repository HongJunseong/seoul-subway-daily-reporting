# src/config/settings.py

from __future__ import annotations
from pathlib import Path
from dotenv import load_dotenv
import os

# 프로젝트 루트: 이 파일 기준으로 상위 3단계 (상황에 따라 조정 가능)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# 디렉터리 없으면 생성
for p in [
    DATA_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
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

def get_arrival_api_key():
    key = os.getenv("SEOUL_ARRIVAL_API_KEY")
    if not key:
        raise RuntimeError("Arrival API key 없음")
    return key

def get_position_api_key():
    key = os.getenv("SEOUL_POSITION_API_KEY")
    if not key:
        raise RuntimeError("Position API key 없음")
    return key

def get_passenger_api_key():
    key = os.getenv("SEOUL_PASSENGER_API_KEY")
    if not key:
        raise RuntimeError("Passenger API key 없음")
    return key