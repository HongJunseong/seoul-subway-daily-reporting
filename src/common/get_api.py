# src/config/settings.py

from __future__ import annotations
import os


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

def get_passenger_api_key():
    key = os.getenv("SEOUL_PASSENGER_API_KEY")
    if not key:
        raise RuntimeError("Passenger API key 없음")
    return key