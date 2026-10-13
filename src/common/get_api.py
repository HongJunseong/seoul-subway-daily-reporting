# src/common/get_api.py

from __future__ import annotations
import os


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