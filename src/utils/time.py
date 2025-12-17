# src/utils/time.py
from datetime import datetime
import pendulum

def now_kst() -> datetime:
    return pendulum.now("Asia/Seoul").naive()
