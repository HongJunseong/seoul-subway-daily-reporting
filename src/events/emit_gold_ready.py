from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Dict, Any

from kafka import KafkaProducer


def _make_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
    )


def _build_paths(target_ymd: str) -> tuple[str, str]:
    """
    Gold 산출물 경로를 환경변수 템플릿으로 만든다.
    - GOLD_ARRIVAL_PATH_TMPL 예: s3://bucket/gold/subway_arrival/dt={target_ymd}/
    - GOLD_POSITION_PATH_TMPL 예: s3://bucket/gold/subway_position/dt={target_ymd}/
    """
    arrival_tmpl = os.environ["GOLD_ARRIVAL_PATH_TMPL"]
    position_tmpl = os.environ["GOLD_POSITION_PATH_TMPL"]
    return (
        arrival_tmpl.format(target_ymd=target_ymd),
        position_tmpl.format(target_ymd=target_ymd),
    )


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "event.pipeline")

    target_ymd = os.environ["TARGET_YMD"]
    gold_arrival_path, gold_position_path = _build_paths(target_ymd)

    event: Dict[str, Any] = {
        "event": "gold_ready",
        "project": "seoul-subway-daily-reporting",
        "target_ymd": target_ymd,
        "gold": {
            "arrival": gold_arrival_path,
            "position": gold_position_path,
        },
        "generated_at": datetime.now().astimezone().isoformat(),
        "run_id": os.getenv("RUN_ID"),
    }

    producer = _make_producer(bootstrap)
    producer.send(topic, key=f"gold_ready:{target_ymd}".encode("utf-8"), value=event)
    producer.flush()

    print("[emit_gold_ready] emitted event:")
    print(json.dumps(event, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
