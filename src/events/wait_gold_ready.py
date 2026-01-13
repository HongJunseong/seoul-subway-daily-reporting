from __future__ import annotations

import os
import json
import time
from typing import Any, Dict

from kafka import KafkaConsumer


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "event.pipeline")
    group_id = os.getenv("KAFKA_GROUP_ID", "airflow-report-waiter")

    target_ymd = os.environ["TARGET_YMD"]
    timeout_sec = int(os.getenv("TIMEOUT_SEC", "900"))

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="latest",     # 지금부터 들어오는 이벤트만 기다림
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    start = time.time()
    print(f"[wait_gold_ready] waiting gold_ready target_ymd={target_ymd} (timeout={timeout_sec}s)")

    for msg in consumer:
        ev: Dict[str, Any] = msg.value

        if ev.get("event") == "gold_ready" and ev.get("target_ymd") == target_ymd:
            print("[wait_gold_ready] received event:")
            print(json.dumps(ev, ensure_ascii=False, indent=2))
            return

        if time.time() - start > timeout_sec:
            raise TimeoutError(f"[wait_gold_ready] timeout: gold_ready not received for {target_ymd}")


if __name__ == "__main__":
    main()
