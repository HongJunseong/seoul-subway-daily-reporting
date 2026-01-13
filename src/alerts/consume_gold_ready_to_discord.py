from __future__ import annotations

import os
import json
import requests
from kafka import KafkaConsumer

def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "event.pipeline")
    group_id = os.getenv("KAFKA_GROUP_ID", "alert-discord-gold-ready")
    webhook = os.environ["DISCORD_WEBHOOK_URL"]

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    for msg in consumer:
        ev = msg.value
        if ev.get("event") != "gold_ready":
            continue

        ymd = ev.get("target_ymd")
        arrival = ev.get("gold", {}).get("arrival")
        position = ev.get("gold", {}).get("position")
        run_id = ev.get("run_id")

        content = (
            f"**GOLD READY**\n"
            f"- target_ymd: `{ymd}`\n"
            f"- run_id: `{run_id}`\n"
            f"- arrival: {arrival}\n"
            f"- position: {position}\n"
        )

        r = requests.post(webhook, json={"content": content}, timeout=10)
        r.raise_for_status()
        print("[discord] sent")

if __name__ == "__main__":
    main()
