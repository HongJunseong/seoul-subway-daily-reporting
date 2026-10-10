"""Direct optional Discord delivery, isolated from data publication."""
from __future__ import annotations
import logging
import os
import requests

log = logging.getLogger(__name__)


def discord_notify(content):
    webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook:
        log.info("Discord disabled: DISCORD_WEBHOOK_URL is unset")
        return False
    try:
        response = requests.post(webhook, json={"content": content[:1900]}, timeout=10)
        response.raise_for_status()
    except requests.RequestException:
        raise RuntimeError("Discord delivery failed; webhook URL redacted") from None
    return True


def notify_failure(context):
    # A failed callback must not hide the original task failure.
    try:
        task = context["task_instance"]
        discord_notify(f"⚠️ Subway pipeline failed: dag={task.dag_id}, task={task.task_id}, run={task.run_id}")
    except Exception:
        log.warning("Failure notification could not be delivered; inspect task logs")


def notify_gold_ready(**context):
    dates = context["ti"].xcom_pull(task_ids="compute_dates")
    return discord_notify(f"✅ Subway Gold ready: report={dates['target_ymd']}, "
                          f"usage={dates['usage_ymd']}, arrival={dates['arrival_ymd']}. "
                          "Report generation runs independently of this notification.")
