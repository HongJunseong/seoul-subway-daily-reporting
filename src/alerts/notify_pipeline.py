"""Optional Slack incoming-webhook delivery, isolated from data publication."""
from __future__ import annotations
import logging
import os
import requests

log = logging.getLogger(__name__)


def slack_notify(content):
    webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook:
        log.info("Slack disabled: SLACK_WEBHOOK_URL is unset")
        return False
    try:
        response = requests.post(
            webhook, json={"text": content[:4000]}, timeout=10, allow_redirects=False,
        )
        response.raise_for_status()
    except requests.RequestException:
        raise RuntimeError("Slack delivery failed; webhook URL redacted") from None
    if response.status_code != 200 or response.text.strip() != "ok":
        raise RuntimeError("Slack delivery failed; webhook URL redacted")
    return True


def notify_failure(context):
    # A failed callback must not hide the original task failure.
    try:
        task = context["task_instance"]
        message = f"⚠️ Subway pipeline failed: dag={task.dag_id}, task={task.task_id}, run={task.run_id}"
        log_url = getattr(task, "log_url", None)
        if log_url:
            message += f"\nLogs: {log_url}"
        slack_notify(message)
    except Exception:
        log.warning("Failure notification could not be delivered; inspect task logs")


def notify_gold_ready(**context):
    dates = context["ti"].xcom_pull(task_ids="compute_dates")
    return slack_notify(f"✅ Subway Gold ready: report={dates['target_ymd']}, "
                        f"usage={dates['usage_ymd']}, arrival={dates['arrival_ymd']}. "
                        "Report generation runs independently of this notification.")
