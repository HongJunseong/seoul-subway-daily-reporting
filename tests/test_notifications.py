from types import SimpleNamespace
from unittest.mock import Mock
import traceback

import pytest
import requests

from src.alerts import notify_pipeline as notifications


@pytest.fixture
def transport(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/services/private-fixture")
    post = Mock(return_value=SimpleNamespace(status_code=200, text="ok", raise_for_status=Mock()))
    monkeypatch.setattr(notifications.requests, "post", post)
    return post


def test_unset_slack_does_not_use_old_discord_setting(monkeypatch, transport):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", " ")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/must-not-send")
    assert notifications.slack_notify("fixture") is False
    transport.assert_not_called()


def test_slack_payload_is_json_text_and_success_is_acknowledged(transport):
    assert notifications.slack_notify("sampled ETA 관측") is True
    assert transport.call_args.kwargs == {
        "json": {"text": "sampled ETA 관측"}, "timeout": 10, "allow_redirects": False,
    }
    assert transport.call_args.args == ("https://hooks.slack.test/services/private-fixture",)


@pytest.mark.parametrize("error", [requests.Timeout, requests.ConnectionError])
def test_transport_failure_redacts_url_and_original_exception(transport, error):
    secret = "https://hooks.slack.test/services/private-fixture"
    transport.side_effect = error(secret)
    with pytest.raises(RuntimeError, match="Slack delivery failed") as caught:
        notifications.slack_notify("fixture")
    rendered = "".join(traceback.format_exception(caught.type, caught.value, caught.tb))
    assert secret not in rendered
    assert error.__name__ not in rendered


@pytest.mark.parametrize("status,body", [(302, "secret-redirect"), (200, "secret-error"), (204, "")])
def test_non_success_acknowledgement_fails_without_response_leak(transport, status, body):
    transport.return_value = SimpleNamespace(status_code=status, text=body, raise_for_status=Mock())
    with pytest.raises(RuntimeError, match="webhook URL redacted") as caught:
        notifications.slack_notify("fixture")
    if body:
        assert body not in str(caught.value)


def test_failure_notification_contains_identity_and_log_link(transport):
    task = SimpleNamespace(dag_id="usage_daily", task_id="silver_usage", run_id="fixture",
                           log_url="http://airflow.test/log?dag_id=usage_daily")
    notifications.notify_failure({"task_instance": task})
    text = transport.call_args.kwargs["json"]["text"]
    assert "dag=usage_daily, task=silver_usage, run=fixture" in text
    assert task.log_url in text


def test_failed_delivery_callback_preserves_task_failure_and_redacts_logs(transport, caplog):
    transport.side_effect = requests.ConnectionError("private-fixture")
    notifications.notify_failure({"task_instance": SimpleNamespace(dag_id="dag", task_id="task", run_id="run")})
    assert "Failure notification could not be delivered" in caplog.text
    assert "private-fixture" not in caplog.text


def test_gold_notification_keeps_source_dates_and_independent_report(transport):
    ti = SimpleNamespace(xcom_pull=Mock(return_value={
        "target_ymd": "20260201", "usage_ymd": "20260128", "arrival_ymd": "20260131",
    }))
    assert notifications.notify_gold_ready(ti=ti) is True
    ti.xcom_pull.assert_called_once_with(task_ids="compute_dates")
    text = transport.call_args.kwargs["json"]["text"]
    assert "report=20260201, usage=20260128, arrival=20260131" in text
    assert "independently" in text


def test_http_error_response_redacts_webhook_and_body(transport):
    response = requests.Response()
    response.status_code = 403
    response.url = "https://hooks.slack.test/services/private-fixture"
    response._content = b"private-response"
    transport.return_value = response
    with pytest.raises(RuntimeError, match="Slack delivery failed") as caught:
        notifications.slack_notify("fixture")
    rendered = "".join(traceback.format_exception(caught.type, caught.value, caught.tb))
    assert "private-fixture" not in rendered
    assert "private-response" not in rendered
