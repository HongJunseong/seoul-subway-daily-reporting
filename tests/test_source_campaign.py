from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json

import pytest
import requests

from scripts import capture_sources
from scripts import run_source_observation_day as campaign
from src.common.dates import KST


@pytest.fixture
def folder(tmp_path, monkeypatch):
    monkeypatch.setattr(campaign, "ROOT", tmp_path)
    monkeypatch.setattr(capture_sources, "ROOT", tmp_path)
    root = tmp_path / ".work/campaign"
    campaign.initialize(root, "20260129", days=7)
    return root


class Client:
    def __init__(self, day, clock):
        self.attempts = 0
    def close(self):
        pass


def collect(output, usage_day, **kwargs):
    assert usage_day == "20260127"
    assert kwargs["snapshots"] == 3 and kwargs["cadence_seconds"] == 300
    kwargs["session"].attempts = 4
    return {"status": "collected", "logical_get_attempts": 4}


def execute(folder, *, moment="2026-01-31T17:50:00+09:00", collector=collect):
    return campaign.run_day(folder, fetch=True, clock=lambda: datetime.fromisoformat(moment),
                            collector=collector, session_factory=Client)


def test_same_day_is_permanently_reserved_and_d4_remains_its_usage_date(folder):
    first = execute(folder)
    assert first["status"] == "collected"
    assert first["usage_day"] == "20260127"
    second = execute(folder)
    assert second["status"] == "already_reserved" and second["logical_get_attempts"] == 0
    assert campaign.campaign_status(folder)["daily"][2]["logical_get_attempts"] == 4


@pytest.mark.parametrize("moment,status", [
    ("2026-01-28T17:50:00+09:00", "outside_period"),
    ("2026-02-05T17:50:00+09:00", "outside_period"),
    ("2026-01-31T17:49:59+09:00", "not_due"),
    ("2026-01-31T17:55:01+09:00", "missed_start_window"),
])
def test_outside_scope_never_opens_a_client_or_collects(folder, moment, status):
    def forbidden(*args, **kwargs):
        raise AssertionError("unexpected collection")
    result = execute(folder, moment=moment, collector=forbidden)
    assert result["status"] == status
    assert result["logical_get_attempts"] == 0


def test_exact_grace_boundary_can_collect(folder):
    assert execute(folder, moment="2026-01-31T17:55:00+09:00")["status"] == "collected"


def test_interrupted_capture_keeps_reservation_without_logging_private_error(folder):
    def interrupted(output, usage_day, **kwargs):
        kwargs["session"].attempts = 2
        raise RuntimeError("private-key-in-url")
    assert execute(folder, collector=interrupted)["status"] == "interrupted"
    assert execute(folder)["status"] == "already_reserved"
    text = (folder / "days/20260131/state.json").read_text()
    assert "private-key-in-url" not in text
    assert json.loads(text)["logical_get_attempts"] == 2


def test_concurrent_invocations_collect_once(folder):
    calls = []
    def counted(*args, **kwargs):
        calls.append(1)
        return collect(*args, **kwargs)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: execute(folder, collector=counted), range(2)))
    assert sorted(item["status"] for item in results) == ["already_reserved", "collected"]
    assert len(calls) == 1


def test_budget_stops_fourteenth_get_before_network_and_blocks_kst_day_rollover(monkeypatch):
    calls = []
    monkeypatch.setattr(requests.Session, "get", lambda *args, **kwargs: calls.append(1))
    client = campaign.BudgetedSession("20260131", lambda: datetime(2026, 1, 31, 18, tzinfo=KST))
    for _ in range(13):
        client.get("https://example.test")
    with pytest.raises(RuntimeError, match="budget"):
        client.get("https://example.test")
    assert client.attempts == len(calls) == 13
    client = campaign.BudgetedSession("20260131", lambda: datetime(2026, 2, 1, tzinfo=KST))
    with pytest.raises(RuntimeError, match="budget day"):
        client.get("https://example.test")
    assert client.attempts == 0 and len(calls) == 13


def test_finite_plan_covers_month_end_and_requires_explicit_fetch(folder):
    status = campaign.campaign_status(folder, as_of=datetime(2026, 2, 5, tzinfo=KST))
    assert status["plan"]["end_day"] == "20260204"
    assert status["plan"]["max_get_attempts_campaign"] == 91
    assert status["finished"]
    assert len(status["daily"]) == 7
    assert all(item["status"] == "unrecorded_due" for item in status["daily"])
    with pytest.raises(ValueError, match="--fetch"):
        campaign.run_day(folder)
    assert not (folder / "days").exists()


def test_reservation_without_state_after_crash_is_not_treated_as_unattempted(folder):
    target = folder / "days/20260131"
    target.mkdir(parents=True)
    assert execute(folder)["status"] == "already_reserved"
    assert campaign.campaign_status(folder)["daily"][2]["status"] == "reserved_without_state"


def test_changed_budget_or_plan_cannot_enable_collection(folder):
    path = folder / "plan.json"
    plan = json.loads(path.read_text())
    plan["max_get_attempts_per_day"] = 100
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="contract"):
        execute(folder)


@pytest.mark.parametrize("days,clock", [(8, "17:50"), (True, "17:50"), (7, "23:55"), (7, "17:50:01")])
def test_invalid_plan_is_rejected_before_directory_creation(tmp_path, monkeypatch, days, clock):
    monkeypatch.setattr(capture_sources, "ROOT", tmp_path)
    target = tmp_path / ".work/invalid"
    with pytest.raises(ValueError):
        campaign.initialize(target, "20260131", days=days, daily_time=clock)
    assert not target.exists()

def test_shifting_dates_after_approval_breaks_plan_seal(folder):
    path = folder / "plan.json"
    plan = json.loads(path.read_text())
    plan["start_day"] = "20260201"
    plan["end_day"] = "20260207"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="sealed"):
        execute(folder)


def test_bounds_still_reject_invalid_budget_even_if_seal_was_replaced(folder):
    from src.observability.source_baseline import manifest_hash
    path = folder / "plan.json"
    plan = json.loads(path.read_text())
    plan["max_get_attempts_per_day"] = 14
    path.write_text(json.dumps(plan))
    (folder / "plan.sha256").write_text(manifest_hash(plan))
    with pytest.raises(ValueError, match="bounded"):
        execute(folder)
