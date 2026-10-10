import pytest
import requests
from src.common.http import get_json, SourceError
from src.ingestion.ingest_subway_usage_daily import fetch_subway_usage_daily


class Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class Session:
    def __init__(self, payloads):
        self.payloads = iter(payloads)

    def get(self, url, timeout):
        return Response(next(self.payloads))


def page(total, rows):
    return {"CardSubwayStatsNew": {"RESULT": {"CODE": "INFO-000"}, "list_total_count": total, "row": rows}}


def test_pagination_does_not_leak_api_key(monkeypatch, capsys):
    monkeypatch.setenv("SEOUL_PASSENGER_API_KEY", "private-key")
    rows = [{"USE_YMD": "20260128", "SBWY_STNS_NM": str(i)} for i in range(3)]
    session = Session([page(3, rows[:2]), page(3, rows[2:])])
    assert fetch_subway_usage_daily("20260128", page_size=2, session=session) == rows
    assert "private-key" not in capsys.readouterr().out


def test_partial_collection_fails(monkeypatch):
    monkeypatch.setenv("SEOUL_PASSENGER_API_KEY", "private-key")
    session = Session([page(3, [{"USE_YMD": "20260128"}] * 2)])
    with pytest.raises(SourceError, match="incomplete"):
        fetch_subway_usage_daily("20260128", page_size=2, max_pages=1, session=session)


def test_total_changed_during_pagination_fails(monkeypatch):
    monkeypatch.setenv("SEOUL_PASSENGER_API_KEY", "private-key")
    session = Session([page(3, [{"USE_YMD": "20260128"}] * 2), page(4, [{"USE_YMD": "20260128"}])])
    with pytest.raises(SourceError, match="changed"):
        fetch_subway_usage_daily("20260128", page_size=2, session=session)


def test_network_error_sanitizes_url():
    class FailedSession:
        def get(self, url, timeout):
            raise requests.ConnectionError("http://source/private-key")
    with pytest.raises(SourceError) as exc:
        get_json("http://source/private-key", session=FailedSession())
    assert "private-key" not in str(exc.value)
