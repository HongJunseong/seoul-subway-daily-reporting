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
        self.calls = 0

    def get(self, url, timeout):
        self.calls += 1
        return Response(next(self.payloads))


def page(total, rows):
    return {"CardSubwayStatsNew": {"RESULT": {"CODE": "INFO-000"}, "list_total_count": total, "row": rows}}


def usage_row(station, line="2호선", day="20260128"):
    return {"USE_YMD": day, "SBWY_ROUT_LN_NM": line, "SBWY_STNS_NM": station,
            "GTON_TNOPE": 10, "GTOFF_TNOPE": 20, "REG_YMD": "20260130"}


def test_pagination_does_not_leak_api_key(monkeypatch, capsys):
    monkeypatch.setenv("SEOUL_PASSENGER_API_KEY", "private-key")
    rows = [usage_row(str(i)) for i in range(3)]
    session = Session([page(3, rows[:2]), page(3, rows[2:])])
    assert fetch_subway_usage_daily("20260128", page_size=2, session=session) == rows
    assert "private-key" not in capsys.readouterr().out


def test_partial_collection_fails(monkeypatch):
    monkeypatch.setenv("SEOUL_PASSENGER_API_KEY", "private-key")
    session = Session([page(3, [usage_row("A"), usage_row("B")])])
    with pytest.raises(SourceError, match="incomplete"):
        fetch_subway_usage_daily("20260128", page_size=2, max_pages=1, session=session)


def test_total_changed_during_pagination_fails(monkeypatch):
    monkeypatch.setenv("SEOUL_PASSENGER_API_KEY", "private-key")
    session = Session([page(3, [usage_row("A"), usage_row("B")]), page(4, [usage_row("C")])])
    with pytest.raises(SourceError, match="changed"):
        fetch_subway_usage_daily("20260128", page_size=2, session=session)


def test_network_error_sanitizes_url():
    class FailedSession:
        def get(self, url, timeout):
            raise requests.ConnectionError("http://source/private-key")
    with pytest.raises(SourceError) as exc:
        get_json("http://source/private-key", session=FailedSession())
    assert "private-key" not in str(exc.value)


@pytest.fixture
def passenger_key(monkeypatch):
    monkeypatch.setenv("SEOUL_PASSENGER_API_KEY", "private-key")


@pytest.mark.parametrize("total", [True, False, 2.9, 2.0, -1, 0, None, "2.0", "２", "secret-invalid-count"])
def test_invalid_total_metadata_is_not_coerced_or_exposed(passenger_key, total, capsys):
    session = Session([page(total, [usage_row("A"), usage_row("B")])])
    with pytest.raises(SourceError) as caught:
        fetch_subway_usage_daily("20260128", page_size=2, session=session)
    assert session.calls == 1
    assert "secret-invalid-count" not in str(caught.value)
    assert "private-key" not in capsys.readouterr().out


def test_decimal_string_total_and_final_short_page_are_supported(passenger_key):
    rows = [usage_row("A"), usage_row("B"), usage_row("C")]
    session = Session([page(" 3 ", rows[:2]), page(3, rows[2:])])
    assert fetch_subway_usage_daily("20260128", page_size=2, session=session) == rows
    assert session.calls == 2


@pytest.mark.parametrize("second", [
    [usage_row("A"), usage_row("B")],
    [usage_row("B"), usage_row("C")],
    [usage_row(" B "), usage_row("C")],
])
def test_repeated_or_overlapping_pages_do_not_hide_missing_stations(passenger_key, second):
    session = Session([page(4, [usage_row("A"), usage_row("B")]), page(4, second)])
    with pytest.raises(SourceError, match="duplicate natural key"):
        fetch_subway_usage_daily("20260128", page_size=2, session=session)
    assert session.calls == 2


def test_duplicate_key_with_conflicting_revision_is_not_silently_selected(passenger_key):
    old = usage_row("A")
    new = dict(old, GTON_TNOPE=30, REG_YMD="20260131")
    with pytest.raises(SourceError, match="duplicate natural key"):
        fetch_subway_usage_daily("20260128", page_size=2, session=Session([page(2, [old, new])]))


def test_same_station_name_on_different_lines_and_separate_revisions_are_preserved(passenger_key):
    rows = [usage_row("환승역", "1호선"), usage_row("환승역", "2호선")]
    assert fetch_subway_usage_daily("20260128", page_size=2, session=Session([page(2, rows)])) == rows
    corrected = [dict(row, GTON_TNOPE=30, REG_YMD="20260131") for row in rows]
    assert fetch_subway_usage_daily("20260128", page_size=2, session=Session([page(2, corrected)])) == corrected


@pytest.mark.parametrize("batch", [[], [usage_row("A")], [usage_row("A"), usage_row("B"), usage_row("C")]])
def test_bad_page_length_stops_without_requesting_more_pages(passenger_key, batch):
    session = Session([page(4, batch)])
    with pytest.raises(SourceError, match="incomplete"):
        fetch_subway_usage_daily("20260128", page_size=2, session=session)
    assert session.calls == 1


@pytest.mark.parametrize("row", [None, [], "private-row", {}, usage_row("A", day="20260129"),
                               usage_row("A", line=" "), usage_row(123), usage_row("")])
def test_malformed_rows_and_keys_fail_as_sanitized_source_error(passenger_key, row):
    with pytest.raises(SourceError) as caught:
        fetch_subway_usage_daily("20260128", page_size=1, session=Session([page(1, [row])]))
    assert "private-row" not in str(caught.value)


@pytest.mark.parametrize("result", [None, [], "private-result", {}, {"CODE": "ERROR"}])
def test_malformed_result_is_a_controlled_source_failure(passenger_key, result):
    payload = page(1, [usage_row("A")])
    payload["CardSubwayStatsNew"]["RESULT"] = result
    with pytest.raises(SourceError, match="did not report success"):
        fetch_subway_usage_daily("20260128", session=Session([payload]))


def test_known_total_above_request_budget_fails_after_first_page(passenger_key):
    session = Session([page(5, [usage_row("A"), usage_row("B")])])
    with pytest.raises(SourceError, match="pagination budget"):
        fetch_subway_usage_daily("20260128", page_size=2, max_pages=2, session=session)
    assert session.calls == 1


@pytest.mark.parametrize("page_size,max_pages", [(True, 10), (2.5, 10), (2, True), (2, 1.5), (0, 10), (1001, 10), (2, 0)])
def test_invalid_pagination_limits_do_not_make_requests(passenger_key, page_size, max_pages):
    session = Session([])
    with pytest.raises(ValueError, match="pagination limits"):
        fetch_subway_usage_daily("20260128", page_size=page_size, max_pages=max_pages, session=session)
    assert session.calls == 0


def test_main_does_not_append_invalid_collection_to_bronze(passenger_key, monkeypatch):
    import sys
    from unittest.mock import Mock
    from src.ingestion import ingest_subway_usage_daily as collector
    session = Session([page(2, [usage_row("A"), usage_row("A")])])
    monkeypatch.setattr(collector, "get_json", lambda url, **kwargs: session.get(url, 15).json())
    save = Mock()
    monkeypatch.setattr(collector, "save_to_bronze_delta", save)
    monkeypatch.setattr(sys, "argv", ["ingest_subway_usage_daily.py", "20260128"])
    with pytest.raises(SourceError, match="duplicate natural key"):
        collector.main()
    save.assert_not_called()
