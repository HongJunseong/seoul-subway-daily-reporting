import pandas as pd
import pytest
from src.report.build_report_context_daily import summarize_usage, summarize_arrival, report_dates, ReportDataError
from src.report.generate_daily_report import render_report
from src.alerts.notify_arrival_eta_15m import summarize_alert


def usage():
    return pd.DataFrame([
        dict(use_ymd="20260128", station_name="환승역", line_name="2호선", ride_count=60, alight_count=40, total_count=100),
        dict(use_ymd="20260128", station_name="환승역", line_name="7호선", ride_count=120, alight_count=80, total_count=200)])


def arrival():
    return pd.DataFrame([
        dict(line_id=1002, station_id=10020001, station_name="환승역", total_events=1, valid_eta_n=1,
             avg_eta_sec=120., p95_eta_sec=120., long_eta_ratio_5m=0., snapshot_count=1),
        dict(line_id=1002, station_id=10020002, station_name="다른역", total_events=9, valid_eta_n=9,
             avg_eta_sec=600., p95_eta_sec=900., long_eta_ratio_5m=1., snapshot_count=3),
        dict(line_id=1007, station_id=10070001, station_name="환승역", total_events=1, valid_eta_n=1,
             avg_eta_sec=60., p95_eta_sec=60., long_eta_ratio_5m=0., snapshot_count=1)])


def test_arrival_summary_keeps_line_station_identity_without_usage_values():
    context = summarize_arrival(arrival())
    assert len(context["long_eta_stations"]) == 3
    assert all("total_count" not in row for row in context["long_eta_stations"])


def test_line_means_are_weighted_and_quantiles_are_not_averaged():
    line = summarize_arrival(arrival())["line_wait_stats"][0]
    assert line["avg_eta_sec"] == 552
    assert line["long_eta_ratio_5m"] == 0.9
    assert line["max_station_p95_eta_sec"] == 900
    assert "p95_eta_sec" not in line


def test_invalid_usage_is_not_published():
    df = usage()
    df.loc[0, "total_count"] = 101
    with pytest.raises(ReportDataError, match="reconcile"):
        summarize_usage(df)


def test_report_dates_and_semantics():
    assert report_dates("20260201") == ("20260128", "20260131")
    ctx = dict(target_ymd="20260201", usage_ymd="20260128", arrival_ymd="20260131",
               usage=summarize_usage(usage()), arrival=summarize_arrival(arrival()))
    text = render_report(ctx)
    assert "20260128" in text and "20260131" in text
    assert "300회" in text
    assert "실제 대기시간" in text
    assert render_report(ctx) == text
    ctx["data_kind"] = "synthetic"
    assert "합성 fixture 검증용" in render_report(ctx)


def test_alert_does_not_average_group_quantiles():
    rows = [dict(valid_eta_n=200, long_ratio_10m=0.1, p90_eta_sec=100),
            dict(valid_eta_n=2, long_ratio_10m=1., p90_eta_sec=10000)]
    result = summarize_alert(rows)
    assert result["should_alert"] is False
    assert result["severe_group_ratio"] == 0


@pytest.mark.parametrize("column", ["total_events", "valid_eta_n"])
def test_missing_arrival_denominator_blocks_report(column):
    df = arrival()
    df.loc[0, column] = float("nan")
    with pytest.raises(ReportDataError, match="denominators"):
        summarize_arrival(df)


def test_observation_range_is_rendered_in_kst_from_delta_utc_timestamps():
    df = arrival()
    df["first_snapshot_ts"] = pd.Timestamp("2026-01-31T14:50:00Z")
    df["last_snapshot_ts"] = pd.Timestamp("2026-01-31T14:55:00Z")
    df["max_source_age_sec"] = 12
    observed = summarize_arrival(df)["observation_range"]
    assert observed["first_snapshot_ts"] == "2026-01-31T23:50:00+09:00"
    assert observed["last_snapshot_ts"] == "2026-01-31T23:55:00+09:00"
    assert observed["max_source_age_sec"] == 12
