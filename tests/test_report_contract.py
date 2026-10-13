import json

import pandas as pd
import pytest

from scripts.verify_report import verify_report
from src.report import build_report_context_daily as context
from src.report import generate_daily_report as publication
from src.report.build_report_context_daily import ReportDataError, summarize_arrival, summarize_usage
from test_report import arrival, usage


@pytest.mark.parametrize("value", [0.5, float("inf"), float("-inf"), float("nan"), True, "60"])
def test_usage_counts_are_finite_integers_not_coerced(value):
    df = usage().astype(object)
    df.loc[0, "ride_count"] = value
    with pytest.raises(ReportDataError):
        summarize_usage(df)


@pytest.mark.parametrize("column", ["use_ymd", "station_name", "line_name"])
@pytest.mark.parametrize("value", [" ", None, 123])
def test_usage_identity_is_required(column, value):
    df = usage().astype(object)
    df.loc[0, column] = value
    with pytest.raises(ReportDataError, match="identity"):
        summarize_usage(df)


@pytest.mark.parametrize("date", ["20260230", "2026-01-28", "20260129"])
def test_usage_date_is_valid_and_single(date):
    df = usage()
    df.loc[0, "use_ymd"] = date
    with pytest.raises(ReportDataError, match="date"):
        summarize_usage(df)


@pytest.mark.parametrize("column,value", [
    ("line_id", 0), ("station_id", None), ("station_id", 1.5), ("station_name", " "),
    ("total_events", 1.5), ("valid_eta_n", 0.5), ("snapshot_count", 1.5),
    ("snapshot_count", 2), ("total_events", float("inf")),
    ("valid_eta_n", True), ("valid_eta_n", "1"),
    ("avg_eta_sec", float("inf")), ("p95_eta_sec", -1),
    ("p95_eta_sec", float("inf")), ("long_eta_ratio_5m", float("nan")),
    ("long_eta_ratio_5m", 1.1),
])
def test_invalid_arrival_gold_cannot_enter_report(column, value):
    df = arrival().astype(object)
    df.loc[0, column] = value
    with pytest.raises(ReportDataError):
        summarize_arrival(df)


def test_zero_valid_eta_retains_observations_but_requires_null_metrics():
    df = arrival()
    df.loc[0, "valid_eta_n"] = 0
    with pytest.raises(ReportDataError, match="without valid samples"):
        summarize_arrival(df)
    df.loc[0, ["avg_eta_sec", "p95_eta_sec", "long_eta_ratio_5m"]] = float("nan")
    result = summarize_arrival(df)
    assert result["total_observations"] == 11
    assert result["valid_eta_observations"] == 10
    assert result["line_wait_stats"][0]["avg_eta_sec"] == 600


def timed_arrival():
    df = arrival()
    df["first_snapshot_ts"] = pd.Timestamp("2026-01-31T14:50:00Z")
    df["last_snapshot_ts"] = pd.Timestamp("2026-01-31T14:55:00Z")
    return df


@pytest.mark.parametrize("kind", ["missing", "reversed", "incomplete", "wrong_day"])
def test_invalid_observation_range_is_not_silently_omitted(kind):
    df = timed_arrival()
    if kind == "missing":
        df.loc[0, "first_snapshot_ts"] = pd.NaT
    elif kind == "reversed":
        df.loc[0, "first_snapshot_ts"] = pd.Timestamp("2026-01-31T14:56:00Z")
    elif kind == "incomplete":
        df = df.drop(columns="last_snapshot_ts")
    else:
        df.loc[0, "last_snapshot_ts"] = pd.Timestamp("2026-01-31T15:00:00Z")
    with pytest.raises(ReportDataError):
        summarize_arrival(df, expected_ymd="20260131")


def test_kst_midnight_and_month_boundary_are_checked_after_timezone_conversion():
    df = timed_arrival()
    valid = summarize_arrival(df, expected_ymd="20260131")
    assert valid["observation_range"]["last_snapshot_ts"] == "2026-01-31T23:55:00+09:00"
    df["first_snapshot_ts"] = df["last_snapshot_ts"] = pd.Timestamp("2026-01-31T15:00:00Z")
    assert summarize_arrival(df, expected_ymd="20260201")["observation_range"]["first_snapshot_ts"].startswith("2026-02-01T00:00:00")
    with pytest.raises(ReportDataError, match="observation date"):
        summarize_arrival(df, expected_ymd="20260131")


def test_source_age_is_finite_but_future_source_time_is_not_clamped():
    df = arrival()
    df["max_source_age_sec"] = -2.
    assert summarize_arrival(df)["observation_range"]["max_source_age_sec"] == -2
    df.loc[0, "max_source_age_sec"] = float("inf")
    with pytest.raises(ReportDataError, match="source age"):
        summarize_arrival(df)


def test_count_aggregates_do_not_wrap_signed_int64():
    df = usage()
    df["ride_count"], df["alight_count"], df["total_count"] = 5_000_000_000_000_000_000, 0, 5_000_000_000_000_000_000
    df["line_name"] = "2호선"
    df["station_name"] = ["역A", "역B"]
    result = summarize_usage(df)
    assert result["total_count"] == result["line_usage_stats"][0]["total_count"] == 10_000_000_000_000_000_000
    df = arrival()
    df["total_events"] = df["valid_eta_n"] = 5_000_000_000_000_000_000
    df["avg_eta_sec"] = 1e308
    result = summarize_arrival(df)
    assert result["total_observations"] == result["valid_eta_observations"] == 15_000_000_000_000_000_000
    assert result["line_wait_stats"][0]["valid_eta_n"] == 10_000_000_000_000_000_000
    assert result["line_wait_stats"][0]["avg_eta_sec"] == 1e308
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("kind", ["fractional_usage", "empty_key", "fractional_eta", "invalid_p95", "wrong_day"])
def test_bad_gold_preserves_all_files_of_previous_publication(tmp_path, monkeypatch, kind):
    frames = {"usage": usage(), "arrival": timed_arrival()}
    def load(prefix, day):
        result = frames["usage" if "usage" in prefix else "arrival"].copy()
        result.attrs.update(source_path=prefix, delta_version=0)
        return result
    monkeypatch.setattr(context, "load_gold_day", load)
    monkeypatch.setenv("SSDR_REPORT_MODE", "template")
    monkeypatch.setenv("SSDR_REPORT_OUTPUT_DIR", str(tmp_path))
    report = publication.generate_daily_report("20260201", save_to_s3=False)
    verify_report(report)
    original = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    if kind == "fractional_usage":
        frames["usage"] = frames["usage"].astype(object)
        frames["usage"].loc[0, ["ride_count", "alight_count", "total_count"]] = [0.5, 0.5, 1.]
    elif kind == "empty_key":
        frames["usage"].loc[0, "station_name"] = " "
    elif kind == "fractional_eta":
        frames["arrival"] = frames["arrival"].astype(object)
        frames["arrival"].loc[0, "valid_eta_n"] = 0.5
    elif kind == "invalid_p95":
        frames["arrival"].loc[0, "p95_eta_sec"] = -1.
    else:
        frames["arrival"].loc[0, "last_snapshot_ts"] = pd.Timestamp("2026-01-31T15:00:00Z")
    with pytest.raises(ReportDataError):
        publication.generate_daily_report("20260201", save_to_s3=False)
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == original
    verify_report(report)
