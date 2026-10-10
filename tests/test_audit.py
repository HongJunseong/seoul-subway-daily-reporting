from datetime import datetime
import json
import subprocess
import sys
import pandas as pd
import pytest
from src.observability.audit import audit_lake, file_metrics


class Reader:
    def __init__(self, frames):
        self.frames = frames
        self.sources = {}

    def read(self, key, dates, columns):
        frame = self.frames.get(key)
        self.sources[key] = {"path": key, "delta_version": 3,
                             "status": "available" if frame is not None and len(frame) else "unavailable"}
        return frame


def frames():
    usage = pd.DataFrame([
        dict(use_ymd="20260128", line_name="L2", station_name="transfer", ride_count=10, alight_count=10, total_count=20),
        dict(use_ymd="20260128", line_name="L7", station_name="transfer", ride_count=20, alight_count=20, total_count=40)])
    bronze_usage = pd.DataFrame([
        dict(USE_YMD="20260128", SBWY_ROUT_LN_NM="L2", SBWY_STNS_NM="transfer", record_hash="a", run_id="first"),
        dict(USE_YMD="20260128", SBWY_ROUT_LN_NM="L7", SBWY_STNS_NM="transfer", record_hash="b", run_id="first"),
        dict(USE_YMD="20260128", SBWY_ROUT_LN_NM="L2", SBWY_STNS_NM="transfer", record_hash="a", run_id="retry")])
    stamps = pd.to_datetime(["2026-01-31T14:50:02Z", "2026-01-31T14:55:07Z", "2026-01-31T15:00:03Z"])
    raw = pd.DataFrame(dict(snapshot_ts=stamps, run_id=["a", "b", "c"], response_at_limit=[False]*3))
    fact = pd.DataFrame(dict(snapshot_ts=stamps, line_id=[1002]*3, station_id=[100200001]*3,
                             updn_line=["up"]*3, train_no=["train"]*3, dest_station_id=[None]*3,
                             arrival_seconds=[120, None, 600], recptn_dt=stamps, source_age_sec=[0, 0, 0]))
    return {"bronze/subway_usage_delta": bronze_usage, "silver/fact_subway_usage_delta": usage,
            "gold/subway_usage/station_daily": usage.copy(), "bronze/subway_arrival_delta": raw,
            "silver/fact_subway_arrival_delta": fact}


def audit(data=None, end_minute=5):
    return audit_lake(Reader(frames() if data is None else data), "20260128", datetime(2026, 1, 31, 23, 50),
                      datetime(2026, 2, 1, 0, end_minute), datetime(2026, 2, 1, 0, end_minute))


def test_jitter_and_duplicate_capture_do_not_inflate_bucket_coverage():
    data = frames()
    raw = data["bronze/subway_arrival_delta"]
    data["bronze/subway_arrival_delta"] = pd.concat([raw, raw.iloc[[0]]], ignore_index=True)
    result = audit(data)
    assert result["status"] == "healthy"
    assert result["arrival"]["observed_time_buckets"] == 3
    assert result["arrival"]["observed_time_bucket_ratio"] == 1
    assert result["arrival"]["missing_eta_rows"] == 1
    assert result["arrival"]["valid_eta_rows"] == 2
    assert result["usage"]["bronze"]["exact_repeat_rows"] == 1


def test_window_is_half_open_and_crosses_month_boundary():
    data = frames()
    extra = pd.DataFrame([dict(snapshot_ts=pd.Timestamp("2026-01-31T15:10:00Z"), run_id="outside", response_at_limit=True)])
    data["bronze/subway_arrival_delta"] = pd.concat([data["bronze/subway_arrival_delta"], extra], ignore_index=True)
    result = audit(data, end_minute=10)
    assert result["status"] == "degraded"
    assert result["arrival"]["bronze_rows"] == 3
    assert result["arrival"]["response_at_limit_snapshots"] == 0
    assert result["arrival"]["missing_bucket_examples"] == ["2026-02-01T00:05:00+09:00"]


def test_equal_totals_cannot_hide_swapped_station_values():
    data = frames()
    gold = data["gold/subway_usage/station_daily"]
    gold[["ride_count", "alight_count", "total_count"]] = [[20, 20, 40], [10, 10, 20]]
    result = audit(data)
    assert result["usage"]["silver"]["total_count"] == result["usage"]["gold"]["total_count"]
    assert result["status"] == "failed"
    assert result["usage"]["silver_gold_business_match"] is False


def test_missing_source_is_unknown_and_blocks_healthy_status():
    data = frames()
    del data["bronze/subway_arrival_delta"]
    result = audit(data)
    assert result["status"] == "failed"
    assert "observed_time_buckets" not in result["arrival"]
    assert result["arrival"]["freshness_seconds"] is None


def test_processing_backlog_is_distinct_from_collection_gaps():
    data = frames()
    data["silver/fact_subway_arrival_delta"] = data["silver/fact_subway_arrival_delta"].iloc[:2].copy()
    result = audit(data)
    assert result["arrival"]["missing_time_buckets"] == 0
    assert result["arrival"]["unprocessed_snapshot_count"] == 1
    assert {i["code"] for i in result["issues"]} == {"arrival_unprocessed_snapshots"}


def test_missing_source_clock_is_not_assumed_fresh():
    data = frames()
    fact = data["silver/fact_subway_arrival_delta"]
    fact["recptn_dt"] = pd.NaT
    fact["source_age_sec"] = 0
    result = audit(data)
    assert result["arrival"]["missing_source_timestamp_rows"] == 3
    assert result["arrival"]["max_source_age_seconds"] is None


def test_file_inventory_uses_committed_date_candidates():
    actions = pd.DataFrame(dict(size_bytes=[10, 20, 500], **{
        "partition.year": ["2026"]*3, "partition.month": ["01", "01", "02"], "partition.day": ["31", "31", "01"]}))
    result = file_metrics(actions, ["20260131"])
    assert result["active_table_bytes"] == 530
    assert result["candidate_date_files"] == 2
    assert result["candidate_date_bytes"] == 30


@pytest.mark.parametrize("end,as_of", [
    (datetime(2026, 1, 31, 23, 50), datetime(2026, 2, 1)),
    (datetime(2026, 2, 1), datetime(2026, 1, 31)),
    (datetime(2026, 4, 1), datetime(2026, 4, 1))])
def test_invalid_or_open_ranges_are_rejected(end, as_of):
    with pytest.raises(ValueError):
        audit_lake(Reader({}), "20260128", datetime(2026, 1, 31, 23, 50), end, as_of)


def test_cli_missing_lake_writes_failure_evidence_and_returns_two(tmp_path):
    result = subprocess.run([sys.executable, "scripts/audit_lake.py", "--local-root", str(tmp_path/"missing"),
                             "--usage-day", "20260128", "--start", "2026-01-31 23:50:00",
                             "--end", "2026-02-01 00:10:00", "--as-of", "2026-02-01 00:10:00",
                             "--data-kind", "synthetic", "--output-dir", str(tmp_path/"out")], capture_output=True, text=True)
    assert result.returncode == 2, result.stderr
    report = json.loads((tmp_path/"out/quality.json").read_text())
    assert report["status"] == "failed"
    assert all(source["status"] == "unavailable" for source in report["sources"].values())
    assert (tmp_path/"out/quality.md").exists()


@pytest.mark.parametrize("bad", [None, "broken", -1, 1.5, float("inf")])
def test_invalid_counts_are_failed_without_a_partial_total(bad):
    data = frames()
    data["gold/subway_usage/station_daily"]["ride_count"] = data["gold/subway_usage/station_daily"]["ride_count"].astype(object)
    data["gold/subway_usage/station_daily"].loc[0, "ride_count"] = bad
    result = audit(data)
    assert result["status"] == "failed"
    assert result["usage"]["gold"]["invalid_rows"] == 1
    assert result["usage"]["gold"]["total_count"] is None


def test_matching_layers_cannot_publish_a_different_usage_date():
    data = frames()
    for key in ["silver/fact_subway_usage_delta", "gold/subway_usage/station_daily"]:
        data[key]["use_ymd"] = "20260129"
    result = audit(data)
    assert result["usage"]["silver_gold_business_match"] is True
    assert result["status"] == "failed"
    assert any(issue["code"] == "usage_gold_date_mismatch" for issue in result["issues"])

@pytest.mark.parametrize("column,bad", [
    ("snapshot_ts", pd.NaT), ("updn_line", " "), ("arrival_seconds", float("inf")),
    ("source_age_sec", "broken")])
def test_invalid_arrival_values_cannot_be_hidden_by_window_or_null_filter(column, bad):
    data = frames()
    fact = data["silver/fact_subway_arrival_delta"]
    fact[column] = fact[column].astype(object)
    fact.loc[0, column] = bad
    result = audit(data)
    assert result["status"] == "failed"
    assert any(issue["code"] == "arrival_contract" for issue in result["issues"])
    json.dumps(result, allow_nan=False)
    if column == "arrival_seconds":
        assert result["arrival"]["valid_eta_rows"] == 1
        assert result["arrival"]["missing_eta_rows"] == 1
