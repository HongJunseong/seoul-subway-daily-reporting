"""Audit/report agreement and exact counts, including actual local Delta CLI reads."""
import json
import os
from pathlib import Path
import subprocess
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from deltalake import write_deltalake

from src.report.build_report_context_daily import ReportDataError, summarize_usage
from test_audit import audit, frames


@pytest.mark.parametrize("value", ["10", True, np.bool_(True), None, float("nan"), float("inf"), -1, 0.5])
def test_audit_and_report_reject_the_same_typed_count_fault(value):
    data = frames()
    for key in ("silver/fact_subway_usage_delta", "gold/subway_usage/station_daily"):
        frame = data[key].astype(object)
        frame.loc[0, "ride_count"] = value
        data[key] = frame
    result = audit(data)
    assert result["status"] == "failed"
    assert result["usage"]["gold"]["invalid_rows"] == 1
    assert result["usage"]["gold"]["total_count"] is None
    assert result["usage"]["silver_gold_business_match"] is None
    with pytest.raises(ReportDataError):
        summarize_usage(data["gold/subway_usage/station_daily"])
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("column", ["line_name", "station_name"])
@pytest.mark.parametrize("value", [123, True, " "])
def test_matching_invalid_identity_cannot_be_healthy(column, value):
    data = frames()
    for key in ("silver/fact_subway_usage_delta", "gold/subway_usage/station_daily"):
        data[key][column] = data[key][column].astype(object)
        data[key].loc[0, column] = value
    result = audit(data)
    assert result["status"] == "failed"
    assert result["usage"]["gold"]["invalid_rows"] == 1


def test_exact_total_above_int64_matches_report_without_mutating_input():
    data = frames()
    for key in ("silver/fact_subway_usage_delta", "gold/subway_usage/station_daily"):
        frame = data[key]
        frame["ride_count"] = 2**62
        frame["alight_count"] = 0
        frame["total_count"] = 2**62
    before = {key: frame.copy(deep=True) for key, frame in data.items()}
    result = audit(data)
    assert result["status"] == "healthy"
    assert result["usage"]["gold"]["total_count"] == 2**63
    assert summarize_usage(data["gold/subway_usage/station_daily"])["total_count"] == 2**63
    assert json.loads(json.dumps(result))["usage"]["gold"]["total_count"] == 2**63
    for key in data:
        pd.testing.assert_frame_equal(data[key], before[key])


def test_integral_float_and_integer_layers_match_by_business_values():
    data = frames()
    for column in ("ride_count", "alight_count", "total_count"):
        data["gold/subway_usage/station_daily"][column] = data["gold/subway_usage/station_daily"][column].astype(float)
    result = audit(data)
    assert result["status"] == "healthy"
    assert result["usage"]["silver_gold_business_match"] is True


def test_large_valid_row_is_not_rounded_next_to_invalid_row():
    data = frames()
    for key in ("silver/fact_subway_usage_delta", "gold/subway_usage/station_daily"):
        frame = data[key].astype(object)
        frame.loc[0, ["ride_count", "alight_count", "total_count"]] = [2**62 + 1, 1, 2**62 + 2]
        frame.loc[1, "ride_count"] = None
        data[key] = frame
    result = audit(data)
    assert result["usage"]["gold"]["invalid_rows"] == 1
    assert result["usage"]["gold"]["total_count"] is None


@pytest.mark.parametrize("fault,expected_exit", [("large_sum", 0), ("boolean_counts", 2), ("numeric_strings", 2)])
def test_real_delta_audit_cli_keeps_exact_counts_and_rejects_schema_faults(tmp_path, fault, expected_exit):
    data = frames()
    for key in ("silver/fact_subway_usage_delta", "gold/subway_usage/station_daily"):
        frame = data[key]
        if fault == "large_sum":
            frame["ride_count"] = 2**62
            frame["alight_count"] = 0
            frame["total_count"] = 2**62
        elif fault == "boolean_counts":
            frame["ride_count"] = True
            frame["alight_count"] = False
            frame["total_count"] = True
        else:
            for column in ("ride_count", "alight_count", "total_count"):
                frame[column] = frame[column].astype(str)
    for key, frame in data.items():
        frame = frame.copy()
        if "snapshot_ts" in frame:
            dates = frame.snapshot_ts.dt.tz_convert("Asia/Seoul").dt.strftime("%Y%m%d")
        else:
            dates = frame["use_ymd" if "use_ymd" in frame else "USE_YMD"]
        frame["year"], frame["month"], frame["day"] = dates.str[:4], dates.str[4:6], dates.str[6:8]
        if "dest_station_id" in frame:
            frame["dest_station_id"] = "100200002"
        write_deltalake(str(tmp_path / "lake" / "v2" / key), pa.Table.from_pandas(frame, preserve_index=False),
                       partition_by=["year", "month", "day"])
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, str(root / "scripts/audit_lake.py"), "--local-root", str(tmp_path / "lake"),
         "--usage-day", "20260128", "--start", "2026-01-31 23:50:00", "--end", "2026-02-01 00:05:00",
         "--as-of", "2026-02-01 00:05:00", "--data-kind", "synthetic", "--output-dir", str(tmp_path / "audit")],
        cwd=tmp_path, env={key: value for key, value in os.environ.items() if key != "PYTHONPATH"},
        capture_output=True, text=True, timeout=30)
    assert result.returncode == expected_exit, result.stderr
    evidence = json.loads((tmp_path / "audit" / "quality.json").read_text())
    assert all(source["delta_version"] == 0 and source["status"] == "available"
               for source in evidence["sources"].values())
    if fault == "large_sum":
        assert evidence["status"] == "healthy"
        assert evidence["usage"]["gold"]["total_count"] == 2**63
        assert evidence["usage"]["silver_gold_business_match"] is True
    else:
        assert evidence["status"] == "failed"
        assert evidence["usage"]["gold"]["invalid_rows"] == 2
        assert evidence["usage"]["gold"]["total_count"] is None
        assert evidence["usage"]["silver_gold_business_match"] is None
