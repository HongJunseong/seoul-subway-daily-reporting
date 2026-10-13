"""Read committed Gold snapshots and keep usage and arrival evidence separate."""
from __future__ import annotations
import os
import math
from numbers import Real
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
from src.common.dates import date_parts, validate_ymd
from src.common.numbers import valid_number
from src.common.storage import table_path, public_uri


class ReportDataError(RuntimeError):
    pass


def report_dates(target_ymd):
    anchor = datetime.strptime(validate_ymd(target_ymd), "%Y%m%d")
    return (anchor - timedelta(days=4)).strftime("%Y%m%d"), (anchor - timedelta(days=1)).strftime("%Y%m%d")


def load_gold_day(prefix, ymd):
    from deltalake import DeltaTable
    y, m, d = date_parts(ymd)
    path = public_uri(table_path(prefix))
    options = None if os.getenv("SSDR_DATA_ROOT") else {
        "AWS_REGION": os.getenv("AWS_DEFAULT_REGION") or os.getenv("SSDR_AWS_REGION", "ap-northeast-2")}
    table = DeltaTable(path, storage_options=options)
    df = table.to_pandas(partitions=[("year", "=", y), ("month", "=", m), ("day", "=", d)])
    if df.empty:
        raise ReportDataError(f"Required Gold date unavailable: {prefix} {ymd}")
    df.attrs["delta_version"] = table.version()
    df.attrs["source_path"] = path
    return df


def require_keys(df, columns, name):
    for column in columns:
        if not df[column].map(lambda value: isinstance(value, str) and bool(value.strip())).all():
            raise ReportDataError(f"{name} has empty or invalid identity")


def require_numbers(df, columns, name, *, integer=False, minimum=0):
    for column in columns:
        if not df[column].map(lambda value: valid_number(value, integer=integer, minimum=minimum)).all():
            raise ReportDataError(f"{name} has invalid numeric values")


def observation_range(df, expected_ymd=None):
    columns = ["first_snapshot_ts", "last_snapshot_ts"]
    coverage = {}
    if any(column in df for column in columns):
        if not all(column in df for column in columns):
            raise ReportDataError("Arrival Gold has incomplete observation range")
        stamps = {}
        for column in columns:
            values = []
            for value in df[column]:
                try:
                    if pd.isna(value) or isinstance(value, (Real, bool, np.bool_)):
                        raise ValueError("missing or numeric timestamp")
                    stamp = pd.Timestamp(value)
                    if pd.isna(stamp):
                        raise ValueError("missing timestamp")
                    stamp = stamp.tz_localize("Asia/Seoul") if stamp.tzinfo is None else stamp.tz_convert("Asia/Seoul")
                except (ValueError, TypeError, OverflowError) as error:
                    raise ReportDataError("Arrival Gold has invalid observation timestamp") from error
                if expected_ymd and stamp.strftime("%Y%m%d") != expected_ymd:
                    raise ReportDataError("Arrival Gold observation date differs from report contract")
                values.append(stamp)
            stamps[column] = values
        if any(first > last for first, last in zip(stamps[columns[0]], stamps[columns[1]])):
            raise ReportDataError("Arrival Gold observation range is reversed")
        coverage[columns[0]] = min(stamps[columns[0]]).isoformat()
        coverage[columns[1]] = max(stamps[columns[1]]).isoformat()
    if "max_source_age_sec" in df:
        known = df[df.max_source_age_sec.notna()]
        require_numbers(known, ["max_source_age_sec"], "Arrival source age", minimum=None)
        if not known.empty:
            coverage["max_source_age_sec"] = float(known.max_source_age_sec.max())
    return coverage


def summarize_usage(df):
    required = {"use_ymd", "station_name", "line_name", "total_count", "ride_count", "alight_count"}
    if not required <= set(df.columns) or df.empty:
        raise ReportDataError("Usage Gold missing required data")
    require_keys(df, ["use_ymd", "line_name", "station_name"], "Usage Gold")
    try:
        days = {validate_ymd(value) for value in df.use_ymd}
    except ValueError as error:
        raise ReportDataError("Usage Gold has invalid usage date") from error
    if len(days) != 1:
        raise ReportDataError("Usage Gold contains multiple usage dates")
    if df.duplicated(["use_ymd", "line_name", "station_name"]).any():
        raise ReportDataError("Usage Gold contains duplicate natural keys")
    require_numbers(df, ["total_count", "ride_count", "alight_count"], "Usage Gold counts", integer=True)
    # Python integers preserve aggregate counts above signed int64 without wrapping.
    df = df.copy()
    for column in ["total_count", "ride_count", "alight_count"]:
        df[column] = df[column].map(int).astype(object)
    if (df.total_count != df.ride_count + df.alight_count).any():
        raise ReportDataError("Usage Gold totals do not reconcile")
    top = df.sort_values(["total_count", "line_name", "station_name"], ascending=[False, True, True]).head(10)
    records = top[["station_name", "line_name", "total_count"]].to_dict("records")
    for rank, row in enumerate(records, 1):
        row["rank"] = rank
    lines = (df.groupby("line_name", as_index=False).agg(total_count=("total_count", "sum"),
                                                      station_count=("station_name", "count"))
             .sort_values(["total_count", "line_name"], ascending=[False, True]))
    return {"has_data": True, "total_count": sum(df.total_count),
            "station_line_count": len(df), "top_stations_by_total": records,
            "line_usage_stats": lines.to_dict("records")}


def summarize_arrival(df, df_usage=None, *, expected_ymd=None):
    required = {"line_id", "station_id", "station_name", "total_events", "valid_eta_n",
                "avg_eta_sec", "p95_eta_sec", "long_eta_ratio_5m", "snapshot_count"}
    if not required <= set(df.columns) or df.empty:
        raise ReportDataError("Arrival Gold missing required data")
    require_keys(df, ["station_name"], "Arrival Gold")
    require_numbers(df, ["line_id", "station_id"], "Arrival Gold identity", integer=True, minimum=1)
    if df.duplicated(["line_id", "station_id"]).any():
        raise ReportDataError("Arrival Gold contains duplicate natural keys")
    if df[["total_events", "valid_eta_n", "snapshot_count"]].isna().any().any():
        raise ReportDataError("Arrival Gold has missing sample denominators")
    require_numbers(df, ["total_events", "valid_eta_n", "snapshot_count"], "Arrival Gold denominators", integer=True)
    if (df.total_events <= 0).any() or (df.snapshot_count <= 0).any() or (df.snapshot_count > df.total_events).any():
        raise ReportDataError("Arrival Gold has invalid observation counts")
    if (df.valid_eta_n > df.total_events).any():
        raise ReportDataError("Arrival Gold has invalid sample denominators")
    df = df.copy()
    for column in ["total_events", "valid_eta_n", "snapshot_count"]:
        df[column] = df[column].map(int).astype(object)
    valid = df[df.valid_eta_n > 0].copy()
    if valid.empty or valid[["avg_eta_sec", "p95_eta_sec", "long_eta_ratio_5m"]].isna().any().any():
        raise ReportDataError("Arrival Gold has no valid ETA evidence")
    require_numbers(valid, ["avg_eta_sec", "p95_eta_sec", "long_eta_ratio_5m"], "Arrival Gold ETA metrics")
    if df.loc[df.valid_eta_n == 0, ["avg_eta_sec", "p95_eta_sec", "long_eta_ratio_5m"]].notna().any().any():
        raise ReportDataError("Arrival Gold has ETA metrics without valid samples")
    if (valid.long_eta_ratio_5m > 1).any():
        raise ReportDataError("Arrival Gold has invalid ETA metrics")
    line_stats = []
    for line, group in valid.groupby("line_id"):
        denominator = sum(group.valid_eta_n)
        line_stats.append({"line_id": int(line), "valid_eta_n": denominator,
                           "avg_eta_sec": math.fsum(float(value) * (count / denominator)
                                                    for value, count in zip(group.avg_eta_sec, group.valid_eta_n)),
                           "long_eta_ratio_5m": math.fsum(float(value) * (count / denominator)
                                                          for value, count in zip(group.long_eta_ratio_5m, group.valid_eta_n)),
                           "max_station_p95_eta_sec": float(group.p95_eta_sec.max())})
    top = valid.sort_values(["avg_eta_sec", "line_id", "station_id"], ascending=[False, True, True]).head(10)
    columns = ["station_name", "line_id", "station_id", "avg_eta_sec", "p95_eta_sec",
               "long_eta_ratio_5m", "valid_eta_n", "snapshot_count"]
    coverage = observation_range(df, expected_ymd)
    return {"observation_range": coverage, "has_data": True, "total_observations": sum(df.total_events),
            "valid_eta_observations": sum(df.valid_eta_n), "line_wait_stats": line_stats,
            "long_eta_stations": top[columns].to_dict("records"),
            "max_snapshots_per_station": int(df.snapshot_count.max()),
            "coverage": "API ALL response (maximum 1000 observations per capture); partial network/time coverage",
            "interpretation": "Sampled train-station ETA observations; not measured passenger wait or timetable delay"}


def build_report_context(target_ymd):
    usage_ymd, arrival_ymd = report_dates(target_ymd)
    usage = load_gold_day("gold/subway_usage/station_daily", usage_ymd)
    arrival = load_gold_day("gold/subway_arrival/station_daily_kpi", arrival_ymd)
    usage_summary = summarize_usage(usage)
    if set(usage.use_ymd) != {usage_ymd}:
        raise ReportDataError("Usage Gold date does not match report contract")
    return {"schema_version": 2, "data_kind": os.getenv("SSDR_DATA_KIND", "api_observations"),
            "target_ymd": target_ymd, "usage_ymd": usage_ymd,
            "arrival_ymd": arrival_ymd, "usage": usage_summary,
            "arrival": summarize_arrival(arrival, expected_ymd=arrival_ymd),
            "sources": {"usage": {"path": usage.attrs["source_path"], "delta_version": usage.attrs["delta_version"]},
                        "arrival": {"path": arrival.attrs["source_path"], "delta_version": arrival.attrs["delta_version"]}},
            "limitations": ["Usage is D-4 from report production date; not today's crowding",
                            "Arrival covers the previous calendar day; observations may be incomplete",
                            "No cross-source station-name-only join or inferred operational delay"]}
