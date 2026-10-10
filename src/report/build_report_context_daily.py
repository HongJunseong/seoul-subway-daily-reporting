"""Read committed Gold snapshots and keep usage and arrival evidence separate."""
from __future__ import annotations
import os
from datetime import datetime, timedelta
import pandas as pd
from src.common.dates import date_parts, validate_ymd
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


def summarize_usage(df):
    required = {"use_ymd", "station_name", "line_name", "total_count", "ride_count", "alight_count"}
    if not required <= set(df.columns) or df.empty:
        raise ReportDataError("Usage Gold missing required data")
    if df.duplicated(["use_ymd", "line_name", "station_name"]).any():
        raise ReportDataError("Usage Gold contains duplicate natural keys")
    for column in ["total_count", "ride_count", "alight_count"]:
        if df[column].isna().any() or (df[column] < 0).any():
            raise ReportDataError("Usage Gold has invalid counts")
    if (df.total_count != df.ride_count + df.alight_count).any():
        raise ReportDataError("Usage Gold totals do not reconcile")
    top = df.sort_values(["total_count", "line_name", "station_name"], ascending=[False, True, True]).head(10)
    records = top[["station_name", "line_name", "total_count"]].to_dict("records")
    for rank, row in enumerate(records, 1):
        row["rank"] = rank
    lines = (df.groupby("line_name", as_index=False).agg(total_count=("total_count", "sum"),
                                                      station_count=("station_name", "count"))
             .sort_values(["total_count", "line_name"], ascending=[False, True]))
    return {"has_data": True, "total_count": int(df.total_count.sum()),
            "station_line_count": len(df), "top_stations_by_total": records,
            "line_usage_stats": lines.to_dict("records")}


def summarize_arrival(df, df_usage=None):
    required = {"line_id", "station_id", "station_name", "total_events", "valid_eta_n",
                "avg_eta_sec", "p95_eta_sec", "long_eta_ratio_5m", "snapshot_count"}
    if not required <= set(df.columns) or df.empty:
        raise ReportDataError("Arrival Gold missing required data")
    if df.duplicated(["line_id", "station_id"]).any():
        raise ReportDataError("Arrival Gold contains duplicate natural keys")
    if df[["total_events", "valid_eta_n", "snapshot_count"]].isna().any().any():
        raise ReportDataError("Arrival Gold has missing sample denominators")
    if (df.total_events <= 0).any() or (df.snapshot_count <= 0).any():
        raise ReportDataError("Arrival Gold has invalid observation counts")
    if (df.valid_eta_n < 0).any() or (df.valid_eta_n > df.total_events).any():
        raise ReportDataError("Arrival Gold has invalid sample denominators")
    valid = df[df.valid_eta_n > 0].copy()
    if valid.empty or valid[["avg_eta_sec", "p95_eta_sec", "long_eta_ratio_5m"]].isna().any().any():
        raise ReportDataError("Arrival Gold has no valid ETA evidence")
    if (valid.avg_eta_sec < 0).any() or ((valid.long_eta_ratio_5m < 0) | (valid.long_eta_ratio_5m > 1)).any():
        raise ReportDataError("Arrival Gold has invalid ETA metrics")
    line_stats = []
    for line, group in valid.groupby("line_id"):
        denominator = int(group.valid_eta_n.sum())
        line_stats.append({"line_id": int(line), "valid_eta_n": denominator,
                           "avg_eta_sec": float((group.avg_eta_sec * group.valid_eta_n).sum() / denominator),
                           "long_eta_ratio_5m": float((group.long_eta_ratio_5m * group.valid_eta_n).sum() / denominator),
                           "max_station_p95_eta_sec": float(group.p95_eta_sec.max())})
    top = valid.sort_values(["avg_eta_sec", "line_id", "station_id"], ascending=[False, True, True]).head(10)
    columns = ["station_name", "line_id", "station_id", "avg_eta_sec", "p95_eta_sec",
               "long_eta_ratio_5m", "valid_eta_n", "snapshot_count"]
    coverage = {}
    for column, aggregate in [("first_snapshot_ts", "min"), ("last_snapshot_ts", "max")]:
        if column in df and df[column].notna().any():
            stamp = pd.Timestamp(getattr(df[column], aggregate)())
            stamp = stamp.tz_localize("Asia/Seoul") if stamp.tzinfo is None else stamp.tz_convert("Asia/Seoul")
            coverage[column] = stamp.isoformat()
    if "max_source_age_sec" in df and df.max_source_age_sec.notna().any():
        coverage["max_source_age_sec"] = float(df.max_source_age_sec.max())
    return {"observation_range": coverage, "has_data": True, "total_observations": int(df.total_events.sum()),
            "valid_eta_observations": int(df.valid_eta_n.sum()), "line_wait_stats": line_stats,
            "long_eta_stations": top[columns].to_dict("records"),
            "max_snapshots_per_station": int(df.snapshot_count.max()),
            "coverage": "API ALL response (maximum 1000 observations per capture); partial network/time coverage",
            "interpretation": "Sampled train-station ETA observations; not measured passenger wait or timetable delay"}


def build_report_context(target_ymd):
    usage_ymd, arrival_ymd = report_dates(target_ymd)
    usage = load_gold_day("gold/subway_usage/station_daily", usage_ymd)
    arrival = load_gold_day("gold/subway_arrival/station_daily_kpi", arrival_ymd)
    if set(usage.use_ymd.astype(str)) != {usage_ymd}:
        raise ReportDataError("Usage Gold date does not match report contract")
    return {"schema_version": 2, "data_kind": os.getenv("SSDR_DATA_KIND", "api_observations"),
            "target_ymd": target_ymd, "usage_ymd": usage_ymd,
            "arrival_ymd": arrival_ymd, "usage": summarize_usage(usage),
            "arrival": summarize_arrival(arrival),
            "sources": {"usage": {"path": usage.attrs["source_path"], "delta_version": usage.attrs["delta_version"]},
                        "arrival": {"path": arrival.attrs["source_path"], "delta_version": arrival.attrs["delta_version"]}},
            "limitations": ["Usage is D-4 from report production date; not today's crowding",
                            "Arrival covers the previous calendar day; observations may be incomplete",
                            "No cross-source station-name-only join or inferred operational delay"]}
