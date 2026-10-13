"""Read-only audits of committed Delta snapshots; no API collection."""
from __future__ import annotations
import math
import time
from datetime import timedelta
import numpy as np
import pandas as pd
import pyarrow as pa
from src.common.dates import KST, validate_ymd
from src.common.numbers import valid_number

USAGE_KEY = ["use_ymd", "line_name", "station_name"]
USAGE_VALUES = USAGE_KEY + ["ride_count", "alight_count", "total_count"]
ARRIVAL_KEY = ["snapshot_ts", "line_id", "station_id", "updn_line", "train_no", "dest_station_id"]


def kst(value):
    stamp = pd.Timestamp(value)
    return stamp.tz_localize(KST) if stamp.tzinfo is None else stamp.tz_convert(KST)


def date_range(start, end):
    date = start.date()
    last = (end - timedelta(microseconds=1)).date()
    result = []
    while date <= last:
        result.append(date.strftime("%Y%m%d"))
        date += timedelta(days=1)
    return result


def file_metrics(actions, dates):
    selected = actions
    if len(actions):
        keys = actions["partition.year"] + actions["partition.month"] + actions["partition.day"]
        selected = actions[keys.isin(dates)]
    sizes = selected["size_bytes"]
    return {"active_table_files": len(actions), "active_table_bytes": int(actions.size_bytes.sum()),
            "candidate_date_files": len(selected), "candidate_date_bytes": int(sizes.sum()),
            "candidate_min_file_bytes": int(sizes.min()) if len(sizes) else None,
            "candidate_max_file_bytes": int(sizes.max()) if len(sizes) else None}


class DeltaAuditReader:
    """Root Delta reads with projected columns and date partition pruning."""
    def __init__(self, root, prefix="v2"):
        from pathlib import Path
        if not prefix or any(p in ("", ".", "..") for p in prefix.split("/")) or "\\" in prefix:
            raise ValueError("prefix must be a relative directory")
        self.root = Path(root).resolve() / prefix
        self.prefix = prefix
        self.sources = {}

    def read(self, key, dates, columns):
        from deltalake import DeltaTable
        started = time.perf_counter()
        source = {"path": f"{self.prefix}/{key}", "requested_dates": dates}
        self.sources[key] = source
        try:
            table = DeltaTable(str(self.root / key))
            source["delta_version"] = table.version()
            actions = pa.table(table.get_add_actions(flatten=True)).select(
                ["size_bytes", "partition.year", "partition.month", "partition.day"]).to_pandas()
            source["files"] = file_metrics(actions, dates)
            frames = [table.to_pandas(columns=columns, partitions=[
                ("year", "=", day[:4]), ("month", "=", day[4:6]), ("day", "=", day[6:8])]) for day in dates]
            frame = pd.concat(frames, ignore_index=True)
            source.update(status="available" if len(frame) else "empty_date", rows=len(frame))
            return frame
        except Exception as error:
            source.update(status="unavailable", error_type=type(error).__name__)
            return None
        finally:
            source["read_seconds"] = round(time.perf_counter() - started, 6)


def usage_metrics(bronze, silver, gold):
    metrics = {}
    if bronze is not None:
        keys = ["USE_YMD", "SBWY_ROUT_LN_NM", "SBWY_STNS_NM"]
        metrics["bronze"] = {
            "rows": len(bronze), "natural_keys": len(bronze.drop_duplicates(keys)),
            "repeated_key_rows": int(bronze.duplicated(keys).sum()),
            "exact_repeat_rows": int(bronze.duplicated(keys + ["record_hash"]).sum()),
            "collection_runs": int(bronze.run_id.nunique()),
            "keys_with_payload_revisions": int((bronze.groupby(keys).record_hash.nunique() > 1).sum())}
    normalized = {}
    for name, frame in [("silver", silver), ("gold", gold)]:
        if frame is not None:
            keys_valid = frame[USAGE_KEY].apply(
                lambda column: column.map(lambda value: isinstance(value, str) and bool(value.strip()))).all(axis=1)
            counts = ["ride_count", "alight_count", "total_count"]
            valid = frame[counts].apply(lambda column: column.map(lambda value: valid_number(value, integer=True)))
            # Explicit object dtype avoids both int64 sum wrap and float inference near invalid rows.
            numeric = pd.DataFrame({
                column: pd.Series([int(value) if good else None for value, good in zip(frame[column], valid[column])],
                                  index=frame.index, dtype=object) for column in counts})
            reconciled = pd.Series(
                [good and total == ride + alight for good, ride, alight, total in
                 zip(valid.all(axis=1), numeric.ride_count, numeric.alight_count, numeric.total_count)],
                index=frame.index, dtype=bool)
            bad = ~keys_valid | ~valid.all(axis=1) | ~reconciled
            metrics[name] = {"rows": len(frame), "duplicate_key_rows": int(frame.duplicated(USAGE_KEY).sum()),
                             "invalid_rows": int(bad.sum()),
                             "total_count": sum(numeric.total_count) if not bad.any() else None}
            if not bad.any():
                normalized[name] = pd.concat([frame[USAGE_KEY], numeric], axis=1).sort_values(USAGE_KEY).reset_index(drop=True)
    if silver is not None and gold is not None:
        metrics["silver_gold_business_match"] = (
            normalized["silver"].equals(normalized["gold"]) if len(normalized) == 2 else None)
    return metrics

def arrival_metrics(bronze, silver, start, end, as_of, cadence_seconds=300):
    start, end, as_of = map(kst, (start, end, as_of))
    expected = math.ceil((end - start).total_seconds() / cadence_seconds)
    result = {"expected_time_buckets": expected, "cadence_seconds": cadence_seconds,
              "missing_bucket_examples": [], "latest_snapshot_kst": None, "freshness_seconds": None}
    if bronze is not None:
        raw = bronze.copy()
        raw["snapshot_ts"] = pd.to_datetime(raw.snapshot_ts, utc=True, errors="coerce")
        result["invalid_snapshot_rows"] = int(raw.snapshot_ts.isna().sum())
        raw = raw[(raw.snapshot_ts >= start) & (raw.snapshot_ts < end)]
        stamps = raw.snapshot_ts.dropna().drop_duplicates()
        observed = set(((stamps - start).dt.total_seconds() // cadence_seconds).astype(int))
        missing = sorted(set(range(expected)) - observed)
        result.update(bronze_rows=len(raw), collection_runs=int(raw.run_id.nunique()),
                      observed_snapshots=len(stamps), observed_time_buckets=len(observed),
                      missing_time_buckets=len(missing), observed_time_bucket_ratio=len(observed) / expected,
                      missing_bucket_examples=[(start + timedelta(seconds=i * cadence_seconds)).isoformat()
                                               for i in missing[:50]],
                      missing_examples_truncated=len(missing) > 50,
                      response_at_limit_snapshots=int(raw.loc[raw.response_at_limit.fillna(False),
                                                              "snapshot_ts"].nunique()))
        if len(stamps):
            latest = stamps.max().tz_convert(KST)
            result["latest_snapshot_kst"] = latest.isoformat()
            result["freshness_seconds"] = (as_of - latest).total_seconds()
    raw_stamps = set(stamps) if bronze is not None else None
    if silver is not None:
        fact = silver.copy()
        fact["snapshot_ts"] = pd.to_datetime(fact.snapshot_ts, utc=True, errors="coerce")
        result["invalid_silver_snapshot_rows"] = int(fact.snapshot_ts.isna().sum())
        fact = fact[(fact.snapshot_ts >= start) & (fact.snapshot_ts < end)]
        fact_stamps = set(fact.snapshot_ts.dropna())
        if raw_stamps is not None:
            result["unprocessed_snapshot_count"] = len(raw_stamps - fact_stamps)
            result["silver_only_snapshot_count"] = len(fact_stamps - raw_stamps)
        required_keys = fact[["line_id", "station_id", "updn_line", "train_no"]]
        bad_keys = required_keys.isna().any(axis=1)
        bad_keys |= required_keys.astype("string").apply(lambda column: column.str.strip().eq("")).any(axis=1)
        result["invalid_natural_key_rows"] = int(bad_keys.sum())
        eta = pd.to_numeric(fact.arrival_seconds, errors="coerce")
        valid_eta = eta.notna() & np.isfinite(eta) & (eta >= 0) & (eta % 1 == 0)
        source_age_raw = fact.loc[fact.recptn_dt.notna(), "source_age_sec"]
        source_age = pd.to_numeric(source_age_raw, errors="coerce")
        result["invalid_source_age_rows"] = int((source_age_raw.notna() & (source_age.isna() | ~np.isfinite(source_age))).sum())
        source_age = source_age[np.isfinite(source_age)].dropna()
        result.update(silver_rows=len(fact), duplicate_key_rows=int(fact.duplicated(ARRIVAL_KEY).sum()),
                      valid_eta_rows=int(valid_eta.sum()), missing_eta_rows=int(fact.arrival_seconds.isna().sum()),
                      missing_eta_ratio=float(fact.arrival_seconds.isna().mean()) if len(fact) else None,
                      invalid_eta_rows=int((fact.arrival_seconds.notna() & ~valid_eta).sum()), missing_source_timestamp_rows=int(fact.recptn_dt.isna().sum()),
                      future_source_timestamp_rows=int((source_age < 0).sum()),
                      max_source_age_seconds=float(source_age.max()) if len(source_age) else None,
                      median_source_age_seconds=float(source_age.median()) if len(source_age) else None,
                      p95_source_age_seconds=float(source_age.quantile(0.95)) if len(source_age) else None,
                      observed_line_count=int(fact.line_id.nunique()),
                      observed_station_line_identity_count=len(fact[["line_id", "station_id"]].drop_duplicates()))
    return result


def audit_lake(reader, usage_day, start, end, as_of, cadence_seconds=300, stale_after_seconds=600):
    validate_ymd(usage_day)
    start, end, as_of = map(kst, (start, end, as_of))
    if end <= start or end - start > timedelta(days=1) or as_of < end:
        raise ValueError("audit requires a closed positive window of at most 24 hours")
    if cadence_seconds <= 0 or stale_after_seconds < 0:
        raise ValueError("invalid audit cadence/freshness threshold")
    usage_bronze = reader.read("bronze/subway_usage_delta", [usage_day],
                              ["USE_YMD", "SBWY_ROUT_LN_NM", "SBWY_STNS_NM", "record_hash", "run_id"])
    usage_silver = reader.read("silver/fact_subway_usage_delta", [usage_day], USAGE_VALUES)
    usage_gold = reader.read("gold/subway_usage/station_daily", [usage_day], USAGE_VALUES)
    arrival_bronze = reader.read("bronze/subway_arrival_delta", date_range(start, end),
                                ["snapshot_ts", "run_id", "response_at_limit"])
    arrival_silver = reader.read("silver/fact_subway_arrival_delta", date_range(start, end),
                                ARRIVAL_KEY + ["arrival_seconds", "recptn_dt", "source_age_sec"])
    usage = usage_metrics(usage_bronze, usage_silver, usage_gold)
    arrival = arrival_metrics(arrival_bronze, arrival_silver, start, end, as_of, cadence_seconds)
    issues = []
    def issue(severity, code):
        issues.append({"severity": severity, "code": code})
    for key, source in reader.sources.items():
        if source["status"] != "available":
            issue("error", f"source_{source['status']}:{key}")
    for name, frame, column in [("bronze", usage_bronze, "USE_YMD"), ("silver", usage_silver, "use_ymd"), ("gold", usage_gold, "use_ymd")]:
        if frame is not None and (frame[column].astype(str) != usage_day).any():
            issue("error", f"usage_{name}_date_mismatch")
    for name in ["silver", "gold"]:
        if usage.get(name, {}).get("invalid_rows") or usage.get(name, {}).get("duplicate_key_rows"):
            issue("error", f"usage_{name}_contract")
    if usage.get("silver_gold_business_match") is False:
        issue("error", "usage_silver_gold_mismatch")
    if arrival.get("invalid_snapshot_rows") or arrival.get("invalid_silver_snapshot_rows") or arrival.get("invalid_source_age_rows") or arrival.get("duplicate_key_rows") or arrival.get("invalid_eta_rows") or arrival.get("invalid_natural_key_rows") or arrival.get("silver_only_snapshot_count"):
        issue("error", "arrival_contract")
    if arrival.get("bronze_rows") == 0 or arrival.get("silver_rows") == 0:
        issue("error", "arrival_window_empty")
    if arrival.get("valid_eta_rows") == 0:
        issue("error", "arrival_no_valid_eta")
    if arrival.get("unprocessed_snapshot_count"):
        issue("warning", "arrival_unprocessed_snapshots")
    if arrival.get("missing_time_buckets"):
        issue("warning", "arrival_unobserved_time_buckets")
    if arrival.get("freshness_seconds") is not None and arrival["freshness_seconds"] > stale_after_seconds:
        issue("warning", "arrival_stale")
    if arrival.get("response_at_limit_snapshots"):
        issue("warning", "arrival_response_at_limit")
    if arrival.get("future_source_timestamp_rows"):
        issue("warning", "arrival_source_clock_ahead")
    status = "failed" if any(i["severity"] == "error" for i in issues) else ("degraded" if issues else "healthy")
    return {"schema_version": 1, "status": status, "usage_day": usage_day,
            "window": {"start_kst": start.isoformat(), "end_kst": end.isoformat(), "as_of_kst": as_of.isoformat(),
                       "stale_after_seconds": stale_after_seconds},
            "usage": usage, "arrival": arrival, "sources": dict(reader.sources), "issues": issues,
            "limitations": ["Unobserved buckets are not proof of a failed API request",
                            "Time-bucket coverage is not station/network coverage",
                            "File bytes describe active date candidates, not measured IO cost",
                            "No task duration or pool wait inferred from Delta timestamps"]}
