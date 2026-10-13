"""Compare bounded source capture manifests without reading raw rows or calling services."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, time, timedelta
import hashlib
import json
import math
from statistics import median

from src.common.dates import KST, validate_ymd
from src.observability.source_integrity import capture_version, validate_checksum

ARRIVAL_STATES = ("collected", "failed", "missed_schedule", "not_recorded_due", "future")
USAGE_STATES = ("collected", "failed", "not_recorded")


def stamp(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(hours=9):
        raise ValueError("timestamps must explicitly identify KST")
    return parsed


def integer(value, low, high, label):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"invalid {label}")
    return value


def finite(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"invalid {label}")
    return float(value)


def manifest_hash(manifest):
    encoded = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def distribution(values):
    if not values:
        return {"samples": 0, "min": None, "median": None, "p95": None, "max": None}
    values = sorted(values)
    position = (len(values) - 1) * .95
    lo, hi = math.floor(position), math.ceil(position)
    return {"samples": len(values), "min": values[0], "median": median(values),
            "p95": values[lo] + (values[hi] - values[lo]) * (position - lo), "max": values[-1]}


def validate_request(item, as_of, *, arrival):
    if item.get("status") not in ("collected", "failed"):
        raise ValueError("unknown request status")
    begun, ended = stamp(item["request_started_at_kst"]), stamp(item["request_completed_at_kst"])
    if begun > ended or ended > as_of:
        raise ValueError("request boundaries outside analysis time")
    attempts = integer(item["logical_get_attempts"], 0, 1 if arrival else 10, "GET attempts")
    duration = finite(item["elapsed_seconds"], "elapsed seconds")
    if item["status"] == "collected":
        if attempts == 0:
            raise ValueError("collected request requires a GET attempt")
        integer(item["rows"], 1, 1000 if arrival else 10000, "response row limit")
        if not begun <= stamp(item["captured_at_kst"]) <= ended:
            raise ValueError("capture outside request boundaries")
    elif any(key in item for key in ("rows", "captured_at_kst")):
        raise ValueError("failed request cannot contain successful observations")
    return attempts, duration


def validate_manifest(manifest, as_of):
    version = capture_version(manifest)
    if manifest.get("data_kind") != "observed":
        raise ValueError("expected observed source capture")
    status = manifest.get("status")
    if status not in ("running", "collected", "incomplete"):
        raise ValueError("unknown capture status")
    started = stamp(manifest["started_at_kst"])
    if started > as_of:
        raise ValueError("capture starts after analysis time")
    validate_ymd(manifest["usage_day"])
    planned = integer(manifest["planned_snapshots"], 1, 3, "planned snapshots")
    cadence = integer(manifest["cadence_seconds"], 300, 3600, "cadence")
    slots = manifest["arrival_slots"]
    if not isinstance(slots, list) or len(slots) > planned:
        raise ValueError("invalid arrival slot list")
    attempts, latest = 0, None
    for i, item in enumerate(slots):
        validate_checksum(item, version)
        if type(item.get("slot")) is not int or item["slot"] != i:
            raise ValueError("slots must be a chronological prefix of the declared plan")
        due = started + timedelta(seconds=i * cadence)
        if stamp(item["planned_at_kst"]) != due or due > as_of:
            raise ValueError("recorded slot outside declared plan or analysis time")
        if item.get("status") == "missed_schedule":
            integer(item.get("logical_get_attempts"), 0, 0, "missed slot GET attempts")
            if any(
                    key in item for key in ("rows", "captured_at_kst", "request_started_at_kst")):
                raise ValueError("missed schedule cannot be a request")
        else:
            calls, _ = validate_request(item, as_of, arrival=True)
            if (stamp(item["request_started_at_kst"]) - due).total_seconds() < -1:
                raise ValueError("request preceded schedule beyond clock tolerance")
            attempts += calls
            if item["status"] == "collected":
                captured = stamp(item["captured_at_kst"])
                if latest is not None and captured <= latest:
                    raise ValueError("snapshots must be distinct and chronological")
                latest = captured
    usage = manifest.get("usage")
    if usage is not None:
        validate_checksum(usage, version)
        calls, _ = validate_request(usage, as_of, arrival=False)
        if stamp(usage["request_started_at_kst"]) < started:
            raise ValueError("usage request preceded capture")
        attempts += calls
    if status != "running":
        completed = stamp(manifest["completed_at_kst"])
        recorded_ends = [stamp(s["request_completed_at_kst"]) for s in slots
                         if s["status"] != "missed_schedule"]
        if usage is not None:
            recorded_ends.append(stamp(usage["request_completed_at_kst"]))
        if not max([started] + recorded_ends) <= completed <= as_of:
            raise ValueError("invalid completion time")
        if len(slots) != planned or usage is None:
            raise ValueError("finished capture missing terminal records")
        integer(manifest["logical_get_attempts"], 0, planned + 10, "total GET attempts")
        if manifest["logical_get_attempts"] != attempts:
            raise ValueError("total GET attempts differ from request records")
        all_collected = all(s["status"] == "collected" for s in slots) and usage["status"] == "collected"
        if (status == "collected") != all_collected:
            raise ValueError("capture status differs from request outcomes")
    return started


def summarize_captures(manifests, *, start_day, end_day, as_of):
    """Only declared capture slots form a success-rate denominator; unplanned days are unknown."""
    start = datetime.combine(datetime.strptime(validate_ymd(start_day), "%Y%m%d").date(), time(), KST)
    end = datetime.combine(datetime.strptime(validate_ymd(end_day), "%Y%m%d").date(), time(), KST)
    as_of = stamp(as_of) if isinstance(as_of, str) else stamp(as_of.isoformat())
    days = (end - start).days + 1
    if not 1 <= days <= 31 or as_of < start:
        raise ValueError("analysis needs 1..31 calendar days and an as-of at or after the start")
    rows = {}
    for offset in range(days):
        day = (start + timedelta(days=offset)).strftime("%Y%m%d")
        rows[day] = {"planned_day_kst": day, "arrival": Counter({key: 0 for key in ARRIVAL_STATES}),
                     "arrival_get_attempts": 0, "arrival_response_rows": 0, "response_at_limit_snapshots": 0,
                     "usage": Counter({key: 0 for key in USAGE_STATES}), "usage_get_attempts": 0,
                     "usage_observations": [], "capture_starts": 0,
                     "capture_status_counts": {"collected": 0, "incomplete": 0, "running": 0},
                     "latencies": [], "snapshots": []}
    seen, origins, hashes = set(), {}, []
    duplicates = 0
    for manifest in manifests:
        origin = validate_manifest(manifest, as_of)
        digest = manifest_hash(manifest)
        if digest in seen:
            duplicates += 1
            continue
        if origin in origins:
            raise ValueError("conflicting manifests for the same capture start; select one revision")
        seen.add(digest)
        origins[origin] = digest
        hashes.append(digest)
        origin_day = origin.strftime("%Y%m%d")
        if origin_day in rows:
            daily = rows[origin_day]
            daily["capture_starts"] += 1
            daily["capture_status_counts"][manifest["status"]] += 1
            usage = manifest.get("usage")
            daily["usage"][usage["status"] if usage else "not_recorded"] += 1
            if usage:
                daily["usage_get_attempts"] += usage["logical_get_attempts"]
                if usage["status"] == "collected":
                    daily["usage_observations"].append({"usage_day": manifest["usage_day"],
                                                        "rows": usage["rows"]})
        for i in range(manifest["planned_snapshots"]):
            due = origin + timedelta(seconds=i * manifest["cadence_seconds"])
            day = due.strftime("%Y%m%d")
            if day not in rows:
                continue
            daily = rows[day]
            item = manifest["arrival_slots"][i] if i < len(manifest["arrival_slots"]) else None
            state = item["status"] if item else ("not_recorded_due" if due <= as_of else "future")
            daily["arrival"][state] += 1
            if item and state in ("collected", "failed"):
                daily["arrival_get_attempts"] += item["logical_get_attempts"]
                daily["latencies"].append(item["elapsed_seconds"])
                if state == "collected":
                    daily["arrival_response_rows"] += item["rows"]
                    daily["response_at_limit_snapshots"] += int(item["rows"] == 1000)
                    daily["snapshots"].append(stamp(item["captured_at_kst"]))
    for daily in rows.values():
        states = daily["arrival"]
        due = sum(states.values()) - states["future"]
        requests = states["collected"] + states["failed"]
        daily["declared_slots"] = sum(states.values())
        daily["request_outcome_success_rate"] = states["collected"] / requests if requests else None
        daily["declared_due_slot_collection_rate"] = states["collected"] / due if due else None
        daily["arrival_request_elapsed_seconds"] = distribution(daily.pop("latencies"))
        snapshots = daily.pop("snapshots")
        latest = max(snapshots) if snapshots else None
        daily["latest_snapshot_at_kst"] = latest.isoformat() if latest else None
        daily["latest_snapshot_age_at_as_of_seconds"] = (as_of - latest).total_seconds() if latest else None
        daily["usage_observations"].sort(key=lambda item: (item["usage_day"], item["rows"]))
        daily["arrival"] = dict(states)
        daily["usage"] = dict(daily["usage"])
    return {
        "schema_version": 1, "data_kind": "observed", "metadata_only": True, "as_of_kst": as_of.isoformat(),
        "start_day": start_day, "end_day": end_day, "calendar_days": days,
        "unique_capture_manifests": len(seen), "duplicate_manifests_ignored": duplicates,
        "source_manifest_sha256": sorted(hashes),
        "days_without_declared_capture_slots": [d for d, row in rows.items() if not row["declared_slots"]],
        "days_with_collected_snapshots": [d for d, row in rows.items() if row["arrival"]["collected"]],
        "daily": list(rows.values()),
        "limitations": [
            "Rates use only slots declared in supplied capture manifests, not a full-day schedule",
            "A day without a manifest has unknown collection coverage; it is not an API failure",
            "not_recorded_due is missing metadata, not proof of a failed or skipped API request",
            "Elapsed seconds measure the request and local-save attempt duration; snapshot age is measured at the explicit as-of",
            "Response rows are observations, not unique stations or complete network coverage",
            "Usage observations describe usage_day; repeated dates are not added as unique daily usage",
            "Metadata-only analysis validates checksum syntax, not raw-file bytes, Silver quality or Delta replay",
            "GET counts cover these captures only, not other collectors or the API key's total usage",
            "Daily arrival outcomes are assigned by planned slot date, usage outcomes by capture start date",
        ],
    }


def render_summary(report):
    lines = ["# 원천 수집 기록 비교", "",
             f"관측 날짜(KST): {report['start_day']}–{report['end_day']}; 분석 기준: {report['as_of_kst']}.",
             f"서로 다른 캡처 {report['unique_capture_manifests']}개, 동일 기록 중복 제외 {report['duplicate_manifests_ignored']}개.",
             f"실제 수집된 슬롯이 있는 날짜 {len(report['days_with_collected_snapshots'])}일. 날짜별 계획이 없는 구간은 수집 여부 미확인이다.",
             "", "| KST 계획 날짜 | 선언 슬롯 | 성공 | 실패 기록 | 일정 놓침 | 기록 없음(도래) | 향후 | 응답 상한 | GET(도착/승하차) |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for daily in report["daily"]:
        counts = daily["arrival"]
        values = [daily["planned_day_kst"], daily["declared_slots"]] + [counts[k] for k in ARRIVAL_STATES]
        values += [daily["response_at_limit_snapshots"],
                   f"{daily['arrival_get_attempts']}/{daily['usage_get_attempts']}"]
        lines.append("| " + " | ".join(str(v) for v in values) + " |")
    lines += ["", "슬롯은 입력 capture.json의 계획만 센다. 파일이 없는 날짜의 수집 여부는 알 수 없다.",
              "실패 기록은 요청 실패이고, 일정 놓침은 요청하지 않은 슬롯이다. 기록 없음은 둘 중 어느 쪽인지 단정하지 않는다.",
              "응답 1,000건은 전체 역 확보의 증거가 아니다. 요청·저장 시도 시간과 분석 시점의 마지막 관측 나이는 JSON에 별도로 기록한다.",
              "승하차 이용 날짜와 건수도 JSON에 별도로 기록하며, 수집 날짜의 혼잡도로 해석하지 않는다.",
              "원문·정제 행·Delta 재처리 검증은 이 메타데이터 비교에 포함하지 않는다.", ""]
    return "\n".join(lines)
