"""Reconcile a sealed sampling plan with recorded outcomes; never infer absent requests."""
from datetime import timedelta

from src.observability.source_baseline import integer, stamp, summarize_captures, validate_manifest

STATES = {"not_due", "unrecorded_due", "reserved", "reserved_without_state",
          "missed_start_window", "interrupted", "incomplete", "collected"}


def summarize_campaign(status, captures):
    """status comes from the sealed-plan campaign_status CLI; captures are keyed by reserved day."""
    plan, as_of = status["plan"], stamp(status["as_of_kst"])
    days = status["daily"]
    if len(days) != plan["days"] or len({d["capture_day_kst"] for d in days}) != len(days):
        raise ValueError("campaign calendar is incomplete or duplicated")
    if set(captures) - {d["capture_day_kst"] for d in days}:
        raise ValueError("capture outside campaign calendar")
    if (days[0]["capture_day_kst"] != plan["start_day"]
            or days[-1]["capture_day_kst"] != plan["end_day"]):
        raise ValueError("campaign calendar differs from sealed period")
    baseline = summarize_captures(list(captures.values()), start_day=plan["start_day"],
                                  end_day=plan["end_day"], as_of=as_of)
    daily = []
    for index, record in enumerate(days):
        day, due, state = record["capture_day_kst"], stamp(record["planned_at_kst"]), record["status"]
        expected_day = stamp(days[0]["planned_at_kst"]) + timedelta(days=index)
        if due != expected_day or due.strftime("%Y%m%d") != day or due.strftime("%H:%M") != plan["daily_time"]:
            raise ValueError("campaign calendar differs from sealed plan")
        if state not in STATES or (state == "not_due") != (as_of < due):
            raise ValueError("campaign state conflicts with analysis time")
        expected_usage = (due - timedelta(days=4)).strftime("%Y%m%d")
        if "usage_day" in record and record["usage_day"] != expected_usage:
            raise ValueError("campaign usage day differs from D-4")
        attempts = record.get("logical_get_attempts")
        if attempts is not None:
            integer(attempts, 0, plan["max_get_attempts_per_day"], "campaign GET attempts")
        if state in {"not_due", "unrecorded_due", "reserved_without_state"} and attempts is not None:
            raise ValueError("unrecorded day cannot declare GET attempts")
        if state == "interrupted" and attempts is None:
            raise ValueError("interrupted state must record GET attempts")
        if state == "missed_start_window" and attempts != 0:
            raise ValueError("missed start window must record zero GETs")
        manifest = captures.get(day)
        observed = baseline["daily"][index]
        if manifest is not None:
            started = validate_manifest(manifest, as_of)
            if (state in {"not_due", "unrecorded_due", "reserved_without_state", "missed_start_window"}
                    or not due <= started or started.strftime("%Y%m%d") != day
                    or manifest["planned_snapshots"] != plan["snapshots"]
                    or manifest["cadence_seconds"] != plan["cadence_seconds"]
                    or manifest["usage_day"] != expected_usage):
                raise ValueError("capture conflicts with campaign reservation or plan")
            recorded_calls = observed["arrival_get_attempts"] + observed["usage_get_attempts"]
            if state in {"collected", "incomplete"}:
                if manifest["status"] != state or attempts != recorded_calls:
                    raise ValueError("terminal campaign state differs from capture")
            elif attempts is not None and attempts < recorded_calls:
                raise ValueError("campaign GET count is below recorded requests")
        elif state in {"collected", "incomplete"}:
            raise ValueError("terminal capture metadata is missing")
        due_slots = sum(due + timedelta(seconds=i * plan["cadence_seconds"]) <= as_of
                        for i in range(plan["snapshots"]))
        successes = observed["arrival"]["collected"]
        failed = observed["arrival"]["failed"]
        skipped = observed["arrival"]["missed_schedule"]
        missed_start = due_slots if state == "missed_start_window" else 0
        unknown = due_slots - successes - failed - skipped - missed_start
        if unknown < 0:
            raise ValueError("recorded outcomes exceed due campaign slots")
        daily.append({
            "capture_day_kst": day, "planned_at_kst": due.isoformat(), "run_status": state,
            "expected_usage_day": expected_usage,
            "planned_slots": plan["snapshots"], "due_slots": due_slots,
            "future_slots": plan["snapshots"] - due_slots,
            "collected_slots": successes, "failed_request_slots": failed,
            "missed_capture_schedule_slots": skipped, "missed_start_window_slots": missed_start,
            "unconfirmed_due_slots": unknown,
            "recorded_get_attempts": attempts,
            "manifest_recorded_get_attempts": observed["arrival_get_attempts"] + observed["usage_get_attempts"],
            "response_at_limit_snapshots": observed["response_at_limit_snapshots"],
            "planned_due_slot_collection_rate": successes / due_slots if due_slots else None,
        })
    totals = {k: sum(d[k] for d in daily) for k in (
        "planned_slots", "due_slots", "future_slots", "collected_slots", "failed_request_slots",
        "missed_capture_schedule_slots", "missed_start_window_slots", "unconfirmed_due_slots")}
    due = totals["due_slots"]
    totals["planned_due_slot_collection_rate"] = totals["collected_slots"] / due if due else None
    totals["known_recorded_get_attempts"] = sum(
        d["recorded_get_attempts"] if d["recorded_get_attempts"] is not None else d["manifest_recorded_get_attempts"]
        for d in daily)
    totals["days_with_unknown_get_count"] = [d["capture_day_kst"] for d in daily
        if d["due_slots"] and d["recorded_get_attempts"] is None]
    return {
        "schema_version": 1, "data_kind": "observed", "metadata_only": True, "plan": plan,
        "as_of_kst": as_of.isoformat(), "daily": daily, "totals": totals,
        "capture_summary": baseline,
        "limitations": [
            "Denominator is the sealed three-snapshot daily plan, not continuous five-minute operation",
            "Unconfirmed slots are missing evidence, not proof of an API failure or zero requests",
            "Known GET counts are a lower bound when a due day's count is unknown",
            "Capture timestamps retain their actual start; the sealed plan is a separate schedule",
            "Raw rows, processing quality, replay and complete network coverage are not validated here",
        ],
    }


def render_campaign(report):
    lines = ["# 계획과 원천 관측 기록", "",
             f"분석 기준(KST): {report['as_of_kst']}", "",
             "| 수집 날짜 | 실행 기록 | 도래/계획 슬롯 | 성공 | 요청 실패 | 슬롯 놓침 | 시작 놓침 | 미확인 | GET 기록 |",
             "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for d in report["daily"]:
        values = [d["capture_day_kst"], d["run_status"], f"{d['due_slots']}/{d['planned_slots']}",
                  d["collected_slots"], d["failed_request_slots"], d["missed_capture_schedule_slots"],
                  d["missed_start_window_slots"], d["unconfirmed_due_slots"],
                  d["recorded_get_attempts"] if d["recorded_get_attempts"] is not None else "미확인"]
        lines.append("| " + " | ".join(str(v) for v in values) + " |")
    lines += ["", "분모는 고정된 하루 3회 표본 계획이다. 운영 DAG의 연속 5분 수집 커버리지가 아니다.",
              "미확인은 요청 실패나 호출 0회를 뜻하지 않는다. 알려진 GET 합계는 미확인 날짜가 있으면 하한이다.",
              "승하차는 수집 날짜 D-4의 이용 날짜로 표시한다. 원문·정제 품질·재처리는 별도 검증이다.", ""]
    return "\n".join(lines)
