"""One bounded source capture per planned KST day; permanent reservations prevent duplicate GETs."""
from __future__ import annotations

import argparse
from datetime import datetime, time, timedelta
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.capture_sources import CountedSession, capture, new_output, write_json
from src.common.dates import KST, validate_ymd
from src.observability.source_baseline import stamp, manifest_hash

DAILY_GET_LIMIT = 13
START_GRACE_SECONDS = 300


def initialize(folder, start_day, *, days=7, daily_time="17:50"):
    start = datetime.strptime(validate_ymd(start_day), "%Y%m%d").date()
    if type(days) is not int or not 1 <= days <= 7:
        raise ValueError("campaign permits 1..7 days")
    clock_time = time.fromisoformat(daily_time)
    if clock_time.tzinfo is not None or clock_time.second or clock_time.microsecond:
        raise ValueError("daily time must be an HH:MM KST wall clock")
    # Keep this bounded experiment in one API-budget calendar day.
    if clock_time > time(23, 30):
        raise ValueError("daily capture must start by 23:30 KST")
    plan = {"schema_version": 1, "timezone": "Asia/Seoul", "start_day": start_day,
            "end_day": (start + timedelta(days=days - 1)).strftime("%Y%m%d"),
            "days": days, "daily_time": clock_time.strftime("%H:%M"),
            "snapshots": 3, "cadence_seconds": 300, "start_grace_seconds": START_GRACE_SECONDS,
            "max_get_attempts_per_day": DAILY_GET_LIMIT, "max_get_attempts_campaign": days * DAILY_GET_LIMIT}
    output = new_output(folder)
    write_json(output / "plan.json", plan)
    seal = output / "plan.sha256"
    seal.write_text(manifest_hash(plan), encoding="utf-8")
    seal.chmod(0o600)
    return plan


def load_plan(folder):
    folder = Path(folder).resolve()
    private = (ROOT / ".work").resolve()
    if not folder.is_relative_to(private) or folder == private:
        raise ValueError("campaign must be inside private repository .work")
    plan = json.loads((folder / "plan.json").read_text(encoding="utf-8"))
    if (folder / "plan.sha256").read_text(encoding="utf-8") != manifest_hash(plan):
        raise ValueError("campaign plan changed outside its sealed collection contract")
    start = datetime.strptime(validate_ymd(plan["start_day"]), "%Y%m%d").date()
    days = plan["days"]
    if type(days) is not int or not 1 <= days <= 7:
        raise ValueError("invalid campaign duration")
    if (type(plan.get("schema_version")) is not int or plan.get("schema_version") != 1 or plan.get("timezone") != "Asia/Seoul"
            or plan.get("snapshots") != 3 or plan.get("cadence_seconds") != 300
            or plan.get("start_grace_seconds") != START_GRACE_SECONDS
            or plan.get("max_get_attempts_per_day") != DAILY_GET_LIMIT
            or plan.get("max_get_attempts_campaign") != days * DAILY_GET_LIMIT
            or plan["end_day"] != (start + timedelta(days=days - 1)).strftime("%Y%m%d")):
        raise ValueError("campaign plan exceeds bounded collection contract")
    wall = time.fromisoformat(plan["daily_time"])
    if wall.tzinfo is not None or wall.second or wall.microsecond or wall > time(23, 30):
        raise ValueError("invalid daily KST time")
    return folder, plan, start, wall


class BudgetedSession(CountedSession):
    def __init__(self, day, clock=lambda: datetime.now(KST)):
        super().__init__()
        self.day, self.clock = day, clock

    def get(self, url, **kwargs):
        if self.attempts >= DAILY_GET_LIMIT:
            raise RuntimeError("daily GET budget exhausted")
        if stamp(self.clock().isoformat()).strftime("%Y%m%d") != self.day:
            raise RuntimeError("capture cannot cross its reserved KST budget day")
        return super().get(url, **kwargs)


def run_day(folder, *, fetch=False, clock=lambda: datetime.now(KST), collector=capture,
            session_factory=BudgetedSession):
    if not fetch:
        raise ValueError("--fetch is required for actual source observation")
    folder, plan, start, wall = load_plan(folder)
    now = stamp(clock().isoformat())
    day = now.strftime("%Y%m%d")
    if not plan["start_day"] <= day <= plan["end_day"]:
        return {"status": "outside_period", "capture_day_kst": day, "logical_get_attempts": 0}
    due = datetime.combine(now.date(), wall, KST)
    if now < due:
        return {"status": "not_due", "capture_day_kst": day, "logical_get_attempts": 0}
    days_root = folder / "days"
    days_root.mkdir(exist_ok=True)
    target = days_root / day
    if not target.resolve().is_relative_to(folder):
        raise ValueError("daily reservation escaped campaign directory")
    try:
        target.mkdir(mode=0o700)
    except FileExistsError:
        return {"status": "already_reserved", "capture_day_kst": day,
                "logical_get_attempts": 0, "note": "No retry after a started, failed or interrupted capture"}
    state = {"status": "reserved", "capture_day_kst": day, "planned_at_kst": due.isoformat(),
             "reserved_at_kst": now.isoformat(), "max_get_attempts": DAILY_GET_LIMIT}
    if (now - due).total_seconds() > START_GRACE_SECONDS:
        state.update(status="missed_start_window", logical_get_attempts=0)
        write_json(target / "state.json", state)
        return state
    # Reserve before loading credentials or opening a client. A crash leaves a permanent budget reservation.
    write_json(target / "state.json", state)
    client = None
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env", override=False)
        client = session_factory(day, clock)
        usage_day = (now.date() - timedelta(days=4)).strftime("%Y%m%d")
        result = collector(target / "capture", usage_day, snapshots=3, cadence_seconds=300,
                           session=client, clock=clock)
        if (result.get("status") not in ("collected", "incomplete")
                or result.get("logical_get_attempts") != client.attempts or client.attempts > DAILY_GET_LIMIT):
            raise ValueError("capture attempt total differs from bounded session")
        state.update(status=result["status"], logical_get_attempts=client.attempts, usage_day=usage_day)
    except Exception as exc:
        state.update(status="interrupted", error_type=type(exc).__name__,
                     logical_get_attempts=client.attempts if client else 0)
    finally:
        if client:
            client.close()
    write_json(target / "state.json", state)
    return state


def campaign_status(folder, *, as_of=None):
    folder, plan, start, wall = load_plan(folder)
    now = stamp((as_of or datetime.now(KST)).isoformat())
    daily = []
    for offset in range(plan["days"]):
        day_date = start + timedelta(days=offset)
        day = day_date.strftime("%Y%m%d")
        due = datetime.combine(day_date, wall, KST)
        target = folder / "days" / day
        if not target.resolve().is_relative_to(folder):
            raise ValueError("daily reservation escaped campaign directory")
        state_path = target / "state.json"
        if not state_path.resolve().is_relative_to(folder):
            raise ValueError("daily state escaped campaign directory")
        if state_path.exists():
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            item = {key: saved[key] for key in ("status", "logical_get_attempts", "usage_day") if key in saved}
        elif target.exists():
            item = {"status": "reserved_without_state"}
        else:
            item = {"status": "not_due" if now < due else "unrecorded_due"}
        daily.append({"capture_day_kst": day, "planned_at_kst": due.isoformat(), **item})
    return {"plan": plan, "as_of_kst": now.isoformat(), "daily": daily,
            "finished": now.date() > datetime.strptime(plan["end_day"], "%Y%m%d").date(),
            "limitations": ["Missing state does not prove an API failure",
                            "Reservations survive interruption; never delete them to retry a live capture",
                            "This budget excludes API use by other collectors with the same key"]}


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init")
    init.add_argument("--campaign-dir", required=True, type=Path)
    init.add_argument("--start-day", required=True)
    init.add_argument("--days", type=int, default=7)
    init.add_argument("--daily-time", default="17:50")
    run = sub.add_parser("run")
    run.add_argument("--campaign-dir", required=True, type=Path)
    run.add_argument("--fetch", action="store_true")
    status = sub.add_parser("status")
    status.add_argument("--campaign-dir", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "init":
        result = initialize(args.campaign_dir, args.start_day, days=args.days, daily_time=args.daily_time)
    elif args.command == "run":
        if not args.fetch:
            parser.error("--fetch is required")
        result = run_day(args.campaign_dir, fetch=True)
    else:
        result = campaign_status(args.campaign_dir)
    print(json.dumps(result, ensure_ascii=False, allow_nan=False))
    return 2 if result.get("status") in ("interrupted", "incomplete", "missed_start_window") else 0


if __name__ == "__main__":
    raise SystemExit(main())
