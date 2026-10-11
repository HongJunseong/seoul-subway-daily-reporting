"""Small opt-in source experiment; raw observations stay in an ignored local directory."""
from __future__ import annotations
import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.common.dates import KST, validate_ymd
from src.ingestion.ingest_subway_arrival_realtime import fetch_realtime_arrival_all_json
from src.ingestion.ingest_subway_usage_daily import fetch_subway_usage_daily


def write_json(path, value):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    temp.chmod(0o600)
    temp.replace(path)


class CountedSession(requests.Session):
    """No automatic retries; count logical GET attempts without recording credential URLs."""
    def __init__(self):
        super().__init__()
        self.attempts = 0

    def get(self, url, **kwargs):
        self.attempts += 1
        return super().get(url, allow_redirects=False, **kwargs)


def new_output(output):
    output = Path(output).resolve()
    private = (ROOT / ".work").resolve()
    if not output.is_relative_to(private) or output == private:
        raise ValueError("source experiment requires a new directory below repository .work")
    output.mkdir(parents=True, exist_ok=False)
    output.chmod(0o700)
    return output


def capture(output, usage_day, *, snapshots=3, cadence_seconds=300,
            session=None, clock=lambda: datetime.now(KST), monotonic=time.monotonic,
            sleep=time.sleep, arrival_fetch=fetch_realtime_arrival_all_json,
            usage_fetch=fetch_subway_usage_daily):
    validate_ymd(usage_day)
    if snapshots not in range(1, 4) or not 300 <= cadence_seconds <= 3600:
        raise ValueError("experiment permits 1..3 snapshots at a cadence of 300..3600 seconds")
    output = new_output(output)
    owned = session is None
    client = session or CountedSession()
    start, origin = clock(), monotonic()
    evidence = {
        "schema_version": 1, "data_kind": "observed", "status": "running",
        "started_at_kst": start.isoformat(), "usage_day": usage_day,
        "cadence_seconds": cadence_seconds, "planned_snapshots": snapshots,
        "arrival_scope": "ALL response, maximum 1000 observations; partial coverage",
        "retry_policy": "one logical GET per arrival slot; usage pagination at most 10 GETs; no automatic retry",
        "arrival_slots": [], "usage": None,
        "limitations": [
            "Three snapshots are a short source experiment, not a sustained operational baseline",
            "Observed station/line identities are a sample, not complete network coverage",
            "Uncollected historical realtime observations cannot be recovered",
            "ETA is not observed service delay or passenger waiting time",
            "Usage counts describe usage_day, not current crowding",
        ],
    }

    def attempt(fetch, payload_name, extra):
        begun, tick = clock(), monotonic()
        before = client.attempts
        item = {**extra, "request_started_at_kst": begun.isoformat()}
        try:
            rows = fetch()
            if not rows or not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
                raise ValueError("invalid source rows")
            ended = clock()
            write_json(output / payload_name, {"captured_at_kst": ended.isoformat(), "rows": rows})
            item.update(status="collected", rows=len(rows), captured_at_kst=ended.isoformat())
        except Exception as error:
            # Exception strings, source messages and URLs may contain credentials.
            item.update(status="failed", error_type=type(error).__name__)
        item.update(request_completed_at_kst=clock().isoformat(),
                    elapsed_seconds=round(monotonic() - tick, 6),
                    logical_get_attempts=client.attempts - before)
        return item

    try:
        for slot in range(snapshots):
            due = origin + slot * cadence_seconds
            while monotonic() < due:
                sleep(min(30, due - monotonic()))
            planned = (start + timedelta(seconds=slot * cadence_seconds)).isoformat()
            if monotonic() - due >= cadence_seconds:
                item = {"slot": slot, "planned_at_kst": planned, "status": "missed_schedule",
                        "logical_get_attempts": 0}
            else:
                item = attempt(lambda: arrival_fetch(session=client), f"arrival-{slot:02d}.json",
                               {"slot": slot, "planned_at_kst": planned})
            evidence["arrival_slots"].append(item)
            write_json(output / "capture.json", evidence)
            print(json.dumps({"arrival_slot": slot, "status": item["status"],
                              "rows": item.get("rows")}), flush=True)
            if slot == 0:
                evidence["usage"] = attempt(
                    lambda: usage_fetch(usage_day, max_pages=10, session=client), "usage.json", {})
                write_json(output / "capture.json", evidence)
                print(json.dumps({"usage_status": evidence["usage"]["status"],
                                  "rows": evidence["usage"].get("rows")}), flush=True)
        evidence.update(status="collected" if evidence["usage"]["status"] == "collected"
                        and all(s["status"] == "collected" for s in evidence["arrival_slots"])
                        else "incomplete",
                        completed_at_kst=clock().isoformat(), logical_get_attempts=client.attempts)
        write_json(output / "capture.json", evidence)
        return evidence
    finally:
        if owned:
            client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch", action="store_true", help="explicit opt-in to live Seoul API GETs")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--usage-day", type=validate_ymd)
    parser.add_argument("--snapshots", type=int, default=3)
    parser.add_argument("--cadence-seconds", type=int, default=300)
    args = parser.parse_args()
    if not args.fetch:
        parser.error("--fetch is required; saved-source processing never implies API access")
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
    day = args.usage_day or (datetime.now(KST) - timedelta(days=4)).strftime("%Y%m%d")
    result = capture(args.output_dir, day, snapshots=args.snapshots, cadence_seconds=args.cadence_seconds)
    return 0 if result["status"] == "collected" else 2


if __name__ == "__main__":
    raise SystemExit(main())
