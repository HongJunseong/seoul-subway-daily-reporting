"""Read private source metadata; optionally reconcile captures with a sealed campaign plan."""
import argparse
from datetime import datetime
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.capture_sources import new_output, write_json
from scripts.run_source_observation_day import campaign_status
from src.observability.source_baseline import render_summary, stamp, summarize_captures
from src.observability.source_campaign import render_campaign, summarize_campaign


def read_campaign(folder, as_of):
    status = campaign_status(folder, as_of=datetime.fromisoformat(as_of))
    folder = folder.resolve()
    captures = {}
    for record in status["daily"]:
        source = folder / "days" / record["capture_day_kst"] / "capture" / "capture.json"
        if not source.resolve().is_relative_to(folder):
            raise ValueError("capture metadata escaped campaign directory")
        if source.exists():
            captures[record["capture_day_kst"]] = json.loads(source.read_text(encoding="utf-8"))
    return summarize_campaign(status, captures)


def main():
    parser = argparse.ArgumentParser()
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--capture-dir", action="append", type=Path)
    inputs.add_argument("--campaign-dir", type=Path)
    parser.add_argument("--start-day")
    parser.add_argument("--end-day")
    parser.add_argument("--as-of", required=True, help="Explicit ISO timestamp with +09:00")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    stamp(args.as_of)
    if args.campaign_dir:
        if args.start_day or args.end_day:
            parser.error("campaign dates come from the sealed plan")
        result = read_campaign(args.campaign_dir, args.as_of)
        rendered = render_campaign(result)
    else:
        if not args.start_day or not args.end_day:
            parser.error("--start-day and --end-day are required for capture metadata")
        manifests = []
        for folder in args.capture_dir:
            folder = folder.resolve()
            if not folder.is_relative_to((ROOT / ".work").resolve()):
                parser.error("capture directories must be inside the private repository .work directory")
            manifests.append(json.loads((folder / "capture.json").read_text(encoding="utf-8")))
        result = summarize_captures(manifests, start_day=args.start_day, end_day=args.end_day, as_of=args.as_of)
        rendered = render_summary(result)
    output = new_output(args.output_dir)
    write_json(output / "summary.json", result)
    markdown = output / "summary.md"
    markdown.write_text(rendered, encoding="utf-8")
    markdown.chmod(0o600)
    if args.campaign_dir:
        print(f"Due slots={result['totals']['due_slots']}; "
              f"unconfirmed={result['totals']['unconfirmed_due_slots']}")
    else:
        print(f"Capture manifests={result['unique_capture_manifests']}; calendar days={result['calendar_days']}; "
              f"days without declared slots={len(result['days_without_declared_capture_slots'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
