"""Read private capture metadata and compare daily collection outcomes; no service calls."""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.capture_sources import new_output, write_json
from src.observability.source_baseline import render_summary, summarize_captures


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--capture-dir", action="append", type=Path, required=True)
    parser.add_argument("--start-day", required=True)
    parser.add_argument("--end-day", required=True)
    parser.add_argument("--as-of", required=True, help="Explicit ISO timestamp with +09:00")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    manifests = []
    for folder in args.capture_dir:
        folder = folder.resolve()
        if not folder.is_relative_to((ROOT / ".work").resolve()):
            parser.error("capture directories must be inside the private repository .work directory")
        manifests.append(json.loads((folder / "capture.json").read_text(encoding="utf-8")))
    result = summarize_captures(manifests, start_day=args.start_day, end_day=args.end_day, as_of=args.as_of)
    output = new_output(args.output_dir)
    write_json(output / "summary.json", result)
    (output / "summary.md").write_text(render_summary(result), encoding="utf-8")
    print(f"Capture manifests={result['unique_capture_manifests']}; calendar days={result['calendar_days']}; "
          f"days without declared slots={len(result['days_without_declared_capture_slots'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
