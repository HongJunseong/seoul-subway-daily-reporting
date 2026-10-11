"""Aggregate SSDR lifecycle events from copied task logs, without contacting Airflow."""
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.observability.task_events import summarize_events


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", nargs="+", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    def lines():
        for path in args.log_file:
            with path.open(encoding="utf-8") as stream:
                yield from stream
    result = summarize_events(lines())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    print(f"Task groups={len(result['tasks'])}, malformed={result['malformed_events']}")
    return 2 if result["malformed_events"] or not result["unique_events"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
