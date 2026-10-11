"""Verify a report and its input context against the publication manifest."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path


def verify_report(path):
    path = Path(path)
    stem = path.with_suffix("")
    context = Path(str(stem) + ".context.json")
    manifest = json.loads(Path(str(stem) + ".manifest.json").read_text(encoding="utf-8"))
    for file, key in [(path, "report_sha256"), (context, "context_sha256")]:
        if hashlib.sha256(file.read_bytes()).hexdigest() != manifest[key]:
            raise ValueError(f"Publication hash mismatch: {file.name}")
    ctx = json.loads(context.read_text(encoding="utf-8"))
    for key in ["target_ymd", "usage_ymd", "arrival_ymd"]:
        if ctx[key] != manifest[key]:
            raise ValueError(f"Publication date mismatch: {key}")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify_report(args.report), ensure_ascii=False, indent=2))
