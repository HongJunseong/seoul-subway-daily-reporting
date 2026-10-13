"""Verify a canonical publication object or legacy report export set."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.report.publication import read_publication, strict_json, verify_contents


def verify_report(path):
    path = Path(path)
    if path.name.endswith(".publication.json"):
        return read_publication(path)["manifest"]
    stem = path.with_suffix("")
    context = Path(str(stem) + ".context.json")
    manifest = strict_json(Path(str(stem) + ".manifest.json").read_bytes())
    verify_contents(path.read_bytes(), context.read_bytes(), manifest)
    bundle = Path(str(stem) + ".publication.json")
    if bundle.exists() and read_publication(bundle)["manifest"] != manifest:
        raise ValueError("Publication exports differ from committed bundle")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify_report(args.report), ensure_ascii=False, indent=2))
