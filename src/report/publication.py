"""Single-object report publication; flat files are compatibility exports."""
from __future__ import annotations
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile

from src.report.build_report_context_daily import report_dates


def strict_json(content):
    def reject(value):
        raise ValueError("Non-finite JSON value in publication")
    def finite_float(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            return reject(value)
        return parsed
    return json.loads(content, parse_constant=reject, parse_float=finite_float)


def verify_contents(report, context, manifest):
    if not isinstance(manifest, dict) or type(manifest.get("schema_version")) is not int or manifest["schema_version"] != 2:
        raise ValueError("Unsupported report manifest schema")
    if manifest.get("report_mode") not in ("template", "llm"):
        raise ValueError("Unsupported publication report mode")
    for content, key in [(report, "report_sha256"), (context, "context_sha256")]:
        if hashlib.sha256(content).hexdigest() != manifest.get(key):
            raise ValueError(f"Publication hash mismatch: {key}")
    ctx = strict_json(context)
    if not isinstance(ctx, dict):
        raise ValueError("Publication context must be an object")
    for key in ["target_ymd", "usage_ymd", "arrival_ymd"]:
        if ctx.get(key) != manifest.get(key):
            raise ValueError(f"Publication date mismatch: {key}")
    target = manifest.get("target_ymd")
    if not isinstance(target, str):
        raise ValueError("Publication target date must be YYYYMMDD")
    usage, arrival = report_dates(target)
    if (manifest.get("usage_ymd"), manifest.get("arrival_ymd")) != (usage, arrival):
        raise ValueError("Publication dates differ from D-4/D-1 contract")
    return ctx


def decode_publication(content):
    bundle = strict_json(content)
    if (not isinstance(bundle, dict) or type(bundle.get("schema_version")) is not int
            or bundle["schema_version"] != 1):
        raise ValueError("Unsupported publication bundle schema")
    if not isinstance(bundle.get("report"), str) or not isinstance(bundle.get("context"), str):
        raise ValueError("Publication must contain exact report and context strings")
    context = verify_contents(bundle["report"].encode("utf-8"), bundle["context"].encode("utf-8"),
                              bundle.get("manifest"))
    return {"manifest": bundle["manifest"], "report": bundle["report"], "context": context}


def read_publication(path):
    # Read one snapshot once: later replacement cannot mix this report and context.
    return decode_publication(Path(path).read_bytes())


def publication_outputs(report, context, manifest):
    verify_contents(report.encode("utf-8"), context.encode("utf-8"), manifest)
    stem = f"subway_report_{manifest['target_ymd']}"
    bundle = {"schema_version": 1, "manifest": manifest, "report": report, "context": context}
    return {
        f"{stem}.md": report.encode("utf-8"),
        f"{stem}.context.json": context.encode("utf-8"),
        f"{stem}.manifest.json": json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False).encode("utf-8"),
        f"{stem}.publication.json": json.dumps(bundle, ensure_ascii=False, sort_keys=True,
                                             allow_nan=False).encode("utf-8"),
    }


def validate_outputs(outputs):
    names = list(outputs)
    if not names or not names[-1].endswith(".publication.json"):
        raise ValueError("Publication object must be written last")
    decoded = decode_publication(outputs[names[-1]])
    context = strict_json(outputs[names[-1]])["context"]
    if outputs != publication_outputs(decoded["report"], context, decoded["manifest"]):
        raise ValueError("Publication exports differ from bundle")
    return names


def atomic_write(path, content):
    path = Path(path)
    # Unique same-directory temporaries avoid writer collisions and cross-device rename.
    temp = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as stream:
            temp = Path(stream.name)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        temp.replace(path)
    finally:
        if temp is not None:
            temp.unlink(missing_ok=True)


def save_local_publication(output_dir, outputs):
    names = validate_outputs(outputs)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in names:
        atomic_write(output_dir / name, outputs[name])


def save_s3_publication(client, bucket, prefix, outputs):
    names = validate_outputs(outputs)
    for name in names:
        key = "/".join(p for p in [prefix.strip("/"), name] if p)
        client.put_object(Bucket=bucket, Key=key, Body=outputs[name],
                          ContentType="text/markdown; charset=utf-8" if name.endswith(".md") else "application/json")
