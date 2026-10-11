"""Bounded, version-pinned full-row hashes for local verification experiments."""
from __future__ import annotations
import hashlib


def delta_fingerprint(spark, path, at_version, *, max_rows=10000, allow_empty=False):
    if type(max_rows) is not int or max_rows <= 0:
        raise ValueError("max_rows must be a positive integer")
    if type(at_version) is not int or at_version < 0:
        raise ValueError("at_version must be a nonnegative integer")
    from pyspark.sql import functions as F
    frame = spark.read.format("delta").option("versionAsOf", at_version).load(str(path))
    payloads = frame.select(F.to_json(F.struct(*sorted(frame.columns)), {
        "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX", "ignoreNullFields": "false",
    }).alias("payload")).limit(max_rows + 1).collect()
    if len(payloads) > max_rows or (not payloads and not allow_empty):
        raise ValueError("empty or oversized bounded fingerprint input")
    rows = sorted(row.payload for row in payloads)
    return {"rows": len(rows), "sha256": hashlib.sha256(("\n".join(rows) + "\n").encode()).hexdigest()}
