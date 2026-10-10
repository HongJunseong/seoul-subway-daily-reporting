"""Immutable API observations with nullable source schemas and lineage."""
from __future__ import annotations
import hashlib
import json
from datetime import datetime
from pyspark.sql import functions as F, types as T
from src.quality.checks import DataQualityError

USAGE_COLUMNS = ["USE_YMD", "SBWY_ROUT_LN_NM", "SBWY_STNS_NM", "GTON_TNOPE", "GTOFF_TNOPE", "REG_YMD"]
ARRIVAL_COLUMNS = ["recptnDt", "statnId", "statnNm", "subwayId", "updnLine", "trainLineNm",
                   "statnFid", "statnTid", "bstatnId", "bstatnNm", "btrainNo", "btrainSttus",
                   "barvlDt", "arvlCd", "arvlMsg2", "arvlMsg3", "lstcarAt"]


def bronze_frame(spark, rows: list[dict], columns: list[str], captured_at: datetime, run_id: str):
    if not rows:
        raise DataQualityError("API returned no rows; refusing empty Bronze write")
    schema = T.StructType([T.StructField(c, T.StringType(), True) for c in columns +
                           ["raw_payload", "record_hash", "run_id", "captured_at"]])
    records = []
    for row in rows:
        payload = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        records.append(tuple(None if row.get(c) is None else str(row[c]) for c in columns) +
                       (payload, hashlib.sha256(payload.encode()).hexdigest(), run_id, captured_at.isoformat()))
    return (spark.createDataFrame(records, schema)
            .withColumn("ingested_at", F.to_timestamp("captured_at")).drop("captured_at"))
