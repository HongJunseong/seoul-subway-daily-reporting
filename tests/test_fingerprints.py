from datetime import datetime
import hashlib
import json
import pytest
from src.observability.fingerprints import delta_fingerprint
from src.common.dates import KST


@pytest.mark.parametrize("revision,cap", [(0, 0), (0, -1), (0, True), (-1, 2), (True, 2), (None, 2)])
def test_invalid_bounds_are_rejected_before_reading(revision, cap):
    with pytest.raises(ValueError):
        delta_fingerprint(None, "unused", revision, max_rows=cap)


@pytest.mark.spark
def test_full_row_hash_preserves_null_duplicates_microseconds_pinned_version_and_bound(spark, tmp_path):
    from pyspark.sql import types as T
    schema = T.StructType([T.StructField("value", T.LongType()), T.StructField("observed_at", T.TimestampType()),
                           T.StructField("note", T.StringType())])
    stamp = datetime(2026, 1, 31, 23, 59, 59, 123456, tzinfo=KST)
    frame = spark.createDataFrame([(7, stamp, None)]*2, schema)
    root = tmp_path/"original"
    frame.write.format("delta").save(str(root))
    result = delta_fingerprint(spark, root, 0, max_rows=2)
    line = json.dumps({"note": None, "observed_at": "2026-01-31T23:59:59.123456+09:00", "value": 7},
                      separators=(",", ":"))
    assert result == {"rows": 2, "sha256": hashlib.sha256((line+"\n"+line+"\n").encode()).hexdigest()}
    reordered = tmp_path/"reordered"
    frame.select("note", "value", "observed_at").repartition(1).write.format("delta").save(str(reordered))
    assert delta_fingerprint(spark, reordered, 0, max_rows=2) == result
    spark.createDataFrame([(7, stamp, "changed extra column")], schema).write.format("delta").mode("append").save(str(root))
    assert delta_fingerprint(spark, root, 0, max_rows=2) == result
    assert delta_fingerprint(spark, root, 1, max_rows=3) != result
    with pytest.raises(ValueError, match="oversized"):
        delta_fingerprint(spark, root, 1, max_rows=2)
    empty = tmp_path/"empty"
    frame.limit(0).write.format("delta").save(str(empty))
    with pytest.raises(ValueError, match="empty"):
        delta_fingerprint(spark, empty, 0)
    assert delta_fingerprint(spark, empty, 0, allow_empty=True)["sha256"] == hashlib.sha256(b"\n").hexdigest()
