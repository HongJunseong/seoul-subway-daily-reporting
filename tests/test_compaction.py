from datetime import datetime, timedelta, timezone
from copy import deepcopy
import pytest
from scripts.compaction_experiment import closed_hour, predicate, layout, verify_change, experiment
from src.common.dates import KST


def test_closed_hour_handles_midnight_month_end_and_exact_boundary():
    start = datetime(2026, 1, 31, 23, tzinfo=KST)
    scope = closed_hour(start, datetime(2026, 2, 1, tzinfo=KST))
    assert scope == dict(year="2026", month="01", day="31", hour="23")
    assert predicate(scope) == "year='2026' AND month='01' AND day='31' AND hour='23'"


@pytest.mark.parametrize("fault", ["naive", "utc", "minute", "microsecond", "open", "future"])
def test_invalid_or_open_hour_cannot_be_selected(fault):
    start = datetime(2026, 1, 31, 23, tzinfo=KST)
    as_of = start + timedelta(hours=1)
    if fault == "naive":
        as_of = as_of.replace(tzinfo=None)
    elif fault == "utc":
        as_of = as_of.astimezone(timezone.utc)
    elif fault == "minute":
        start += timedelta(minutes=1)
    elif fault == "microsecond":
        start += timedelta(microseconds=1)
    elif fault == "open":
        as_of -= timedelta(microseconds=1)
    else:
        start += timedelta(hours=2)
    with pytest.raises(ValueError):
        closed_hour(start, as_of)


def test_partition_predicate_does_not_accept_free_form_sql():
    with pytest.raises(ValueError):
        predicate(dict(year="2026", month="01", day="31", hour="23' OR true"))
    with pytest.raises(ValueError):
        predicate(dict(year="2026", month="01", day="31", hour="23", extra="value"))


def active_file(path, hour, size=10):
    return {"path": path, "size_bytes": size, "partition.year": "2026", "partition.month": "01",
            "partition.day": "31", "partition.hour": hour}


def fixture():
    partition = closed_hour(datetime(2026, 1, 31, 23, tzinfo=KST), datetime(2026, 2, 1, tzinfo=KST))
    before = layout([active_file("a", "23"), active_file("b", "23"), active_file("neighbor", "22")], partition)
    after = layout([active_file("packed", "23", 15), active_file("neighbor", "22")], partition)
    rows = {"rows": 3, "sha256": "same full row digest"}
    return rows, before, after, [{"add": {"dataChange": False}}, {"remove": {"dataChange": False}}]


def test_file_measurement_does_not_expose_paths_or_claim_io_cost():
    rows, before, after, actions = fixture()
    verify_change(rows, rows, before, after, actions)
    assert before["selected_hour_files"] == 2 and after["selected_hour_files"] == 1
    assert before["selected_hour_bytes"] == 20
    assert "neighbor" not in str(before) and "io_bytes" not in before


@pytest.mark.parametrize("fault", ["rows", "neighbor", "data_change", "missing_flag", "no_changes", "no_reduction"])
def test_logical_or_physical_invariant_failure_cannot_pass(fault):
    rows, before, after, actions = fixture()
    other = deepcopy(rows)
    if fault == "rows":
        other["sha256"] = "changed"
    elif fault == "neighbor":
        after["outside_paths_and_sizes_sha256"] = "rewritten"
    elif fault == "data_change":
        actions[0]["add"]["dataChange"] = True
    elif fault == "missing_flag":
        del actions[0]["add"]["dataChange"]
    elif fault == "no_changes":
        actions = [{"commitInfo": {}}]
    else:
        after["selected_hour_files"] = 2
    with pytest.raises(ValueError):
        verify_change(rows, other, before, after, actions)


@pytest.mark.parametrize("snapshots,rows", [(24, 1000), (49, 1), (36, 0), (36, 1001), (True, 1)])
def test_oversized_or_insufficient_fixture_is_rejected_before_any_output(tmp_path, snapshots, rows):
    output = tmp_path/"should-not-exist"
    with pytest.raises(ValueError):
        experiment(output, snapshots=snapshots, rows_per_snapshot=rows)
    assert not output.exists()


@pytest.mark.spark
def test_actual_delta_compaction_preserves_duplicate_null_microsecond_rows_and_neighbor(spark, storage):
    import json
    from delta.tables import DeltaTable
    from src.common.storage import table_path
    from src.ingestion.ingest_subway_arrival_realtime import save_to_bronze_delta
    from scripts.compaction_experiment import fingerprint, version, inventory

    hour = datetime(2026, 1, 31, 23, tzinfo=KST)
    row = dict(statnId="10020001", statnNm="가상역", subwayId="1002", updnLine="상행",
               btrainNo="2001", bstatnId="10020002", barvlDt=None, lstcarAt="0")
    for stamp in (hour+timedelta(minutes=50, microseconds=123456),
                  hour+timedelta(minutes=55, microseconds=654321), hour+timedelta(hours=1)):
        save_to_bronze_delta([row, row], stamp, spark=spark, run_id="synthetic-compaction-test")
    path = table_path("bronze/subway_arrival_delta")
    scope = closed_hour(hour, hour+timedelta(hours=1))
    original_version = version(spark, path)
    before = fingerprint(spark, path, original_version)
    files_before = inventory(path, original_version, scope)
    table = DeltaTable.forPath(spark, path)
    table.optimize().where(predicate(scope)).executeCompaction().collect()
    latest = version(spark, path)
    after = fingerprint(spark, path, latest)
    files_after = inventory(path, latest, scope)
    actions = [json.loads(line) for line in
               (storage/"v2/bronze/subway_arrival_delta/_delta_log"/f"{latest:020d}.json").read_text().splitlines()]
    verify_change(before, after, files_before, files_after, actions)
    original = spark.read.format("delta").option("versionAsOf", original_version).load(path)
    current = spark.read.format("delta").load(path)
    assert original.exceptAll(current).count() == current.exceptAll(original).count() == 0
    assert current.where("barvlDt IS NULL").count() == 6
    assert current.select("snapshot_ts").first()[0].microsecond in (123456, 654321, 0)
    second = table.optimize().where(predicate(scope)).executeCompaction().first()["metrics"]
    assert second["numFilesAdded"] == second["numFilesRemoved"] == 0
    assert version(spark, path) == latest
