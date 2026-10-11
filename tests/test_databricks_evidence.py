from copy import deepcopy
import pytest
from scripts.record_databricks_run import validate_run, TASKS


def inputs():
    snapshot = {"silver": {"rows": 6, "sha256": "fixture-hash"}}
    run = {"job_id": 1, "run_id": 2, "start_time": 1000, "end_time": 3000,
           "state": {"result_state": "SUCCESS"},
           "tasks": [{"task_key": key, "run_id": i, "state": {"result_state": "SUCCESS"}}
                     for i, key in enumerate(sorted(TASKS))]}
    result = {"execution_environment": "databricks-serverless", "verified": True, "synthetic": True,
              "totals": {"20260128": 680, "20260129": 660}, "bronze_unchanged": True,
              "business_snapshot": snapshot}
    local = {"execution_environment": "local-spark-managed-delta-contract",
             "business_snapshot": deepcopy(snapshot)}
    return run, result, local


def test_native_failure_and_skipped_downstream_cannot_be_public_success():
    run, result, local = inputs()
    run["state"]["result_state"] = "FAILED"
    with pytest.raises(ValueError, match="Job"):
        validate_run(run, result, local)
    run["state"]["result_state"] = "SUCCESS"
    run["tasks"][0]["state"]["result_state"] = "SKIPPED"
    with pytest.raises(ValueError, match="four"):
        validate_run(run, result, local)


def test_equal_totals_do_not_hide_changed_business_rows():
    run, result, local = inputs()
    local["business_snapshot"]["silver"]["sha256"] = "different-rows"
    with pytest.raises(ValueError, match="hashes"):
        validate_run(run, result, local)


def test_local_execution_cannot_be_mislabeled_as_native():
    run, result, local = inputs()
    result["execution_environment"] = "local-spark-managed-delta-contract"
    with pytest.raises(ValueError, match="Databricks"):
        validate_run(run, result, local)


def test_recorded_duration_is_actual_job_wall_time():
    evidence = validate_run(*inputs())
    assert evidence["elapsed_seconds"] == 2.0
    assert evidence["cross_engine_business_match"] is True
