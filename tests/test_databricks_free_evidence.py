from copy import deepcopy
import pytest
from scripts.record_databricks_free import decode_statement, validate_job


def sql_success():
    return {"statement_id": "statement-fixture", "status": {"state": "SUCCEEDED"},
            "manifest": {"total_chunk_count": 1, "total_row_count": 1, "truncated": False,
                         "schema": {"columns": [{"position": 0, "name": "total_count", "type_name": "BIGINT"},
                                                {"position": 1, "name": "use_ymd", "type_name": "STRING"}]}},
            "result": {"data_array": [["700", "20260128"]]}}


@pytest.mark.parametrize("native_type", ["BIGINT", "LONG"])
def test_sql_manifest_types_are_preserved_for_cross_compute_comparison(native_type):
    response = sql_success()
    response["manifest"]["schema"]["columns"][0]["type_name"] = native_type
    response["manifest"]["schema"]["columns"][0]["type_text"] = "BIGINT"
    result = decode_statement(response)
    assert result["rows"] == [{"total_count": 700, "use_ymd": "20260128"}]


@pytest.mark.parametrize("mutate", [
    lambda r: r["manifest"].update(truncated=True),
    lambda r: r["manifest"].update(total_chunk_count=2),
    lambda r: r["result"].update(next_chunk_internal_link="/next/chunk"),
    lambda r: r["result"].update(data_array=[]),
    lambda r: r["status"].update(state="FAILED"),
])
def test_missing_failed_or_partial_sql_is_never_successful_evidence(mutate):
    response = deepcopy(sql_success())
    mutate(response)
    with pytest.raises(ValueError):
        decode_statement(response)


def test_failed_native_task_blocks_publication_even_with_a_successful_job():
    run = {"state": {"result_state": "SUCCESS"},
           "tasks": [{"task_key": "quality_experiment", "state": {"result_state": "SUCCESS"}},
                     {"task_key": "serving_views", "state": {"result_state": "FAILED"}}]}
    with pytest.raises(ValueError, match="both native tasks"):
        validate_job(run, {}, {}, "workspace.ssdr_v2_fixture_quality_test")


def test_serving_only_update_requires_the_selected_task_to_succeed():
    from scripts.record_databricks_free import validate_serving_update
    run = {"state": {"result_state": "SUCCESS"},
           "tasks": [{"task_key": "serving_views", "state": {"result_state": "SUCCESS"}},
                     {"task_key": "quality_experiment", "state": {"life_cycle_state": "SKIPPED", "result_state": "DISABLED"}}]}
    assert validate_serving_update(run)["task_key"] == "serving_views"
    run["tasks"][0]["state"]["result_state"] = "FAILED"
    with pytest.raises(ValueError):
        validate_serving_update(run)
