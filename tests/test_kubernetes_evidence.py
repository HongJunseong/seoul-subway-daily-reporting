from copy import deepcopy
from types import SimpleNamespace as NS
import pytest
from scripts.kubernetes_fixture import validate_evidence, worker_record


def observed_success():
    names = {
        "usage_daily": ["compute_dates", "choose_input", "silver_usage", "gold_usage", "trigger_gold_daily"],
        "gold_daily": ["compute_dates", "silver_arrival_day", "gold_arrival", "notify_gold_ready", "trigger_report_daily"],
        "report_daily": ["compute_target_ymd", "generate_report", "upload_report"],
    }
    runs = [{"dag_id": dag, "state": "success",
             "tasks": [{"task_id": task, "state": "success"} for task in tasks]}
            for dag, tasks in names.items()]
    runs[0]["tasks"].append({"task_id": "bronze_usage", "state": "skipped"})
    workers = [{"uid": f"{dag}-{task}-1", "dag_id": dag, "task_id": task,
                "phase": "Succeeded", "try_number": 1, "exit_codes": [0]}
               for dag, tasks in names.items() for task in tasks]
    return runs, workers


def test_db_success_requires_completed_pod_for_every_executed_task():
    runs, workers = observed_success()
    validate_evidence(runs, workers)
    with pytest.raises(ValueError, match="missing"):
        validate_evidence(runs, workers[:-1])
    workers[-1]["exit_codes"] = [1]
    with pytest.raises(ValueError, match="missing"):
        validate_evidence(runs, workers)


def test_recovery_requires_different_uid_and_later_attempt():
    runs, workers = observed_success()
    deleted = next(p for p in workers if p["task_id"] == "gold_usage").copy()
    with pytest.raises(ValueError, match="different"):
        validate_evidence(runs, workers, deleted)
    retried = deepcopy(workers)
    gold = next(p for p in retried if p["task_id"] == "gold_usage")
    gold["uid"] = "new-worker"
    with pytest.raises(ValueError, match="different"):
        validate_evidence(runs, retried, deleted)
    gold["try_number"] = 2
    validate_evidence(runs, retried, deleted)


def test_live_collection_or_failed_dag_cannot_be_verified():
    runs, workers = observed_success()
    runs[0]["tasks"][-1]["state"] = "success"
    with pytest.raises(ValueError):
        validate_evidence(runs, workers)
    runs, workers = observed_success()
    runs[0]["state"] = "failed"
    with pytest.raises(ValueError):
        validate_evidence(runs, workers)


def test_worker_observations_exclude_other_namespace_and_run():
    labels = {"kubernetes_executor": "True", "run_id": "this-run", "try_number": "1",
              "dag_id": "usage_daily", "task_id": "gold_usage"}
    pod = NS(metadata=NS(labels=labels, namespace="ssdr-fixture-test", uid="uid", name="name"),
             spec=NS(node_name="node", containers=[NS(name="base", image="fixture:image",
                         resources=NS(to_dict=lambda: {"requests": {"cpu": "1"}}))]),
             status=NS(phase="Running", container_statuses=[]))
    assert worker_record(pod, "other-namespace", {"this-run"}) is None
    assert worker_record(pod, "ssdr-fixture-test", {"other-run"}) is None
    record = worker_record(pod, "ssdr-fixture-test", {"this-run"})
    assert record["uid"] == "uid" and record["exit_codes"] == []
