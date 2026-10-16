from pathlib import Path
from types import SimpleNamespace
import pendulum
import pytest
pytest.importorskip("airflow", reason="Install the isolated Airflow test environment")
from airflow.models import DagBag

pytestmark = pytest.mark.airflow


@pytest.fixture(scope="module")
def bag():
    result = DagBag(dag_folder=str(Path("dags").resolve()), include_examples=False, safe_mode=False)
    assert not result.import_errors, result.import_errors
    return result


def test_collection_is_independent_of_processing(bag):
    collect = bag.dags["arrival_collect_5m"]
    processing = bag.dags["pipeline_arrival_5m"]
    assert len(collect.tasks) == 1
    assert all("ingest" not in task.task_id for task in processing.tasks)
    assert all(task.env["AS_OF"] == processing.tasks[0].env["AS_OF"] for task in processing.tasks)


def test_notification_failure_does_not_block_report(bag):
    dag = bag.dags["gold_daily"]
    report = dag.get_task("trigger_report_daily")
    assert report.upstream_task_ids == {"gold_arrival"}
    assert "notify_gold_ready" not in report.upstream_task_ids


def test_daily_usage_is_scheduled_and_replay_can_skip_api(bag):
    dag = bag.dags["usage_daily"]
    assert dag.schedule_interval == "0 6 * * *"
    branch = dag.get_task("choose_input").python_callable
    assert branch(dag_run=SimpleNamespace(conf={"reprocess_only": True})) == "silver_usage"
    assert branch(dag_run=SimpleNamespace(conf={})) == "bronze_usage"
    assert dag.get_task("silver_usage").trigger_rule == "none_failed_min_one_success"


def test_daily_dates_use_interval_end_and_validate_manual_input(bag):
    compute = bag.dags["usage_daily"].get_task("compute_dates").python_callable
    dates = compute(dag_run=SimpleNamespace(conf={}),
                    data_interval_end=pendulum.datetime(2026, 2, 1, 6, tz="Asia/Seoul"))
    assert dates == dict(target_ymd="20260201", usage_ymd="20260128", arrival_ymd="20260131")
    with pytest.raises(ValueError):
        compute(dag_run=SimpleNamespace(conf={"target_ymd": "20260101; bad"}))


def test_bash_uses_quoted_environment_for_parameters(bag):
    task = bag.dags["pipeline_arrival_5m"].get_task("gold_arrival_metrics_15m")
    task.render_template_fields({
        "data_interval_end": pendulum.datetime(2026, 2, 1, 0, 5, tz="Asia/Seoul")
    })
    assert task.env["AS_OF"] == "2026-02-01 00:05:00"
    assert '--as_of "$AS_OF"' in task.bash_command
    assert task.append_env is True


def test_spark_writers_share_one_serial_pool(bag):
    from airflow.operators.bash import BashOperator
    writers = [task for dag in bag.dags.values() for task in dag.tasks
               if isinstance(task, BashOperator)]
    assert writers
    assert {task.pool for task in writers} == {"ssdr_delta_writer"}
    assert all(task.pool_slots == 1 for task in writers)


def test_all_tasks_record_lifecycle_without_removing_failure_notification(bag):
    from src.observability.task_events import on_task_start, on_task_success, on_task_failure, on_task_retry
    for dag in bag.dags.values():
        for task in dag.tasks:
            assert task.on_execute_callback is on_task_start
            assert task.on_success_callback is on_task_success
            assert task.on_failure_callback is on_task_failure
            assert task.on_retry_callback is on_task_retry


def test_actual_airflow_attempt_number_is_stable_after_failure(bag):
    from airflow.models import TaskInstance
    from src.observability.task_events import task_event
    ti = TaskInstance(task=bag.dags["usage_daily"].get_task("silver_usage"), run_id="metric-test")
    ti.try_number = 1
    ti.state = "running"
    ctx = {"task_instance": ti}
    assert task_event(ctx, "started")["try_number"] == 1
    for state in ["failed", "up_for_retry"]:
        ti.state = state
        assert ti.try_number == 2
        assert task_event(ctx, "failed")["try_number"] == 1


def test_all_tasks_have_finite_execution_limits_and_five_minute_collection_is_retained(bag):
    from datetime import timedelta
    for dag in bag.dags.values():
        assert timedelta(0) < dag.dagrun_timeout <= timedelta(minutes=45)
        for task in dag.tasks:
            assert timedelta(0) < task.execution_timeout < dag.dagrun_timeout
    collector = bag.dags['arrival_collect_5m']
    assert collector.schedule_interval == '*/5 * * * *'
    task = collector.tasks[0]
    # Two bounded attempts plus retry delay fit within one scheduled collection run.
    assert task.execution_timeout * (task.retries + 1) + task.retry_delay <= collector.dagrun_timeout
