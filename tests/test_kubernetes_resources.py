import json
import pytest
from scripts.summarize_kubernetes_resources import summarize, validate_metrics, quantity
from src.observability.task_events import PREFIX
from src.observability.container_resources import summarize_samples
def snapshot(usage=1_000_000):
    return dict(memory_current_bytes=100, memory_peak_bytes=200, memory_limit_bytes=1000,
                cpu_limit_cores=2, cpu_usage_usec=usage, cpu_periods=10,
                cpu_throttled_periods=2, cpu_throttled_usec=500, oom_kill_count=0)


def fixture():
    metrics = summarize_samples([(0, snapshot()), (1, snapshot(2_000_000))])
    tasks = {
        "usage_daily": ["compute_dates", "choose_input", "silver_usage", "gold_usage", "trigger_gold_daily"],
        "gold_daily": ["compute_dates", "silver_arrival_day", "gold_arrival", "notify_gold_ready", "trigger_report_daily"],
        "report_daily": ["compute_target_ymd", "generate_report"],
    }
    workers, lines = [], []
    for dag, names in tasks.items():
        for name in names:
            workers.append(dict(dag_id=dag, task_id=name, run_id="manual__safe", try_number=1, uid=dag+name,
                                phase="Succeeded", exit_codes=[0], image_id="sha256:fixture", profile="control",
                                resources={"requests": {"cpu": "100m", "memory": "500"},
                                           "limits": {"cpu": "2", "memory": "1000"}}))
            event = dict(schema_version=1, dag_id=dag, task_id=name, run_id="manual__safe", try_number=1, event="succeeded",
                         duration_seconds=1, executor_queue_seconds=10, container_resources=metrics)
            lines.append("log prefix " + PREFIX + json.dumps(event))
    runtime = dict(verified=True, synthetic=True, actual_pod_execution=True, namespace="ssdr-fixture-test",
                   workers=workers, started_at_utc="2026-10-11T09:00:00+00:00", elapsed_seconds=10, writer_pool_slots=1)
    return runtime, lines


def test_actual_workers_join_callbacks_without_inventing_pool_wait():
    runtime, lines = fixture()
    result = summarize(runtime, lines+lines)
    assert result["measured_workers"] == 12
    assert result["tasks"][0]["container"]["cgroup_scope_matches_pod_limits"]
    assert result["profiles"][0]["maximum_memory_peak_to_request_ratio"] == 0.4
    assert result["tasks"][0]["executor_queue_seconds"] == 10
    assert all("pool_wait_seconds" not in task for task in result["tasks"])


@pytest.mark.parametrize("fault", ["missing_callback", "wrong_scope", "partial", "oom", "unknown_queue", "wrong_attempt"])
def test_missing_or_mismatched_metrics_cannot_be_verified(fault):
    runtime, lines = fixture()
    event = json.loads(lines[0].split(PREFIX, 1)[1])
    if fault == "missing_callback":
        lines.pop(0)
    elif fault == "wrong_scope":
        runtime["workers"][0]["resources"]["limits"]["memory"] = "2Gi"
    elif fault == "partial":
        event["container_resources"]["status"] = "partial"
    elif fault == "oom":
        event["container_resources"]["oom_kill_delta"] = 1
    elif fault == "unknown_queue":
        event["executor_queue_seconds"] = None
    else:
        event["try_number"] = 2
    if fault not in ("missing_callback", "wrong_scope"):
        lines[0] = PREFIX+json.dumps(event)
    with pytest.raises(ValueError):
        summarize(runtime, lines)


def test_conflicting_measurements_and_non_fixture_cannot_be_verified():
    runtime, lines = fixture()
    event = json.loads(lines[0].split(PREFIX, 1)[1])
    event["duration_seconds"] = 2
    with pytest.raises(ValueError, match="conflicting"):
        summarize(runtime, lines+[PREFIX+json.dumps(event)])
    runtime["synthetic"] = False
    with pytest.raises(ValueError, match="synthetic"):
        summarize(runtime, lines)


@pytest.mark.parametrize("field", ["mean_cpu_cores", "memory_container_peak_bytes", "cpu_throttled_seconds"])
def test_nonfinite_values_do_not_become_valid_statistics(field):
    runtime, lines = fixture()
    metrics = json.loads(lines[0].split(PREFIX, 1)[1])["container_resources"]
    metrics[field] = float("nan")
    with pytest.raises(ValueError):
        validate_metrics(metrics, runtime["workers"][0]["resources"])


def test_resource_quantities_preserve_millicores_and_binary_memory():
    assert quantity("500m", "cpu") == 0.5
    assert quantity("2Gi", "memory") == 2*1024**3
    with pytest.raises(ValueError):
        quantity("100m", "memory")


@pytest.mark.parametrize("fault", ["interval_type", "unobserved_peak", "malformed_schema"])
def test_cpu_coverage_and_event_schema_are_validated(fault):
    runtime, lines = fixture()
    event = json.loads(lines[0].split(PREFIX, 1)[1])
    if fault == "interval_type":
        event["container_resources"]["cpu_interval_samples"] = "PRIVATE_RAW_VALUE"
    elif fault == "unobserved_peak":
        event["container_resources"]["cpu_interval_samples"] = 0
    else:
        event["schema_version"] = 2
    lines[0] = PREFIX+json.dumps(event)
    with pytest.raises(ValueError):
        summarize(runtime, lines)
