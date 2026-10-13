from types import SimpleNamespace
import pytest
from src.observability import container_resources as metrics


def snapshot(usage=1_000_000, memory=100, peak=200, periods=10, throttled=2, throttled_usec=500):
    return dict(memory_current_bytes=memory, memory_peak_bytes=peak, memory_limit_bytes=1000,
                cpu_limit_cores=2, cpu_usage_usec=usage, cpu_periods=periods,
                cpu_throttled_periods=throttled, cpu_throttled_usec=throttled_usec, oom_kill_count=0)


def test_v2_reader_is_read_only_and_preserves_unlimited_vs_missing(tmp_path):
    files = {"memory.current": "100", "memory.peak": "200", "memory.max": "max", "cpu.max": "max 100000",
             "cpu.stat": "usage_usec 1000000\nnr_periods 10\nnr_throttled 2\nthrottled_usec 500\n",
             "memory.events": "oom 0\noom_kill 0\n"}
    for name, text in files.items():
        (tmp_path / name).write_text(text)
    result = metrics.read_snapshot(tmp_path)
    assert result["memory_limit_bytes"] is result["cpu_limit_cores"] is None
    assert result["memory_peak_bytes"] == 200
    assert {p.name: p.read_text() for p in tmp_path.iterdir()} == files
    (tmp_path / "memory.peak").unlink()
    assert metrics.read_snapshot(tmp_path)["memory_peak_bytes"] is None


def test_cpu_is_interval_average_and_kernel_peak_includes_bootstrap():
    rows = [(0, snapshot()), (1, snapshot(3_000_000, memory=300, peak=500)),
            (3, snapshot(5_000_000, memory=100, peak=700, periods=30, throttled=8, throttled_usec=1000500))]
    result = metrics.summarize_samples(rows)
    assert result["status"] == "observed"
    assert result["mean_cpu_cores"] == pytest.approx(4 / 3)
    assert result["sampled_peak_cpu_cores"] == 2
    assert result["memory_container_peak_bytes"] == 700
    assert result["sampled_memory_peak_bytes"] == 300
    assert result["cpu_throttled_periods"] == 6
    assert result["cpu_throttled_seconds"] == 1
    assert result["oom_kill_delta"] == 0


@pytest.mark.parametrize("kind", ["clock", "usage", "throttled", "limit"])
def test_counter_regression_and_scope_changes_are_unknown(kind):
    first, last = snapshot(), snapshot(2_000_000)
    end = 1
    if kind == "clock":
        end = 0
    elif kind == "usage":
        last["cpu_usage_usec"] = 0
    elif kind == "throttled":
        last["cpu_throttled_periods"] = 0
    else:
        last["memory_limit_bytes"] = 2000
    result = metrics.summarize_samples([(0, first), (end, last)])
    assert result["status"] == "unavailable"
    assert "mean_cpu_cores" not in result


def test_absent_file_single_sample_and_failed_reads_are_not_zero_usage(tmp_path):
    with pytest.raises(OSError):
        metrics.read_snapshot(tmp_path)
    assert metrics.summarize_samples([])["status"] == "unavailable"
    partial = metrics.summarize_samples([(0, snapshot())])
    assert partial["status"] == "partial" and partial["mean_cpu_cores"] is None
    assert partial["sampled_peak_cpu_cores"] is None
    assert metrics.summarize_samples([(0, snapshot()), (1, snapshot(2_000_000))], 1)["status"] == "partial"


def context():
    return {"task_instance": SimpleNamespace(dag_id="fixture", task_id="work", run_id="manual__safe",
                                            try_number=1, prev_attempted_tries=1)}


def test_opt_in_and_worker_start_required_for_scheduler_callback(monkeypatch):
    monkeypatch.delenv("SSDR_TASK_RESOURCE_METRICS", raising=False)
    metrics.begin(context())
    assert metrics.finish(context()) is None
    monkeypatch.setenv("SSDR_TASK_RESOURCE_METRICS", "true")
    assert metrics.finish(context()) == {"status": "unavailable", "reason": "no_worker_start_monitor"}


def test_callbacks_capture_terminal_resources_without_source_messages(monkeypatch, caplog):
    from src.observability import task_events
    calls = []
    monkeypatch.setattr(metrics, "begin", lambda ctx: calls.append("begin"))
    monkeypatch.setattr(metrics, "finish", lambda ctx: {"status": "observed", "samples": 2})
    with caplog.at_level("INFO"):
        task_events.on_task_start(context())
        task_events.on_task_success(context())
    assert calls == ["begin"]
    assert '"container_resources": {"status": "observed"' in caplog.text
    assert "SECRET" not in caplog.text


def test_short_tail_is_not_presented_as_an_instantaneous_cpu_peak():
    rows = [(0, snapshot()), (1, snapshot(2_000_000)), (1.001, snapshot(2_020_000))]
    result = metrics.summarize_samples(rows)
    assert result["sampled_peak_cpu_cores"] == 1
    assert result["cpu_interval_samples"] == 1
    assert result["minimum_cpu_sample_interval_seconds"] == 1


def test_monitor_stops_and_failed_reader_does_not_leak_details():
    def failed():
        raise OSError("PRIVATE_CREDENTIAL_IN_PATH")
    monitor = metrics.Monitor(read=failed, interval=100).start()
    result = monitor.finish()
    assert not monitor.thread.is_alive()
    assert result["status"] == "unavailable"
    assert result["sample_errors"] == 2
    assert "PRIVATE" not in str(result)
