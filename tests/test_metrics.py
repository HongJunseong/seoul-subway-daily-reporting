from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
import json
import threading
from types import SimpleNamespace
from urllib.request import urlopen

import pytest

from src.observability import metrics
from src.observability.task_events import task_event, emit
from scripts.serve_metrics import handler
from scripts.render_monitoring import render, fixture_job


def sample(start=None, attempt=1):
    start = start or datetime.now(timezone.utc) - timedelta(seconds=2)
    ti = SimpleNamespace(dag_id='arrival_collect_5m', task_id='bronze_ingest_arrival',
                         run_id='private-run-must-not-be-a-label', try_number=attempt, prev_attempted_tries=attempt,
                         start_date=start, end_date=start + timedelta(seconds=1))
    return {'task_instance': ti}


def publish(args):
    ctx, event, directory = args
    metrics.publish_task(ctx, event, directory)


@pytest.fixture
def directory(tmp_path, monkeypatch):
    monkeypatch.setenv('SSDR_DATA_KIND', 'synthetic')
    monkeypatch.setenv('SSDR_METRICS_DIR', str(tmp_path))
    return tmp_path


def test_success_failure_retry_are_deduplicated_and_old_callbacks_do_not_replace_current(directory):
    first = sample()
    event = task_event(first, 'succeeded')
    metrics.publish_task(first, event)
    second = sample(first['task_instance'].start_date + timedelta(seconds=10), 2)
    failed = task_event(second, 'failed')
    metrics.publish_task(second, failed)
    metrics.publish_task(second, failed)
    metrics.publish_task(first, event)
    text = metrics.render_metrics(directory, 'synthetic', [('arrival_collect_5m', 'bronze_ingest_arrival')])
    assert 'ssdr_task_observed_failures_total{dag_id="arrival_collect_5m",data_kind="synthetic",task_id="bronze_ingest_arrival"} 1' in text
    assert 'ssdr_task_observed_retries_total{dag_id="arrival_collect_5m",data_kind="synthetic",task_id="bronze_ingest_arrival"} 1' in text
    assert 'private-run' not in text and 'run_id' not in text
    row = json.loads(next((directory / 'synthetic').glob('task-*.json')).read_text())
    assert row['event'] == 'failed' and row['last_success_timestamp_seconds'] == first['task_instance'].end_date.timestamp()
    assert row['attempts_total'] == 2


def test_parallel_duplicate_callback_has_one_failed_attempt(directory):
    ctx = sample()
    event = task_event(ctx, 'failed')
    with ProcessPoolExecutor(3) as pool:
        list(pool.map(publish, [(ctx, event, str(directory))] * 8))
    row = json.loads(next((directory / 'synthetic').glob('task-*.json')).read_text())
    assert row['attempts_total'] == row['failures_total'] == 1


def test_invalid_task_record_does_not_export_false_success_or_zero_failures(directory):
    folder = directory / 'synthetic'
    folder.mkdir()
    (folder / 'task-invalid.json').write_text(json.dumps({'schema_version': 1, 'data_kind': 'synthetic',
        'dag_id': 'arrival_collect_5m', 'task_id': 'bronze_ingest_arrival', 'event': 'succeeded'}))
    text = metrics.render_metrics(directory, 'synthetic', [('arrival_collect_5m', 'bronze_ingest_arrival')])
    assert 'ssdr_task_last_attempt_failed' not in text
    assert 'ssdr_task_record_available{dag_id="arrival_collect_5m",data_kind="synthetic",task_id="bronze_ingest_arrival"} 0' in text
    assert 'ssdr_metrics_read_errors{data_kind="synthetic"} 1' in text


def quality():
    return {'data_kind': 'synthetic', 'status': 'degraded',
            'window': {'as_of_kst': '2026-02-01T00:10:00+09:00'},
            'arrival': {'latest_snapshot_kst': '2026-02-01T00:05:00+09:00',
                        'missing_time_buckets': 1, 'unprocessed_snapshot_count': None,
                        'median_source_age_seconds': -3, 'p95_source_age_seconds': -1}}


def test_data_age_keeps_historical_reference_and_negative_source_clock_difference(directory):
    metrics.publish_quality(quality())
    row = json.loads((directory / 'synthetic/quality.json').read_text())
    assert row['reference_timestamp_seconds'] - row['latest_snapshot_timestamp_seconds'] == 300
    text = metrics.render_metrics(directory, 'synthetic')
    assert 'ssdr_quality_median_source_age_seconds{data_kind="synthetic"} -3' in text
    assert 'ssdr_quality_unprocessed_snapshot_count' not in text
    assert 'ssdr_metrics_read_errors{data_kind="synthetic"} 0' in text


def test_absent_quality_is_not_healthy_zero_and_data_kinds_do_not_mix(directory):
    metrics.publish_quality(quality())
    text = metrics.render_metrics(directory, 'observed')
    assert 'ssdr_quality_record_available{data_kind="observed"} 0' in text
    assert 'ssdr_quality_failed' not in text


@pytest.mark.parametrize('broken', ['{', '{}', '{"schema_version":1,"data_kind":"synthetic","status":"unknown"}'])
def test_corrupt_quality_is_unavailable_and_reported(directory, broken):
    (directory / 'synthetic').mkdir()
    (directory / 'synthetic/quality.json').write_text(broken)
    text = metrics.render_metrics(directory, 'synthetic')
    assert 'ssdr_quality_record_available{data_kind="synthetic"} 0' in text
    assert 'ssdr_metrics_read_errors{data_kind="synthetic"} 1' in text


def test_optional_metrics_failure_does_not_mask_original_task_result(directory, monkeypatch, caplog):
    monkeypatch.setattr('src.observability.task_events.publish_task',
                        lambda *args: (_ for _ in ()).throw(OSError('SECRET-PATH')))
    emit(sample(), 'succeeded')
    assert 'task outcome preserved' in caplog.text
    assert 'SECRET-PATH' not in caplog.text


def test_exporter_http_readonly_endpoint_and_missing_task_state(directory):
    server = ThreadingHTTPServer(('127.0.0.1', 0), handler(directory, 'synthetic', [('a', 'b')]))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        text = urlopen(f'http://127.0.0.1:{server.server_port}/metrics', timeout=2).read().decode()
        assert 'ssdr_task_record_available{dag_id="a",data_kind="synthetic",task_id="b"} 0' in text
        assert not list(directory.iterdir())  # A scrape must never create or repair input.
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_worker_measurements_omit_unknown_fields_and_label_last_measurement(directory):
    ctx = sample()
    event = task_event(ctx, 'succeeded')
    event['container_resources'] = {'status': 'partial', 'memory_container_peak_bytes': 1024,
                                    'mean_cpu_cores': 0.2, 'sampled_peak_cpu_cores': None}
    metrics.publish_task(ctx, event)
    text = metrics.render_metrics(directory, 'synthetic')
    assert 'ssdr_worker_last_memory_peak_bytes' in text
    assert 'ssdr_worker_last_sampled_peak_cpu_cores' not in text
    assert 'ssdr_worker_last_measurement_complete' in text


def test_monitoring_manifest_has_no_host_ports_credentials_or_cluster_permissions():
    manifest = render('ssdr-monitoring-test')
    for item in manifest['items']:
        assert item['kind'] not in {'Secret', 'ClusterRole', 'ClusterRoleBinding', 'Role', 'RoleBinding'}
        if item['kind'] == 'Service':
            assert item['spec']['type'] == 'ClusterIP'
        if item['kind'] == 'Deployment':
            spec = item['spec']['template']['spec']
            assert spec['automountServiceAccountToken'] is False
            assert all('envFrom' not in c for c in spec['containers'])
    exporter = next(i for i in manifest['items'] if i['kind'] == 'Deployment' and i['metadata']['name'] == 'ssdr-metrics')
    assert exporter['spec']['template']['spec']['containers'][0]['volumeMounts'][0]['readOnly']


def test_reusing_airflow_claim_does_not_mutate_claim_or_lose_subdirectory():
    manifest = render('ssdr-fixture-test', claim='ssdr-fixture-logs', reuse_claim=True, subdir='ssdr-metrics')
    assert not any(i['kind'] == 'PersistentVolumeClaim' and i['metadata']['name'] == 'ssdr-fixture-logs' for i in manifest['items'])
    exporter = next(i for i in manifest['items'] if i['kind'] == 'Deployment' and i['metadata']['name'] == 'ssdr-metrics')
    assert '/metrics/ssdr-metrics' in exporter['spec']['template']['spec']['containers'][0]['args']


@pytest.mark.parametrize('namespace,subdir', [('default', ''), ('ssdr-test', '../private'), ('ssdr-test', '/tmp')])
def test_render_rejects_unrelated_namespace_or_escaped_directory(namespace, subdir):
    with pytest.raises(ValueError):
        render(namespace, subdir=subdir)

def test_real_callback_default_and_report_kind_share_audit_namespace(directory, monkeypatch):
    monkeypatch.delenv('SSDR_DATA_KIND')
    ctx = sample()
    metrics.publish_task(ctx, task_event(ctx, 'succeeded'))
    monkeypatch.setenv('SSDR_DATA_KIND', 'api_observations')
    ctx = sample()
    metrics.publish_task(ctx, task_event(ctx, 'succeeded'))
    row = quality()
    row['data_kind'] = 'observed'
    metrics.publish_quality(row)
    text = metrics.render_metrics(directory, 'observed', [('arrival_collect_5m', 'bronze_ingest_arrival')])
    assert 'ssdr_quality_record_available{data_kind="observed"} 1' in text
    assert 'ssdr_task_record_available{dag_id="arrival_collect_5m",data_kind="observed",task_id="bronze_ingest_arrival"} 1' in text
    assert not (directory / 'api_observations').exists()


def test_fixture_job_cannot_write_to_existing_airflow_namespace():
    with pytest.raises(ValueError):
        fixture_job('ssdr-fixture-20261011', 'recovery')
    job = fixture_job('ssdr-monitoring', 'recovery')
    spec = job['spec']['template']['spec']
    assert spec['automountServiceAccountToken'] is False
    container = spec['containers'][0]
    assert 'envFrom' not in container
    assert {'name': 'SSDR_DATA_KIND', 'value': 'synthetic'} in container['env']
    assert spec['volumes'][0]['persistentVolumeClaim']['claimName'] == 'ssdr-metrics'

def test_corrupt_previous_snapshot_cannot_break_success_callback(directory, caplog):
    ctx = sample()
    metrics.publish_task(ctx, task_event(ctx, 'succeeded'))
    path = next((directory / 'synthetic').glob('task-*.json'))
    path.write_text('[]')
    emit(ctx, 'succeeded')
    assert 'task outcome preserved' in caplog.text
    assert path.read_text() == '[]'


@pytest.mark.parametrize('corruption', ['oversized', 'resources'])
def test_invalid_snapshot_size_or_worker_shape_is_not_available(directory, corruption):
    ctx = sample()
    metrics.publish_task(ctx, task_event(ctx, 'succeeded'))
    path = next((directory / 'synthetic').glob('task-*.json'))
    if corruption == 'oversized':
        path.write_text(' ' * 65537)
    else:
        row = json.loads(path.read_text())
        row['resources'] = []
        path.write_text(json.dumps(row))
    text = metrics.render_metrics(directory, 'synthetic', [('arrival_collect_5m', 'bronze_ingest_arrival')])
    assert 'ssdr_task_record_available{dag_id="arrival_collect_5m",data_kind="synthetic",task_id="bronze_ingest_arrival"} 0' in text
    assert 'ssdr_metrics_read_errors{data_kind="synthetic"} 1' in text
