"""Optional, bounded task snapshots for a read-only Prometheus exporter."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
from itertools import islice
import json
import math
import os
from pathlib import Path
import re
import tempfile

KINDS = {'synthetic', 'observed'}
IDENTITY = re.compile(r'[A-Za-z0-9_.-]{1,100}\Z')


def metric_kind(value):
    # Report metadata calls real API input api_observations; audits call it observed.
    return 'observed' if value == 'api_observations' else value


def number(value, signed=False):
    return type(value) in (int, float) and math.isfinite(value) and (signed or value >= 0)


def timestamp(value):
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        raise ValueError('metric timestamp requires a timezone')
    return stamp.timestamp()


def read_snapshot(path):
    with Path(path).open('rb') as stream:
        content = stream.read(65537)
    if len(content) > 65536:
        raise ValueError('metric snapshot exceeds 64 KiB')
    row = json.loads(content)
    if not isinstance(row, dict):
        raise ValueError('metric snapshot must be an object')
    return row


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, prefix='.metric-', delete=False,
                                         encoding='utf-8') as stream:
            temporary = Path(stream.name)
            json.dump(value, stream, allow_nan=False, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.chmod(0o644)
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


@contextmanager
def state_lock(path):
    import fcntl  # Runtime is Linux; keep ordinary imports usable on other platforms.
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix('.lock').open('a') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def publish_task(context, event, directory=None):
    directory = directory or os.getenv('SSDR_METRICS_DIR')
    if not directory:
        return
    kind = metric_kind(os.getenv('SSDR_DATA_KIND', 'observed'))
    dag, task = event['dag_id'], event['task_id']
    ti = context['task_instance']
    start = ti.start_date
    if start is None or kind not in KINDS or not IDENTITY.fullmatch(dag) or not IDENTITY.fullmatch(task):
        raise ValueError('invalid task metric identity')
    start = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start
    attempt = event['try_number']
    if type(attempt) is not int or attempt < 1 or event['event'] not in {'started', 'succeeded', 'failed'}:
        raise ValueError('invalid task metric attempt')
    key = [start.timestamp(), attempt]
    path = Path(directory) / kind / ('task-' + hashlib.sha256(f'{dag}/{task}'.encode()).hexdigest()[:24] + '.json')
    with state_lock(path):
        old = read_snapshot(path) if path.exists() else {}
        if old and (old.get('schema_version') != 1 or old.get('data_kind') != kind
                    or old.get('dag_id') != dag or old.get('task_id') != task):
            raise ValueError('invalid prior task metric identity')
        previous_key = old.get('attempt_key', [-1, -1])
        if key < previous_key or (key == previous_key and (
                old['event'] == event['event'] or (old['event'] != 'started' and event['event'] == 'started'))):
            return
        if key == previous_key and old['event'] != 'started' and event['event'] != 'started':
            raise ValueError('conflicting task terminal observations')
        fresh = key != previous_key
        value = dict(old, schema_version=1, data_kind=kind, dag_id=dag, task_id=task, attempt_key=key,
                     event=event['event'], updated_timestamp_seconds=timestamp(event['observed_at_utc']),
                     attempts_total=old.get('attempts_total', 0) + int(fresh),
                     retries_total=old.get('retries_total', 0) + int(fresh and attempt > 1),
                     failures_total=old.get('failures_total', 0) + int(event['event'] == 'failed'))
        if event['event'] == 'succeeded':
            end = getattr(ti, 'end_date', None)
            end = (end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end) if end else None
            value['last_success_timestamp_seconds'] = end.timestamp() if end else timestamp(event['observed_at_utc'])
        if event['duration_seconds'] is not None:
            value['last_duration_seconds'] = event['duration_seconds']
        resources = event.get('container_resources', {})
        if resources.get('status') in {'observed', 'partial'}:
            value['resources'] = {key: resources.get(key) for key in (
                'memory_container_peak_bytes', 'mean_cpu_cores', 'sampled_peak_cpu_cores')}
            value['resources_complete'] = int(resources['status'] == 'observed')
        atomic_json(path, value)


def publish_quality(quality, directory=None):
    directory = directory or os.getenv('SSDR_METRICS_DIR')
    if not directory:
        return
    kind = metric_kind(quality['data_kind'])
    if kind not in KINDS or quality['status'] not in {'healthy', 'degraded', 'failed'}:
        raise ValueError('invalid quality metric identity')
    arrival = quality['arrival']
    if not isinstance(arrival, dict):
        raise ValueError('invalid quality metric input')
    value = {'schema_version': 1, 'data_kind': kind, 'status': quality['status'],
             'evaluated_timestamp_seconds': datetime.now(timezone.utc).timestamp(),
             'reference_timestamp_seconds': timestamp(quality['window']['as_of_kst']),
             'latest_snapshot_timestamp_seconds': timestamp(arrival['latest_snapshot_kst'])
                if arrival.get('latest_snapshot_kst') else None}
    for key in ('missing_time_buckets', 'unprocessed_snapshot_count', 'response_at_limit_snapshots',
                'median_source_age_seconds', 'p95_source_age_seconds'):
        value[key] = arrival.get(key)
    atomic_json(Path(directory) / kind / 'quality.json', value)


def render_metrics(directory, kind, expected_tasks=()):
    if kind not in KINDS:
        raise ValueError('invalid metric data kind')
    lines, errors, found = [], 0, set()

    def metric(name, value, labels=None, counter=False, signed=False):
        nonlocal errors
        if value is None:
            return
        if not number(value, signed=signed):
            errors += 1
            return
        labels = dict(labels or {}, data_kind=kind)
        encoded = ','.join(f'{key}={json.dumps(str(value))}' for key, value in sorted(labels.items()))
        if not any(line.startswith(f'# TYPE {name} ') for line in lines):
            lines.append(f'# TYPE {name} ' + ('counter' if counter else 'gauge'))
        lines.append(f'{name}{{{encoded}}} {value}')

    folder = Path(directory) / kind
    files = list(islice(folder.glob('task-*.json'), 101))
    if len(files) > 100:
        raise ValueError('more than 100 stable task identities configured')
    for path in sorted(files):
        try:
            row = read_snapshot(path)
            if row['schema_version'] != 1 or row['data_kind'] != kind:
                raise ValueError('invalid task schema')
            dag, task = row['dag_id'], row['task_id']
            if not IDENTITY.fullmatch(dag) or not IDENTITY.fullmatch(task) or row['event'] not in {'started', 'succeeded', 'failed'}:
                raise ValueError('invalid task identity')
            if (dag, task) in found or not all(number(row[key]) for key in ('attempts_total', 'failures_total', 'retries_total')):
                raise ValueError('invalid or duplicate task counter')
            if not isinstance(row.get('resources', {}), dict):
                raise ValueError('invalid worker metric object')
            labels = {'dag_id': dag, 'task_id': task}
            found.add((dag, task))
            metric('ssdr_task_last_attempt_failed', int(row['event'] == 'failed'), labels)
            metric('ssdr_task_success_record_available', int(row.get('last_success_timestamp_seconds') is not None), labels)
            for key in ('attempts_total', 'failures_total', 'retries_total'):
                metric('ssdr_task_observed_' + key, row[key], labels, counter=True)
            for key in ('last_success_timestamp_seconds', 'last_duration_seconds', 'updated_timestamp_seconds'):
                metric('ssdr_task_' + key, row.get(key), labels)
            for key, name in [('memory_container_peak_bytes', 'last_memory_peak_bytes'),
                              ('mean_cpu_cores', 'last_mean_cpu_cores'), ('sampled_peak_cpu_cores', 'last_sampled_peak_cpu_cores')]:
                metric('ssdr_worker_' + name, row.get('resources', {}).get(key), labels)
            metric('ssdr_worker_last_measurement_complete', row.get('resources_complete'), labels)
        except (ValueError, KeyError, TypeError, OSError):
            errors += 1
    for dag, task in expected_tasks:
        metric('ssdr_task_record_available', int((dag, task) in found), {'dag_id': dag, 'task_id': task})
    quality_file = folder / 'quality.json'
    quality_available = False
    if quality_file.exists():
        try:
            row = read_snapshot(quality_file)
            if (row['schema_version'] != 1 or row['data_kind'] != kind or row['status'] not in {'healthy', 'degraded', 'failed'}
                    or not all(number(row[key]) for key in ('evaluated_timestamp_seconds', 'reference_timestamp_seconds'))):
                raise ValueError('invalid quality schema')
            for key in ('evaluated_timestamp_seconds', 'reference_timestamp_seconds', 'latest_snapshot_timestamp_seconds',
                        'missing_time_buckets', 'unprocessed_snapshot_count', 'response_at_limit_snapshots',
                        'median_source_age_seconds', 'p95_source_age_seconds'):
                metric('ssdr_quality_' + key, row.get(key), signed=key.endswith('source_age_seconds'))
            metric('ssdr_quality_failed', int(row['status'] == 'failed'))
            quality_available = True
        except (ValueError, KeyError, TypeError, OSError):
            errors += 1
    metric('ssdr_quality_record_available', int(quality_available))
    metric('ssdr_metrics_read_errors', errors)
    return '\n'.join(lines) + '\n'
