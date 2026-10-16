from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile

from src.observability.health import pipeline_health


def quality(*codes):
    return {'data_kind': 'synthetic', 'issues': [{'code': code, 'severity': severity} for code, severity in codes]}


def test_missing_observation_and_unprocessed_bronze_have_different_recovery_paths():
    source = quality(('arrival_unobserved_time_buckets', 'warning'), ('arrival_unprocessed_snapshots', 'warning'))
    before = deepcopy(source)
    result = pipeline_health(source)
    assert result['status'] == 'degraded'
    assert [d['category'] for d in result['diagnostics']] == ['collection', 'processing']
    assert 'API 실패를 단정' in result['diagnostics'][0]['action']
    assert 'API 재수집은 필요하지 않다' in result['diagnostics'][1]['action']
    assert source == before


def test_storage_or_quality_errors_prevent_healthy_outcome():
    result = pipeline_health(quality(('source_missing:bronze/arrival', 'error'), ('usage_silver_contract', 'error')))
    assert result['status'] == 'failed'
    assert [d['category'] for d in result['diagnostics']] == ['storage', 'quality']


def test_historical_staleness_and_source_time_are_distinct():
    result = pipeline_health(quality(('arrival_stale', 'warning'), ('arrival_source_clock_ahead', 'warning')))
    assert [d['category'] for d in result['diagnostics']] == ['freshness', 'source_time']
    assert '과거 구간' in result['diagnostics'][0]['action']


def test_failed_attempt_then_success_does_not_claim_final_task_failure():
    tasks = {'malformed_events': 0, 'unique_events': 4,
             'tasks': [{'failed': 1, 'succeeded': 1, 'incomplete': 0, 'conflicting': 0}]}
    result = pipeline_health(quality(), tasks)
    assert result['status'] == 'degraded'
    assert result['diagnostics'][0]['code'] == 'task_attempts_failed'
    assert '현재 장애로 단정하지 않는다' in result['diagnostics'][0]['action']
    assert pipeline_health(quality())['tasks'] is None


def test_empty_logs_and_unconfirmed_campaign_are_explicit_unknowns():
    tasks = {'malformed_events': 0, 'unique_events': 0, 'tasks': []}
    campaign = {'totals': {'unconfirmed_due_slots': 3, 'failed_request_slots': 0,
                           'missed_start_window_slots': 0, 'missed_capture_schedule_slots': 0}}
    result = pipeline_health(quality(), tasks, campaign)
    assert {d['code'] for d in result['diagnostics']} == {'task_logs_unusable', 'unconfirmed_due_slots'}
    assert '호출 0회로 간주' in result['diagnostics'][1]['action']


def test_direct_cli_missing_lake_exits_failed_and_preserves_previous_output(tmp_path):
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix='health-cli-', dir=root / '.work') as output:
        destination = Path(output) / 'check'
        command = [sys.executable, 'scripts/check_pipeline.py', '--local-root', str(tmp_path / 'missing'),
                   '--usage-day', '20260128', '--start', '2026-01-31 23:50:00',
                   '--end', '2026-02-01 00:00:00', '--as-of', '2026-02-01 00:00:00',
                   '--data-kind', 'synthetic', '--output-dir', str(destination)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=20)
        assert result.returncode == 2, result.stderr
        evidence = json.loads((destination / 'health.json').read_text())
        assert evidence['status'] == 'failed' and evidence['tasks'] is None
        assert len([d for d in evidence['diagnostics'] if d['category'] == 'storage']) == 5
        saved = (destination / 'health.json').read_bytes()
        again = subprocess.run(command, capture_output=True, text=True, timeout=20)
        assert again.returncode != 0
        assert (destination / 'health.json').read_bytes() == saved
