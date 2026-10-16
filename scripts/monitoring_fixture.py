"""Synthetic metric inputs from real local TaskInstance callbacks; no API or Delta execution."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import shlex
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'dags')]


def run(phase, output):
    if os.getenv('SSDR_DATA_KIND') != 'synthetic' or os.getenv('SSDR_FIXTURE_MODE') != 'true':
        raise ValueError('metric fixture requires explicit synthetic mode')
    if not os.getenv('SSDR_METRICS_DIR'):
        raise ValueError('metric fixture requires its isolated metrics directory')
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    os.environ.update(AIRFLOW_HOME=str(output / 'airflow'), AIRFLOW_CONFIG=str(output / 'airflow/airflow.cfg'),
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{output / 'airflow/airflow.db'}",
        AIRFLOW__CORE__LOAD_EXAMPLES='false', AIRFLOW__CORE__UNIT_TEST_MODE='true',
        AIRFLOW__CORE__EXECUTOR='SequentialExecutor', AIRFLOW__CORE__FERNET_KEY='',
        SLACK_WEBHOOK_URL='', OPENAI_API_KEY='', SSDR_TASK_RESOURCE_METRICS='true')
    import pendulum
    from airflow import DAG
    from airflow.models import TaskInstance, Pool
    from airflow.operators.bash import BashOperator
    from airflow.exceptions import AirflowException
    from airflow.utils import db
    from airflow.utils.session import create_session
    from src.observability.task_events import on_task_start, on_task_success, on_task_retry, on_task_failure
    from src.observability.metrics import publish_quality

    db.initdb()
    with create_session() as session:
        session.add(Pool(pool='ssdr_delta_writer', slots=1, include_deferred=False, description='metric fixture'))
    states = []
    for dag_id, task_id in [('arrival_collect_5m', 'bronze_ingest_arrival'),
                            ('pipeline_arrival_5m', 'gold_arrival_metrics_15m')]:
        stamp = pendulum.now('UTC')
        with DAG(dag_id, start_date=stamp.subtract(days=1), schedule=None) as dag:
            task = BashOperator(task_id=task_id, pool='ssdr_delta_writer', retries=1,
                retry_delay=timedelta(0), on_execute_callback=on_task_start, on_success_callback=on_task_success,
                on_retry_callback=on_task_retry, on_failure_callback=on_task_failure,
                execution_timeout=timedelta(seconds=30),
                bash_command='python -c ' + shlex.quote('import time; sum(i*i for i in range(250000)); time.sleep(1.2)'))
        run_id = 'manual__metrics_' + stamp.strftime('%Y%m%dT%H%M%S%f')
        with create_session() as session:
            dag.create_dagrun(run_id=run_id, run_type='manual', state='running',
                              execution_date=stamp, data_interval=(stamp, stamp), session=session)
        ti = TaskInstance(task, run_id=run_id)
        should_fail = dag_id == 'arrival_collect_5m' and phase in {'failure', 'recovery'}
        success_command = task.bash_command
        if should_fail:
            task.bash_command = 'exit 1'
        try:
            ti.run(ignore_all_deps=True)
        except AirflowException:
            if not should_fail:
                raise
        ti.refresh_from_db()
        if should_fail and ti.state != 'up_for_retry':
            raise AssertionError('fixture failed attempt was not recorded')
        if phase == 'recovery' and should_fail:
            task.bash_command = success_command
            ti.task = task  # Airflow copies the operator while executing an attempt.
            ti.run(ignore_all_deps=True, ignore_ti_state=True)
            ti.refresh_from_db()
            assert ti.state == 'success' and ti.prev_attempted_tries == 2
        states.append({'dag_id': dag_id, 'task_id': task_id, 'state': ti.state,
                       'executed_attempts': ti.prev_attempted_tries})
    now = datetime.now(timezone.utc)
    publish_quality({'data_kind': 'synthetic', 'status': 'healthy',
        'window': {'as_of_kst': now.isoformat()},
        'arrival': {'latest_snapshot_kst': now.isoformat(), 'missing_time_buckets': 0,
                    'unprocessed_snapshot_count': int(phase == 'failure'), 'response_at_limit_snapshots': 0,
                    'median_source_age_seconds': 10, 'p95_source_age_seconds': 30}})
    records = [json.loads(p.read_text()) for p in (Path(os.environ['SSDR_METRICS_DIR']) / 'synthetic').glob('task-*.json')]
    assert len(records) == 2
    for row in records:
        expected = 'failed' if phase == 'failure' and row['dag_id'] == 'arrival_collect_5m' else 'succeeded'
        assert row['event'] == expected
        if phase == 'recovery' and row['dag_id'] == 'arrival_collect_5m':
            assert row['retries_total'] >= 1
    result = {'data_kind': 'synthetic', 'phase': phase, 'tasks': states,
              'scope': 'actual TaskInstance callbacks and worker cgroup; generated quality fixture; no source collection or Delta transforms'}
    (output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase', required=True, choices=['baseline', 'failure', 'recovery'])
    parser.add_argument('--output-dir', required=True, type=Path)
    args = parser.parse_args()
    run(args.phase, args.output_dir)


if __name__ == '__main__':
    main()
