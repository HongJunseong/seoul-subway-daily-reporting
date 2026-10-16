"""Verify a timed-out Bash child stops and the next local Airflow run succeeds."""
from __future__ import annotations
import argparse
from datetime import timedelta
import json
import os
from pathlib import Path
import shlex
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def run(output):
    output = Path(output).resolve()
    if not output.is_relative_to(ROOT / '.work'):
        raise ValueError('timeout experiment must use private .work')
    output.mkdir(parents=True, exist_ok=False)
    os.environ.update(AIRFLOW_HOME=str(output / 'airflow'), AIRFLOW_CONFIG=str(output / 'airflow/airflow.cfg'),
                      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{output / 'airflow/airflow.db'}",
                      AIRFLOW__CORE__LOAD_EXAMPLES='false', AIRFLOW__CORE__UNIT_TEST_MODE='true',
                      AIRFLOW__CORE__EXECUTOR='SequentialExecutor', AIRFLOW__CORE__FERNET_KEY='')
    import pendulum
    from airflow import DAG
    from airflow.exceptions import AirflowTaskTimeout
    from airflow.models import TaskInstance, Pool
    from airflow.operators.bash import BashOperator
    from airflow.utils import db
    from airflow.utils.session import create_session

    db.initdb()
    with create_session() as session:
        session.add(Pool(pool='ssdr_delta_writer', slots=1, include_deferred=False, description='timeout fixture'))
    pidfile, forbidden, recovered = (output / name for name in ('child.pid', 'must-not-exist', 'recovered'))
    code = (f'from pathlib import Path; import os,time; Path({str(pidfile)!r}).write_text(str(os.getpid())); '
            f'time.sleep(60); Path({str(forbidden)!r}).touch()')
    with DAG('timeout_fixture', start_date=pendulum.datetime(2026, 1, 1, tz='UTC'), schedule=None) as dag:
        task = BashOperator(task_id='bounded_worker', pool='ssdr_delta_writer', retries=0,
                            execution_timeout=timedelta(seconds=1),
                            bash_command=f'{shlex.quote(sys.executable)} -c {shlex.quote(code)}')

    def instance(run_id, minute):
        stamp = pendulum.datetime(2026, 1, 1, 0, minute, tz='UTC')
        with create_session() as session:
            dag.create_dagrun(run_id=run_id, run_type='manual', execution_date=stamp,
                              data_interval=(stamp, stamp), state='running', session=session)
        return TaskInstance(task, run_id=run_id)

    failed = instance('manual__timeout', 0)
    started = time.monotonic()
    try:
        failed.run()
    except AirflowTaskTimeout:
        pass
    else:
        raise AssertionError('hanging child did not time out')
    elapsed = time.monotonic() - started
    failed.refresh_from_db()
    assert failed.state == 'failed' and not forbidden.exists()
    pid = int(pidfile.read_text())
    state = Path(f'/proc/{pid}/stat')
    for _ in range(20):
        try:
            stopped = state.read_text().split()[2] == 'Z'
        except FileNotFoundError:
            stopped = True
        if stopped:
            break
        time.sleep(0.05)
    else:
        raise AssertionError('timed-out Bash child is still running')
    task.bash_command = f'touch {shlex.quote(str(recovered))}'
    following = instance('manual__next_run', 5)
    following.run()
    following.refresh_from_db()
    assert following.state == 'success' and recovered.exists() and not forbidden.exists()
    evidence = {'data_kind': 'synthetic', 'timed_out_state': failed.state,
                'child_stopped': True, 'late_output_absent': True, 'next_run_state': following.state,
                'elapsed_seconds': round(elapsed, 3),
                'scope': 'actual local TaskInstance.run and Bash child; no scheduler, Kubernetes or S3 execution'}
    (output / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir)))


if __name__ == '__main__':
    main()
