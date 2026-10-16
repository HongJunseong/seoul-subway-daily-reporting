from __future__ import annotations
from datetime import timedelta
import pendulum
from airflow import DAG
from pod_profiles import task_executor_config
from airflow.operators.bash import BashOperator
from common import runtime_schedule, spark_command, DELTA_WRITE_POOL
from src.observability.task_events import on_task_start, on_task_success, on_task_failure, on_task_retry

with DAG(dag_id="arrival_collect_5m", start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
         schedule=runtime_schedule("*/5 * * * *"), catchup=False, max_active_runs=1,
         dagrun_timeout=timedelta(minutes=5),
         default_args={"execution_timeout": timedelta(minutes=2), "executor_config": task_executor_config("control"), "retries": 1, "retry_delay": timedelta(seconds=30),
                       "on_execute_callback": on_task_start, "on_success_callback": on_task_success,
                       "on_retry_callback": on_task_retry, "on_failure_callback": on_task_failure},
         tags=["arrival", "collection"]) as dag:
    collect = BashOperator(executor_config=task_executor_config("spark"), pool=DELTA_WRITE_POOL, task_id="bronze_ingest_arrival",
                           bash_command=spark_command("src/ingestion/ingest_subway_arrival_realtime.py"))
