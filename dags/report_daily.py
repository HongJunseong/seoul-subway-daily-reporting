from __future__ import annotations
from datetime import timedelta
import pendulum
from airflow import DAG
from pod_profiles import task_executor_config
from airflow.operators.python import PythonOperator
from common import compute_target_ymd
from src.observability.task_events import on_task_start, on_task_success, on_task_failure, on_task_retry


def run_generate_report(**context):
    from src.report.generate_daily_report import generate_daily_report
    target = context["ti"].xcom_pull(task_ids="compute_target_ymd")
    return str(generate_daily_report(target, save_to_s3=True))


with DAG(dag_id="report_daily", start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
         schedule=None, catchup=False, max_active_runs=1,
         dagrun_timeout=timedelta(minutes=20),
         default_args={"execution_timeout": timedelta(minutes=1), "executor_config": task_executor_config("control"), "retries": 2, "retry_delay": timedelta(minutes=2),
                       "on_execute_callback": on_task_start, "on_success_callback": on_task_success,
                       "on_retry_callback": on_task_retry, "on_failure_callback": on_task_failure},
         tags=["report", "validated"]) as dag:
    target = PythonOperator(task_id="compute_target_ymd", python_callable=compute_target_ymd)
    report = PythonOperator(execution_timeout=timedelta(minutes=5), executor_config=task_executor_config("report"), task_id="generate_report", python_callable=run_generate_report)
    target >> report
