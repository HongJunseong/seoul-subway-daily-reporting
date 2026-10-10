from __future__ import annotations
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from common import compute_dates, choose_usage_input, spark_command, DATE_ENV, DELTA_WRITE_POOL
from src.observability.task_events import on_task_start, on_task_success, on_task_failure, on_task_retry

with DAG(dag_id="usage_daily", start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
         schedule="0 6 * * *", catchup=False, max_active_runs=1,
         default_args={"retries": 2, "retry_delay": timedelta(minutes=2),
                       "on_execute_callback": on_task_start, "on_success_callback": on_task_success,
                       "on_retry_callback": on_task_retry, "on_failure_callback": on_task_failure},
         tags=["batch", "usage", "replay"]) as dag:
    dates = PythonOperator(task_id="compute_dates", python_callable=compute_dates)
    branch = BranchPythonOperator(task_id="choose_input", python_callable=choose_usage_input)
    bronze = BashOperator(pool=DELTA_WRITE_POOL, task_id="bronze_usage", env=DATE_ENV, append_env=True,
                          bash_command=spark_command("src/ingestion/ingest_subway_usage_daily.py",
                                                     '"$USAGE_YMD"'))
    silver = BashOperator(pool=DELTA_WRITE_POOL, task_id="silver_usage", env=DATE_ENV, append_env=True,
                          trigger_rule="none_failed_min_one_success",
                          bash_command=spark_command("src/processing/build_fact_subway_usage_daily_spark.py",
                                                     '"$USAGE_YMD"'))
    gold = BashOperator(pool=DELTA_WRITE_POOL, task_id="gold_usage", env=DATE_ENV, append_env=True,
                        bash_command=spark_command("src/processing/build_gold_subway_usage_daily_spark.py",
                                                   '"$USAGE_YMD"'))
    next_dag = TriggerDagRunOperator(task_id="trigger_gold_daily", trigger_dag_id="gold_daily",
                                    trigger_run_id="usage__{{ run_id }}", reset_dag_run=True,
                                    conf={"target_ymd": DATE_ENV["TARGET_YMD"]})
    dates >> branch >> [bronze, silver]
    bronze >> silver >> gold >> next_dag
