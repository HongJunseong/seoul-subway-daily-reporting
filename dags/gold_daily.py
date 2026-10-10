from __future__ import annotations
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from common import compute_dates, spark_command, DATE_ENV, DELTA_WRITE_POOL
from src.alerts.notify_pipeline import notify_failure, notify_gold_ready

with DAG(dag_id="gold_daily", start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
         schedule=None, catchup=False, max_active_runs=1,
         default_args={"retries": 2, "retry_delay": timedelta(minutes=2),
                       "on_failure_callback": notify_failure},
         tags=["batch", "gold"]) as dag:
    dates = PythonOperator(task_id="compute_dates", python_callable=compute_dates)
    silver = BashOperator(pool=DELTA_WRITE_POOL, task_id="silver_arrival_day", env=DATE_ENV, append_env=True,
                          bash_command=spark_command("src/processing/build_fact_subway_arrival_spark.py",
                                                     '--day "$ARRIVAL_YMD"'))
    gold = BashOperator(pool=DELTA_WRITE_POOL, task_id="gold_arrival", env=DATE_ENV, append_env=True,
                        bash_command=spark_command("src/processing/build_gold_subway_arrival_spark.py",
                                                   '"$ARRIVAL_YMD"'))
    notify = PythonOperator(task_id="notify_gold_ready", python_callable=notify_gold_ready)
    report = TriggerDagRunOperator(task_id="trigger_report_daily", trigger_dag_id="report_daily",
                                   trigger_run_id="gold__{{ run_id }}", reset_dag_run=True,
                                   conf={"target_ymd": DATE_ENV["TARGET_YMD"]})
    dates >> silver >> gold >> [notify, report]
