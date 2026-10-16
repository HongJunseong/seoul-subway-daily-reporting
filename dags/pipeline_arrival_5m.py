from __future__ import annotations
from datetime import timedelta
import pendulum
from airflow import DAG
from pod_profiles import task_executor_config
from airflow.operators.bash import BashOperator
from common import runtime_schedule, spark_command, AS_OF_ENV, DELTA_WRITE_POOL
from src.observability.task_events import on_task_start, on_task_success, on_task_failure, on_task_retry

with DAG(dag_id="pipeline_arrival_5m", start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
         schedule=runtime_schedule("*/5 * * * *"), catchup=False, max_active_runs=1,
         dagrun_timeout=timedelta(minutes=15),
         default_args={"execution_timeout": timedelta(minutes=5), "executor_config": task_executor_config("control"), "retries": 2, "retry_delay": timedelta(minutes=1),
                       "on_execute_callback": on_task_start, "on_success_callback": on_task_success,
                       "on_retry_callback": on_task_retry, "on_failure_callback": on_task_failure},
         tags=["arrival", "processing", "replay"]) as dag:
    silver = BashOperator(executor_config=task_executor_config("spark"), pool=DELTA_WRITE_POOL, task_id="silver_fact_arrival", env=AS_OF_ENV, append_env=True,
                          bash_command=spark_command("src/processing/build_fact_subway_arrival_spark.py",
                                                     '--as_of "$AS_OF"'))
    gold = BashOperator(executor_config=task_executor_config("spark"), pool=DELTA_WRITE_POOL, task_id="gold_arrival_metrics_15m", env=AS_OF_ENV, append_env=True,
                        bash_command=spark_command("src/processing/build_gold_arrival_metrics_15m_spark.py",
                                                   '--as_of "$AS_OF"'))
    qa = BashOperator(executor_config=task_executor_config("spark"), pool=DELTA_WRITE_POOL, task_id="gold_arrival_eta_qa_notify", env=AS_OF_ENV, append_env=True,
                      bash_command=spark_command("src/alerts/notify_arrival_eta_15m.py",
                                                 '--as_of "$AS_OF"'))
    silver >> gold >> qa
