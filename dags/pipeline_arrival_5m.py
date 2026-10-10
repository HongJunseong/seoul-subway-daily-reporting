from __future__ import annotations
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from common import spark_command, AS_OF_ENV, DELTA_WRITE_POOL
from src.alerts.notify_pipeline import notify_failure

with DAG(dag_id="pipeline_arrival_5m", start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
         schedule="*/5 * * * *", catchup=False, max_active_runs=1,
         default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
                       "on_failure_callback": notify_failure},
         tags=["arrival", "processing", "replay"]) as dag:
    silver = BashOperator(pool=DELTA_WRITE_POOL, task_id="silver_fact_arrival", env=AS_OF_ENV, append_env=True,
                          bash_command=spark_command("src/processing/build_fact_subway_arrival_spark.py",
                                                     '--as_of "$AS_OF"'))
    gold = BashOperator(pool=DELTA_WRITE_POOL, task_id="gold_arrival_metrics_15m", env=AS_OF_ENV, append_env=True,
                        bash_command=spark_command("src/processing/build_gold_arrival_metrics_15m_spark.py",
                                                   '--as_of "$AS_OF"'))
    qa = BashOperator(pool=DELTA_WRITE_POOL, task_id="gold_arrival_eta_qa_notify", env=AS_OF_ENV, append_env=True,
                      bash_command=spark_command("src/alerts/check_gold_arrival_delay_15m_and_notify.py",
                                                 '--as_of "$AS_OF"'))
    silver >> gold >> qa
