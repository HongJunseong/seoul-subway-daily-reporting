from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

PROJECT_DIR = "/opt/airflow"

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

DELTA_CONF = (
    '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
    '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
    '--conf "spark.sql.session.timeZone=Asia/Seoul" '
)

SPARK_SUBMIT = f'spark-submit --packages "{SPARK_PACKAGES}" {DELTA_CONF}'

with DAG(
    dag_id="pipeline_arrival_5m",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule="*/5 * * * *",   # 5분마다
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "silver", "gold", "arrival"],
) as dag:

    bronze_ingest = BashOperator(
        task_id="bronze_ingest_arrival",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        export PYTHONPATH="{PROJECT_DIR}:${{PYTHONPATH:-}}"

        {SPARK_SUBMIT} "{PROJECT_DIR}/src/ingestion/ingest_subway_arrival_realtime.py"
        """,
    )

    silver_fact = BashOperator(
        task_id="silver_fact_arrival",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        export PYTHONPATH="{PROJECT_DIR}:${{PYTHONPATH:-}}"

        {SPARK_SUBMIT} "{PROJECT_DIR}/src/processing/build_fact_subway_arrival_spark.py"
        """,
    )

    gold_15m = BashOperator(
        task_id="gold_arrival_metrics_15m",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        export PYTHONPATH="{PROJECT_DIR}:${{PYTHONPATH:-}}"

        {SPARK_SUBMIT} "{PROJECT_DIR}/src/processing/build_gold_arrival_metrics_15m_spark.py"
        """,
    )

    gold_qa = BashOperator(
    task_id="gold_arrival_delay_qa_notify",
    bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        export PYTHONPATH="{PROJECT_DIR}:${{PYTHONPATH:-}}"

        {SPARK_SUBMIT} "{PROJECT_DIR}/src/alerts/check_gold_arrival_delay_15m_and_notify.py" \
        --as_of "{{{{ data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S') }}}}"
        """,
    )

    bronze_ingest >> silver_fact >> gold_15m >> gold_qa
