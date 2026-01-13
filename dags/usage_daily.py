from __future__ import annotations

import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

PROJECT_ROOT = Path("/opt/airflow/seoul-subway-daily-reporting")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

PROJECT_DIR = "/opt/airflow/seoul-subway-daily-reporting"

USAGE_LAG_DAYS = 4

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

# KST 기준 D-4 날짜를 Jinja로 계산
TARGET_YMD = "{{ (logical_date.in_timezone('Asia/Seoul') - macros.timedelta(days=4)).strftime('%Y%m%d') }}"

with DAG(
    dag_id="usage_daily",
    description="usage daily pipeline: Bronze -> Silver -> Gold (D-4)",
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    schedule=None,  # test setting
    catchup=False,
    max_active_runs=1,
    tags=["usage", "bronze", "silver", "gold"],
) as dag:

    # Bronze spark-submit
    bronze = BashOperator(
        task_id="bronze_usage",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        export PYTHONPATH="{PROJECT_DIR}:${{PYTHONPATH:-}}"

        {SPARK_SUBMIT} "{PROJECT_DIR}/src/ingestion/ingest_subway_usage_daily.py" "{TARGET_YMD}"
        """,
    )

    # Silver spark-submit
    silver = BashOperator(
    task_id="silver_usage",
    bash_command=f"""
    set -euo pipefail
    cd "{PROJECT_DIR}"
    export PYTHONPATH="{PROJECT_DIR}:${{PYTHONPATH:-}}"

    {SPARK_SUBMIT} "{PROJECT_DIR}/src/processing/build_fact_subway_usage_daily_spark.py" "{TARGET_YMD}"
    """,
    )

    # Gold spark-submit
    gold = BashOperator(
        task_id="gold_usage",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        export PYTHONPATH="{PROJECT_DIR}:${{PYTHONPATH:-}}"

        {SPARK_SUBMIT} "{PROJECT_DIR}/src/processing/build_gold_subway_usage_daily_spark.py" "{TARGET_YMD}"
        """,
    )

    bronze >> silver >> gold
