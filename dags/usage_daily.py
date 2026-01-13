from __future__ import annotations

import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum

PROJECT_ROOT = Path("/opt/airflow/seoul-subway-daily-reporting")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

PROJECT_DIR = "/opt/airflow/seoul-subway-daily-reporting"

USAGE_LAG_DAYS = 4

SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

# KST 기준 D-4 날짜를 Jinja로 계산
TARGET_YMD = "{{ (logical_date.in_timezone('Asia/Seoul') - macros.timedelta(days=4)).strftime('%Y%m%d') }}"

def run_bronze_usage(**context):
    # target_ymd를 Airflow 템플릿으로 계산한 값으로 강제
    logical_date = context["logical_date"].in_timezone("Asia/Seoul")
    target_ymd = logical_date.subtract(days=USAGE_LAG_DAYS).format("YYYYMMDD")

    from src.ingestion import ingest_subway_usage_daily as mod  # type: ignore

    # Airflow argv 오염 방지: 날짜로 강제
    sys.argv = ["ingest_subway_usage_daily.py", target_ymd]
    mod.main()

with DAG(
    dag_id="usage_daily",
    description="usage daily pipeline: Bronze -> Silver -> Gold (D-4)",
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    schedule=None,  # test setting
    catchup=False,
    max_active_runs=1,
    tags=["usage", "bronze", "silver", "gold"],
) as dag:

    bronze = PythonOperator(
        task_id="bronze_usage",
        python_callable=run_bronze_usage,
    )

    silver = BashOperator(
        task_id="silver_usage",
        bash_command=f"""
        set -euo pipefail
        cd {PROJECT_DIR}
        spark-submit --packages "{SPARK_PACKAGES}" src/processing/build_fact_subway_usage_daily_spark.py "{TARGET_YMD}"
        """,
    )

    gold = BashOperator(
        task_id="gold_usage",
        bash_command=f"""
        set -euo pipefail
        cd {PROJECT_DIR}
        spark-submit --packages "{SPARK_PACKAGES}" src/processing/build_gold_subway_usage_daily_spark.py "{TARGET_YMD}"
        """,
    )

    bronze >> silver >> gold