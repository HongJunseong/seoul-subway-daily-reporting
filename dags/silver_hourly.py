from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

PROJECT_DIR = "/opt/airflow/seoul-subway-daily-reporting"

# S3A 읽기용 (네가 전에 쓰던 조합)
SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

with DAG(
    dag_id="silver_hourly",
    description="Build Silver fact (arrival + position) from Bronze",
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    schedule=None,  # ✅ 필요할 때만 실행
    catchup=False,
    max_active_runs=1,
    tags=["silver"],
) as dag:

    fact_arrival = BashOperator(
        task_id="fact_arrival",
        bash_command=f"""
        set -euo pipefail
        cd {PROJECT_DIR}
        spark-submit --packages "{SPARK_PACKAGES}" src/processing/build_fact_subway_arrival_spark.py
        """,
    )

    fact_position = BashOperator(
        task_id="fact_position",
        bash_command=f"""
        set -euo pipefail
        cd {PROJECT_DIR}
        spark-submit --packages "{SPARK_PACKAGES}" src/processing/build_fact_subway_position_spark.py
        """,
    )

    fact_arrival >> fact_position
