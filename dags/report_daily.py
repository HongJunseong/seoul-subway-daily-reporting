from __future__ import annotations

import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

PROJECT_ROOT = Path("/opt/airflow")


def compute_target_ymd(**context) -> str:
    """
    - 기본: logical_date를 KST로 변환해서 YYYYMMDD
    - override: Trigger 시 conf로 {"target_ymd":"20251216"} 주면 그 값 사용
    """
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("target_ymd"):
        return str(dag_run.conf["target_ymd"])

    logical_date = context["logical_date"]  # UTC 기반 pendulum
    kst_dt = logical_date.in_timezone("Asia/Seoul")
    return kst_dt.format("YYYYMMDD")


def run_generate_report(**context):
    #if str(PROJECT_ROOT) not in sys.path:
    #    sys.path.insert(0, str(PROJECT_ROOT))

    target_ymd = context["ti"].xcom_pull(task_ids="compute_target_ymd")

    from src.report.generate_daily_report import generate_daily_report  # type: ignore

    out_path = generate_daily_report(target_ymd=target_ymd, save_to_s3=True)
    print(f"[OK] report generated: {out_path}")


with DAG(
    dag_id="report_daily",
    description="Generate daily LLM report from GOLD (arrival + position) and upload to S3",
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    schedule=None,   # test setting
    catchup=False,
    max_active_runs=1,
    tags=["report", "llm"],
) as dag:

    t_target = PythonOperator(
        task_id="compute_target_ymd",
        python_callable=compute_target_ymd,
    )

    t_report = PythonOperator(
        task_id="generate_report",
        python_callable=run_generate_report,
    )

    t_target >> t_report
