from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from common import compute_target_ymd


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
