from __future__ import annotations
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

PROJECT_ROOT = Path("/opt/airflow/seoul-subway-daily-reporting")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def run_ingest_arrival():
    # 프로젝트 모듈 import (컨테이너에서 /opt/airflow/seoul-subway-daily-reporting 가 PYTHONPATH에 잡혀있어야 함)
    from src.ingestion.ingest_subway_arrival_realtime import main  # type: ignore
    main()


with DAG(
    dag_id="bronze_arrival",
    description="Ingest realtime subway arrival snapshots to Bronze (S3)",
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "arrival"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=run_ingest_arrival,
    )

    ingest