from __future__ import annotations

import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

PROJECT_ROOT = Path("/opt/airflow/seoul-subway-daily-reporting")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def run_ingest_position():
    from src.ingestion.ingest_subway_position_realtime import main
    main()


with DAG(
    dag_id="bronze_position",
    description="Ingest realtime subway position snapshots to Bronze (S3)",
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "position"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=run_ingest_position,
    )

    ingest