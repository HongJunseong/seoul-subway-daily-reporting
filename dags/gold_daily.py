from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

from common import PROJECT_DIR, SPARK_SUBMIT, compute_target_ymd


with DAG(
    dag_id="gold_daily",
    description="Build Gold (arrival + position) from Silver (daily)",
    start_date=pendulum.datetime(2025, 12, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["gold"],
) as dag:

    target_ymd = PythonOperator(
        task_id="compute_target_ymd",
        python_callable=compute_target_ymd,
    )

    gold_arrival = BashOperator(
        task_id="gold_arrival",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        export PYTHONPATH="{PROJECT_DIR}:${{PYTHONPATH:-}}"
        export TARGET_YMD="{{{{ ti.xcom_pull(task_ids='compute_target_ymd') }}}}"

        {SPARK_SUBMIT} "{PROJECT_DIR}/src/processing/build_gold_subway_arrival_spark.py"
        """,
    )

    emit_gold_ready = BashOperator(
        task_id="emit_gold_ready",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"

        export TARGET_YMD="{{{{ ti.xcom_pull(task_ids='compute_target_ymd') }}}}"
        export RUN_ID="{{{{ run_id }}}}"
        export GOLD_ARRIVAL_PATH_TMPL="s3://${{SSDR_S3_BUCKET}}/gold/subway_arrival/dt=${{TARGET_YMD}}/"
        export GOLD_POSITION_PATH_TMPL="s3://${{SSDR_S3_BUCKET}}/gold/subway_position/dt=${{TARGET_YMD}}/"

        export KAFKA_TOPIC="event.pipeline"

        python src/events/emit_gold_ready.py
        """,
    )

    # gold 끝나면 report_daily를 자동 실행
    trigger_report_daily = TriggerDagRunOperator(
        task_id="trigger_report_daily",
        trigger_dag_id="report_daily",
        conf={
            "target_ymd": "{{ ti.xcom_pull(task_ids='compute_target_ymd') }}"
        },
        wait_for_completion=False,  # gold_daily는 report 끝까지 기다릴 필요 없음(원하면 True도 가능)
    )


    target_ymd >> gold_arrival >> emit_gold_ready >> trigger_report_daily
