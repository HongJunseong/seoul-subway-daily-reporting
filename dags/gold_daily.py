from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

PROJECT_DIR = "/opt/airflow/seoul-subway-daily-reporting"

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

def compute_target_ymd(**context) -> str:
    """
    - 기본: DAG 실행 logical_date를 KST로 바꿔 YYYYMMDD 생성
    - 수동 override: Trigger DAG 시 conf로 {"target_ymd": "20251216"} 주면 그 값 사용
    """
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("target_ymd"):
        return str(dag_run.conf["target_ymd"])

    logical_date = context["logical_date"]   # UTC 기반 pendulum
    kst_dt = logical_date.in_timezone("Asia/Seoul")
    return kst_dt.format("YYYYMMDD")


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

        {SPARK_SUBMIT} "{PROJECT_DIR}/src/processing/build_gold_subway_arrival_spark.py"
        """,
    )
    
    emit_gold_ready = BashOperator(
        task_id="emit_gold_ready",
        bash_command=f"""
        set -euo pipefail
        cd {PROJECT_DIR}

        export TARGET_YMD="{{{{ ti.xcom_pull(task_ids='compute_target_ymd') }}}}"
        export RUN_ID="{{{{ run_id }}}}"
        export GOLD_ARRIVAL_PATH_TMPL="s3://seoul-subway-daily-reporting/gold/subway_arrival/dt={{target_ymd}}/"
        export GOLD_POSITION_PATH_TMPL="s3://seoul-subway-daily-reporting/gold/subway_position/dt={{target_ymd}}/"

        export KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
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
