from __future__ import annotations

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


def compute_target_ymd(**context) -> str:
    """
    - 기본: DAG 실행 logical_date를 KST로 바꿔 YYYYMMDD 생성
    - 수동 override: Trigger DAG 시 conf로 {"target_ymd": "20251216"} 주면 그 값 사용
    """
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("target_ymd"):
        return str(dag_run.conf["target_ymd"])

    logical_date = context["logical_date"]
    kst_dt = logical_date.in_timezone("Asia/Seoul")
    return kst_dt.format("YYYYMMDD")
