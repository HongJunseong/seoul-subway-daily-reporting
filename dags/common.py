from __future__ import annotations
from datetime import datetime, timedelta
import os
from pathlib import Path
import shlex
from src.common.dates import validate_ymd

PROJECT_DIR = os.getenv("SSDR_PROJECT_DIR", "/opt/airflow")
SPARK_BINARY = shlex.quote(os.getenv("SSDR_SPARK_BINARY", "spark-submit"))
DELTA_WRITE_POOL = "ssdr_delta_writer"
SPARK_PACKAGES = ("io.delta:delta-spark_2.12:3.2.0,"
                  "org.apache.hadoop:hadoop-aws:3.3.4,"
                  "com.amazonaws:aws-java-sdk-bundle:1.12.262")
if os.getenv("SSDR_DATA_ROOT"):
    SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.2.0"
SPARK_SUBMIT = (f'{SPARK_BINARY} --master "local[2]" --driver-memory 1g --packages "{SPARK_PACKAGES}" '
                '--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
                '--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
                '--conf spark.sql.session.timeZone=Asia/Seoul '
                '--conf spark.databricks.delta.snapshotPartitions=8')
if os.getenv("SSDR_DATA_ROOT"):
    SPARK_SUBMIT += (f' --conf spark.jars.ivy={shlex.quote(str(Path(PROJECT_DIR) / ".work/ivy"))}'
                     f' --conf spark.jars.ivySettings={shlex.quote(str(Path(PROJECT_DIR) / "tests/ivysettings.xml"))}')


def compute_target_ymd(**context):
    conf = context["dag_run"].conf or {}
    if conf.get("target_ymd"):
        return validate_ymd(str(conf["target_ymd"]))
    end = context.get("data_interval_end") or context["logical_date"]
    return end.in_timezone("Asia/Seoul").strftime("%Y%m%d")


def compute_dates(**context):
    anchor = compute_target_ymd(**context)
    date = datetime.strptime(anchor, "%Y%m%d")
    return {"target_ymd": anchor, "usage_ymd": (date - timedelta(days=4)).strftime("%Y%m%d"),
            "arrival_ymd": (date - timedelta(days=1)).strftime("%Y%m%d")}


def choose_usage_input(**context):
    value = (context["dag_run"].conf or {}).get("reprocess_only", False)
    if not isinstance(value, bool):
        raise ValueError("reprocess_only must be a JSON boolean")
    return "silver_usage" if value else "bronze_usage"


def spark_command(script, arguments=""):
    return (f'set -euo pipefail\ncd {shlex.quote(PROJECT_DIR)}\n'
            f'export PYTHONPATH={shlex.quote(PROJECT_DIR)}:"${{PYTHONPATH:-}}"\n'
            f'{SPARK_SUBMIT} {shlex.quote(str(Path(PROJECT_DIR) / script))} {arguments}\n')


DATE_ENV = {key.upper(): "{{ ti.xcom_pull(task_ids='compute_dates')['" + key + "'] }}"
            for key in ["target_ymd", "usage_ymd", "arrival_ymd"]}
AS_OF_ENV = {"AS_OF": "{{ data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S') }}"}

def runtime_schedule(schedule):
    mode = os.getenv("SSDR_FIXTURE_MODE", "false").lower()
    if mode not in {"true", "false"}:
        raise ValueError("SSDR_FIXTURE_MODE must be true or false")
    # Fixture Pods must never start timed live API collection.
    return None if mode == "true" else schedule
