"""Execute the actual daily DAG chain locally with synthetic Bronze and isolated SQLite."""
from __future__ import annotations
import argparse
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "dags")]


def require(condition, message):
    if not condition:
        raise RuntimeError("Offline Airflow verification failed: " + message)


def prepare_environment(output, pipeline_python):
    output = Path(output).resolve()
    pipeline_python = Path(pipeline_python).absolute()
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise ValueError("Offline Airflow run requires a new/empty output directory")
    if not pipeline_python.is_file() or not (pipeline_python.parent / "spark-submit").is_file():
        raise ValueError("pipeline-python must point to the pipeline venv with spark-submit")
    output.mkdir(parents=True, exist_ok=True)
    values = {
        "AIRFLOW_HOME": str(output / "airflow"),
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{output / 'airflow/airflow.db'}",
        "AIRFLOW__CORE__DAGS_FOLDER": str(ROOT / "dags"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        "AIRFLOW__CORE__EXECUTOR": "SequentialExecutor",
        "AIRFLOW__CORE__UNIT_TEST_MODE": "true",
        "AIRFLOW__CORE__FERNET_KEY": "",
        "AIRFLOW__CORE__DAG_RUN_CONF_OVERRIDES_PARAMS": "true",
        "SSDR_PROJECT_DIR": str(ROOT),
        "SSDR_SPARK_BINARY": str(pipeline_python.parent / "spark-submit"),
        "SSDR_DATA_ROOT": str(output / "input/lake"),
        "SSDR_STORAGE_PREFIX": "v2", "SSDR_SHUFFLE_PARTITIONS": "2",
        "SSDR_REPORT_OUTPUT_DIR": str(output / "reports"),
        "SSDR_REPORT_MODE": "template", "SSDR_DATA_KIND": "synthetic",
        "PYSPARK_PYTHON": str(pipeline_python), "PYSPARK_DRIVER_PYTHON": str(pipeline_python),
        "SPARK_LOCAL_IP": "127.0.0.1", "AWS_EC2_METADATA_DISABLED": "true",
        "SSDR_METRICS_DIR": "", "SLACK_WEBHOOK_URL": "", "OPENAI_API_KEY": "", "SEOUL_API_KEY": "",
        "SSDR_S3_BUCKET": "", "AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": "",
        "AWS_SESSION_TOKEN": "", "AIRFLOW_CONFIG": str(output / "airflow/airflow.cfg"),
    }
    # Override inherited service configuration before importing Airflow.
    for key in list(os.environ):
        if key.startswith("AIRFLOW__"):
            del os.environ[key]
    os.environ.update(values)
    return output, pipeline_python


def execute_run(dag, run_id):
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from airflow.utils import timezone
    with create_session() as session:
        run = session.query(DagRun).filter_by(dag_id=dag.dag_id, run_id=run_id).one()
        run.dag = dag
        run.state = "running"
        run.start_date = timezone.utcnow()
        session.commit()
        for _ in range(50):
            session.expire_all()
            run = session.query(DagRun).filter_by(dag_id=dag.dag_id, run_id=run_id).one()
            run.dag = dag
            ready, _ = run.update_state(session=session)
            session.commit()
            if run.state != "running":
                require(run.state == "success", "DAG did not succeed: " + dag.dag_id)
                return {"dag_id": dag.dag_id, "run_id": run_id, "state": run.state,
                        "tasks": sorted([{"task_id": ti.task_id, "state": ti.state,
                                         "executed_attempts": ti.prev_attempted_tries}
                                        for ti in run.get_task_instances(session=session)],
                                       key=lambda item: item["task_id"])}
            require(bool(ready), "no runnable task; no scheduler/retry waiting is simulated")
            for ti in ready:
                ti.task = dag.get_task(ti.task_id)
                # Public execution path checks dependencies, sets start/attempt, renders, executes, emits callbacks.
                ti.run(session=session)
                ti.refresh_from_db(session=session)
                require(ti.state in ("success", "skipped"), "task failed or requires retry: " + ti.task_id)
        raise RuntimeError("Offline DAG step limit exceeded")


def triggered_run(parent_dag, parent_run_id, task_id, target_dag):
    from airflow.models import TaskInstance, DagRun
    from airflow.utils.session import create_session
    with create_session() as session:
        ti = session.query(TaskInstance).filter_by(dag_id=parent_dag, run_id=parent_run_id, task_id=task_id).one()
        run_id = ti.xcom_pull(task_ids=task_id, key="trigger_run_id", session=session)
        run = session.query(DagRun).filter_by(dag_id=target_dag, run_id=run_id).one()
        require(run.state == "queued" and run.conf == {"target_ymd": "20260201"}, "trigger conf/state")
        return run_id


def run(output):
    import pendulum
    import requests
    from airflow.models import DagBag, Pool
    from airflow.utils import db
    from airflow.utils.session import create_session
    from src.observability.task_events import summarize_events
    from scripts.verify_report import verify_report
    from src.report.publication import read_publication

    def block_http(*args, **kwargs):
        raise RuntimeError("HTTP access is disabled in the offline Airflow experiment")
    requests.sessions.Session.request = block_http
    db.initdb()
    bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False, safe_mode=False)
    require(not bag.import_errors, "DAG import errors")
    bag.sync_to_db()
    with create_session() as session:
        session.add(Pool(pool="ssdr_delta_writer", slots=1, description="isolated offline writer", include_deferred=False))
    event_log = output / "task-events.log"
    handler = logging.FileHandler(event_log, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger("src.observability.task_events").addHandler(handler)
    logging.getLogger("src.observability.task_events").setLevel(logging.INFO)
    started = time.perf_counter()
    try:
        dag = bag.dags["usage_daily"]
        with create_session() as session:
            dag.create_dagrun(run_id="manual__offline_fixture", run_type="manual", state="running", external_trigger=True,
                              execution_date=pendulum.datetime(2026, 2, 1, 6, tz="Asia/Seoul"),
                              data_interval=(pendulum.datetime(2026, 2, 1, tz="Asia/Seoul"),
                                             pendulum.datetime(2026, 2, 1, 6, tz="Asia/Seoul")),
                              conf={"target_ymd": "20260201", "reprocess_only": True}, session=session)
        runs = [execute_run(dag, "manual__offline_fixture")]
        next_id = triggered_run("usage_daily", runs[-1]["run_id"], "trigger_gold_daily", "gold_daily")
        runs.append(execute_run(bag.dags["gold_daily"], next_id))
        next_id = triggered_run("gold_daily", next_id, "trigger_report_daily", "report_daily")
        runs.append(execute_run(bag.dags["report_daily"], next_id))
    finally:
        handler.close()
        logging.getLogger("src.observability.task_events").removeHandler(handler)
    elapsed = time.perf_counter() - started
    summary = summarize_events(event_log.read_text().splitlines())
    states = {row["task_id"]: row["state"] for row in runs[0]["tasks"]}
    require(states["bronze_usage"] == "skipped", "API collection must be skipped")
    require(summary["malformed_events"] == 0, "event log parse")
    require(sum(task["succeeded"] for task in summary["tasks"]) == 13, "13 executed tasks")
    require(all(task["failed"] == task["incomplete"] == task["conflicting"] == 0 for task in summary["tasks"]),
            "all observed lifecycles complete")
    require(all(task["duration_seconds"]["samples"] == 1 for task in summary["tasks"]), "actual task timing")
    report = output / "reports/subway_report_20260201.md"
    verify_report(report)
    verify_report(report.with_suffix(".publication.json"))
    context = read_publication(report.with_suffix(".publication.json"))["context"]
    require(context["data_kind"] == "synthetic" and context["usage"]["total_count"] == 680, "report usage")
    expected = json.loads((output / "input/seed.json").read_text())
    require(context["arrival"]["total_observations"] == expected["expected_daily_arrival_rows"]
            and context["arrival"]["valid_eta_observations"] == expected["expected_daily_valid_eta_rows"],
            "report D-1 observation counts")
    evidence = {"schema_version": 1, "data_kind": "synthetic", "verified": True, "runs": runs,
                "executed_tasks": 13, "skipped_api_tasks": 1, "elapsed_seconds": round(elapsed, 6),
                "report_hashes_verified": True, "usage_total": 680, "daily_arrival_observations": 4,
                "daily_valid_eta_observations": expected["expected_daily_valid_eta_rows"],
                "limitations": ["Local sequential TaskInstance.run loop; not a running scheduler/executor deployment",
                                "No scheduler/pool/executor queue delay measurement",
                                "Synthetic fixture timing; no live API, S3, Slack or LLM validation"]}
    (output / "task-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "evidence.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "evidence.md").write_text(render_evidence(evidence, summary), encoding="utf-8")
    return evidence


def render_evidence(evidence, summary):
    metrics = {(task["dag_id"], task["task_id"]): task for task in summary["tasks"]}
    lines = ["# 로컬 Airflow 연결 실행 결과", "",
             "> 합성 원본을 실제 Airflow TaskInstance와 Spark 태스크로 처리한 결과입니다.", "",
             f"- 검증 성공: {evidence['verified']}",
             f"- 성공 태스크: {evidence['executed_tasks']}, API 수집 skipped: {evidence['skipped_api_tasks']}",
             f"- DAG 실행 전체 시간: {evidence['elapsed_seconds']:.3f}초",
             f"- 승하차 정정 합계: {evidence['usage_total']}",
             f"- D-1 도착 관측 / 유효 ETA: {evidence['daily_arrival_observations']} / {evidence['daily_valid_eta_observations']}",
             f"- 보고서/context 해시 검증: {evidence['report_hashes_verified']}", "",
             "## 태스크 상태와 실제 로컬 실행 시간", "",
             "| DAG | 태스크 | 상태 | 실행 시간(초) |", "|---|---|---|---:|"]
    for run in evidence["runs"]:
        for task in run["tasks"]:
            metric = metrics.get((run["dag_id"], task["task_id"]))
            seconds = metric["duration_seconds"]["mean"] if metric else None
            duration = f"{seconds:.3f}" if seconds is not None else "미실행"
            lines.append(f"| {run['dag_id']} | {task['task_id']} | {task['state']} | {duration} |")
    lines += ["", "## 검증과 해석", "",
              "- 실제 trigger가 만든 DagRun ID와 target_ymd를 확인하고 해당 실행을 이어 처리했습니다.",
              "- 날짜 XCom·branch·Bash Spark 제출·보고서 callable·callback은 실제 DAG 코드입니다.",
              "- 일간 도착은 1월 31일만 포함합니다. NULL ETA가 있는 다음 날 자정 관측은 제외합니다.",
              "- executor queue와 scheduled 시작 지연 표본은 없습니다. 운영 대기 0초라는 뜻이 아닙니다.",
              "- 전체 시간은 원본 준비·DB 초기화를 제외합니다. 태스크 시간에는 Spark 제출·JVM 시작이 포함됩니다.",
              "- 순차 로컬 실행이며 실제 scheduler·분산 executor·S3·실수집 운영 검증은 별도로 필요합니다.", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--pipeline-python", required=True, type=Path)
    args = parser.parse_args()
    output, pipeline_python = prepare_environment(args.output_dir, args.pipeline_python)
    with (output / "seed.log").open("w", encoding="utf-8") as stream:
        subprocess.run([str(pipeline_python), str(ROOT / "scripts/seed_bronze.py"),
                        "--output-dir", str(output / "input")], cwd=ROOT, stdout=stream,
                       stderr=subprocess.STDOUT, check=True, timeout=300)
    print(json.dumps(run(output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
