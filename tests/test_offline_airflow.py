from pathlib import Path
import json
import os
import subprocess
import sys
import pytest
from scripts.airflow_offline import prepare_environment
from scripts.seed_bronze import seed


@pytest.mark.skipif(os.name != "posix", reason="DAG commands require Bash")
def test_local_spark_command_preserves_literal_paths_and_excludes_cloud_packages(tmp_path):
    project = tmp_path / "project $(touch should-not-exist) 'dir"
    project.mkdir()
    binary = tmp_path / "spark submit"
    binary.write_text('#!' + sys.executable + '\nimport json,os,sys\nprint(json.dumps({"args":sys.argv[1:],"path":os.environ["PYTHONPATH"]}))\n')
    binary.chmod(0o755)
    env = dict(os.environ, SSDR_PROJECT_DIR=str(project), SSDR_SPARK_BINARY=str(binary),
               SSDR_DATA_ROOT=str(tmp_path / "lake"), PYTHONPATH="inherited")
    generator = subprocess.run([sys.executable, "-c", "import runpy;print(runpy.run_path('dags/common.py')['spark_command']('src/job.py'))"],
                               env=env, capture_output=True, text=True, check=True)
    result = subprocess.run(["bash", "-c", generator.stdout], env=env, capture_output=True, text=True, check=True)
    value = json.loads(result.stdout)
    assert value["path"] == str(project) + ":inherited"
    assert str(project / "src/job.py") in value["args"]
    assert "io.delta:delta-spark_2.12:3.2.0" in value["args"]
    assert "hadoop-aws" not in result.stdout
    assert not (project / "should-not-exist").exists()


def test_default_spark_command_retains_cloud_dependencies():
    env = {key: value for key, value in os.environ.items()
           if key not in ("SSDR_DATA_ROOT", "SSDR_PROJECT_DIR", "SSDR_SPARK_BINARY")}
    result = subprocess.run([sys.executable, "-c", "import runpy;print(runpy.run_path('dags/common.py')['spark_command']('src/job.py'))"],
                            env=env, capture_output=True, text=True, check=True)
    assert "hadoop-aws:3.3.4" in result.stdout
    assert "aws-java-sdk-bundle:1.12.262" in result.stdout
    assert "/opt/airflow/src/job.py" in result.stdout


def test_offline_environment_keeps_venv_symlink_and_isolates_database_and_services(tmp_path, monkeypatch):
    binary = tmp_path / "venv/bin"
    binary.mkdir(parents=True)
    python = binary / "python"
    python.symlink_to(Path(sys.executable))
    (binary / "spark-submit").write_text("fixture")
    inherited = dict(os.environ, AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql://must-not-connect",
                     AIRFLOW__SMTP__SMTP_HOST="must-not-send", SLACK_WEBHOOK_URL="must-not-send",
                     SSDR_DATA_ROOT="must-not-write", SSDR_METRICS_DIR="must-not-write", SSDR_REPORT_MODE="llm")
    monkeypatch.setattr(os, "environ", inherited)
    output, configured = prepare_environment(tmp_path / "run", python)
    assert configured == python
    assert os.environ["SSDR_SPARK_BINARY"] == str(binary / "spark-submit")
    assert os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] == "sqlite:///" + str(output / "airflow/airflow.db")
    assert "AIRFLOW__SMTP__SMTP_HOST" not in os.environ
    assert os.environ["SLACK_WEBHOOK_URL"] == ""
    assert os.environ["SSDR_METRICS_DIR"] == ""
    assert os.environ["SSDR_REPORT_MODE"] == "template"
    assert os.environ["SSDR_DATA_ROOT"] == str(output / "input/lake")


@pytest.mark.parametrize("operation", ["seed", "airflow"])
def test_offline_commands_preserve_existing_output(tmp_path, operation):
    original = tmp_path / "existing.txt"
    original.write_text("preserve")
    with pytest.raises(ValueError, match="new/empty"):
        if operation == "seed":
            seed(tmp_path)
        else:
            prepare_environment(tmp_path, Path(sys.executable))
    assert original.read_text() == "preserve"


def test_daily_fixture_excludes_null_eta_at_next_midnight():
    from scripts.seed_bronze import fixture_expectations
    expected = fixture_expectations()
    assert expected["expected_daily_arrival_rows"] == 4
    assert expected["expected_daily_valid_eta_rows"] == 4
