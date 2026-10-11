import pytest
pytest.importorskip("airflow")
pytest.importorskip("kubernetes")
from kubernetes.client import models as k8s
from airflow.models import DagBag
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
from airflow.serialization.serialized_objects import SerializedDAG
from dags.pod_profiles import task_executor_config

pytestmark = pytest.mark.airflow


def test_override_keeps_worker_image_credentials_and_persistent_mounts(monkeypatch):
    monkeypatch.setenv("SSDR_TASK_POD_PROFILES", "true")
    base = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="worker"), spec=k8s.V1PodSpec(
        service_account_name="worker", restart_policy="Never",
        volumes=[k8s.V1Volume(name="lake", persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
            claim_name="lake-pvc"))],
        containers=[k8s.V1Container(name="base", image="reviewed:fixture",
            env=[k8s.V1EnvVar(name="SSDR_DATA_ROOT", value="/lake")],
            env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="service-secrets"))],
            volume_mounts=[k8s.V1VolumeMount(name="lake", mount_path="/lake")])]))
    merged = PodGenerator.reconcile_pods(base, task_executor_config("spark")["pod_override"])
    container = merged.spec.containers[0]
    assert container.image == "reviewed:fixture"
    assert container.resources.requests == {"cpu": "1", "memory": "2Gi"}
    assert container.resources.limits == {"cpu": "2", "memory": "3Gi"}
    assert container.env_from[0].secret_ref.name == "service-secrets"
    assert container.env[0].value == "/lake"
    assert container.volume_mounts[0].mount_path == "/lake"
    assert merged.spec.volumes[0].persistent_volume_claim.claim_name == "lake-pvc"
    assert merged.spec.service_account_name == "worker"


def test_real_dags_serialize_and_select_task_resources(monkeypatch):
    monkeypatch.setenv("SSDR_TASK_POD_PROFILES", "true")
    bag = DagBag("dags", include_examples=False, safe_mode=False)
    assert not bag.import_errors
    from airflow.operators.bash import BashOperator
    for dag in bag.dags.values():
        SerializedDAG.to_dict(dag)
        for task in dag.tasks:
            pod = task.executor_config["pod_override"]
            if isinstance(task, BashOperator):
                assert pod.metadata.labels["ssdr-task-profile"] == "spark"
                assert task.pool == "ssdr_delta_writer" and task.pool_slots == 1
            elif task.task_id == "generate_report":
                assert pod.metadata.labels["ssdr-task-profile"] == "report"
            else:
                assert pod.metadata.labels["ssdr-task-profile"] == "control"


def test_local_execution_does_not_require_pod_config(monkeypatch):
    monkeypatch.delenv("SSDR_TASK_POD_PROFILES", raising=False)
    assert task_executor_config("spark") == {}
    monkeypatch.setenv("SSDR_TASK_POD_PROFILES", "yes")
    with pytest.raises(ValueError, match="true or false"):
        task_executor_config("spark")

def test_fixture_profile_disables_timed_api_collection(monkeypatch):
    monkeypatch.setenv("SSDR_FIXTURE_MODE", "true")
    monkeypatch.setenv("SSDR_TASK_POD_PROFILES", "true")
    bag = DagBag("dags", include_examples=False, safe_mode=False)
    assert not bag.import_errors
    assert all(dag.schedule_interval is None for dag in bag.dags.values())
    branch = bag.dags["usage_daily"].get_task("choose_input").python_callable
    from types import SimpleNamespace
    assert branch(dag_run=SimpleNamespace(conf={"reprocess_only": True})) == "silver_usage"
