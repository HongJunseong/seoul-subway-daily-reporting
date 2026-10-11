"""Merge all real DAG overrides with the rendered Helm template via Airflow."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-template", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists")
    os.environ["AIRFLOW_HOME"] = str(root / ".work/kubernetes/airflow-home")
    os.environ["SSDR_TASK_POD_PROFILES"] = "true"
    sys.path[:0] = [str(root), str(root / "dags")]
    from airflow.models import DagBag
    from airflow.operators.bash import BashOperator
    from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
    from airflow.serialization.serialized_objects import SerializedDAG
    bag = DagBag(str(root / "dags"), include_examples=False, safe_mode=False)
    if bag.import_errors:
        raise ValueError("DAG imports failed")
    base = PodGenerator.deserialize_model_file(str(args.worker_template))
    tasks = []
    for dag in sorted(bag.dags.values(), key=lambda d: d.dag_id):
        SerializedDAG.to_dict(dag)
        for task in sorted(dag.tasks, key=lambda t: t.task_id):
            override = task.executor_config["pod_override"]
            pod = PodGenerator.reconcile_pods(base, override)
            container, original = pod.spec.containers[0], base.spec.containers[0]
            if (container.image != original.image or container.env != original.env
                or (container.env_from or []) != (original.env_from or [])
                or container.volume_mounts != original.volume_mounts
                or pod.spec.volumes != base.spec.volumes
                or pod.spec.service_account_name != base.spec.service_account_name):
                raise ValueError("override lost worker image/environment/storage/RBAC identity")
            if isinstance(task, BashOperator) and (task.pool != "ssdr_delta_writer" or task.pool_slots != 1):
                raise ValueError("Spark writer serialization lost")
            tasks.append({"dag_id": dag.dag_id, "task_id": task.task_id,
                          "profile": override.metadata.labels["ssdr-task-profile"],
                          "resources": container.resources.to_dict()})
    evidence = {"verified": True, "validation_scope": "airflow-pod-merge-and-dag-serialization",
                "actual_pod_execution": False, "tasks": tasks}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(f"Verified {len(tasks)} real DAG task overrides; no Pods launched")


if __name__ == "__main__":
    main()
