"""Task-specific KubernetesExecutor resources; enabled explicitly by Helm."""
import os

PROFILES = {
    "control": {"requests": {"cpu": "250m", "memory": "512Mi"},
                "limits": {"cpu": "1", "memory": "1Gi"}},
    "spark": {"requests": {"cpu": "1", "memory": "2Gi"},
              "limits": {"cpu": "2", "memory": "3Gi"}},
    "report": {"requests": {"cpu": "250m", "memory": "512Mi"},
               "limits": {"cpu": "1", "memory": "2Gi"}},
}


def task_executor_config(profile):
    resources = PROFILES[profile]
    enabled = os.getenv("SSDR_TASK_POD_PROFILES", "false").lower()
    if enabled not in {"true", "false"}:
        raise ValueError("SSDR_TASK_POD_PROFILES must be true or false")
    if enabled == "false":
        return {}
    from kubernetes.client import models as k8s
    return {"pod_override": k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(labels={"ssdr-task-profile": profile}),
        spec=k8s.V1PodSpec(containers=[k8s.V1Container(
            name="base", resources=k8s.V1ResourceRequirements(**resources)
        )]),
    )}
