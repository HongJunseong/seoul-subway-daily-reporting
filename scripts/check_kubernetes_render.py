"""Validate the pinned Helm chart and embedded worker template without a cluster.

Only the worker template (Secret references) is saved. Rendered Secret values
are never printed or written to evidence. This is not a Pod execution test.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import yaml


def validate_documents(documents, expected_image, fixture=False):
    configs = [d for d in documents if d and d.get("kind") == "ConfigMap"
               and "pod_template_file.yaml" in d.get("data", {})]
    if len(configs) != 1:
        raise ValueError("expected one worker pod template")
    pod = yaml.safe_load(configs[0]["data"]["pod_template_file.yaml"])
    spec = pod["spec"]
    base = spec["containers"][0]
    if base["name"] != "base" or base["image"] != expected_image:
        raise ValueError("worker must inherit the reviewed Airflow image in base container")
    if base["imagePullPolicy"] != "Never":
        raise ValueError("local worker image must not pull a different image")
    if spec.get("restartPolicy") != "Never" or not spec.get("serviceAccountName"):
        raise ValueError("worker restart policy / service account missing")
    env = {e["name"]: e.get("value") for e in base.get("env", [])}
    if env.get("SSDR_TASK_POD_PROFILES") != "true":
        raise ValueError("task-specific resource profiles disabled")
    if not base.get("resources", {}).get("requests") or not base["resources"].get("limits"):
        raise ValueError("worker baseline resources missing")
    if any(d and d.get("kind") in {"ClusterRole", "ClusterRoleBinding"} for d in documents):
        raise ValueError("local deployment must use namespace-scoped RBAC")
    roles = [d for d in documents if d and d.get("kind") == "Role"]
    if not any("pods" in r.get("resources", []) and "create" in r.get("verbs", [])
               for d in roles for r in d.get("rules", [])):
        raise ValueError("scheduler needs namespace-scoped Pod creation permission")
    if fixture:
        bootstrap = {d.get("metadata", {}).get("labels", {}).get("component"): d
                     for d in documents if d and d.get("kind") == "Job"}
        for name in ("run-airflow-migrations", "create-user-job"):
            job = bootstrap.get(name)
            if not job or "helm.sh/hook" in (job["metadata"].get("annotations") or {}):
                raise ValueError("fixture bootstrap must be regular Jobs before Helm readiness wait")
        if env.get("SSDR_TASK_RESOURCE_METRICS") != "true":
            raise ValueError("fixture worker resource measurements must be enabled")
        if (env.get("SSDR_FIXTURE_MODE") != "true" or env.get("SSDR_DATA_KIND") != "synthetic"
                or env.get("SSDR_REPORT_OUTPUT_DIR") != "/opt/airflow/fixture-lake/reports"):
            raise ValueError("fixture must disable schedules and persist explicitly synthetic reports")
        if base.get("envFrom") or env.get("SSDR_DATA_ROOT") != "/opt/airflow/fixture-lake/input/lake":
            raise ValueError("fixture must use local storage without live Secret injection")
        mounts = {v["name"]: v["mountPath"] for v in base.get("volumeMounts", [])}
        claims = {v["name"]: v.get("persistentVolumeClaim", {}).get("claimName")
                  for v in spec.get("volumes", [])}
        if mounts.get("fixture-lake") != "/opt/airflow/fixture-lake":
            raise ValueError("fixture worker lake mount missing")
        if claims.get("fixture-lake") != "ssdr-fixture-lake" or claims.get("logs") != "ssdr-fixture-logs":
            raise ValueError("fixture data/logs must survive individual worker deletion")
    return pod


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("--chart-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fixture", action="store_true")
    args = parser.parse_args()
    expected = (root / "k8s/chart-version.txt").read_text().strip()
    metadata = yaml.safe_load((args.chart_dir / "Chart.yaml").read_text())
    if metadata["version"] != expected:
        parser.error(f"chart version must be {expected}")
    if args.output_dir.exists():
        parser.error("output already exists; preserve prior evidence")
    command = ["helm", "template", "ssdr-fixture", str(args.chart_dir), "--namespace", "ssdr-fixture",
               "-f", str(root / "k8s/airflow-values.yaml")]
    if args.fixture:
        command.extend(["-f", str(root / "k8s/fixture-values.yaml")])
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        # Helm errors may include manifest fragments. Do not dump them to public logs.
        raise RuntimeError("Helm render failed; inspect values/schema locally")
    docs = list(yaml.safe_load_all(result.stdout))
    images = yaml.safe_load((root / "k8s/airflow-values.yaml").read_text())["images"]
    overrides = yaml.safe_load((root / "k8s/fixture-values.yaml").read_text()).get("images", {}) if args.fixture else {}
    effective = {name: images[name] | overrides.get(name, {}) for name in ("airflow", "pod_template")}
    if any(effective["airflow"][key] != effective["pod_template"][key] for key in ("repository", "tag")):
        raise ValueError("scheduler and worker must use the same reviewed image")
    image = effective["airflow"]["repository"] + ":" + effective["airflow"]["tag"]
    pod = validate_documents(docs, image, args.fixture)
    args.output_dir.mkdir(parents=True)
    template = yaml.safe_dump(pod, sort_keys=False)
    (args.output_dir / "worker-pod.yaml").write_text(template, encoding="utf-8")
    evidence = {"verified": True, "validation_scope": "helm-render-only",
                "chart_version": expected, "airflow_version": "2.7.3",
                "image": image, "fixture": args.fixture,
                "worker_template_sha256": hashlib.sha256(template.encode()).hexdigest(),
                "task_resource_metrics_enabled": any(e["name"] == "SSDR_TASK_RESOURCE_METRICS"
                                                     and e.get("value") == "true" for e in pod["spec"]["containers"][0].get("env", [])),
                "namespace_scoped_rbac": True, "actual_pod_execution": False}
    (args.output_dir / "evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
