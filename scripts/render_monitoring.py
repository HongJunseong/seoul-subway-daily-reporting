"""Render a small local Kubernetes monitoring stack; does not apply or contact services."""
import argparse
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def render(namespace, *, data_kind='synthetic', image='ssdr-airflow:monitoring-v2',
           claim='ssdr-metrics', reuse_claim=False, subdir=''):
    if not re.fullmatch(r'ssdr-[a-z0-9-]{1,45}', namespace):
        raise ValueError('use a dedicated ssdr-* namespace')
    if data_kind not in {'synthetic', 'observed'}:
        raise ValueError('invalid data kind')
    if not re.fullmatch(r'[a-z0-9-]{1,63}', claim):
        raise ValueError('invalid metrics claim')
    if subdir and not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', subdir):
        raise ValueError('metrics subdir must be a single relative directory')
    config = {p.name: p.read_text() for p in (ROOT / 'monitoring').iterdir()
              if p.name in {'prometheus.yml', 'alerts.yml', 'datasource.yml', 'dashboard-provider.yml', 'dashboard.json'}}
    dashboard = json.loads(config['dashboard.json'])
    dashboard['templating']['list'][0].update(query=data_kind, current={'text': data_kind, 'value': data_kind})
    config['dashboard.json'] = json.dumps(dashboard)
    images = json.loads((ROOT / 'monitoring/images.json').read_text())
    digest = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    items = [{'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': namespace}},
             {'apiVersion': 'v1', 'kind': 'ConfigMap',
              'metadata': {'name': 'ssdr-monitoring-config', 'namespace': namespace}, 'data': config}]

    def pvc(name):
        return {'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim',
                'metadata': {'name': name, 'namespace': namespace},
                'spec': {'accessModes': ['ReadWriteOnce'], 'resources': {'requests': {'storage': '1Gi'}}}}
    if not reuse_claim:
        items.append(pvc(claim))
    items.append(pvc('ssdr-prometheus-data'))

    def component(name, container, volumes, user, memory, cpu='300m'):
        labels = {'app': name, 'app.kubernetes.io/part-of': 'ssdr-monitoring'}
        container.setdefault('imagePullPolicy', 'IfNotPresent')
        container.update(name=name, resources={
            'requests': {'cpu': '50m', 'memory': '64Mi'}, 'limits': {'cpu': cpu, 'memory': memory}},
            securityContext={'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                             'capabilities': {'drop': ['ALL']}})
        container['readinessProbe'] = {'httpGet': {'path': '/healthz' if name == 'ssdr-metrics' else (
            '/-/ready' if name == 'ssdr-prometheus' else '/api/health'), 'port': container['ports'][0]['containerPort']},
            'initialDelaySeconds': 3, 'periodSeconds': 5}
        items.append({'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': {'name': name, 'namespace': namespace},
                      'spec': {'replicas': 1, 'strategy': {'type': 'Recreate'}, 'selector': {'matchLabels': {'app': name}},
                               'template': {'metadata': {'labels': labels, 'annotations': {'checksum/config': digest}},
                                            'spec': {'automountServiceAccountToken': False,
                                                     'securityContext': {'runAsNonRoot': True, 'runAsUser': user,
                                                                         'fsGroup': 0 if name == 'ssdr-metrics' else user},
                                                     'containers': [container], 'volumes': volumes}}}})
        port = container['ports'][0]['containerPort']
        items.append({'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': name, 'namespace': namespace},
                      'spec': {'type': 'ClusterIP', 'selector': {'app': name},
                               'ports': [{'name': 'http', 'port': port, 'targetPort': port}]}})
    config_volume = {'name': 'config', 'configMap': {'name': 'ssdr-monitoring-config'}}
    metrics_dir = '/metrics' + ('/' + subdir if subdir else '')
    component('ssdr-metrics', {'image': image, 'imagePullPolicy': 'Never',
        'command': ['/opt/pipeline/bin/python', '/opt/airflow/scripts/serve_metrics.py'],
        'args': ['--directory', metrics_dir, '--data-kind', data_kind, '--host', '0.0.0.0',
                 '--expected-task', 'arrival_collect_5m/bronze_ingest_arrival',
                 '--expected-task', 'pipeline_arrival_5m/gold_arrival_metrics_15m'],
        'env': [{'name': 'PYTHONDONTWRITEBYTECODE', 'value': '1'}],
        'ports': [{'containerPort': 9109}],
        'volumeMounts': [{'name': 'metrics', 'mountPath': '/metrics', 'readOnly': True}]},
        [{'name': 'metrics', 'persistentVolumeClaim': {'claimName': claim}}], 50000, '128Mi', '200m')
    component('ssdr-prometheus', {'image': images['prometheus'], 'ports': [{'containerPort': 9090}],
        'args': ['--config.file=/etc/prometheus/prometheus.yml', '--storage.tsdb.path=/prometheus',
                 '--storage.tsdb.retention.time=2d', '--storage.tsdb.retention.size=512MB'],
        'volumeMounts': [{'name': 'config', 'mountPath': '/etc/prometheus', 'readOnly': True},
                         {'name': 'data', 'mountPath': '/prometheus'}]},
        [config_volume, {'name': 'data', 'persistentVolumeClaim': {'claimName': 'ssdr-prometheus-data'}}], 65534, '384Mi')
    env = {'GF_AUTH_ANONYMOUS_ENABLED': 'true', 'GF_AUTH_ANONYMOUS_ORG_ROLE': 'Viewer',
           'GF_AUTH_DISABLE_LOGIN_FORM': 'true', 'GF_SECURITY_DISABLE_INITIAL_ADMIN_CREATION': 'true',
           'GF_USERS_ALLOW_SIGN_UP': 'false', 'GF_ANALYTICS_REPORTING_ENABLED': 'false',
           'GF_ANALYTICS_CHECK_FOR_UPDATES': 'false', 'GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES': 'false',
           'GF_PLUGINS_PREINSTALL_DISABLED': 'true'}
    component('ssdr-grafana', {'image': images['grafana'], 'ports': [{'containerPort': 3000}],
        'env': [{'name': key, 'value': value} for key, value in env.items()],
        'volumeMounts': [{'name': 'config', 'mountPath': path, 'subPath': key, 'readOnly': True} for key, path in [
            ('datasource.yml', '/etc/grafana/provisioning/datasources/ssdr.yml'),
            ('dashboard-provider.yml', '/etc/grafana/provisioning/dashboards/ssdr.yml'),
            ('dashboard.json', '/var/lib/grafana/dashboards/ssdr.json')]] + [
                {'name': 'grafana-data', 'mountPath': '/var/lib/grafana'},
                {'name': 'tmp', 'mountPath': '/tmp'}]},
        [config_volume, {'name': 'grafana-data', 'emptyDir': {}}, {'name': 'tmp', 'emptyDir': {}}], 472, '1Gi')
    return {'apiVersion': 'v1', 'kind': 'List', 'items': items}



def fixture_job(namespace, phase, run_id='demo', image='ssdr-airflow:monitoring-v2'):
    if not re.fullmatch(r'ssdr-monitoring(?:-[a-z0-9-]{1,25})?', namespace):
        raise ValueError('metric fixtures require a dedicated ssdr-monitoring namespace')
    if phase not in {'baseline', 'failure', 'recovery'} or not re.fullmatch(r'[a-z0-9-]{1,20}', run_id):
        raise ValueError('invalid metric fixture phase or run id')
    env = {'SSDR_FIXTURE_MODE': 'true', 'SSDR_DATA_KIND': 'synthetic',
           'SSDR_METRICS_DIR': '/metrics', 'SSDR_TASK_POD_PROFILES': 'false',
           'PYTHONDONTWRITEBYTECODE': '1'}
    return {'apiVersion': 'batch/v1', 'kind': 'Job',
            'metadata': {'name': f'ssdr-metrics-{phase}-{run_id}', 'namespace': namespace},
            'spec': {'backoffLimit': 0, 'activeDeadlineSeconds': 120,
                     'template': {'spec': {'restartPolicy': 'Never', 'automountServiceAccountToken': False,
                        'securityContext': {'runAsNonRoot': True, 'runAsUser': 50000, 'fsGroup': 0},
                        'containers': [{'name': 'fixture', 'image': image, 'imagePullPolicy': 'Never',
                            'command': ['python', '/opt/airflow/scripts/monitoring_fixture.py'],
                            'args': ['--phase', phase, '--output-dir', '/tmp/metric-fixture'],
                            'env': [{'name': key, 'value': value} for key, value in env.items()],
                            'resources': {'requests': {'cpu': '100m', 'memory': '256Mi'},
                                          'limits': {'cpu': '1', 'memory': '768Mi'}},
                            'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                                                'capabilities': {'drop': ['ALL']}},
                            'volumeMounts': [{'name': 'metrics', 'mountPath': '/metrics'},
                                             {'name': 'tmp', 'mountPath': '/tmp'}]}],
                        'volumes': [{'name': 'metrics', 'persistentVolumeClaim': {'claimName': 'ssdr-metrics'}},
                                    {'name': 'tmp', 'emptyDir': {}}]}}}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--namespace', default='ssdr-monitoring')
    parser.add_argument('--data-kind', default='synthetic', choices=['synthetic', 'observed'])
    parser.add_argument('--image', default='ssdr-airflow:monitoring-v2')
    parser.add_argument('--claim', default='ssdr-metrics')
    parser.add_argument('--reuse-claim', action='store_true')
    parser.add_argument('--subdir', default='')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--fixture-phase', choices=['baseline', 'failure', 'recovery'])
    parser.add_argument('--run-id', default='demo')
    args = parser.parse_args()
    result = (fixture_job(args.namespace, args.fixture_phase, args.run_id, args.image) if args.fixture_phase else
              render(args.namespace, data_kind=args.data_kind, image=args.image, claim=args.claim,
                     reuse_claim=args.reuse_claim, subdir=args.subdir))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
    print('Rendered fixture job' if args.fixture_phase else f'Rendered {len(result["items"])} resources; no cluster mutation')


if __name__ == '__main__':
    main()
