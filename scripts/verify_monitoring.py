"""Verify live Prometheus alert transitions and the provisioned Grafana data source."""
import argparse
from datetime import datetime, timezone
import json
import time
from urllib.parse import urlencode
from urllib.request import urlopen


def fetch(url):
    with urlopen(url, timeout=5) as response:
        return json.load(response)


def verify(prometheus, grafana, phase, timeout=60):
    def query(expression, base=prometheus):
        result = fetch(base.rstrip('/') + '/api/v1/query?' + urlencode({'query': expression}))
        if result['status'] != 'success':
            raise ValueError('Prometheus query did not succeed')
        return result['data']['result']

    start = time.monotonic()
    while time.monotonic() - start < timeout:
        alerts = fetch(prometheus.rstrip('/') + '/api/v1/alerts')['data']['alerts']
        firing = sorted({a['labels']['alertname'] for a in alerts if a['state'] == 'firing'})
        up = query('up{job="ssdr-pipeline"}')
        target_up = len(up) == 1 and float(up[0]['value'][1]) == 1
        if phase == 'exporter-down':
            ready = not target_up and 'SSDRExporterDown' in firing
        else:
            expected = {'SSDRTaskFailed', 'SSDRProcessingBehind'} if phase == 'failure' else set()
            actual = set(firing)
            ready = target_up and expected <= actual and not (actual - expected)
            records = query('ssdr_task_record_available{data_kind="synthetic"}')
            ready = ready and len(records) == 2 and all(float(r['value'][1]) == 1 for r in records)
        if ready:
            break
        time.sleep(2)
    else:
        raise AssertionError('Expected live alert transition was not observed: ' + phase)
    dashboard = fetch(grafana.rstrip('/') + '/api/dashboards/uid/ssdr-pipeline')
    assert dashboard['dashboard']['uid'] == 'ssdr-pipeline'
    assert dashboard['meta']['provisioned'] and not dashboard['dashboard']['editable']
    assert fetch(grafana.rstrip('/') + '/api/health')['database'] == 'ok'
    # Verify the same query through Grafana's provisioned datasource proxy.
    proxied = query('up{job="ssdr-pipeline"}', grafana.rstrip('/') + '/api/datasources/proxy/uid/ssdr-prometheus')
    assert len(proxied) == 1
    retries = query('sum(ssdr_task_observed_retries_total{data_kind="synthetic"})') if target_up else []
    retry_count = float(retries[0]['value'][1]) if retries else None
    if phase == 'recovery':
        assert retry_count is not None and retry_count >= 1
        assert all(float(v['value'][1]) == 0 for v in query('ssdr_task_last_attempt_failed{data_kind="synthetic"}'))
        assert all(float(v['value'][1]) == 0 for v in query('ssdr_quality_unprocessed_snapshot_count{data_kind="synthetic"}'))
    return {'data_kind': 'synthetic', 'phase': phase, 'checked_at_utc': datetime.now(timezone.utc).isoformat(),
            'exporter_up': target_up, 'firing_alerts': firing, 'observed_retry_count': retry_count,
            'dashboard_provisioned': True, 'grafana_datasource_query_verified': True,
            'scope': 'Kubernetes monitoring and metric fixture; no live source collection or Delta processing'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prometheus-url', default='http://127.0.0.1:19090')
    parser.add_argument('--grafana-url', default='http://127.0.0.1:13000')
    parser.add_argument('--phase', choices=['baseline', 'failure', 'recovery', 'exporter-down'], required=True)
    parser.add_argument('--timeout-seconds', type=int, default=60)
    args = parser.parse_args()
    print(json.dumps(verify(args.prometheus_url, args.grafana_url, args.phase, args.timeout_seconds)))


if __name__ == '__main__':
    main()
