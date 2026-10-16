# 파이프라인 모니터링

수집 태스크는 성공했는데 저장 원본이 처리되지 않거나, 오래된 결과가 정상처럼 보이는 상황을 구분하기 위해 Prometheus와 Grafana를 붙였다. 기존 callback과 품질 감사가 작은 JSON을 기록하고, 읽기 전용 exporter가 이를 노출한다. exporter는 Delta·Airflow DB·외부 API를 조회하지 않는다.

`callback / audit_lake → 공유 PVC → exporter → Prometheus → Grafana`

## 로컬 설치

Docker Desktop Kubernetes와 kubectl, Python 3.11이 필요하다. 저장소 루트에서 실행한다. 공식 이미지 버전과 digest는 [images.json](../monitoring/images.json)에 고정했다. 아래 명령은 전용 namespace와 PVC를 만들며 서울 API·AWS·Slack·LLM을 호출하지 않는다.

```bash
docker build -f docker/dockerfile -t ssdr-airflow:monitoring-v2 .
python scripts/render_monitoring.py --output .work/monitoring/stack.json
kubectl --context docker-desktop apply -f .work/monitoring/stack.json
kubectl --context docker-desktop -n ssdr-monitoring rollout status deployment/ssdr-grafana
kubectl --context docker-desktop -n ssdr-monitoring port-forward --address=127.0.0.1 service/ssdr-grafana 13000:3000
```

[Grafana 대시보드](http://127.0.0.1:13000/d/ssdr-pipeline)는 datasource와 함께 자동 등록된다. 초기에는 입력이 없으므로 미검증 상태가 정상이다. anonymous Viewer인 로컬 구성으로, ClusterIP와 loopback port-forward만 사용한다. 공용 서비스로 노출하려면 별도 인증 구성이 필요하다.

## 실패와 복구 확인

다음 세 Job을 **차례로** 실행한다. run-id와 출력 파일은 새 실험마다 바꾼다. 실제 Airflow TaskInstance와 callback을 실행하지만 API·Spark 대신 짧은 합성 명령을 사용한다. 품질 값도 경고 검증용으로 생성한다.

```bash
python scripts/render_monitoring.py --fixture-phase baseline --run-id demo01 --output .work/monitoring/baseline.json
kubectl --context docker-desktop apply -f .work/monitoring/baseline.json
kubectl --context docker-desktop -n ssdr-monitoring wait --for=condition=complete job/ssdr-metrics-baseline-demo01 --timeout=120s
# 같은 절차로 phase를 failure, recovery로 바꿔 실행한다.
```

Prometheus도 별도 터미널에서 연결한 뒤 각 단계의 상태를 확인한다.

```bash
kubectl --context docker-desktop -n ssdr-monitoring port-forward --address=127.0.0.1 service/ssdr-prometheus 19090:9090
python scripts/verify_monitoring.py --phase baseline
# failure Job 완료 후 --phase failure, recovery Job 완료 후 --phase recovery
```

실패 단계는 실제 첫 시도를 up_for_retry로 만들고, 복구 단계는 두 번째 시도의 성공을 검증한다. 경고는 15초 이상 지속해야 firing이 되므로 조회 도구가 최대 60초 기다린다. exporter만 replicas=0으로 내리면 `--phase exporter-down`, 다시 replicas=1로 올리면 `--phase recovery`로 확인할 수 있다. 실제 [실행 요약](../examples/operations/monitoring.json)을 보관했다.

## 기존 Airflow에 연결

실제 적용 전 새 코드를 포함한 scheduler·worker 이미지를 빌드한다. 기존 Helm values 뒤에 [monitoring-airflow-values.yaml](../k8s/monitoring-airflow-values.yaml)을 추가하면 callback이 `/opt/airflow/logs/ssdr-metrics`에 기록한다. 기존에 extraEnv를 설정했다면 두 설정을 합친다. 이 파일은 env·Secret 참조·DAG 일정을 바꾸지 않는다.

exporter는 **같은 namespace**에서 기존 logs PVC를 읽기 전용으로 사용한다. 예를 들어 fixture의 경우:

```bash
python scripts/render_monitoring.py --namespace ssdr-fixture-example \
  --reuse-claim --claim ssdr-fixture-logs --subdir ssdr-metrics \
  --data-kind synthetic --output .work/monitoring/airflow-stack.json
```

실데이터는 `--data-kind observed`로 분리한다. 보고서 설정의 api_observations도 지표에서는 observed로 통일한다. 스크레이프만으로 품질 감사가 실행되지는 않는다. [audit_lake.py](OBSERVABILITY.md#로컬-데이터-감사)를 **완료된 처리 구간**에 실행할 때 동일한 `SSDR_METRICS_DIR`을 지정해야 한다. 감사 갱신 일정은 별도 운영 작업이며, 과거 구간의 감사 기준 시각과 현재 감사 실행 시각은 따로 표시한다.

worker CPU·메모리는 `SSDR_TASK_RESOURCE_METRICS=true`일 때 마지막 callback의 cgroup 측정값이다. fixture에서는 켜져 있다. 현재 실행 중인 Pod·node 사용량이나 태스크별 독립 peak를 보장하지 않는다.

## 지표와 경고

| 관측 | 해석 / 초기 경고 조건 |
|---|---|
| 태스크 성공 후 경과 시간 | 두 5분 DAG의 최근 성공이 600초보다 오래되면 경고. 원천 생성 시각과 다르다. |
| 실패·재시도 | 기록된 callback 시도만 누적. 중복 callback은 제외하고 Airflow 전체 이력과 구분한다. 최신 시도 실패가 15초 지속하면 경고. |
| 미관측·미처리 | 미관측 bucket을 표시하고 Bronze 미처리 snapshot이 15초 지속하면 경고. API 실패 원인을 단정하지 않는다. |
| 감사 결과·갱신 | failed 결과는 15초, 마지막 감사 실행 후 900초 초과는 30초 지속 시 경고. |
| 기록 없음·연결 장애 | 필수 기록 누락/손상과 exporter 단절을 별도로 경고. 결측을 정상 0으로 채우지 않는다. |
| worker 자원 | 마지막 측정의 메모리 peak·구간 CPU. 측정 불가 필드는 생략한다. |

임계값은 로컬 점검의 초기값이며 운영 SLA가 아니다. [alerts.yml](../monitoring/alerts.yml)과 [규칙 회귀 검사](../monitoring/alerts.test.yml)를 함께 관리한다. Prometheus는 경고 **상태**를 평가하며 Alertmanager나 Slack 전송은 이 구성에 없다. [공식 경고 규칙](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/)의 pending/firing 의미를 따른다.

메트릭 label은 dag_id·task_id·입력 종류로 제한한다. run ID·원문·키는 넣지 않는다. 태스크 기록은 identity당 하나, 최대 100개·파일당 64KiB이며 같은 로컬 PVC의 writer는 파일 lock과 atomic replace를 사용한다. 지표 쓰기 실패는 원래 태스크·감사 결과를 바꾸지 않는다.

Prometheus는 PVC에 2일/512MB 한도로 보관하고, callback 누적 값은 metrics PVC 삭제 시 초기화된다. Grafana는 재시작 때 [파일 provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/)으로 복원된다. 로컬 fixture에서 경고 전환과 재시작 후 기록 유지까지 검증했으며, 장기 운영·외부 알림·S3 상시 감사는 별도다.
