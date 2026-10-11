# Kubernetes 실행 구성과 검증 범위

KubernetesExecutor는 Airflow 태스크마다 worker Pod를 만들고 scheduler와 계산 자원을 분리한다. Spark 작업은 그 Pod 안의 local[2] 프로세스다. 분산 Spark executor를 Kubernetes에 띄우는 구성은 아니다.

## 버전과 자원

Apache Airflow 2.7.3, Kubernetes provider 7.8.0, client 23.6.0, 공식 Helm chart 1.11.0을 함께 고정했다. 최신 chart의 기본값을 이 환경에 섞지 않는다. 이미지에는 src·DAG·재현 스크립트와 합성 fixture를 포함하며 Secret/.env는 포함하지 않는다.

| 프로필 | 사용 태스크 | CPU 요청/상한 | 메모리 요청/상한 |
|---|---|---|---|
| control | 날짜·분기·트리거·일반 알림 | 100m / 500m | 256Mi / 1Gi |
| spark | 수집·변환·Spark QA | 1 / 2 | 2Gi / 3Gi |
| report | 보고서 생성 | 250m / 1 | 512Mi / 2Gi |

Spark 수집도 JVM을 실행하므로 spark 프로필을 사용한다. 이 값은 초기 실행 예산이며 실제 Pod 메모리/CPU 측정으로 조정해야 한다. driver heap은 1g다. 자원 요청은 S3 writer 직렬화를 대신하지 않으며 모든 Spark 태스크의 ssdr_delta_writer pool(1 slot)은 유지한다.

pod_override는 base 컨테이너의 자원과 프로필 label만 지정한다. Helm template의 이미지·환경변수·Secret reference·로그/데이터 mount·service account는 Airflow PodGenerator 병합으로 보존한다. SSDR_TASK_POD_PROFILES=true일 때만 적용하며 기본 로컬 실행은 Kubernetes client 없이 가능하다.

## 클러스터 없이 재현하는 검사

```bash
helm repo add apache-airflow https://airflow.apache.org
helm pull apache-airflow/airflow --version "$(cat k8s/chart-version.txt)" \
  --destination .work/kubernetes --untar
python scripts/check_kubernetes_render.py \
  --chart-dir .work/kubernetes/airflow --output-dir .work/kubernetes/render-fixture --fixture
.venv-airflow/bin/python scripts/check_kubernetes_overrides.py \
  --worker-template .work/kubernetes/render-fixture/worker-pod.yaml \
  --output .work/kubernetes/render-fixture/overrides.json
```

별도 Airflow 환경에 cncf.kubernetes extra를 공식 constraints로 설치하고 pytest를 설치한다. tests/test_dags.py와 tests/test_kubernetes_profiles.py를 함께 실행한다. 렌더 검사는 values schema, 같은 worker 이미지, namespace RBAC, base 컨테이너, Secret 없는 합성 모드, 데이터·로그 PVC reference를 확인한다. 병합 검사는 실제 DAG 태스크 17개와 Airflow 직렬화를 사용한다. 결과에 actual_pod_execution=false를 명시한다.

전체 Helm manifest에는 생성된 Secret 값이 있을 수 있어 출력하거나 증거에 저장하지 않는다. 공개 파일은 Secret reference만 포함하는 worker template의 hash와 제한된 검사 결과다.

## 로컬 fixture 배포와 실제 실행

docker-desktop context와 단일 Ready node를 먼저 확인한다. 기존 namespace/릴리스를 재사용하거나 삭제하지 않고 새로운 ssdr-fixture-* namespace를 사용한다.

```bash
docker info
kubectl --context docker-desktop get nodes
docker build -f docker/dockerfile -t ssdr-airflow:portfolio-v2 .
kubectl --context docker-desktop create namespace ssdr-fixture-demo
kubectl --context docker-desktop -n ssdr-fixture-demo apply -f k8s/local-pvcs.yaml
helm upgrade --install ssdr-fixture apache-airflow/airflow \
  --kube-context docker-desktop --namespace ssdr-fixture-demo \
  --version "$(cat k8s/chart-version.txt)" \
  -f k8s/airflow-values.yaml -f k8s/fixture-values.yaml --wait --timeout 10m
```

이 조합으로 실제 Docker Desktop 설치·migration·scheduler 준비와 PVC binding을 확인했다. 새 환경에서도 각 단계를 확인한다. fixture의 migration/create-user Job은 일반 bootstrap Job으로 실행한다. post-install hook과 --wait를 함께 쓰면 migration을 기다리는 Pod와 hook이 서로 기다리는 초기 설치 문제가 있었다. fixture profile에는 서비스 Secret을 넣지 않는다. SSDR_FIXTURE_MODE=true는 모든 시간 기반 DAG schedule을 제거해 자동 실수집을 막는다. 수동 usage_daily에는 반드시 reprocess_only=true와 target_ymd=20260201을 제공한다.

RWO PVC 두 개는 Docker Desktop의 단일 node에 있는 합성 lake와 로그용이다. 여러 node에서 공유할 운영 로그는 RWX 또는 별도로 검증한 remote logging이 필요하다. 운영 airflow-values.yaml의 기본 로그 PVC는 이를 제공하는 storage class가 필요하다. 로컬 PVC를 운영 다중 node 설정으로 해석하지 않는다.

DAG 실행 전 scheduler 컨테이너에서 ssdr_delta_writer pool을 1 slot으로 생성하고 확인한다. 같은 lake를 별도 CLI와 동시에 쓰지 않는다. 아래는 새 전용 namespace의 재현 명령이다. 원본 준비는 한 번만 수행하며 출력 경로를 덮어쓰지 않는다.

```bash
kubectl --context docker-desktop -n ssdr-fixture-demo exec deployment/ssdr-fixture-scheduler -c scheduler -- \
  /opt/pipeline/bin/python /opt/airflow/scripts/seed_bronze.py --output-dir /opt/airflow/fixture-lake/input
kubectl --context docker-desktop -n ssdr-fixture-demo exec deployment/ssdr-fixture-scheduler -c scheduler -- \
  airflow pools set ssdr_delta_writer 1 serialize_delta_writers
for dag in usage_daily gold_daily report_daily; do
  kubectl --context docker-desktop -n ssdr-fixture-demo exec deployment/ssdr-fixture-scheduler -c scheduler -- \
    airflow dags unpause "$dag"
done
kubectl --context docker-desktop -n ssdr-fixture-demo exec deployment/ssdr-fixture-scheduler -c scheduler -- \
  python /opt/airflow/scripts/kubernetes_fixture.py --namespace ssdr-fixture-demo \
  --output-dir /opt/airflow/fixture-lake/evidence/recovery \
  --run-id manual__kubernetes_fixture_demo_a --delete-once-gold-worker
kubectl --context docker-desktop -n ssdr-fixture-demo exec deployment/ssdr-fixture-scheduler -c scheduler -- \
  python /opt/airflow/scripts/capture_kubernetes_files.py \
  --runtime-evidence /opt/airflow/fixture-lake/evidence/recovery/evidence.json \
  --output /opt/airflow/fixture-lake/evidence/recovery/files.json
kubectl --context docker-desktop -n ssdr-fixture-demo exec deployment/ssdr-fixture-scheduler -c scheduler -- \
  python /opt/airflow/scripts/kubernetes_fixture.py --namespace ssdr-fixture-demo \
  --output-dir /opt/airflow/fixture-lake/evidence/replay --run-id manual__kubernetes_fixture_demo_b
kubectl --context docker-desktop -n ssdr-fixture-demo exec deployment/ssdr-fixture-scheduler -c scheduler -- \
  python /opt/airflow/scripts/capture_kubernetes_files.py \
  --runtime-evidence /opt/airflow/fixture-lake/evidence/replay/evidence.json \
  --output /opt/airflow/fixture-lake/evidence/replay/files.json \
  --compare /opt/airflow/fixture-lake/evidence/recovery/files.json
```

삭제 flag는 이 실험이 만든 Gold worker만 UID precondition으로 한 번 종료한다. 전용 namespace·합성 환경·전용 run ID를 요구한다. Silver 성공과 Gold 실행 중 상태를 확인한 후 삭제하며 다른 UID·증가한 시도 번호·성공 종료를 요구한다. runner는 실제 DagRun과 task 상태, API 수집 skipped, worker 성공, 보고서 manifest를 확인한다. PVC 도구는 Delta 루트를 읽어 전체 컬럼 해시와 원본 버전을 비교하고 삭제된 Pod의 시도별 로그가 남았는지 확인한다. 공개 JSON은 생성 결과를 복사하며 보고서와 raw 로그는 ignored .work에 보존한다.

## 확인한 한계

2026-10-11 사용자가 Kubernetes를 활성화한 뒤 Docker engine 28.3.0·Kubernetes 1.32.2의 실제 Ready node에서 검증했다. Helm 릴리스와 lake/log/metadata PVC가 준비됐고 실제 scheduler가 12개 태스크를 worker Pod로 실행했다. 첫 실험의 Pod 13개 중 Gold 첫 시도를 삭제했고 다른 Pod의 두 번째 시도가 성공했다. 로그 13개가 PVC에 남았다. 최종 이미지 재실행은 Pod 12개·로그 12개·7개 테이블 전체 해시 일치·Bronze 버전 불변을 확인했다. [공개 증거](../examples/kubernetes/README.md)에 두 실행을 분리해 보존했다.

이 검증은 단일 node와 소량 합성 원본이다. 운영 API/S3·다중 node 저장·peak 자원·순수 pool 대기·대규모 성능은 아직 검증하지 않았다. 현재 fixture 릴리스와 PVC는 추가 확인을 위해 남겨 두었고 시간 기반 schedule은 비활성화돼 있다.

근거: [Airflow 2.7 KubernetesExecutor](https://airflow.apache.org/docs/apache-airflow/2.7.0/core-concepts/executor/kubernetes.html), [chart 1.11.0 설정](https://airflow.apache.org/docs/helm-chart/1.11.0/parameters-ref.html), [2.7.3 공식 constraints](https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.11.txt).

## 이미지와 metadata DB

이미지는 검증한 Linux amd64 Airflow/JRE digest를 고정하고 Java를 별도 stage에서 가져온다. 기존 Airflow 이미지의 종료된 apt 저장소에 의존하지 않는다. Airflow는 기본 환경, Spark는 /opt/pipeline 가상환경을 사용하며 두 환경의 pip check와 Airflow provider의 compiled dependency import를 빌드 단계에서 실행한다. 기존 Airflow provider와 NumPy 2의 ABI 오류를 확인해 Airflow 환경의 NumPy만 1.26.4로 고정했다. pipeline 환경은 별도로 유지한다. DAG의 SSDR_SPARK_BINARY와 PYSPARK_PYTHON이 같은 pipeline 환경을 가리킨다.

기존 chart의 bitnami/postgresql:11은 실제 registry에서 manifest가 없어 그대로 설치할 수 없었다. 운영 후보 values는 embedded PostgreSQL을 비활성화하고 ssdr-metadata Secret으로 유지보수되는 외부 DB를 요구한다. fixture만 bitnamilegacy PostgreSQL 15.4의 확인한 amd64 digest를 사용한다. 보관 이미지이며 운영 DB 선택·보안 업데이트를 검증한 것은 아니다. 현재 Airflow/chart 버전은 검증한 호환 조합이며 유지보수 버전으로의 migration은 별도 로드맵이다.
