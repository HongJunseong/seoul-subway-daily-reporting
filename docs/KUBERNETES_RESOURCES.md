# Kubernetes worker 자원 측정과 조정

작업 종류별 자원 프로필을 실제 worker 측정으로 확인한다. 2026-10-11 전용 합성 namespace에서 같은 Bronze를 두 번 처리하고 control 예약량·CPU 상한을 조정했다. [실제 전후 결과](../examples/kubernetes/resources.md)는 Pod UID, 실제 자원 설정, 이미지 ID, callback 로그와 Delta 재처리 검사를 연결한다.

## 측정 대상과 실패 조건

fixture-values.yaml만 SSDR_TASK_RESOURCE_METRICS=true를 설정한다. 기본 운영 설정은 비활성이다. 새 서비스나 metrics-server 권한을 추가하지 않고 worker 프로세스가 cgroup v2 파일을 읽는다. scheduler callback에 worker 시작 monitor가 없으면 unavailable로 표시한다. kernel이 제공하는 CPU·메모리 상한과 해당 worker Pod limits가 일치해야 공개 검증에 통과한다.

| 수치 | 범위 |
|---|---|
| memory_container_peak_bytes | memory.peak: 컨테이너 시작부터 terminal callback까지의 kernel 최고값. JVM·자식 프로세스·파일 cache 포함. RSS나 태스크 코드만의 메모리가 아니다. |
| sampled_memory_peak_bytes | 약 1초마다 읽은 memory.current의 최고값. kernel 최고값과 구분한다. |
| mean_cpu_cores | 시작 callback부터 종료 callback까지 cpu.stat 사용량 증가 / 단조 시계 경과 시간. 컨테이너 bootstrap 전체 CPU를 포함하지 않는다. |
| sampled_peak_cpu_cores | 최소 0.25초인 표본 구간의 최대 평균 CPU. 더 짧은 순간 burst는 측정하지 않는다. 짧은 태스크에 해당 구간이 없으면 null이다. |
| cpu_throttled_periods / cpu_periods | 관측 구간에서 CPU quota 제한이 발생한 period 수 / 전체 period 수. throttled_seconds는 wall-clock 지연이나 절약 시간을 뜻하지 않는다. |
| executor_queue_seconds | Airflow queued_dttm부터 start까지. Pod 생성·scheduling·bootstrap 포함이며 순수 pool 대기가 아니다. |

CPU는 CFS period 경계·파일 읽기 시점·짧은 구간의 영향을 받는다. 표본 구간 평균이 설정된 quota와 조금 다를 수 있어 순간 peak나 CPU limit 위반으로 단정하지 않는다. sampler 자체의 작은 비용도 포함한다. terminal callback 이후 종료 작업은 측정하지 않는다.

관측 파일 누락·counter/시계 역행·읽기 오류는 unavailable/partial이다. 성공한 12개 worker의 callback이 모두 있어야 검증하며 missing/partial·OOM·다른 cgroup 상한·충돌 이벤트·유효하지 않은 숫자를 거부한다. 강제 종료된 worker에 callback이 없으면 정상 0으로 채우지 않는다. 이 수집기는 성공한 합성 실행의 기준 측정이며 강제 종료 자원 관측을 대체하지 않는다.

## 재현

[Kubernetes 기본 절차](KUBERNETES.md)로 새 전용 namespace·PVC·합성 원본·pool 1 slot을 준비한다. 같은 lake의 다른 writer가 없어야 한다. 모든 시간 기반 schedule은 None, 서비스 Secret은 주입하지 않는다. 기존 출력 경로를 덮어쓰지 않는다.

아래 명령은 base Python으로 Airflow 실행/로그를, pipeline Python으로 Delta 파일 검사를 수행한다. 두 환경을 섞지 않는다.

```bash
kubectl --context docker-desktop -n ssdr-fixture-demo exec deployment/ssdr-fixture-scheduler -c scheduler -- \
  python /opt/airflow/scripts/kubernetes_fixture.py \
  --namespace ssdr-fixture-demo \
  --output-dir /opt/airflow/fixture-lake/evidence/resources-a \
  --run-id manual__kubernetes_fixture_resource_a
kubectl --context docker-desktop -n ssdr-fixture-demo exec deployment/ssdr-fixture-scheduler -c scheduler -- \
  python /opt/airflow/scripts/summarize_kubernetes_resources.py \
  --runtime-evidence /opt/airflow/fixture-lake/evidence/resources-a/evidence.json \
  --log-root /opt/airflow/logs \
  --output /opt/airflow/fixture-lake/evidence/resources-a/resources.json
kubectl --context docker-desktop -n ssdr-fixture-demo exec deployment/ssdr-fixture-scheduler -c scheduler -- \
  /opt/pipeline/bin/python /opt/airflow/scripts/capture_kubernetes_files.py \
  --runtime-evidence /opt/airflow/fixture-lake/evidence/resources-a/evidence.json \
  --output /opt/airflow/fixture-lake/evidence/resources-a/files.json
```

자원 조정 후에는 다른 run ID·출력 경로로 실행하고 files.json 생성에 이전 files.json의 --compare를 지정한다. 공개 JSON은 실제 생성 파일을 복사한다. 원문 callback 로그·보고서는 PVC와 ignored .work에만 보존한다.

## 선택의 근거와 남은 검증

최초 control의 DAG 트리거가 예약 메모리 256MiB보다 많은 약 463MiB를 사용했다. 해당 트리거의 CPU 관측 period 40개 모두 제한에 걸렸다. control 메모리 요청을 512MiB로, CPU 요청/상한을 250m/1로 조정했다. 메모리 상한 1Gi는 유지한다. 요청은 scheduler의 예약 기준이며 실제 사용량의 hard limit과 구분한다.

Spark는 local[2]·driver heap 1g이며 요청/상한 2Gi/3Gi와 CPU 1/2를 유지한다. report의 512Mi/2Gi와 CPU 250m/1도 유지한다. 작은 입력의 여유만으로 운영 자원을 줄이지 않는다. 모든 Spark writer의 ssdr_delta_writer 1-slot pool은 유지한다.

단일 node·소량 합성 두 실행은 운영 용량이나 성능 개선의 인과관계를 입증하지 않는다. 운영 원본 크기·동시 실행·node 경쟁과 순수 pool 대기를 분리 측정한 뒤 자원 예산과 SLA를 판단한다.
