# 실제 worker 자원과 control 조정

2026-10-11 Docker Desktop Kubernetes 1.32.2의 기존 전용 ssdr-fixture-20261011 namespace에서 같은 합성 Bronze를 두 번 처리했다. Airflow 2.7.3·chart 1.11.0·KubernetesExecutor이며 Spark는 Pod 내부 local[2]다. 첫 실행은 portfolio-v3, 조정 후는 portfolio-v4 이미지다. 서울 API·AWS·Databricks·Discord·LLM 호출은 없다.

| 항목 | 조정 전 | 조정 후 |
|---|---|---|
| 실제 worker / 성공 태스크 | 12 / 12 | 12 / 12 |
| API 수집 태스크 | 1 skipped | 1 skipped |
| control CPU 요청 / 상한 | 100m / 500m | 250m / 1 |
| control 메모리 요청 / 상한 | 256Mi / 1Gi | 512Mi / 1Gi |
| control 최대 메모리 | 462.89MiB | 463.99MiB |
| control 최대 메모리 / 요청 | 1.8082 | 0.9062 |
| DAG 트리거 CPU 제한 period | 40 / 40 | 0 / 21 |
| 같은 트리거 태스크 실행 시간 | 4.326457초 | 2.329449초 |
| control 최대 executor queue | 13.368605초 | 7.611208초 |
| 전체 실행 시간 | 213.322658초 | 175.796396초 |

DAG 트리거는 다른 DAG를 로드해 실행을 연결한다. 자식 DAG 완료를 기다리는 태스크는 아니다. 처음 약 463MiB의 사용량은 메모리 예약 256MiB를 넘었으나 상한 1Gi에는 도달하지 않았다. 메모리 예약을 512MiB로 올리고 CPU 예약·상한을 조정했다. 두 번째 실행의 메모리도 약 464MiB였으며 관측한 트리거 CPU period에서 throttling은 없었다. 다른 태스크의 짧은 구간 평균 CPU나 queue 변화까지 모두 설정 변경의 효과로 단정하지 않는다.

Spark 최대 메모리는 1216.44→1186.43MiB, report는 381.01→325.96MiB였다. Spark CPU 1/2·메모리 2Gi/3Gi, report CPU 250m/1·메모리 512Mi/2Gi는 유지했다. 작은 입력의 여유만으로 운영 자원을 줄이지 않는다.

## 실제 파일과 연결

- [조정 전 자원](resources-before.json)·[실제 실행](resources-before-runtime.json)·[Delta와 로그 확인](resources-before-files.json)
- [조정 후 자원](resources-after.json)·[실제 실행](resources-after-runtime.json)·[Delta와 로그 비교](resources-after-files.json)
- [최종 chart 렌더](resources-config.json)·[태스크 17개 병합·직렬화](resources-overrides.json)

자원 JSON은 실제 성공 worker의 DAG/task/run/시도와 PVC terminal callback을 연결하고 kernel CPU·메모리 상한이 해당 Pod limits와 일치하는지 검사했다. 모든 worker에서 읽기 오류·OOM 증가가 없었다. 각 실행의 로그 12개가 PVC에서 읽혔다.

7개 Delta 테이블 루트의 전체 행 해시·행 수가 같고 Bronze usage 버전 2·arrival 버전 5가 유지됐다. 이전 장애·재처리 실험의 결과와도 전체 행 해시가 같다. Silver/Gold 버전 2→3은 같은 원본 재처리의 새 커밋이다. 합성 보고서의 이용량 680, D-1 도착/유효 ETA 4/4와 manifest 해시를 두 번 모두 확인했다.

자원과 native 실행 JSON은 실제 생성 파일 그대로 복사했다. 첫 자원 결과는 최종 collector 계약으로 재검증해 byte 동일성을 확인했다. runtime JSON 자체는 worker 상태 기록으로 자원 측정이 없다는 limitation을 유지하며 별도의 resources JSON이 callback 계측을 제공한다. 구성 JSON은 actual_pod_execution=false, 실제 실행·자원 JSON은 true다. 원문 로그와 보고서는 공개하지 않는다.

실제 worker image ID는 조정 전 sha256:4051f60185dd42847279904b75e9eafa41829bdc2349eb262b1eef792e530052, 조정 후 sha256:f74c87c1d0003407041d402bbad7920d685939a6fbb46be7252ec1cc2f82bd00다. Pod UID와 전체 image_id는 JSON에 보존했다.

## 해석 범위

메모리는 bootstrap·JVM·자식 프로세스·cache를 포함해 terminal callback까지의 kernel peak다. CPU는 callback 관측 구간 평균이며 순간 peak가 아니다. 0.25초 미만만 관측한 짧은 태스크의 표본 CPU peak는 null이다. 종료 콜백 이후 활동·종료 이벤트가 없는 강제 종료 worker는 이 계측으로 완전히 관측하지 못한다.

executor queue는 Pod 생성·scheduler·bootstrap 시간을 포함하며 순수 pool 대기를 분리한 수치가 아니다. writer pool은 1 slot을 유지했다. 단일 node·합성 두 실행이므로 운영 SLA·처리량·성능 개선율·운영 크기의 메모리 적정성을 주장하지 않는다. 더 큰 원본·동시 실행·순수 pool 대기와 유지보수 플랫폼 검증이 남는다. [재현과 계산 정의](../../docs/KUBERNETES_RESOURCES.md)를 함께 제공한다.
