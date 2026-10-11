# Kubernetes 실제 실행과 복구 증거

2026-10-11 Docker Desktop Kubernetes 1.32.2의 단일 Ready node에서 공식 Helm chart 1.11.0·Airflow 2.7.3·KubernetesExecutor를 실행했다. 전용 ssdr-fixture-20261011 namespace와 합성 Bronze만 사용했다.

| 실험 | 실제 태스크 | worker Pod | 시간 | 검증 |
|---|---|---|---|---|
| 장애·복구 | 12 success, API 수집 1 skipped | 13 | 376.452561초 | Gold 실행 중 worker 삭제 → 새 UID의 시도 2 성공 |
| 같은 원본 재처리 | 12 success, API 수집 1 skipped | 12 | 240.209483초 | 7개 테이블 전체 데이터 해시 일치, Bronze 버전 불변 |

- [장애·복구 실행](runtime-recovery.json): 실제 DAG/task 상태, Pod UID·시도·자원·이미지·종료 코드, 삭제한 worker와 재시도 결과.
- [첫 실행 PVC 확인](runtime-recovery-files.json): Delta 루트별 행 수·전체 해시·버전, 종료·삭제된 worker의 로그 13개 bytes/hash.
- [같은 원본 재실행](runtime-replay.json): 최종 이미지에서 새 수동 실행의 Pod 12개 성공과 보고서 해시 검증.
- [재실행 PVC 비교](runtime-replay-files.json): 전체 테이블 해시·원본 버전 일치, worker 로그 12개 보존.
- [공식 chart 렌더](evidence.json)와 [태스크 17개 병합·직렬화](overrides.json)는 구성 검사다. 이 두 파일은 actual_pod_execution=false이며 실제 실행 JSON은 true다.

이용 날짜 20260128 합계 680, 20260131 도착/유효 ETA 4/4의 합성 보고서를 검증했다. 동일 데이터 재처리는 Silver/Gold의 새 Delta 커밋을 만들지만 행 내용은 같고 Bronze 버전은 유지됐다. worker 로그의 원문은 공개하지 않는다.

최종 재실행의 worker image ID는 sha256:15fbf2798c006efef73add9d61f747b0f6e1eaabe28e3135f9ed314fa10f7d4c다. 첫 실험의 이미지 ID는 runtime-recovery.json에 별도로 보존했다. Airflow 환경의 NumPy ABI 문제를 수정한 최종 이미지에서 provider import와 두 Python 환경의 pip check를 통과했다.

fixture schedule은 None이고 API·AWS·Discord·LLM Secret을 주입하지 않는다. Spark는 Pod 내부 local[2]다. RWO PVC는 단일 node 검증이며 분산 Spark, 다중 node 로그, peak CPU/메모리·순수 pool 대기·운영 SLA·대규모 처리량은 측정하지 않았다. 시간에는 태스크 Pod 준비·Spark/JVM 시작·DAG 트리거가 포함되고 첫 실험에는 재시도 대기도 포함된다.

전체 Helm manifest와 Secret 값은 공개하지 않는다. [재현 절차](../../docs/KUBERNETES.md)에 배포·원본 준비·삭제 실험·재처리 비교를 기록했다.
