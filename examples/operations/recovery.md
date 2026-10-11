# 장애·복구 실험

> 합성 fixture와 실제 로컬 Delta 테이블로 실행한 예외 주입 실험입니다.

검증 성공: True

## Silver 저장 직후 중단

중단 지점: after Silver commit, before Gold

| 단계 | Silver 합계 | Gold 합계 | Silver 버전 | Gold 버전 |
|---|---:|---:|---:|---:|
| before | 660 | 660 | 0 | 0 |
| after_failure | 680 | 660 | 1 | 0 |
| recovered | 680 | 680 | 2 | 1 |
| replayed | 680 | 680 | 3 | 2 |

복구 처리 시간: 4.032초

## 잘못된 최신 원천 값 차단

중단 지점: latest source count is negative, rejected before Silver write

| 단계 | Silver 합계 | Gold 합계 | Silver 버전 | Gold 버전 |
|---|---:|---:|---:|---:|
| before | 660 | 660 | 4 | 3 |
| after_failure | 660 | 660 | 4 | 3 |
| recovered | 660 | 660 | 5 | 4 |

복구 처리 시간: 4.273초

## 검증 의미

- 중단된 처리의 기존 Gold를 유지하고 저장된 Bronze로 정정 결과를 복구했습니다.
- 같은 원본의 반복 재처리에서 업무 컬럼 전체의 SHA-256이 유지됐습니다.
- 품질 오류를 올바른 원천 정정으로 해결했고 이전 정상 결과가 복원됐습니다.
- Delta 버전은 테이블 전체의 커밋 버전입니다. 파일명·버전은 재처리로 바뀔 수 있습니다.

## 측정 한계

- 실제 프로세스 종료·S3 장애·Kubernetes 복구를 시험한 결과는 아닙니다.
- 시간은 소량 로컬 fixture 처리의 측정값이며 Spark 시작·검증 읽기를 제외합니다.
- 실제 운영 RTO·SLA·pool 대기 시간으로 사용하지 않습니다.
