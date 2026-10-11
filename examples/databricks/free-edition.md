# Free Edition 품질·변경 감사·SQL 제공 결과

2026-10-11 기존 Free Edition의 serverless Job과 SQL warehouse를 실제로 사용했다. 데이터는 합성이고 namespace는 workspace.ssdr_v2_fixture_quality_20261011_a다.

| 확인 | 결과 |
|---|---|
| 원래 날짜별 합계 | 20260128=680, 20260129=660 |
| 음수 최신 정정 | Silver 출판 차단, 이전 Silver/Gold 내용·버전 유지 |
| 정상 정정 후 | 20260128=700, 20260129=660 |
| 과거 Delta 버전 | 이전 680/660 및 전체 역 Gold 해시 유지 |
| 정정 원본 재처리 | 모든 업무 컬럼 해시 일치, Bronze 버전 불변 |
| SQL 제공 | view 3개, 품질 PASS 2일, 역/노선 순위 6행 |
| SQL warehouse | 실제 SELECT 4개 SUCCEEDED, notebook 전체 view 행과 일치 |
| Catalog 메타데이터 | view 유형·source와 같은 COMMENT 3개 확인 |

[품질 Job](https://dbc-e26a3e87-9542.cloud.databricks.com/jobs/1083946371422567/runs/544131271456497?o=7474660778526890)의 두 태스크는 SUCCESS, 전체 284.255초다. [최종 view 갱신](https://dbc-e26a3e87-9542.cloud.databricks.com/jobs/1083946371422567/runs/472610999319561?o=7474660778526890)은 serving_views만 선택 실행해 SUCCESS, 32.706초다. 최종 SQL 모델 source 해시 일치를 확인했다. 전체 Job 시간은 소량 검증 시간이며 처리량 벤치마크가 아니다.

[실제 JSON](free-edition-evidence.json)에는 Job·선택 실행·SQL statement ID, Delta 버전/operation과 업무 해시, 실제 SQL 결과가 있다. 생성 결과를 그대로 복사했으며 사용자 이메일·토큰·raw commit metadata는 공개하지 않았다.

## 설명할 수 있는 경험

공통 PySpark 변환을 serverless Job의 managed Delta MERGE로 실행하고 최신 불량 정정을 출판 전에 차단했다. 정정 전후를 Delta history/time travel로 확인하고 같은 원본 재처리 결과를 검증했다. Unity Catalog에 품질 조건·날짜 의미를 담은 view를 만들고 별도 SQL warehouse의 Statement API 결과가 notebook과 같은지 확인했다.

이 경험은 합성 관리형 처리·SQL 제공의 검증이다. 실제 AWS↔Databricks 통합 운영, enterprise 권한 관리·managed quality monitoring·분산 Spark 튜닝·대용량 비용 개선을 검증한 것은 아니다. [재현과 기술 선택 근거](../../docs/DATABRICKS_FREE.md)에 범위를 기록했다.
