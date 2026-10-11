# Free Edition에서 검증한 데이터 엔지니어링 기능

Databricks는 공통 승하차 변환의 관리형 실행과 SQL 제공을 검증하는 별도 경로다. 현재 Airflow/S3 운영 후보 경로와 자동 연결하지 않는다. 같은 변환을 두 곳에서 중복 운영해야 한다는 주장이 아니다. 기능은 처리 정확성·변경 감사·SQL 소비라는 문제를 기준으로 선택했다.

| 기능 | 해결하는 문제 | 실행 증거 |
|---|---|---|
| Serverless Job·bundle | notebook 수동 실행의 의존성·재현 가능한 배포 | 품질 실험 → view 생성, 두 태스크 SUCCESS |
| Delta MERGE·품질 차단 | 불량 최신 정정이 정상 결과를 덮어쓰는 문제 | 음수 정정 거부, Silver/Gold 내용·버전 불변 |
| History·time travel | 정정 전후 결과와 입력 버전 확인 | 과거 680, 정정 후 700, 인접 날짜 660 보존 |
| Unity Catalog view·COMMENT | 원천 테이블을 직접 사용하는 소비자의 의미 혼동 | 날짜·합성 표시·역/노선 identity·품질 조건을 SQL 모델로 정의 |
| Serverless SQL warehouse | notebook 계산과 SQL 소비의 분리 | 세 view와 Catalog 메타데이터, 실제 Statement API 네 쿼리 성공 |

## 실제 실행

- 전용 schema: workspace.ssdr_v2_fixture_quality_20261011_a. 기존 default와 ssdr_v2_fixture 데이터는 보존했다.
- [품질 실험 Job](https://dbc-e26a3e87-9542.cloud.databricks.com/jobs/1083946371422567/runs/544131271456497?o=7474660778526890): Run 544131271456497, 두 태스크 SUCCESS, 전체 284.255초.
- [최종 SQL view 갱신](https://dbc-e26a3e87-9542.cloud.databricks.com/jobs/1083946371422567/runs/472610999319561?o=7474660778526890): Run 472610999319561, serving_views만 실행해 SUCCESS, 32.706초. 품질 실험 태스크는 제외했고 원본을 다시 만들지 않았다.
- 기존 warehouse b58bef668eb3cbc9: serverless 2X-Small, auto-stop 10분. 새 warehouse나 유료 클러스터를 만들지 않고 기존 설정을 유지했다.
- notebook과 warehouse의 view 전체 행이 일치했다. 실제 적용한 SQL 정의의 SHA256도 현재 저장소 source와 일치했다.
- [전체 공개 증거](../examples/databricks/free-edition-evidence.json)와 [결과 설명](../examples/databricks/free-edition.md). Job/SQL statement ID는 실제 API 응답이며 임의 생성하지 않았다.

소량 합성 fixture의 전체 시간이다. SQL 준비·Spark 질의·검사와 Job 실행을 포함하며 운영 SLA·대규모 처리량·유료 환경 비용 비교가 아니다.

## 품질 실패와 정정

처음 두 날짜의 합계는 680/660이다. 첫 날짜의 최신 음수 정정을 Bronze에 append하면 publish_silver가 DataQualityError로 차단한다. 이 실패가 기존 Silver·Gold 행과 Delta 버전을 변경하지 않았는지 확인한다.

그 뒤 정상 정정을 append하고 첫 날짜만 재처리하면 700/660이 된다. Gold의 이전 Delta 버전을 읽어 680/660과 이전 전체 업무 컬럼 해시가 유지됐는지 확인한다. 정정된 Bronze를 다시 처리한 결과도 동일하며 재처리가 Bronze를 변경하지 않는다. 잘못된 원문은 Bronze의 감사 이력에 남고 최신 정상 정정 선택으로 복구한다.

이 실험은 versionAsOf를 통한 읽기와 정정 원본 재처리다. RESTORE 명령 실행·VACUUM·장기 백업을 검증했다고 설명하지 않는다. time travel 가능 기간은 로그와 데이터 보존 정책의 영향을 받는다. [공식 History·보존 설명](https://docs.databricks.com/aws/en/tables/history).

## SQL 소비 계약

SQL source는 databricks/sql/의 세 파일이다.

- usage_quality_status_v2: 빈/잘못된 키·수치, 중복 자연키, Silver/Gold 합계 및 역/노선별 행 내용, 계층별 누락 날짜를 확인한다.
- daily_usage_overview_v2: PASS인 이용 날짜의 승차·하차·합계·역/노선 수를 제공한다.
- station_usage_rank_v2: PASS인 이용 날짜의 역·노선별 순위를 제공한다. 환승역의 다른 노선을 합치지 않는다.

품질 실패 날짜는 분석 view에 노출하지 않고 품질 view에 FAIL로 남긴다. 합계가 같아도 잘못된 역 identity는 실패해야 한다. 역명을 바꾸거나 Gold 날짜를 제거하는 로컬 실제 Delta 회귀 검사에서 FAIL과 분석 노출 차단을 확인했다. 실제 warehouse 실험은 정상 복구 결과와 메타데이터 조회를 검증했다.

이 품질 view는 직접 구현한 SQL 계약이다. Databricks managed quality monitoring, enterprise RBAC나 column lineage 감사를 수행했다고 주장하지 않는다. 이용 날짜는 실제 원천 날짜이며 당일 혼잡도나 실시간 승객 대기시간이 아니다. 모든 view에 synthetic 표시와 COMMENT를 둔다.

## 재현

공식 CLI OAuth profile과 기존 Free Edition warehouse를 사용한다. 인증 값은 .work 등 ignored 경로에 두고 출력하지 않는다. 매 품질 실험에는 새로운 schema가 필요하다. 기존 실험 테이블이 있으면 덮어쓰기 대신 중단한다.

```bash
databricks bundle validate -t free --profile SSDR_PORTFOLIO
databricks bundle deploy -t free --profile SSDR_PORTFOLIO
databricks bundle run usage_quality -t free --profile SSDR_PORTFOLIO \
  --params namespace=workspace.ssdr_v2_fixture_quality_my_run
python scripts/record_databricks_free.py --profile SSDR_PORTFOLIO \
  --run-id YOUR_SUCCESSFUL_RUN_ID \
  --namespace workspace.ssdr_v2_fixture_quality_my_run \
  --warehouse-id YOUR_EXISTING_WAREHOUSE_ID --execute-sql \
  --output-dir .work/databricks-free/my-run
```

정상 Gold를 보존하며 SQL 모델만 갱신하려면 bundle deploy 후 같은 namespace로 --only serving_views를 실행한다. 기록 도구의 --serving-run-id에 이 선택 실행의 ID를 추가한다. 도구는 성공한 실제 선택 태스크와 다른 태스크의 제외 상태를 확인한다. 같은 schema의 다른 writer와 동시에 실행하지 않는다.

SQL 기록은 명시적인 --execute-sql opt-in이다. Job 결과가 성공하고 SQL source 해시가 일치할 때 기존 warehouse에 bounded SELECT만 실행한다. 불완전 chunk·truncation·누락 행·실패 상태는 성공 증거로 저장하지 않는다. query의 실행이 제한 시간을 넘으면 자신이 만든 statement만 취소한다. workspace SQL 쿼리 이력에서 공개 statement ID로 실행을 확인할 수 있다.

## 범위와 도입 판단

현재 주 경로는 Airflow의 수집·변환·보고서와 S3 저장 후보다. Databricks 실험은 동일 변환을 관리형 테이블에서 실행하고 SQL 소비 계약을 제공하는 경험을 증명한다. 운영 경로로 채택하려면 실제 입력 연결·실수집 기간·운영 부담·비용을 비교해야 한다. 필수 기술로 설명하지 않는다.

Free Edition은 serverless notebook과 2X-Small warehouse, 제한된 사용량을 제공한다. 운영 보장과 enterprise 기능 경험을 이 실험으로 확대하지 않는다. 사용량·지원 기능은 변경될 수 있으므로 [공식 Free Edition 제한](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)을 확인한다. 이번 작업에서는 AWS·서울 API·Discord·LLM을 호출하지 않았다.

파이프라인 회귀 94 passed(111.09초), 최종 SQL 차단 대상 2 passed(63.62초), native recorder 대상 9 passed(0.12초). 대상 검사는 전체 94개에 포함된다. 별도 Airflow/Pod 12개는 이전 검증이며 현재 전체 서로 다른 검사 수는 106개다.
