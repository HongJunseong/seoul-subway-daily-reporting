# 코드 구조와 실행 경로

처음에는 [README의 키 없는 데모](../README.md#키-없이-실행)와 [실행 증거 목록](../examples/README.md)을 본다. 데모는 API나 클라우드 계정 없이 같은 수집 이력·변환·보고서 계약을 재현한다.

## 주 처리 경로

| 위치 | 책임 |
|---|---|
| [src/ingestion](../src/ingestion) | 서울 API 호출과 원문·수집 시각·해시의 Bronze 저장 |
| [src/processing](../src/processing) | 자연키별 Silver 정제, 날짜·15분 구간 Gold 집계 |
| [src/quality/checks.py](../src/quality/checks.py) | 빈 입력·잘못된 날짜·키·수치의 출판 차단 |
| [src/report](../src/report) | 검증된 context, 결정적 Markdown, 입력 버전·해시 manifest |
| [src/alerts](../src/alerts) | 선택적 Discord 전송과 ETA 품질 검사 |
| [src/common](../src/common) | 설정·KST 날짜·HTTP·저장 경로의 공통 계약 |
| [src/jobs](../src/jobs) | Spark 세션 구성과 날짜 지정 승하차 backfill |
| [src/observability](../src/observability) | Delta 감사, 태스크·worker 자원 기록, 실험용 전체 행 해시 |

보고서의 승하차는 생산일 D-4 이용 날짜, 도착은 D-1의 수집 관측이다. ETA 알림은 실제 운행 지연을 판정하지 않는다. 공통 날짜 처리는 src/common/dates.py에 모으며 사용하지 않는 별도 역 Excel 로더는 유지하지 않는다.

## 실행과 플랫폼

| 위치 | 선택한 역할 |
|---|---|
| [dags](../dags) | 수집·처리·일간 집계·보고서의 의존성과 실행 구간 |
| [docker/dockerfile](../docker/dockerfile), [requirements](../requirements) | Airflow와 Spark Python 환경 분리, 역할별 의존성 목록 |
| [k8s](../k8s), [Kubernetes 실행 절차](KUBERNETES.md) | KubernetesExecutor worker Pod 자원·RBAC·lake/log PVC |
| [src/platforms](../src/platforms), [databricks](../databricks) | 공통 승하차 변환의 managed Delta adapter와 Free Edition Job·SQL 제공 실험 |

DAG는 arrival_collect_5m(도착 원본 수집), pipeline_arrival_5m(저장 원본의 Silver·15분 Gold·ETA 검사), usage_daily(D-4 수집·정제), gold_daily(일간 집계), report_daily(보고서) 다섯 개다. 같은 S3 Delta writer는 ssdr_delta_writer 1-slot pool을 공유한다.

Kubernetes는 태스크 실행 경로이고 Databricks는 별도의 관리형 처리·SQL 제공 검증 경로다. 둘을 함께 운영해야 하는 필수 구성으로 설명하지 않는다. 현재 운영 경로에는 Kafka가 없다. 기술 선택과 제한은 [ADR](adr/001-batch-first.md), [Free Edition 기능 선택](DATABRICKS_FREE.md)에 기록한다.

## 선택적 검증 도구

scripts의 CLI는 아래 문제를 재현하거나 실행 증거를 수집한다. 기본 운영 DAG와 혼동하지 않도록 출력은 새로운 ignored .work 경로에 둔다.

| 목적 | 도구와 절차 |
|---|---|
| 키 없는 재현과 보고서 검증 | demo.py, seed_bronze.py, verify_report.py — [RUNBOOK](RUNBOOK.md) |
| 품질·실패 복구·파일 유지보수 | audit_lake.py, recovery_experiment.py, compaction_experiment.py — [관측](OBSERVABILITY.md), [유지보수](STORAGE_MAINTENANCE.md) |
| 실제 원천 제한 관측과 저장 원본 재처리 | capture_sources.py, process_source_experiment.py, summarize_source_observations.py, run_source_observation_day.py — [원천 관측](SOURCE_OBSERVATION.md); API 호출은 별도 opt-in |
| Airflow 연결과 태스크 요약 | airflow_offline.py, summarize_tasks.py — [로컬 실행](AIRFLOW_OFFLINE.md) |
| Kubernetes 구성·실행·자원 증거 | check_kubernetes_render.py, check_kubernetes_overrides.py, kubernetes_fixture.py, capture_kubernetes_files.py, summarize_kubernetes_resources.py — [Pod 실행](KUBERNETES.md), [자원](KUBERNETES_RESOURCES.md) |
| managed Delta 계약과 실제 Job 증거 | managed_usage_experiment.py, record_databricks_run.py, record_databricks_free.py — [Databricks](../databricks/README.md) |
| 저장된 승하차 Bronze의 날짜·버전 고정 snapshot | [export_usage_snapshot.py](../scripts/export_usage_snapshot.py) — [선택적 export](../databricks/README.md#선택적-bronze-snapshot-export) |

실제 API·AWS·Databricks·Discord·LLM 실행은 필요한 인증과 명시적 opt-in을 갖춘 절차로 분리한다. 저장 원본 재처리는 과거 미수집 실시간 관측을 복원하지 못한다.

## 검증과 공개 범위

- [tests](../tests): 변환·품질·시간 경계·복구·플랫폼 계약의 회귀 검사.
- [.github/workflows/verify.yml](../.github/workflows/verify.yml): 파이프라인, Airflow, 컨테이너, Kubernetes 구성 검사.
- [examples](../examples/README.md): 공개 가능한 실행 증거와 합성/실수집 범위.
- [docs](../docs): 데이터 계약, 선택 근거, 재현·복구 절차. PROJECT_CONTEXT.md는 현재 설계, ROADMAP.md는 미완료 판단, SESSION_HANDOFF.md는 날짜별 실제 작업 기록이다.
- .work, .env, 실제 원문·정제 행·개인 인증 파일은 공개하지 않는다. 역사적 private export 출력의 ignore 규칙은 보존한다.

Secret 생성 진입점은 k8s/create-secret.sh 하나다. dotenv를 읽어 kubectl 표준 입력으로 전달하며 fixture 배포는 이 Secret을 사용하지 않는다. 과거 비활성 코드와 실행 메타데이터는 Git 이력에서 확인하고 현재 실행 경로에 별도 legacy 사본을 두지 않는다.
