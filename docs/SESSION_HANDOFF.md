# 세션 인계

## 2026-10-10 완료한 로컬 고도화

사용자는 Kafka가 필수가 아니라면 제거하고, DE 포트폴리오 고도화·커밋 스킬·agent 파일을 자율적으로 진행하도록 요청했다.
개발 브랜치에서 변경을 로컬 커밋으로 기록했으며 push와 실서비스 배포는 실행하지 않았다.

### 구현

- 저장소용 write-commit 스킬과 AGENTS.md, 맥락·로드맵·인계 문서를 추가했다.
- v2 경로에서 원문·수집 lineage 보존, 승하차 최신 정정, 날짜별 멱등 교체, 도착 snapshot 중복 제거를 구현했다.
- KST 자정·월말 경계를 처리하고 결측 ETA·유효 표본 분모·지표 해석을 보완했다.
- 수집 DAG를 독립시키고 일간 수집→Gold→보고서 자동 실행을 연결했다. 알림과 보고서 의존성을 분리했다.
- Kafka 완료 이벤트 경로를 제거했다. S3 writer는 공유 ssdr_delta_writer pool(1 slot)로 직렬화한다.
- 기본 보고서는 결정적 템플릿이며 context·Delta 버전·해시 manifest·실제 KST 관측 범위를 남긴다.
- 키 없는 Spark/Delta end-to-end 데모, 합성 샘플, CI 정의와 실행/복구 문서를 추가했다.

### 실제 검증 결과

- .venv: Python 3.11.15, Spark 3.5.7, Delta 3.2.0, Java 11.
- `pytest -q --ignore=tests/test_dags.py`: **28 passed**, 68.26초. 실제 Spark/Delta 통합 케이스 4개를 포함한다.
- 합성 데이터 표시 보완 후 report/publication 관련 검사 재실행: **10 passed**.
- 격리된 Airflow 2.7.3 환경의 `pytest -q tests/test_dags.py`: **6 passed**, 0.60초. 기존 Airflow/Pydantic 관련 경고 7개가 있었으며 DAG import 실패는 없다.
- Ruff correctness 검사, Python compileall(별도 pycache prefix), Secret bash 구문 검사, git diff --check 통과.
- 커밋 스킬 quick_validate: **Skill is valid!**
- Docker Compose config --quiet 통과.
- 최종 데모 출력: .work/demo/portfolio-v2. examples/에는 공개 샘플의 경로만 정규화하여 보존했다.
- 실험: 원문 3행을 두 번 저장 → Silver 3행·합계 660 유지, 최신 정정 후 680. 월말 자정 경계 6개 관측 중 유효 ETA 5개. 공개 샘플과 원본 출력의 manifest 검증 통과.
- 데모 실행 시간은 examples/evidence.json에 기록했다. 소량 로컬 실험의 시간이며 처리량·SLA로 사용하지 않는다.

### 로컬 커밋

- 96b02aa: agent 지침·한국어 커밋 스킬.
- fd09514: 원본 보존·품질·멱등 처리·시간 경계와 회귀 검사.
- 2279025: 지표 해석·리포트 provenance·발행 해시 검사.
- c5865fd: 수집/처리 DAG·Kafka 제거·writer pool·실행 설정·CI/데모.
- 포트폴리오 문서·샘플 커밋은 git log에서 확인한다.

### 남은 실운영 검증

Docker Desktop engine이 실행 중이지 않아 이미지 빌드/전체 scheduler 실행을 하지 못했다. Kubernetes/Helm·S3·실제 서울 API 지속 수집·Discord·LLM·원격 GitHub Actions도 검증하지 않았다. 실제 키·서비스와 메시지를 사용하는 검증은 별도 범위다.

다음 작업은 RUNBOOK과 VALIDATION의 live 절차에 따라 실데이터 결측·응답 상한·신선도 baseline, 원본 재처리 복구 시간, Delta 파일 수/크기를 측정하는 것이다. 합성 데이터로 이 항목들을 완료 처리하지 않는다.

### 보존·접근 기록

기존 k8s/create-secret.sh는 초기 Windows Git에서 수정으로 표시되어 보존했다. v2는 새 create-secret-v2.sh를 사용한다. legacy의 Kafka 설정은 현재 실행 경로에서 사용하지 않는다.

UNC 경로 apply_patch와 기본 WSL 접근은 제한되어 승인된 WSL Bash 세션으로 편집·검증·로컬 커밋을 진행했다. 기존 root 소유 dags/__pycache__에 쓰지 않도록 compileall은 .work/pycache prefix를 사용했다.

.env.example 스테이징은 자동 검토가 사용자 변경으로 오인해 한 번 거부했다. 초기 WSL clean 상태와 이번 작업의 diff(비어 있는 키, v2·선택 기능·Compose 설정) 확인 후 재검토가 허용했다. 사용자 변경을 함께 커밋하지 않았다.

## 2026-10-10 후속 고도화 — 관측·복구 증거

사용자의 “좋아 다음작업 진행해줘”에 따라 실운영 측정을 준비하는 도구와 로컬 장애·복구 실험을 추가했다. 외부 API·AWS·Discord·LLM 호출, push·배포는 하지 않았다.

### 구현과 확인한 결과

- `scripts/audit_lake.py`: 로컬 Delta 루트를 읽어 입력 버전, 자연키 중복/원천 정정, Silver·Gold 전체 업무 컬럼 일치, 관측 bucket/미처리 snapshot, ETA 결측/오류, 구간 내 최신 관측 나이, 활성 파일 수·크기·읽기 시간을 JSON/Markdown으로 저장한다.
- 열린/잘못된/24시간 초과 구간, 잘못된 날짜·키·수치·snapshot, 빈 입력과 누락 테이블이 healthy로 통과하지 않도록 검사했다. 감사 종료 코드는 healthy=0, degraded=1, failed=2다.
- 5개 DAG에 정보가 제한된 태스크 lifecycle callback을 연결했다. 재시도 예정 실패도 on_retry_callback으로 기록하고 최종 실패 알림과 구분한다. 실패 알림은 유지한다. Airflow 2.7의 실제 실행 시도 번호, 실행 시간, executor queue, scheduled interval→start를 구분한다.
- `scripts/summarize_tasks.py`: 복사한 태스크 로그를 읽어 중복 전달·재시도·불완전/충돌 이벤트를 구분한다. 로그가 없거나 malformed면 exit 2다.
- `scripts/recovery_experiment.py`: 실제 로컬 Delta 커밋 후 예외를 주입한다. Silver=680·Gold=660의 중간 상태에서 저장된 원본을 재처리해 Gold=680을 복구했다. 다시 재처리해 Silver·역 Gold·노선 Gold의 업무 컬럼 전체 해시와 Bronze 불변을 확인했다.
- 최신 원천 음수 값이 Silver 쓰기 전에 차단되고 이전 Silver/Gold 내용·버전이 유지됐다. 올바른 원천 정정 후 이전 정상 결과 660을 복원했다.
- 실행 경로: `.work/recovery/verified-01`. 복구 처리 시간은 각각 **4.032318초, 4.272651초**다. 소량 로컬 fixture의 변환 시간으로 Spark 시작·검증 읽기를 제외하며 운영 SLA/RTO가 아니다.
- 최종 감사: 4개 시간 bucket 중 3개 관측, ETA 유효 5/전체 6, 미처리 snapshot 0, 승하차 Silver/Gold 일치. 의도적인 미관측 bucket 1개와 명시적 as-of의 1200초 관측 나이로 degraded 판정. CLI exit 1을 확인했다.
- 공개 합성 증거는 `examples/operations/`, 절차는 `docs/OBSERVABILITY.md`다. CI에 같은 복구 실험과 artifact 보존을 추가했다.

### 실제 검증

- `.venv/bin/pytest -q --ignore=tests/test_dags.py`: **60 passed**, 66.31초. 실제 Spark/Delta 통합 검사 포함.
- 격리 Airflow 2.7.3 환경의 `pytest -q tests/test_dags.py`: **8 passed**, 최종 0.98초. 기존 Airflow/Pydantic 경고 9개.
- 감사·이벤트 대상 검사: **32 passed**, 1.70초. 전체 60개에 포함된다. 이후 재시도 callback을 추가하고 이벤트 검사 **12 passed**, 0.38초를 확인했다. 새 회귀 1개를 포함하므로 현재 서로 다른 검사 수는 파이프라인 61개·DAG 8개다.
- Ruff correctness 검사·별도 pycache prefix의 compileall·git diff --check 통과. CI YAML 파싱과 Markdown 내부 링크를 확인했다.
- 실제 복구 실험 두 시나리오 verified=true. 공개 파일은 생성 결과를 복사했으며 정상 판정을 위해 경고를 지우지 않았다.
- 태스크 이벤트 CLI는 합성 로그의 결과/종료 코드로 검사했다. 실제 scheduler를 실행해 수집한 시간 표본은 아직 없다.

### 남은 작업과 한계

실수집 기간의 결측·응답 상한·신선도 baseline, 실제 scheduler 실행/대기 시간, 배포 환경의 S3 복구와 파일 읽기 성능을 측정해야 한다. 로컬 감사 도구는 최대 24시간의 날짜 파티션을 pandas로 읽으며 대규모/S3 감사로 검증하지 않았다. 후보 파일 bytes는 실제 IO 비용이 아니다.

이번 장애 실험은 예외 주입이며 실제 프로세스 강제 종료·Kubernetes 장애·S3 장애를 재현한 것은 아니다. 테이블별 커밋의 중간 상태를 원본 재처리로 복구한 증거다. 미수집 과거 도착 관측은 복원할 수 없다.

### 접근·보존 기록

이번 작업 시작 시 Linux Git은 clean이었다. 기존 Secret 스크립트·.env·서비스 데이터는 수정하거나 스테이징하지 않았다. 승인된 WSL 세션에서 구현·검사·로컬 커밋을 진행했다. 초기 PyArrow 탐색 중 native 종료가 있어 메타데이터 projection과 동기 pandas 읽기로 바꿨고 이후 실제 감사 CLI·회귀 검사는 정상 종료했다. crash dump는 로그/커밋에 포함하지 않도록 ignore했다.

로컬 커밋은 개발 브랜치의 git log에서 확인한다. 다음 검증은 live 자료와 운영 환경 접근이 포함되므로 이번 완료 범위와 구분한다.

후속 로컬 커밋: af1b05d(Delta 감사), 050e59c(태스크 관측), 78406d3(복구 실험). 문서·샘플 커밋은 바로 다음 git log 항목에 기록한다.

## 2026-10-10 추가 고도화 — 실제 로컬 Airflow 연결 실행

사용자의 “다음작업도 진행해줘 더필요한거아냐?”에 따라 DAG 구조 검사와 실제 태스크 실행 사이의 검증 공백을 보완했다. API·AWS·Discord·LLM과 운영 DB를 사용하지 않고 합성 Bronze·별도 SQLite로 진행했다.

### 구현

- `scripts/seed_bronze.py`는 반복 수집·최신 정정·월말 자정 경계의 합성 Bronze만 준비한다. Silver·Gold·보고서는 미리 만들지 않는다.
- `scripts/airflow_offline.py`는 Airflow 설정 import 전에 출력 경로의 DB·로컬 저장·템플릿·합성 표시를 고정한다. 상속 Airflow 서비스 설정을 제거하고 서비스 키를 비우며 HTTP 요청을 차단한다.
- 실제 `TaskInstance.run()`으로 의존성 검사, 날짜 XCom, API 재수집 분기, Spark BashOperator, callback, 보고서 callable을 실행한다. 트리거가 실제 생성한 queued DagRun의 ID/conf를 확인해 같은 실행을 이어 처리한다.
- `dags/common.py`는 SSDR_PROJECT_DIR/SSDR_SPARK_BINARY와 shell quoting을 지원한다. 로컬 모드는 Delta만 제출하고 S3 모드의 Hadoop AWS/SDK 패키지는 유지한다.
- 가상환경 Python symlink를 resolve하면 spark-submit 디렉터리를 잃는 문제를 막고, 기존 출력 보존·DB 격리·공백/따옴표/달러 경로·로컬/S3 패키지·자정 경계를 회귀 검사로 남겼다.
- Airflow 보고서용 의존성을 `requirements/airflow-verification.txt`로 분리했다. CI의 airflow-integration job은 같은 실제 실행을 수행하고 실패 시에도 artifact를 보존한다. 원격 CI는 실행하지 않았다.

### 실제 실행 결과

- 최종 경로: `.work/airflow-offline/verified-03`, CLI exit 0.
- usage_daily → gold_daily → report_daily 모두 success. 실제 태스크 **12개 success**, bronze_usage **1개 skipped**. 시도 번호 모두 1이며 미실행 API 태스크는 0이다.
- 실제 callback **24개**, 종료된 시도 12개·시간 표본 12개. malformed/failed/incomplete/conflicting 0. executor queue·scheduled 시작 지연 표본은 0개이며 미측정이다.
- 승하차 정정 합계 **680**, D-1 도착 관측 **4건·유효 ETA 4건**, report/context/manifest 해시 검증 통과.
- DAG 실행 전체 시간 **92.934660초**. 합성 원본 준비·DB 초기화를 제외하고 Spark 제출·JVM 시작은 태스크 시간에 포함한다. 소량 로컬 시간이며 운영 SLA/처리량이 아니다.
- public JSON은 실제 결과를 그대로 복사해 `examples/airflow/`에 보존했다. README·VALIDATION·ROADMAP·PROJECT_CONTEXT와 AIRFLOW_OFFLINE 절차를 갱신했다.

### 실제 검증과 수정 기록

- 파이프라인 `pytest -q --ignore=tests/test_dags.py`: **67 passed**, 64.10초.
- 격리 Airflow 2.7.3 `pytest -q tests/test_dags.py`: **8 passed**, 0.67초. 기존 Airflow/Pydantic 경고 9개. 서로 다른 회귀 검사 총 75개.
- 신규 범위 대상 검사 **6 passed**, 0.10초이며 위 67개에 포함된다.
- Ruff, 별도 pycache prefix compileall, CI YAML 파싱, Markdown 상대 링크 29개, git diff --check 통과. 공개 evidence/task-summary JSON과 원본 결과의 동일성을 확인했다.
- 초기 환경 준비에서 Python symlink 경로 확인을 수정했다. 첫 DB 실행은 조회한 DagRun의 .dag를 연결하지 않아 중단됐고 보완했다.
- 두 번째 실행은 실제 세 DAG·보고서까지 성공했으나 검증용 D-1 유효 ETA 기대값을 3으로 잘못 지정해 최종 비교가 실패했다. NULL ETA는 다음 날 자정에 속해 D-1의 유효 표본은 4가 맞다. fixture 날짜별 기대값과 회귀 검사를 추가한 최종 실행에서 전체 조건이 통과했다. 이전 실패 출력은 덮어쓰지 않았다.
- 로컬 루프는 scheduler의 queued_duration을 만들지 않아 관련 Airflow 경고가 있다. queue를 0초로 대신 채우지 않는다.

### 남은 작업과 보존

실제 scheduler·분산 executor·Docker/Kubernetes·S3 writer의 동시성/복구, 실제 API 지속 수집·응답 상한·결측·신선도 baseline은 여전히 미검증이다. TaskInstance의 실행 시간과 운영 queue/pool 대기를 구분한다. live 로드맵 항목은 완료로 바꾸지 않았다.

이번 시작의 Linux Git 상태는 clean이었다. .env·기존 Secret 스크립트·운영 데이터는 변경/스테이징하지 않았다. Windows Git의 ownership 경고는 글로벌 설정을 바꾸지 않고 WSL Git으로 확인했다. 변경은 개발 브랜치의 기능별 로컬 커밋으로 기록하며 push·배포는 하지 않는다.

로컬 구현 커밋: 33f3121(실제 Airflow 연결 실행·CI·회귀 검사). 문서와 공개 증거는 다음 로컬 커밋에 기록한다.

## 2026-10-11 플랫폼 고도화 — Databricks와 첫 컨테이너 실행

이 절은 Kubernetes 활성화 전의 중간 기록이다. 최종 이미지·회귀 수·실제 클러스터 결과는 아래 후속 검증 절을 기준으로 한다.

사용자는 Databricks와 Kubernetes를 포트폴리오에서 실제로 활용하도록 요청했다. 기존 Free Edition workspace를 지정하고 공식 CLI OAuth 인증을 완료했다. Docker/WSL도 직접 켰다. 합성 fixture Job과 로컬 컨테이너 검증을 실행했으며 push·서울 API·AWS·Discord·LLM 호출은 하지 않았다.

### 구현

- 공통 승하차 변환에 ANSI-safe try_cast/try_to_timestamp를 적용했다. malformed 수치·overflow·불량 revision 날짜·빈 입력은 정상 출판되지 않는다.
- managed_usage adapter는 기존 변환을 Unity Catalog managed Delta에 적용한다. 입력 Delta 버전을 고정하고 날짜 제한 MERGE로 인접 날짜를 보존한다. Gold는 별도 테이블 트랜잭션이며 재처리로 부분 커밋을 복구한다.
- 네 notebook과 databricks.yml은 seed → Silver → Gold → 재처리 검증 Job을 정의한다. max_concurrent_runs=1, 일정 없음, 전용 workspace.ssdr_v2_fixture namespace만 합성 실험에 사용했다.
- 내부 Parquet 병합 export를 중단했다. Delta 루트/versionAsOf/date 필터로 독립 snapshot을 만들고 lineage·버전 manifest를 남긴다. S3 export는 --allow-s3 opt-in이다.
- scripts/record_databricks_run.py는 실제 Job/task SUCCESS와 native 실행·원본 불변·680/660·로컬 전체 업무 컬럼 해시 비교를 요구한다.
- KubernetesExecutor control/spark/report 프로필을 실제 다섯 DAG에 연결했다. image/env/Secret reference/PVC/service account를 보존하고 모든 Spark writer의 pool 1 slot을 유지한다.
- 공식 chart 1.11.0, Airflow 2.7.3, provider 7.8.0/client 23.6.0의 렌더·PodGenerator 병합·DAG 직렬화 검사와 CI artifact를 추가했다.
- fixture는 시간 기반 schedule을 제거하고 live Secret을 주입하지 않는다. synthetic 보고서는 lake PVC의 reports에 저장한다.
- Docker 이미지에 source·fixture·재현 도구를 포함했다. Airflow와 pipeline Python을 분리하고 고정 JRE stage와 양쪽 pip check를 사용한다.
- 운영 metadata DB는 외부 ssdr-metadata Secret으로 연결하도록 바꿨다. default bitnami PostgreSQL 11 image는 실제 registry에 없었다. fixture만 확인한 bitnamilegacy PostgreSQL 15.4 amd64 digest를 사용하며 운영 검증으로 설명하지 않는다.

### 실제 결과

- Databricks Job 1103992482650397, Run 44336808137994: 네 태스크 모두 SUCCESS. Spark.version=4.2.0, Job 전체 197.11초.
- 날짜별 합계 20260128=680, 20260129=660. Bronze 버전 0 불변, Silver/역 Gold/노선 Gold 버전 3.
- 로컬 managed Delta Spark 3.5.7과 Silver 6행·역 Gold 6행·노선 Gold 4행의 모든 업무 컬럼 SHA256 일치. examples/databricks/에 실제 생성 결과를 복사했다.
- 첫 native Run 471413789256360은 errorifexists 모드를 serverless가 거부해 실패했다. 지원되는 error 모드로 첫 테이블을 생성한 두 번째 Run에서 성공했다. 실패 기록은 보존했다.
- 로컬 OSS saveAsTable overwrite/replaceWhere가 기존 managed 테이블을 truncate하는 문제를 확인해 날짜 제한 MERGE로 교체했다.
- 첫 bundle sync의 넓은 glob이 dependency/cache를 포함했다. 비밀 파일 미업로드를 확인하고 sync.paths를 세 source 디렉터리로 제한했다. 해당 bundle 경로의 불필요한 파일만 제거했다. 기존 default 테이블과 타 notebook은 보존했다.
- 공식 chart의 base/fixture 두 렌더 성공. Airflow의 실제 태스크 17개 Pod 병합·직렬화 성공. public evidence의 actual_pod_execution=false는 그대로 유지했다.
- 최종 Docker 이미지 빌드 성공, image ID sha256:ebd43dbe4b8b002993fa050ffde800702fd0c325d4eb3e83598c85de278f138d. 두 pip check 정상.
- 컨테이너 ssdr-airflow-fixture-20261011-a: exit 0, OOMKilled=false. usage_daily → gold_daily → report_daily 12개 태스크 success·API 수집 1개 skipped·각 시도 1회.
- 컨테이너 내부 연결 실행 55.935353초, 이용량 680·D-1 도착/유효 ETA 4/4·보고서/context 해시 검증 성공. 복사한 보고서도 verify_report로 검증했다. examples/container/에 증거 보존.
- 이미지 내부에서도 Kubernetes 프로필을 활성화해 실제 태스크 17개 직렬화와 모든 fixture schedule=None을 확인했다.
- 소량 합성 시간은 운영 SLA·처리량·대규모 성능으로 해석하지 않는다. 컨테이너 실행은 순차 TaskInstance.run이며 scheduler/분산 executor 실행이 아니다.

### 회귀·빌드 검증

- pipeline: 77 passed, 77.40초 (tests/test_dags.py·test_kubernetes_profiles.py는 별도 환경).
- Airflow/Pod: 12 passed, 0.81초, 기존 Pydantic 경고 8개. 총 89개.
- Airflow 마지막 재실행에서 PYTHONPATH의 dags 경로를 누락해 import 오류가 났고, 문서/CI와 같은 PYTHONPATH 설정으로 수정한 실행에서 통과했다.
- Ruff·별도 pycache compileall·git diff --check 통과. CI YAML와 문서 상대 링크는 최종 확인한다.
- 기존 Airflow base의 pgdg 저장소 종료·Debian 패키지 404로 apt 빌드가 실패했다. digest가 고정된 호환 JRE stage로 해결했다.
- 기존 한 Python 환경은 aiobotocore/botocore 충돌이 있어 분리했다. base PIP_USER 설정이 pipeline venv 설치에 적용되는 오류도 PIP_USER=false로 해결했다.
- Windows에서 전체 컨테이너 폴더를 복사하면 scheduler 로그 symlink 권한 오류가 있어 JSON/보고서만 별도 verified-02 경로로 보존했다. 실패한 부분 복사는 .work에 남겨 덮어쓰지 않았다.
- 로컬 증거: .work/databricks-native/verified-02, .work/managed-usage/verified-01, .work/kubernetes/render-fixture-public, .work/container/verified-02. OAuth/config/deploy 로그는 ignored .work이며 공개/커밋하지 않는다.

### 남은 작업

Docker engine 28.3.0은 응답하지만 docker-desktop Kubernetes API는 127.0.0.1:6443 연결 거부다. 사용자에게 Settings → Kubernetes의 Enable/Running 상태를 확인해 달라고 요청했다. Helm 설치·PVC binding·scheduler→Pod 실행·다른 Pod로의 실패 재시도·Pod 삭제 후 로그 보존은 미검증이다.

유지보수 중인 Airflow/chart/provider와 운영 metadata DB 조합, 실제 Pod 메모리/CPU·queue/pool 대기, 실수집/API·S3 baseline·원격 CI는 후속 검증이다. fixture DB 보관 이미지를 운영용으로 권장하지 않는다. 기존 사용자 파일·.env·Secret과 실서비스 데이터는 보존했다.

write-commit 스킬에 따라 기능별 로컬 커밋을 진행한다. 개발 브랜치에서 기록하며 push하지 않는다.

## 2026-10-11 후속 검증 — 실제 Kubernetes 장애 복구와 재처리

사용자가 Docker Desktop Kubernetes를 활성화했다. 기존 docker-desktop node는 Ready, Kubernetes 1.32.2였고 기존 리소스는 보존했다. 새 namespace ssdr-fixture-20261011에 fixture만 설치했다. API·AWS·Discord·LLM 호출과 push는 수행하지 않았다.

### 최종 실행 결과

- 실제 Helm 설치와 migration 완료, scheduler 2/2·triggerer 2/2·webserver 1/1. lake/log 각 2Gi와 metadata 8Gi의 RWO PVC가 Bound다.
- 첫 실제 scheduler/KubernetesExecutor 실행 runtime-01: 세 DAG success, 태스크 12개 success·API 수집 1개 skipped, worker Pod 13개, 376.452561초.
- Silver 성공 후 실제 running Gold worker UID 121b7c6d-0d19-4167-b100-87df3f946867을 UID precondition으로 종료했다. scheduler가 재시도했고 새 UID b9f0ab4f-2995-40c4-8b15-7995e586ce9c, 시도 2가 exit 0/Succeeded로 끝났다.
- 종료·삭제된 worker 로그 13개가 PVC에서 읽혔다. 7개 Delta 테이블 루트의 전체 컬럼 해시·행 수·버전을 기록했다.
- 최종 이미지의 runtime-02: 세 DAG success, 태스크 12개 success·API 수집 1개 skipped, worker Pod 12개, 240.209483초. 로그 12개 보존.
- 두 실행의 Delta 테이블 7개 전체 데이터 해시 일치, Bronze usage 버전 2/arrival 버전 5 불변. Silver/Gold는 버전 0→1의 새 커밋이며 행 내용은 같다. 보고서 이용량 680·D-1 도착/유효 ETA 4/4, 복사한 보고서 manifest도 검증했다.
- 최종 image ID sha256:15fbf2798c006efef73add9d61f747b0f6e1eaabe28e3135f9ed314fa10f7d4c. 컨테이너 b의 순차 TaskInstance 실행도 exit 0/OOMKilled=false, 12 success·1 skipped, 79.324771초다. 실제 scheduler 증거와 구분한다.
- examples/kubernetes의 runtime-* JSON은 실제 생성 파일을 복사했다. 구성 JSON은 actual_pod_execution=false, 실제 실행 JSON은 true다. examples/container는 최종 b 실행으로 갱신했다.
- private 경로: .work/kubernetes/runtime-01, runtime-02, reports-runtime-01, reports-runtime-02, render-fixture-bootstrap; .work/container/final. Secret·OAuth·원문 로그·보고서는 커밋하지 않는다.

### 수정과 최종 검사

- 첫 Helm --wait는 post-install migration hook과 ready init 사이에서 기다렸다. 자신의 새 릴리스만 정리·재설치했고 fixture bootstrap Job을 일반 Job으로 변경해 해결했다. PVC와 기존 클러스터 리소스는 보존했다.
- Docker Desktop 활성화 전 준비했던 자체 kind fixture cluster는 workload를 배포하지 않았다. Docker ownership label을 확인하고 자신의 kind cluster만 삭제했다. 실제 실행은 docker-desktop에서 수행했다.
- 첫 이미지의 Airflow provider는 NumPy 2 ABI 오류를 출력했다. pipeline Python은 유지하고 Airflow NumPy 1.26.4·Deprecated 1.3.1로 맞췄다. 양쪽 pip check, shapely/google.cloud.bigquery import를 빌드에서 검증했다. 최종 scheduler import와 native 재실행에는 ABI 오류가 없다.
- fixture bootstrap 렌더 검사에서 create-user Job의 실제 component label이 create-user-job인 것을 확인해 guard를 수정했다. 최종 렌더와 태스크 17개 병합·직렬화가 통과했다.
- 파이프라인 전체 81 passed, 123.15초. 새 PVC 비교 검사 2 passed, 0.01초. 별도 Airflow/Pod 12 passed, 1.20초와 기존 Pydantic 경고 8개. 서로 다른 검사 총 95개.
- Airflow pip check: No broken requirements found. 최종 Docker build의 양쪽 pip check와 ABI import, 순차 컨테이너 실행, 실제 Kubernetes 재실행이 모두 성공했다.
- 최종 Ruff correctness·별도 pycache compileall·git diff --check 통과. CI YAML의 4개 job과 문서 내부 링크 47개, 공개 JSON 10개와 생성 원본의 byte 동일성을 확인했다. 실제 runtime JSON을 동일 검증 함수로 재검사하고 Delta snapshot 비교도 통과했다. 원격 CI는 미실행이다.

### 남은 작업과 보존

실제 서울 API 장기 수집의 결측·신선도·페이지 상한 baseline, 운영 S3 읽기/쓰기·재처리 복구, peak Pod 메모리/CPU·queue/pool 대기와 유지보수 Airflow/chart/provider·metadata DB 조합이 남았다. 단일 node의 소량 합성 시간을 SLA·대용량 처리량으로 해석하지 않는다. Spark는 local[2]이며 분산 Spark가 아니다.

fixture 릴리스와 PVC는 추가 확인을 위해 보존했고 모든 시간 기반 schedule은 None이다. 기존 사용자 데이터·Secret·.env·Git 저자를 보존했다. write-commit 스킬로 목적별 로컬 커밋을 기록하며 push하지 않는다.

최종 로컬 구현 커밋: 29b7080(Databricks managed Delta), a3b17f6(Kubernetes 자원·실제 복구·재처리 검사), ff17b97(CI). 문서와 공개 실행 증거는 다음 docs(portfolio) 커밋에 기록한다.

## 2026-10-11 Free Edition — 품질 차단·변경 감사·SQL 제공

사용자는 기술을 무분별하게 넣지 않고 Free Edition으로 Databricks 경험을 실제로 설명할 수 있게 고도화하도록 요청했다. 기존 OAuth profile과 serverless 2X-Small warehouse를 사용했고 유료 설정·AWS·서울 API·Discord·LLM 호출·push는 하지 않았다.

### 구현과 실제 결과

- usage_portfolio는 공통 managed_usage 변환을 호출한다. 새 schema workspace.ssdr_v2_fixture_quality_20261011_a를 만들고 기존 실험 테이블이 있으면 중단한다. 실패 데이터를 reset하거나 기존 schema를 덮어쓰지 않는다.
- 음수 최신 정정이 Silver 출판에서 차단되고 기존 Silver/Gold 전체 업무 해시·버전이 유지됐다. 정상 정정 후 700/660, time travel의 과거 680/660·역 Gold 이전 해시, 최신 원본 재처리의 전체 해시·Bronze 버전 불변을 확인했다.
- 실제 quality Job 1083946371422567, Run 544131271456497: 두 태스크 SUCCESS, 전체 284.255초. source 수정은 이 실행이 끝난 뒤 배포했다.
- 최종 SQL 품질 조건을 추가해 serving_views만 선택한 Run 472610999319561: SUCCESS, 32.706초. quality_experiment는 제외했고 새 원본을 만들지 않았다. 두 실행의 증거를 분리한다.
- 실제 적용 SQL 정의 SHA256과 현재 저장소 source가 일치했다. 기존 warehouse b58bef668eb3cbc9, serverless 2X-Small, auto-stop 10분을 변경하지 않았다.
- SQL Statement API의 품질·일간 이용량·역/노선 순위·Catalog 메타데이터 네 쿼리 SUCCEEDED. notebook의 세 view 전체 행이 warehouse 결과와 같다. 분석 view COMMENT는 합성 데이터·날짜 의미를, 품질 view COMMENT는 자체 SQL 계약을 설명한다.
- SQL 분석은 PASS 날짜만 제공한다. 합계가 같아도 잘못된 역 identity는 FAIL·분석 제외, Gold 날짜 누락도 FAIL·분석 제외를 실제 로컬 Delta 검사로 확인했다. warehouse 음수/identity 장애 주입은 수행하지 않았다.
- recorder는 실제 성공한 Job/태스크/선택 실행, namespace·정정 값·source 해시를 요구한다. SQL chunk/truncation/누락 행/실패·warehouse 결과 차이를 성공으로 저장하지 않는다. 첫 warehouse 결과는 LONG enum 미처리로 비교 실패했고 타입 정규화·회귀 보완 후 새 경로에서 재검증했다. COMMENT 검사도 모든 문장에 synthetic이 있다는 추정 대신 실제 SQL source 문장과의 일치로 수정했다. 최종 verified-03과 실패 출력은 각각 보존했다. SELECT 실행은 --execute-sql opt-in이고 timeout 시 자신의 statement만 취소한다.
- private: .work/databricks-free/verified-03, quality run/start/deploy 로그. 공개 free-edition-evidence.json은 실제 생성 파일을 그대로 복사했다. 인증 파일·raw Job response와 사용자 이메일·원문 commit metadata는 커밋하지 않는다.

### 검사와 범위

- 최종 파이프라인 94 passed, 111.09초. SQL 차단 대상 2 passed, 63.62초; native recorder 9 passed, 0.12초. 대상은 전체에 포함된다. 기존 별도 Airflow/Pod 12개를 포함한 현재 서로 다른 검사는 106개다.
- 앞선 전체 92 passed(115.60초) 후 SQL PASS 소비 조건·선택 실행 검증을 보완했다. 실제 선택 실행의 비선택 태스크는 SKIPPED/DISABLED이고 실제 SQL API의 BIGINT type_name은 LONG인 것을 반영해 guard/decoder를 보완했다. 마지막 전체 94개 검사는 이 수정도 포함한다.
- Ruff·별도 pycache compileall·diff whitespace·bundle validate/deploy를 확인했다. 문서 내부 링크 46개, 실제 public JSON의 byte 동일성, native SQL source hash 3개, SQL statement 4개와 두 bundle Job 설정을 최종 확인했다.
- Free Edition의 관리형 실행·변경 감사·SQL 소비 경험이다. AWS 통합 운영·enterprise RBAC·managed quality monitoring·장기 백업·분산 Spark 튜닝·대용량 비용 비교로 설명하지 않는다.
- S3 운영·실수집 baseline·peak 자원·유지보수 Airflow 조합은 기존 로드맵대로 남는다. 사용자 파일·.env·기존 테이블을 보존했고 commit skill에 따라 로컬 커밋한다.

Free Edition 구현 로컬 커밋: 3eecbfb(품질 차단·Delta 변경 감사·SQL 소비 계약·native 기록·회귀 검사). 문서와 공개 실행 증거는 다음 docs(databricks) 커밋에 기록한다.

## 2026-10-11 실제 원천 제한 관측·재처리

사용자에게 서울 API 소량 호출을 질문했고 허용받았다. 기존 로컬 API 키를 사용했으며 AWS·Databricks·Kubernetes·Discord·LLM 호출·push는 수행하지 않았다.

- capture_sources는 명시적 --fetch와 새 .work 경로, 1..3개 도착 슬롯·300초 이상 간격·승하차 최대 10페이지·자동 retry/redirect 없는 논리 요청 집계를 사용한다. 실패는 예외 종류만 기록하고 빈/불완전 원본을 정상 0건으로 저장하지 않는다. 지연 슬롯의 따라잡기 요청도 피한다.
- 실제 시작 2026-10-11T17:49:53.329397+09:00, 마지막 관측 2026-10-11T17:59:53.438266+09:00. 5분 간격 도착 세 번·승하차 한 번, 논리 GET 4회. 도착 Bronze 3000행, Silver 2987행; 응답 상한 3/3; 표본 노선 19·역/노선 identity 682.
- D-4 승하차 20261007 원천 617행·Silver 617행, Silver/역 Gold 전체 업무 행 일치. 이용 날짜와 관측 날짜를 구분했다.
- 유효 ETA 2987, NULL ETA 0, 누락 원천 시각 0; 원천 생성 시각 나이 중앙값 68.0초·p95 517.0초·최대 1201.0초. 운행 지연이나 승객 대기시간으로 해석하지 않는다.
- process_source_experiment는 저장된 완료 캡처만 읽고 기존 Bronze/Silver/Gold 코드로 새 로컬 Delta lake를 처리했다. 같은 원본 재처리에서 7개 테이블 전체 컬럼/행 SHA256·행 수 일치, Bronze 버전 불변. timestamp 해시는 마이크로초를 보존한다. 최초 처리·재처리·해시 비교 64.225656초다.
- 품질 상태 degraded, 실제 issue [{"severity": "warning", "code": "arrival_response_at_limit"}, {"severity": "warning", "code": "arrival_source_clock_ahead"}]. ALL 응답 상한 때문에 전체 네트워크 관측을 정상 완전 데이터로 설명하지 않는다.
- 원천 반복 자연키 13개는 모두 서로 다른 payload와 recptnDt였다. 같은 capture에서 hash 순서로 과거 관측이 선택될 수 있음을 발견해 ingested_at→recptn_dt→record_hash 순서로 보완했다. source 시각이 없는 행보다 확인된 최신 source 시각을 먼저 선택한다. 실제 저장 원본의 정제 행에서도 가장 최신 source 시각 선택을 별도 확인했다. 원천이 미래인 관측은 시각을 보정하지 않고 경고한다.
- 감사에 표본 identity 수와 원천 생성 시각 나이 중앙값/p95를 추가했다. 원천 시각 없는 fallback 0초는 분포에서 제외한다.
- 최종 회귀: 111 passed in 162.27s (0:02:42). 첫 전체 106 passed, 135.61초 후 opt-in·naive 시각·상한 거부 검사와 작은 표시 시계 차이 검사를 보완했다. 두 번째 전체 108 passed, 135.44초도 보존했다. 첫 저장 원본 검사에서 단조 시계와 표시 시각 간 최대 0.01908초 차이로 계획 시각 검사가 실패했다. 실제 시각을 변경하지 않고 1초 범위만 허용하는 guard와 초과 거부 회귀를 추가한 뒤 새 처리 출력 경로에서 재실행했다. 실패 로그와 이전 처리 결과도 보존했다. 새 source 테스트는 외부 API를 호출하지 않는다. 이전 Airflow/Pod 12개는 이번에 재실행하지 않았다.
- Ruff·별도 pycache compileall·git diff --check 확인. 공개 examples/observed/evidence.json은 생성 원본과 byte 동일성·재처리 조건·기존 API 키 미포함을 확인해 복사했다.
- private: .work/source-observation-20261011-01 (captured raw JSON), .work/source-processing-20261011-03 (lake·audit·evidence), .work/source-capture-01.log, source-processing-01.log(시각 guard 실패), source-processing-02.log(기존 hash 순서), source-processing-03.log(최종 source 시각 순서), regression-source-04.log. 원문·정제 데이터·.env는 커밋하지 않는다.

10분 관측은 장기 baseline의 시작이다. 전체 역/시간대 커버리지 정책·같은 키의 모든 retry를 포함한 일일 요청 예산·장기 신선도·S3 저장/복구·플랫폼 실수집 운영·peak 자원은 미완료다. 이 세 snapshot의 일간 Gold는 일부 관측 요약이며 미수집 D-1 보고서는 만들지 않았다. 사용자 변경·기존 데이터·Git 저자를 보존하며 write-commit 스킬의 로컬 커밋을 기록한다.

구현 로컬 커밋: a30a6f4(같은 수집의 최신 원천 생성 시각 선택), 536d768(제한 수집·원본 처리·감사·회귀). 문서와 실제 집계 증거는 다음 docs(portfolio) 커밋에 기록한다. 원격 push는 하지 않는다.

## 2026-10-11 Kubernetes worker 자원 계측·실측 조정

사용자의 다음 단계 요청에 따라 실제 Pod 자원과 대기 관측을 진행했다. 기존 전용 ssdr-fixture-20261011 릴리스와 합성 Bronze/PVC를 사용했으며 외부 API·AWS·Databricks·Discord·LLM 호출과 push는 하지 않았다. 기존 사용자 파일·원천·Secret·Git 저자를 보존했다.

### 구현과 실행 결과

- fixture에서만 SSDR_TASK_RESOURCE_METRICS=true를 켜 worker 시작/종료 callback에서 읽기 전용 cgroup v2 monitor를 연결했다. kernel 메모리 최고값, 최소 0.25초 표본의 CPU 구간 평균, CPU quota 제한·OOM 증가를 기록한다. 기본 운영 환경은 계측 비활성이다.
- collector는 native worker UID·시도·이미지·resources와 PVC의 성공 이벤트를 연결한다. cgroup 상한과 Pod limits 일치가 필수이며 누락/부분 읽기·scheduler 수치·OOM·충돌 이벤트·비정상 수치·잘못된 CPU 표본 범위를 검증 성공으로 기록하지 않는다. 짧은 CPU 미관측 peak는 null이다.
- metrics-server API가 없었다. 추가 cluster 권한/addon 대신 worker cgroup을 사용했으며 실제 worker에서 scope 일치를 확인했다.
- portfolio-v3 조정 전 run manual__kubernetes_fixture_metrics_a: 12 success·1 수집 skipped·worker 12개·213.322658초. 실제 이미지 manifest ID 4051f60185dd42847279904b75e9eafa41829bdc2349eb262b1eef792e530052.
- control 최대 memory.peak 485371904 bytes(462.89MiB), 요청 256Mi의 1.8082배였다. usage_daily.trigger_gold_daily는 관측 구간 CPU 제한 40/40 period, 태스크 4.326457초, executor queue 12.648656초였다. 상한 1Gi를 넘은 OOM은 아니다.
- control CPU 요청/상한을 100m/500m→250m/1, 메모리 요청을 256Mi→512Mi로 조정했다. 메모리 상한 1Gi와 Spark/report 설정·writer pool 1 slot은 유지했다. 값 선택은 docs/KUBERNETES_RESOURCES.md에 기록했다.
- portfolio-v4 조정 후 run manual__kubernetes_fixture_metrics_b: 12 success·1 수집 skipped·worker 12개·175.796396초. 실제 이미지 manifest ID f74c87c1d0003407041d402bbad7920d685939a6fbb46be7252ec1cc2f82bd00.
- 같은 트리거 메모리 486531072 bytes(463.99MiB), 새 요청의 0.9062배. 관측 CPU 제한 0/21 period, 태스크 2.329449초, executor queue 7.001932초. control 최대 executor queue는 13.368605→7.611208초다.
- Spark 최대 메모리 1216.44→1186.43MiB, report 381.01→325.96MiB. 소량 합성 입력이므로 자원을 줄이지 않았다. CPU는 구간 평균이며 순간 peak와 순수 pool 대기는 측정하지 않았다. 전후 두 실행의 시간 차이를 운영 성능 개선율로 해석하지 않는다.
- 두 실행 모두 7개 Delta 루트 전체 행 해시·행 수 일치, Bronze usage 버전 2·arrival 버전 5 불변, Silver/Gold 버전 2→3이다. 이전 runtime-replay 결과와도 전체 해시 일치. 합성 보고서 이용량 680·도착/유효 ETA 4/4·manifest 해시 검증. worker 로그 각각 12개가 PVC에 보존됐다.
- native runtime의 limitation은 상태 증거 자체에 자원 계측이 없다는 범위로 유지했다. 별도 resources JSON이 worker 계측을 제공한다. 첫 자원 JSON을 최종 collector 계약으로 다시 읽어 byte 동일성을 확인했다.
- Helm 최종 렌더 base/fixture, 같은 v4 이미지·fixture 계측 활성/기본 비활성·namespace 권한·Secret 없는 환경과 태스크 17개 실제 병합·직렬화 검사 통과. 구성 JSON actual_pod_execution=false는 유지했다. CI Docker 태그도 현재 v4와 맞췄으나 원격 CI는 실행하지 않았다.

### 검사와 증거 보존

- 최종 파이프라인 전체 137 passed, 142.99초. 별도 Airflow/Pod 12 passed, 1.68초, 기존 Pydantic 경고 8개. 서로 다른 검사 총 149개다. 자원·callback 대상 38 passed, 0.36초는 전체에 포함된다.
- 첫 전체 122 passed, 156.64초 후 collector 계약 검사를 추가했다. 최종 137개는 schema·CPU 표본 범위·cgroup scope·누락/충돌/OOM/비정상 수치 거부를 포함한다. 모든 테스트는 외부 서비스를 호출하지 않았다.
- 두 Docker build와 Helm upgrade --wait 성공. 최종 Ruff 전체·별도 pycache compileall·diff whitespace 확인. 문서 내부 링크 74개·공개 JSON 8개·CI job 4개와 원본 byte 동일성 및 기존 비밀 값 미포함 검사를 통과했다.
- 공개 examples/kubernetes/resources-* JSON 8개는 native/렌더 원본을 그대로 복사했다. raw 로그·보고서·Helm 전체 manifest/notes·인증 파일은 공개하지 않는다.
- private: .work/kubernetes/resources-01, resources-02, render-metrics-final-base, render-metrics-final-fixture; docker-metrics-build.log, docker-metrics-v4-build.log, helm-metrics-upgrade.log, helm-metrics-v4-upgrade.log, kubernetes-resources-01.log, kubernetes-resources-02.log, regression-metrics-02.log, airflow-metrics-01.log.
- 최초 공개 복사 준비 helper에서 compare_snapshots에 JSON 전체를 넘겨 타입 검사가 실패했다. snapshots 부분으로 고쳐 native 비교와 공개 복사를 확인했다. 공개 복사 이전 검증 helper의 오류이며 native/PVC 실행 결과는 변경하지 않았다.

### 남은 작업

단일 node 소량 합성 범위에서 control 자원 예약·CPU 제한을 측정하고 조정했다. 순수 pool 대기·더 큰 원본/동시 작업의 자원·실수집 장기 커버리지·운영 S3 쓰기/복구·유지보수 Airflow/chart/provider와 metadata DB는 남는다. fixture 릴리스/PVC와 이전 이미지 태그는 확인용으로 보존했고 시간 기반 schedule은 None이다. 승인된 write-commit 스킬 범위에서 기능별 로컬 커밋하며 push하지 않는다.

구현 로컬 커밋: f3c1eca(worker 계측·collector 계약·회귀·fixture opt-in), 64c0d12(실측 control 조정·v4 이미지/CI 태그). 문서와 실제 전후 증거는 다음 docs(portfolio) 커밋에 기록한다. 원격 push는 하지 않았다.

## 2026-10-11 닫힌 시간 compaction·조회 표본·재처리

사용자의 다음 고도화 요청에서 로드맵의 파일 수/읽기 실측 항목을 진행했다. 기존 실수집 도착 3,000행이 활성 파일 6개인 것을 계기로 기존 원본을 바꾸지 않는 별도 합성 실험을 만들었다. 외부 API·AWS·Databricks·Kubernetes·Discord·LLM 호출·배포·push는 없으며 기존 fixture/원본/사용자 변경·Secret·Git 저자를 보존했다.

### 구현과 최종 결과

- scripts/compaction_experiment.py는 새 .work 하위의 합성 lake만 만들며 기존 lake/출력 경로를 받지 않는다. 25..48 snapshot×1..1000행의 bounded 입력만 허용하며 기본 36,000행이다. fingerprint의 driver 수집도 48,000행 이하로 제한한다.
- 기존 save_to_bronze_delta로 KST Jan31 22:00부터 Feb01 00:55까지 5분 합성 입력을 append한다. 실제 수집 시각으로 해석하지 않는다. Spark 3.5.7 local[2]·Delta Spark 3.2.0이다.
- 닫힌 Jan31 23시 파티션만 DeltaTable.optimize().where(predicate).executeCompaction()으로 합친다. KST/정각/자정/월말·열린 시간·미래·predicate 입력을 검사한다. 파일 스캔은 format(delta) root/versionAsOf, 파일 목록은 활성 add metadata다. 내부 Parquet를 직접 읽지 않는다.
- 최종 .work/compaction-20261011-03: Bronze 36,000행 전체 해시 일치, 대상 파일 24→1, bytes 1,532,105→1,174,152, 전체 파일 72→49. 인접 48개 파일의 path/size 해시 유지.
- 유지보수 Bronze 버전 35→36, add/remove dataChange=false. 이후 과거 버전 35의 전체 행을 다시 읽어 동일 해시를 확인했다. 월말 자정 00:00/00:15 Silver 24,000행·15분 Gold 2,000행 전체 해시 유지, 원본 재처리 중 Bronze 버전 36 불변.
- 반복 compaction은 added/removed 0·버전 불변. VACUUM·과거 파일 삭제·자동 일정·운영 S3 writer는 추가하지 않았다.
- 고정 전후 버전을 각각 warm-up하고 5쌍씩 번갈아 조회했다. 한 시간 12,000행의 중앙값 0.661564→0.172950초, 마지막 15분 3,000행은 0.559446→0.156256초. count-only shortcut 대신 ETA 정수 합과 raw_payload 문자 수도 비교했다. 모든 표본을 공개했다.
- 최종 compact 0.939750초·seed 36.227858초·전체 검증 72.477143초. 시작/seed 단계는 별도 Spark 대상 검사와 일부 겹쳐 SparkUI는 4041을 사용했다. 조회 표본은 대상 검사 종료 후 측정했고 전체 시간을 격리된 처리량 벤치마크로 해석하지 않는다.
- 최초 실험 01도 24→1·전체 행/다운스트림 보존을 통과했다. 이후 과거 전체 snapshot 재검사를 추가할 때 광범위 문자열 교체가 함수 선언까지 바꿔 syntax error를 만들었다. 02는 실행 시작 전 문법 오류로 실패했고 lake를 만들지 않았다. 선언을 복구한 최종 03의 실제 실행으로 보완한 guard까지 확인했다. 초기 결과·02 실패 로그는 보존했다.
- 문서 STORAGE_MAINTENANCE.md·examples/operations/compaction.*에 읽기 표본과 해석 범위, 데이터 계약의 유지보수/원본 재처리 차이를 기록했다. CI pipeline job에 같은 실험과 evidence.json artifact를 추가했지만 원격 CI는 실행하지 않았다.

### 검사와 보존

- 전체 파이프라인 158 passed, 143.81초. Airflow/Pod 12 passed, 1.03초, 기존 Pydantic 경고 8개. 서로 다른 검사 170개다.
- 단위 정책 20 passed(0.24초)는 전체에 포함된다. 문법 수정 후 변경 대상 tests/test_compaction.py 21 passed(28.61초), 실제 Delta OPTIMIZE·중복/NULL/마이크로초/인접 파티션/과거 snapshot/반복 no-op 포함. 이 21개도 전체 검사에 포함되며 중복 합산하지 않는다.
- 최종 Ruff 전체·별도 pycache compileall·diff whitespace 통과. 공개 생성 JSON byte 동일성·실제 commit 계약 재검사·문서 내부 링크 72개·CI job 4개와 실험/증거 경로를 확인했다.
- private: .work/compaction-20261011-01, compaction-20261011-03, compaction-01.log, compaction-02.log(문법 오류), compaction-03.log, regression-compaction-01.log, airflow-compaction-01.log, compaction-final-tests.log. 합성 lake·원문 행은 공개/커밋하지 않는다.

### 남은 판단

닫힌 시간 compaction의 물리/논리 보존과 로컬 조회 변화는 검증됐다. 실수집 장기 파일 분포·S3 읽기/추가 쓰기 비용·pool 경합/신선도·파티션 변경·보존 정책은 남는다. 이번 약 1MB 출력과 warm 조회를 운영 개선율/비용 절감으로 주장하지 않는다. 이전 Kubernetes 순수 pool 대기/더 큰 동시 입력·유지보수 플랫폼 검증도 로드맵대로 남는다. write-commit 스킬로 기능/테스트·CI·문서의 목적별 로컬 커밋을 진행한다.

구현 로컬 커밋: c17c086(닫힌 시간 compaction·조회/재처리 검증·Delta 회귀), 157ba85(CI 실험과 evidence artifact). 문서와 최종 실제 실행 JSON은 다음 docs(portfolio) 커밋에 기록한다. 원격 push는 하지 않았다.

## 2026-10-11 포트폴리오 저장소 정리·공개 준비

사용자가 미사용·중복 코드를 정리하고 GitHub push를 요청했다. 시작 시 개발 브랜치 HEAD는 151f026이었고 사용자 staged/untracked 변경은 없었다. 원격 origin/main은 c4a341a이며 기존 고도화 커밋 26개가 아직 원격에 없었다. 같은 작업 브랜치의 누적 고도화와 이번 정리를 공개 대상으로 삼는다. main 직접 병합·force push·Git 저자 변경·실서비스 배포는 범위에 넣지 않는다.

### 현재 경로 정리

- 호출부가 없는 src/utils/station_loader.py(외부 Excel 의존), src/utils/time.py(KST alias), spark_common의 get_bucket/s3a를 제거했다. 공통 날짜/저장 경로와 Spark 세션 구성은 기존 계약을 유지한다.
- 실행이 차단된 databricks/sample_export/merge_parquet.py와 현재 실행 증거가 아닌 legacy 다운로드 manifest를 제거했다. 작동하는 export는 scripts/export_usage_snapshot.py로 이동하고 루트 import bootstrap·회귀 호출부를 맞췄다. Delta 루트·versionAsOf·날짜 필터와 lineage/manifest를 유지한다.
- 알림을 src/alerts/notify_arrival_eta_15m.py로 이름 변경하고 DAG·회귀 import를 맞췄다. ETA를 실제 운행 지연처럼 표현하는 이전 파일명을 없앴다.
- k8s/create-secret.sh를 기존 안전한 v2 구현으로 통합했다. 절대 개인 경로·argv Secret 전달 구현, 중복 create-secret-v2.sh와 사용되지 않는 Secret YAML 템플릿을 제거했다. 프로젝트 상대 dotenv·kubectl 표준 입력 전달을 유지하며 실제 Secret을 생성하지 않았다.
- docker/requirements.txt의 세 목록 재참조 래퍼를 제거하고 Dockerfile에서 같은 requirements 목록을 직접 설치한다. 버전이나 Airflow/Spark 환경 분리는 바꾸지 않았다.
- source/compaction 실험의 같은 canonical JSON 전체 행 해시를 src/observability/fingerprints.py로 공유한다. 입력 버전 고정·행 상한·NULL·중복·마이크로초를 보존한다. managed 업무 컬럼 해시와 Kubernetes 증거 해시는 의미/형식이 달라 별도로 유지한다.
- README의 탐색 순서, REPOSITORY_GUIDE.md의 운영/실험 역할, examples/README.md의 합성/실수집/실제 플랫폼 증거를 정리했다. RUNBOOK·Databricks 경로와 Airflow 검사 명령을 맞추고 잘못된 이중 bash continuation도 고쳤다.
- 과거 private export의 ignore 규칙·실수집 원문·인증·이미지/PVC/클러스터·역사적 실패 증거는 보존한다. 이전 SESSION_HANDOFF 경로는 당시 기록이므로 전체 치환하지 않았다.

### 실제 검증

- 파이프라인 회귀 165 passed, 152.89초. Airflow/Pod 회귀 12 passed, 1.23초, 기존 Pydantic 경고 8개. 서로 다른 검사 총 177개다. 새 공통 hash의 잘못된 경계/버전 및 실제 Delta 전체 행 계약 검사를 포함한다.
- 기존 private compaction lake의 버전 35/36을 새 helper로 읽어 공개 36,000행 해시와 일치했다. 기존 실수집 lake의 7개 테이블 전체 행·Delta 버전도 공개 after_replay와 일치했다. 데이터 변경이나 새 API 호출 없이 읽기만 했다.
- 공식 chart fixture 렌더와 실제 Airflow 태스크 17개의 Pod 병합·DAG 직렬화를 통과했다. 이번 검사는 구성 검증이며 새 Pod 실행이 아니다.
- ssdr-airflow:portfolio-cleanup 로컬 이미지 build, 두 Python 환경 pip check·provider import 성공. 이 이미지 안의 합성 Airflow 연결 실행은 12태스크 SUCCESS, API 태스크 1개 skip, 73.787953초, 이용량 680·도착/유효 ETA 4/4·보고서 해시 검증 성공이다. 순차 TaskInstance.run 루프이며 Kubernetes scheduler 실행 증거와 구분한다.
- 컨테이너 결과 전체 폴더를 Windows 경유 복사할 때 scheduler/latest symlink 권한으로 실패했다. evidence.json·task-summary.json 두 일반 파일만 별도 복사해 보존했다. 실행 자체는 성공했고 컨테이너와 원본 로그는 남아 있다.
- Ruff 전체·별도 pycache compileall·bash -n·export CLI help·diff whitespace 통과. 문서 상대 링크 161개·공개 JSON·모든 DAG의 Spark CLI 경로 존재를 확인했다. 공개 전 기존 미공개 커밋 26개의 서로 다른 변경 blob 256개와 현재 파일에서 로컬 비밀 값 미포함을 확인했다.
- private: .work/cleanup-regression-01.log, cleanup-airflow-01.log, cleanup-hash-check.log, cleanup-audit.log, cleanup-docker-build.log, cleanup-container.log, cleanup-container-evidence/{evidence,task-summary}.json, cleanup-render.log, cleanup-overrides.log, kubernetes/render-cleanup-fixture, kubernetes/overrides-cleanup.json. 현재 Kubernetes v4 fixture 릴리스는 변경하지 않았다.

### 공개와 남은 작업

사용자 요청에 따라 origin의 개발 브랜치에 일반 push하는 범위다. 로컬 검증과 원격 GitHub Actions 결과는 구분하고 push 후 원격 HEAD/CI 상태를 확인한다. 실제 장기 수집·S3 운영/복구·순수 pool 대기·더 큰 입력·유지보수 플랫폼 조합의 검증은 기존 로드맵대로 남는다. 정리는 이 항목의 완료 증거가 아니다.

이번 정리의 구현 커밋: d8bab1b(미사용·중복 경로 정리와 호출부/회귀), 0488b9e(전체 행 해시 공통화와 회귀). 탐색 안내와 실제 검사 기록은 별도 docs(portfolio) 커밋으로 묶는다.

공개 확인: 첫 일반 push 성공 후 원격 작업 브랜치 HEAD가 819d8dba830054275adc36ec6ad5ae1ffd97a156과 일치했다. 기존 26개와 이번 3개 커밋이 공개됐고 GitHub Actions 실행 38140252332가 시작됐다. 커밋 후 최종 비밀/링크 검사도 29개 커밋·279개 변경 blob·161개 링크에서 통과했다. GitHub의 UTC 환경에도 같은 의미가 되도록 새 hash 회귀 fixture의 datetime에 명시적 KST를 지정했다. TZ=UTC에서 fingerprint·compaction 28 passed, 28.66초이며 기존 177개와 중복되는 검사다. 이 테스트 보완과 공개 기록은 일반 후속 커밋으로 push하며 이미 공개된 이력을 재작성하지 않는다.

## 2026-10-11 원격 CI의 시간대 오류 수정·브랜치 규칙

다음 작업을 확인하던 중 PR #1의 원격 CI 결과를 읽었다. 최신 입력 e8495dc의 실행 38140365929(push)·38140415377(PR)에서 dags·airflow-integration·container-integration은 SUCCESS였고 pipeline은 163 passed/2 failed였다. 승인된 기존 정리·공개 작업의 CI 실패를 보완했다.

- 첫 실패는 실제 15분 Gold 코드가 naive datetime을 F.lit으로 넘겨 Python 호스트의 UTC를 적용한 문제였다. KST 월말 자정 입력이 9시간 다른 구간으로 해석돼 빈 입력 gate에서 차단됐다. 경계 선택과 저장 컬럼을 같은 ISO 문자열→Spark Asia/Seoul timestamp로 처리한다. CI 호스트의 TZ는 UTC로 명시해 이 계약을 계속 검사한다.
- 두 번째는 올바른 원천 시각을 Python collect의 호스트 표시 시각과 비교한 검사 문제였다. Spark의 KST date_format으로 확인하고 원천 나이 4초·최신 ETA 60초 선택은 유지했다. 15분 집계 회귀는 저장된 시작/끝 timestamp와 window key의 KST 일치도 검사한다.
- 수정 전 로컬 TZ=UTC 대상 검사도 같은 두 실패를 재현했다(3 passed/2 failed, 61.23초). 수정 후 UTC 전체 파이프라인 165 passed, 157.22초, KST 대상 5 passed, 70.07초. 대상 검사는 전체에 포함되며 중복 합산하지 않는다. Airflow·컨테이너의 세 원격 job 성공과 구분한다. Ruff와 diff whitespace도 통과했다.
- 사용자가 브랜치 접두사 변경을 요청했다. 로컬·원격 이름은 feature/de-portfolio로 정리하고 GitHub rename API의 응답 HEAD가 기존 e8495dc와 같음을 확인했다. GitHub의 head branch rename 동작으로 기존 PR #1이 닫히므로 같은 커밋에서 새 PR을 만든다. 커밋 저자/기존 commit SHA/공유 이력은 재작성하지 않는다.
- AGENTS.md와 write-commit의 브랜치 규칙을 feature/, fix/, refactor/로 맞췄다. 날짜별 과거 기록에서는 당시 개발 브랜치의 불필요한 이름 표기를 생략하고 실제 실행·실패·커밋·출력 증거는 보존한다.
- private: .work/next-pr-checks.json, next-ci-failure.log, ci-utc-repro.log, ci-utc-regression.log, ci-kst-targeted.log, branch-rename.json, branch-fetch.log. 수정 push 후 새 HEAD의 원격 CI를 확인한다.

다음 고도화의 우선순위는 제한된 실제 원천의 여러 날 관측으로 누락·신선도·응답 상한·파일 분포를 판단할 근거를 만드는 것이다. 다음으로 실제 입력의 Databricks 관리형 처리/SQL 제공 및 더 큰 입력의 Kubernetes pool 대기·자원 측정을 검토한다. 현재 질문에서 API 장기 호출·실데이터 workspace upload·새 배포를 시작하지 않았다.

## 2026-10-11 날짜별 원천 수집 기록 비교

사용자 요청의 push는 먼저 실행했으며 feature/de-portfolio가 origin과 같은 0ce2a75라 Everything up-to-date였다. 다음 고도화로 이미 저장한 실제 원천 메타데이터를 여러 날짜에 걸쳐 비교하는 도구를 만들었다. 새 기간의 외부 API 수집은 별도 opt-in 질문에 답이 없어 시작하지 않았다.

- src/observability/source_baseline.py와 scripts/summarize_source_observations.py는 capture.json만 읽고 API·원문·Delta·클라우드·알림에 접근하지 않는다. 관측 날짜는 KST로 나누며 자정을 넘긴 슬롯과 승하차 이용 날짜를 구분한다.
- collected/failed/missed_schedule/not_recorded_due/future를 분리한다. 계획 자체가 없는 날짜는 수집 범위 미확인이다. 시도 결과 성공률과 선언된 도래 슬롯 수집률의 분모를 명시하며 전체 하루 운영 성공률이나 HTTP 가용성으로 표현하지 않는다.
- 실제 논리 GET 수, 응답 행·상한 도달, 요청·저장 시도 시간 표본, 명시적 분석 시점의 마지막 관측 나이를 기록한다. 원천 생성 시각/정제 품질은 기존 처리 감사의 책임이다.
- 같은 메타데이터 사본은 canonical JSON 해시로 제외한다. 같은 시작 시각의 다른 중간/최종 기록은 자동 병합하지 않고 선택을 요구한다. 완료 상태·요청 수·수치·시각·원문 상한이 모순된 기록은 비교 결과를 만들지 않는다. 기존 출력 경로도 보존한다.
- 기존 .work/source-observation-20261011-01의 메타데이터를 .work/source-baseline-20261011-02로 분석했다. 기준 시각은 실제 실행 시점 2026-10-11T22:26:44+09:00이다. 1일·도착 3/3 성공·응답 상한 3회·도착 GET 3회·승하차 GET 1회·20261007 이용량 617행이다. 장기 관측이나 하루 전체 커버리지로 확대하지 않는다.
- examples/observed/collection-summary.json의 manifest 해시가 기존 공개 evidence.json의 capture 객체와 일치한다. 원문·정제 행은 계속 private .work에 남긴다. SOURCE_OBSERVATION·REPOSITORY_GUIDE·ROADMAP에서 실행 방법과 준비/미완료 경계를 기록했다.

### 실제 검증과 공개

- TZ=UTC 전체 파이프라인 193 passed, 148.10초. 추가된 28개 검사는 실패/일정 놓침/미확인 구분, 중단·미래 슬롯, 중복·충돌, 자정/월말, 잘못된 숫자·시각·호출 예산, 순서 독립성, 원문 미출력, CLI·기존 출력 보존을 확인한다.
- Airflow/Pod 12 passed, 1.11초, 기존 Pydantic 경고 8개. 전체 서로 다른 검사 205개다. 처음 로컬 실행은 PYTHONPATH의 dags를 빠뜨려 import 오류(2 failed/8 errors)였으며 문서·CI의 환경 설정을 적용한 후 통과했다. DAG 코드는 변경하지 않았다.
- Ruff 전체와 diff whitespace 통과. 문서 상대 링크 164개·공개 JSON·DAG CLI 경로·현재 파일/기존 32개 커밋의 비밀 값 미포함 검사를 통과했다. 실제 API 호출·새 Pod·Databricks 실행은 이번 검증에 포함하지 않는다.
- private: .work/observation-unit-01.log, observation-unit-02.log, observation-regression-01.log, observation-airflow-{01,02}.log, observation-ruff.log, observation-audit.log, observation-baseline-02.log, source-baseline-20261011-02/{summary.json,summary.md}. 최초 baseline-01은 미래 분석 시각의 개발 출력이므로 공개에 사용하지 않았다.
- 사용자 요청 범위로 같은 개발 브랜치에 일반 push한다. 새 HEAD의 원격 실행 상태는 .work/observation-ci-final.json과 observation-pr-checks-final.json에 별도로 보존한다. 이전 HEAD의 CI 성공을 새 코드의 원격 검증으로 대체하지 않는다.

남은 작업은 승인된 기간/호출량의 실제 여러 날 수집과 결측·identity 변동·원천 시각·처리 품질의 장기 측정이다. 캡처 파일이 없는 전체 날짜의 원인은 이 비교 도구로 추정할 수 없으며 별도 scheduler 계획/실행 기록이 필요하다. 실제 Databricks 입력 upload·S3 운영·더 큰 Kubernetes 입력 검증은 별도 범위다.

## 2026-10-11 승인된 7일 관측·README 소개 틀

사용자가 기간 질문에 “제일좋을것같은걸로해줘”라고 답해 추천한 7일 소량 관측을 선택했다. 2026-10-12~18, 매일 17:50 KST에 도착 3회(5분 간격)와 D-4 승하차 한 날짜를 수집한다. 하루 최대 13 GET, 전체 최대 91 GET이며 원문은 로컬에만 보관한다. AWS·Databricks upload·Discord·LLM은 이 승인에 포함하지 않는다.

- scripts/run_source_observation_day.py는 기존 capture_sources.capture를 재사용한다. 새 캠페인 최대 7일과 SHA-256으로 고정한 계획, 실제 KST 날짜/시각, 5분 시작 허용 범위, session GET 상한과 날짜 변경 차단을 확인한다.
- 원자적인 일간 디렉터리 예약은 중복·동시 실행·실패·프로세스 중단 뒤에도 같은 날 새 API 호출을 막는다. 예약을 삭제하거나 다른 캠페인으로 재시도하지 않는다. 누락 상태를 API 실패로 추론하지 않으며 원본 재처리는 별도 CLI의 책임이다.
- .work/source-campaign-20261012-18/plan.json과 plan.sha256을 만들었다. 초기 status는 7일 모두 not_due이고 오늘 run 점검은 outside_period, 실제 GET 0이다.
- 앱의 heartbeat 자동화 “지하철 원천 7일 소량 관측”(ID 7, ACTIVE)을 이 채팅에 생성했다. 일일 17:50의 7회 예약이며 종료 날짜/CLI 범위로도 추가 호출을 차단한다. 완료된 저장 원본의 처리·품질·7개 해시 검증과 날짜별 집계를 수행하고, 의미 있는 실패/조치 필요/최종 완료 때 알린다. PC와 앱이 실행 중이어야 하며 늦거나 놓친 날은 소급 수집하지 않는다.
- 사용자가 main README의 기존 소개 틀을 선호했다. 기존 배지·소개 문장·구분선과 Project Overview → System Architecture → Data Layer Design → Data Pipelines → Reporting → Databricks → Kubernetes → Validation → Limitations/Future Work → Execution Flow 흐름을 살렸다. 오래된 Kafka 그림·실제 지연 해석·검증되지 않은 S3 운영 설명은 현재 구현과 증거에 맞추고 상세 실험은 연결 문서로 정리했다. 키 없는 실행 anchor는 유지했다.

### 검증과 공개 기록

- 앞 단계의 34514bd push/PR 원격 CI는 네 job 모두 SUCCESS다. push 첫 Airflow 실행은 Spark 시작 전 Maven의 Delta JAR을 찾지 못해 실패했으나 같은 SHA의 PR 실행과 실패 job 재실행은 성공했다. .work/observation-ci-airflow-artifact/seed.log, observation-ci-final.json, observation-ci-pr.json, observation-pr-checks-final.json에 보존하고 PR #2 설명에도 구분했다.
- 관측 제어 추가 후 UTC 전체 파이프라인 209 passed, 149.32초. 계획 해시 고정 보완과 두 검사 추가 후 원천 관련 60 passed, 1.42초. 이 60개는 기존 검사와 중복이며 새 두 개만 별도 추가된다. 앞서 통과한 Airflow/Pod 12개를 포함해 서로 다른 검사 총 223개가 통과했다.
- 새 관측 제어 검사는 기간/시각 밖 호출 차단·월말/D-4·시작 허용 경계·동시 실행 1회·중단 후 예약 보존·14번째 GET/날짜 변경 차단·수정된 계획 거부를 확인한다. 실제 API는 테스트에서 호출하지 않는다.
- Ruff 전체, diff whitespace, 문서 상대 링크 165개·공개 JSON·DAG CLI·비밀 값 미포함 확인을 통과했다. README는 문서 변경이므로 이를 위한 별도 구현 추종 테스트는 만들지 않았다.
- private: .work/campaign-{init.log,status-initial.json,before-period-check.json,unit-01.log,unit-final.log,unit-sealed.log,regression-01.log,ruff-sealed.log,audit-final.log}. main의 README 비교본은 .work/readme-main-reference.md다.
- 관측 제어 기능과 README 소개 구성은 목적별 커밋으로 나누고 사용자 요청 범위에서 같은 feature/de-portfolio에 일반 push한다. 새 원격 결과는 .work/campaign-ci-final.json과 campaign-pr-checks-final.json으로 별도 확인한다.

실제 7일 수집은 아직 시작 전이다. 7일이 지나도 전체 하루/전체 역 커버리지·S3 운영·실데이터 Databricks 실행·분산 Spark 성능은 이 소량 관측으로 증명되지 않는다. 종료 시 확보한 날짜와 슬롯, 미확인 날짜, 응답 상한/identity 변동, 원천 시각과 처리 품질을 그대로 보고한다.

## 2026-10-13 고정 관측 계획과 실행 기록 대조

사용자가 다음 고도화를 요청했다. PR #2는 2026-10-11T14:17:17Z에 main으로 병합됐으며 origin/main c2ab046에서 feature/source-campaign-audit를 만들었다. 시작 시 사용자 변경이나 staged 변경은 없었다. 운영 DAG의 5분 주기·고정 관측 기간/예산·기존 API 예약은 변경하지 않았다.

- summarize_source_observations.py에 --campaign-dir 모드를 추가했다. 기존 --capture-dir 비교를 유지하며, 날짜 범위는 봉인된 plan에서 가져온다. API·원문·Delta·환경 인증을 읽지 않는다.
- src/observability/source_campaign.py는 고정 계획의 도래/향후 슬롯을 집계한다. 요청 실패·collector 슬롯 놓침·일일 시작 놓침·미확인을 분리하고, 알 수 없는 GET을 null로 유지한다. 알려진 합계는 미확인 날짜가 있으면 하한이다.
- 고정 시작 시각과 실제 캡처 시작 시각을 분리하고 KST 월말·D-4 날짜를 검증한다. 상태/캡처의 날짜·간격·완료 상태·호출량 불일치와 성공 상태의 누락 캡처를 거부한다. 캠페인 달력 밖 입력·기간 변경도 거부한다.
- campaign_status의 예약 디렉터리와 state 파일의 symlink 경로 이탈을 차단했다. 캡처 경로에도 같은 경계를 적용한다. 기존 출력·계획 seal·예약은 보존한다.
- README의 지속 관측 표현을 제한 표본 관측으로 정리했다. 운영 DAG의 연속 5분 폴링과 하루 세 번 실험은 별도다. SOURCE_OBSERVATION에 분모·미확인 호출량·CLI와 범위를 설명했다.

### 실제 읽기 전용 적용과 검증

2026-10-13T01:09:20.798127+09:00에 실제 기존 계획을 감사했다. 총 계획 21슬롯 중 도래 3·향후 18이며 10/12의 state/capture가 없어 도래 3슬롯은 미확인이다. 실패 요청으로 계산하지 않았고 실제 총 GET도 단정하지 않았다. 감사 실행의 외부 API 호출은 0회다. private 결과는 .work/source-campaign-20261012-18/summaries/20261013-010920, 공개 메타데이터/해석은 examples/observed/campaign-plan-audit.{json,md}다.

- UTC 전체 파이프라인 231 passed, 136.57초. 이는 새 검사 최초 20개를 포함한 결과다.
- 이후 달력·미확인 호출량·state 파일 경로 이탈 4개를 보완했다. 최종 관측 관련 70 passed, 1.02초이며 이전 전체 검사와 중복인 66개와 새 4개로 구성된다. 기존 캠페인 18개·캡처 비교 28개·새 계획 감사 24개다.
- Airflow/Pod 12 passed, 1.56초, 기존 Pydantic 경고 8개. 전체와 보완 검사에 포함된 서로 다른 검사 총 247개가 통과했다.
- Ruff 전체·diff whitespace·문서 상대 링크 167개·공개 JSON·DAG CLI 존재·현재 공개 파일의 로컬 비밀 값 미포함 검사 통과. 원문·정제 행을 추가하지 않았다.
- private 로그: .work/campaign-report-{unit.log,unit-final.log,regression.log,airflow.log}. 원격 검증은 push 후 새 HEAD에서 별도로 확인한다.

누락 원인은 실행 기록만으로 추정하지 않는다. 과거 관측을 다시 호출하거나 예약을 삭제하지 않았다. 실제 7일 기간 관측·전체 하루 커버리지·실데이터 Databricks upload·S3 운영·순수 pool 대기는 기존 미완료 항목이다. 기존 API opt-in 범위를 늘리지 않는다. write-commit 스킬에 따라 기능·회귀·문서·증거를 한 목적의 커밋으로 묶고 기존 push 승인 범위로 공개한다.

추가 검토에서 terminal state가 아직 없는 부분 캡처도 이미 기록된 GET을 알고 있음을 확인했다. 알려진 GET 하한은 state의 총계가 있으면 그것을 사용하고, 없으면 manifest에 확인된 요청 수를 사용한다. 둘을 더해 이중 계수하지 않는다. reserved 상태의 부분 캡처가 가진 GET 1회를 합계에서 누락하지 않는 회귀를 보완하고 같은 기능의 후속 커밋으로 기록한다.
