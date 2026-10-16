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

## 2026-10-13 main 직접 작업과 수집 원본 무결성

사용자가 PR 없이 main 직접 작업과 병합 브랜치 삭제를 요청했다. 시작 시 사용자 변경·staged 변경은 없었다. PR #3은 2026-10-13T07:30:02Z에 main f061196으로 병합됐다. 원격 main 포함 여부를 확인하고 main을 fast-forward한 뒤 feature/de-portfolio와 feature/source-campaign-audit의 로컬·원격 브랜치를 삭제했다. AGENTS.md와 write-commit 스킬에 main 직접 커밋·일반 push, PR은 요청 시 생성하는 방식을 반영했다. skill-creator의 quick_validate는 Skill is valid!로 통과했다. Git 저자와 공동저자는 변경하지 않았다.

관측 자동화 ID 7은 공식 automation_update로 마지막 공개 기록의 대상만 main으로 수정했다. ACTIVE 상태·매일 17:50 RRULE·2026-10-12~18 기간·하루 13 GET/전체 91 GET·고정 계획 seal은 보존했다. 과거 미수집 관측을 복원하거나 날짜 예약을 삭제하지 않았다.

### 구현과 실제 검증

- capture_sources는 새 capture schema 2에 저장한 정확한 UTF-8 bytes의 raw_sha256을 기록한다. 계획 schema는 1을 유지한다. GET·수집 주기·재시도 정책은 바꾸지 않는다.
- process_source_experiment는 공통 manifest 검증을 재사용하고 수집 당시 해시와 원본을 새 출력 디렉터리/Spark 실행 전에 대조한다. manifest·payload의 symlink 경로 이탈도 차단한다. 행 수·수집 시각이 같아도 값/bytes가 달라지면 거부한다.
- 원래 schema 1은 원본을 바꾸지 않고 처리할 수 있으나 unverified_legacy와 수집 시 검증 파일 수 0으로 기록한다. 처리 시 계산한 해시는 수집 당시 증거로 소급하지 않는다. 메타데이터 감사의 해시 형식 검사와 원본 bytes 검사를 구분한다.
- 2026-10-13T16:46:20.571268+09:00에 합성 원본 4파일로 실제 오류 주입을 실행했다. 원래 4개 해시는 일치했고, 행 수·수집 시각을 유지한 ETA 값 변경은 Spark 시작과 출력 생성 전에 거부됐다. 기존 10/11 실제 관측 4파일은 읽기 전용으로 확인해 legacy 미검증 표시를 확인했다. 이번 실험의 실제 API GET은 0이다. 공개 집계는 examples/observed/source-integrity.json, 원문은 ignored .work/source-integrity-experiment-20261013-01에만 있다.
- UTC 전체 파이프라인 258 passed, 120.12초. Airflow/Pod 12 passed, 1.36초로 서로 다른 회귀 총 270개가 통과했다. 새 무결성 회귀는 23개다. 이후 공통 검증과 중복된 기존 코드를 제거하고 관련 회귀 107 passed, 1.53초·전체 Ruff·diff whitespace를 다시 확인했다. 107개는 전체 검사와 중복이며 별도로 더하지 않는다.
- private 로그: .work/source-integrity-{regression.log,airflow.log,unit-final.log,experiment.log}. 새 원격 HEAD의 CI는 main push 후 별도로 확인한다.

체크섬은 수집 메타데이터 대비 변경을 검출하며 파일·메타데이터 동시 조작의 진본 인증을 제공하지 않는다. 합성 오류 주입은 실제 원천 품질이나 장기 운영을 증명하지 않는다. 실제 7일 관측·실데이터 Databricks·S3 운영·순수 pool 대기와 대규모 성능의 미완료 항목은 그대로다. 기존 API opt-in 범위와 운영 DAG의 연속 5분 주기는 유지한다.

공개 전 문서 상대 링크 171개·공개 JSON·DAG CLI 존재·로컬 비밀 값 미포함 감사를 통과했다. 원문·정제 행·실서비스 비밀 값을 추가하지 않았다.

## 2026-10-13 보고서 Gold 소비 계약 보강

사용자가 다음 고도화를 요청했다. main f9e7f31과 origin/main이 같고 사용자/staged 변경은 없는 상태에서 진행했다. 이전 무결성 변경의 원격 CI 38365969968은 네 job 모두 성공했고 원격 회귀도 258+12개가 통과했다. 이번 작업은 기존 write-commit 지침에 따라 main에 직접 기록하고 일반 push한다.

기존 보고서 소비 검사를 합성 Gold로 재현했다. 승차/하차 0.5·합계 1.0, 공백 역명이 정상 summary로 통과했다. 변환 단계와 별도로 Gold를 읽는 최종 소비 단계에서도 품질 오류를 차단하도록 보강했다.

- 승하차 identity와 유효 단일 이용 날짜, 유한한 비음수 정수 건수·합계 일치를 검증한다. boolean·문자열·소수·비유한 값은 정상 건수로 변환하지 않는다.
- 도착 ID/역명, 정수 분모, snapshot_count ≤ total_events, 유효 표본의 평균/p95/비율을 검증한다. 유효 표본 0의 ETA 지표는 NULL이고 전체 관측은 분모에 남는다.
- 범위 컬럼이 있으면 각 행 시작/끝을 함께 확인한다. 실제 보고서에서는 UTC→KST 변환 후 D-1을 확인해 KST 자정/월말의 다른 날짜 관측을 거부한다. 알려진 source age는 유한해야 하며 음수는 보존한다.
- Python 정수 합계와 정규화한 표본 가중치로 int64 합계 wrap/ETA 중간 곱 overflow를 피한다. 기존 가중 평균·분위수 의미·원천별 날짜·Delta 루트 읽기는 유지한다.
- 2026-10-13T17:15:46.072348+09:00에 기존 보고서 생성/manifest 검증으로 합성 오류 다섯 가지를 실제 주입했다. 모두 ReportDataError로 거부됐고 이전 세 파일의 SHA-256/manifest가 유지됐다. 외부 호출은 0회, 원문/발행 fixture는 ignored .work/report-contract-experiment-20261013-01, 공개 집계와 해석은 examples/operations/report-contract.{json,md}다.

### 검증과 범위

새 회귀 47개를 포함해 UTC 파이프라인 305 passed, 120.64초·Airflow/Pod 12 passed, 1.54초로 서로 다른 검사 총 317개가 통과했다. 큰 합계 fixture의 중복 역 키 때문에 최초 한 검사가 실패했고 고유 역 fixture로 바로잡았다. 이후 중복 조건 정리와 NaT 방어 후 관련 57 passed, 0.60초·전체 Ruff·diff whitespace를 확인했다. 57개는 전체와 중복이며 별도로 더하지 않는다. private 로그는 .work/report-contract-{regression.log,airflow.log,unit.log,experiment.log}다.

이번 실험은 합성 in-memory Gold 소비 검증이다. 실제 Delta 읽기는 기존 전체 회귀/원격 데모에서 별도로 검사한다. 이전 발행 보존은 데이터 검증 실패에 한정되며 다중 파일 쓰기 도중 장애나 동시 발행의 트랜잭션을 구현한 것이 아니다. 실제 7일 관측·S3 운영·실데이터 Databricks·순수 pool 대기는 미완료다. API 예약·예산과 운영 5분 수집 주기를 바꾸거나 외부 서비스를 호출하지 않았다. 새 main HEAD의 원격 CI는 push 후 확인한다.

공개 전 감사에서 문서 상대 링크 176개·공개 JSON·DAG CLI 경로·로컬 비밀 값 미포함을 확인했다. 원문·정제 행과 사용자 변경은 커밋에 포함하지 않는다.

## 2026-10-13 보고서 발행 도중 장애와 소비 객체

사용자가 다음 작업을 요청했다. main/origin/main 49af161이 같고 사용자·staged 변경이 없는 상태에서 시작했다. 이전 Gold 소비 계약의 원격 CI 38368587399는 네 job 모두 성공했고 원격 회귀 305+12개가 통과했다.

- src/report/publication.py가 본문·정확한 context 문자열·manifest를 단일 publication.json에 묶는다. 세 호환 export 뒤에 마지막으로 교체하고 독자는 한 객체 bytes를 한 번 읽어 두 해시와 schema/mode/D-4/D-1을 검증한다.
- 로컬은 같은 디렉터리의 고유 임시 파일·flush/file fsync·replace를 사용한다. S3 adapter는 동일한 순서의 네 PUT이며 마지막이 단일 발행 객체다. local/S3는 독립 커밋이다. AWS 공식 consistency 문서의 단일 key 원자적 갱신 계약을 확인했고 REPORT_PUBLICATION에 링크했다.
- verify_report CLI는 canonical 객체와 기존 .md 세트 모두 지원한다. bundle이 존재하면 export manifest도 일치해야 하고 손상된 bundle에 fallback하지 않는다. 기존 공개 샘플·과거 증거 bytes는 보존했다.
- scripts/demo.py, airflow_offline.py, kubernetes_fixture.py의 재현 소비가 canonical 객체를 검증·읽도록 연결했다. 일반 생성 함수의 .md 반환은 호환을 유지한다.
- 2026-10-13T18:14:09.024286+09:00에 별도 합성 writer를 두 export 교체 뒤 SIGKILL했다. exit -9, 기존 발행 bytes 유지·혼합 export 거부·같은 출력 재시도의 새 발행/반복 bytes 일치를 확인했다. mock S3의 네 단계 실패와 커밋 후 응답 유실도 확인했다. 실제 API/S3 요청은 0회다.
- 공개 집계와 해석은 examples/operations/publication-commit.{json,md}, 합성 보고서/private 증거는 .work/publication-commit-experiment-20261013-01에만 보관한다. 원문·정제 행이나 비밀 값을 추가하지 않았다.

### 검증과 남은 범위

처음 manifest 직렬화의 키 순서 차이로 관련 일곱 검사가 실패했다. manifest 키 순서도 결정적으로 맞춰 통과했다. 모듈 분리 뒤 직접 CLI에서 src import 경로가 없어 실패하는 문제를 확인하고 저장소 루트 설정과 subprocess 회귀 두 개를 추가했다.

UTC 파이프라인 330 passed, 137.76초는 새 최초 25개 검사를 포함한다. 이후 CLI 두 개·실제 SIGKILL 한 개·JSON 지수 overflow 한 개를 추가하고 최종 관련 86 passed, 2.25초를 확인했다. Airflow/Pod 12 passed, 2.14초를 포함해 서로 다른 회귀 총 346개가 통과했다. 86개는 전체와 중복인 82개와 추가 네 개이며 별도로 더하지 않는다. 전체 Ruff·diff whitespace를 확인했다. 로그는 .work/publication-commit-{regression.log,airflow.log,unit-final.log,experiment.log}다.

발행 객체가 마지막에 교체되기 전 실패에서는 이전 객체를 읽는다. 교체 후 응답 유실에서는 새 완전한 객체가 이미 존재할 수 있다. 첫 성공 이전에는 canonical 발행이 없으며 기존 세 파일의 과거 게시물을 소급 변환하지 않는다. 같은 context/게시 출력의 재시도를 검증했고 새로운 Gold 버전이나 LLM 재생성의 동일 bytes를 보장하지 않는다.

소비자가 canonical 객체를 읽는 경우의 일관성/가용성이다. 세 export의 트랜잭션·전원/디스크 장애·클라우드 실제 내구성·동시 writer 최신성·local/S3 분산 트랜잭션을 검증한 것이 아니다. 실제 7일 관측·S3 운영·실데이터 Databricks·순수 pool 대기는 기존 미완료다. API 일정/예산과 운영 5분 수집을 변경하지 않았다. write-commit 지침에 따라 main에서 기능·소비 경로·회귀·문서·증거를 함께 커밋·일반 push하고 새 HEAD의 CI를 확인한다.

공개 전 문서 상대 링크 183개·공개 JSON·DAG CLI 경로·로컬 비밀 값 미포함 감사를 통과했다. 공개 Markdown·집계 JSON만 추가하며 raw/정제 행·비밀 값·사용자 변경을 스테이징하지 않는다.

## 2026-10-13 CI 환경 변경과 Action 유지보수

사용자의 다음 고도화 요청으로 main/origin/main f344d91이 같고 작업 트리가 깨끗한 상태에서 시작했다. 직전 CI 38375889135는 네 job 성공이었다. GitHub 공식 공지는 ubuntu-latest가 2026-10-19~11-19에 Ubuntu 26.04로 전환된다고 안내한다. 코드 변경 없이 검증 OS가 바뀌는 영향을 분리하기 위해 네 job의 runner를 ubuntu-24.04로 명시했다.

- checkout 7.0.1·setup-python 7.0.0·setup-java 6.0.1·upload-artifact 7.0.2·setup-helm 5.0.1의 공식 릴리스/태그와 커밋 SHA를 GitHub API로 대조하고, action.yml의 Node 24 런타임을 확인했다. uses를 전체 SHA와 읽을 수 있는 버전 주석으로 고정했다.
- contents: read 권한, pipeline/dags 실행 상한 15분/10분을 명시했다. 기존 연결 job의 15분/20분 상한은 유지했다.
- pipeline pip cache에 포함된 runtime.txt를 추가해 runtime 의존성 변경도 cache 키에 반영한다.
- Python 3.11·Java 17·Helm 3.20.0·Spark/Delta·Airflow/chart/provider는 변경하지 않았다. RUNBOOK에 검증 역할·갱신 절차·고정 범위와 남은 제약을 기록했다. README의 기존 틀과 데이터 경로는 변경하지 않았다.

### 검증

공식 actionlint 1.7.12 릴리스 archive를 private .work에 받고 배포 checksum과 대조한 뒤 워크플로 검사를 통과했다. UTC 파이프라인 334 passed, 125.25초와 Airflow/Pod 12 passed, 0.89초로 기존 회귀 총 346개가 통과했다. 최초 로컬 Airflow 명령에 PYTHONPATH를 빠뜨려 import 오류가 났으며 CI와 동일한 .:dags 경로를 설정해 재검사했다. 워크플로에는 해당 경로가 이미 있으므로 DAG 구현을 변경하지 않았다. Ruff·diff whitespace·문서 상대 링크 183개·공개 JSON·DAG CLI·비밀 값 미포함 감사를 통과했다.

로그/릴리스 메타데이터는 .work/ci-maintenance-{regression.log,airflow.log,airflow-corrected.log,before.json}, .work/ci-actions-reviewed.json에만 보관한다. CI 설정과 필요한 문서만 main에 커밋·일반 push하고, 새 HEAD의 네 원격 job 및 artifact 업로드를 확인한다. 새 CI에서 실패하면 해당 설정을 수정한 뒤 성공까지 확인한다.

runner 패치 이미지·전이 패키지·Docker base digest까지 고정한 환경은 아니다. 실제 S3·운영 metadata DB·새 Airflow/provider 전환·실수집 Databricks·순수 pool 대기와 장기 관측은 기존 미완료다. 원천 API·AWS·Databricks·Discord·LLM을 호출하거나 7일 예약/예산·운영 5분 수집 주기를 변경하지 않았다.

## 2026-10-13 README Delta Lake 배지

사용자 요청에 따라 README의 Delta Lake 배지에서 3.2.0을 제거하고 포맷 사용 표기만 남겼다. 의존성 설정은 그대로다. 문서·diff·공개 파일 감사를 확인하며 코드 변경이 없어 로컬 파이프라인 회귀는 재실행하지 않는다. 직전 main 5d4c017의 원격 CI 38381845018은 네 job과 artifact 모두 성공했다. 이번 문서 변경은 main에 커밋·일반 push한다.

## 2026-10-13 코드 중심 고도화: 승하차 품질 감사

사용자는 예약 수집 점검 대신 코드와 로컬 검증 중심의 고도화를 요청했다. main/origin/main 9b11d72가 같고 사용자·staged 변경이 없는 상태에서 진행했다. 예약 실행 점검과 외부 호출은 수행하지 않았다.

기존 usage_metrics의 pd.to_numeric/벡터 합계 경로에서 합성 네 사례를 실제 재현했다. 양 계층의 문자열 숫자·boolean 건수·숫자 역 키를 healthy로 판정했고, 두 행의 2**62 합계를 -9223372036854775808로 기록하면서 healthy를 반환했다. 전후 결과는 private .work/usage-audit-{before,after}.log에 보존한다.

- src/common/numbers.py로 기존 보고서의 유한 숫자·비음수·정수·boolean 제외 계약을 공유한다. 보고서의 숫자 허용 정책은 유지한다.
- 감사 Silver/Gold의 identity를 비어 있지 않은 문자열로 확인하고, 숫자 문자열과 boolean을 강제 변환하지 않는다.
- 명시적인 object dtype의 Python 정수로 행별 합계와 전체 합계를 계산한다. 불량 행 옆의 큰 정수가 float 추론으로 반올림되지 않게 한다.
- 양 계층의 정상 정수/정수 값 float를 같은 업무 값으로 비교한다. 불량 행이 있으면 total_count와 silver_gold_business_match를 null로 남기며 failed 판정은 유지한다. 입력 DataFrame은 변경하지 않는다.
- 새 회귀 20개에는 실제 합성 Delta 테이블 다섯 개를 기록하고 별도 CLI 프로세스에서 healthy/failed exit code·읽은 버전·정확한 합계·불량 schema 차단을 검증하는 세 사례가 포함된다. OBSERVABILITY에 타입과 null 계약을 기록했다.

### 실제 검증과 범위

관련 감사/보고서 98 passed, 3.65초 뒤 전체 UTC 파이프라인 354 passed, 123.27초와 Airflow/Pod 12 passed, 1.77초를 확인했다. 서로 다른 회귀 총 366개이며 관련 98개를 다시 더하지 않는다. Ruff·diff whitespace·문서 상대 링크 183개·공개 JSON·DAG CLI·비밀 값 미포함 감사를 통과했다. private 로그는 .work/usage-audit-{unit,regression,airflow}.log다.

합성 in-memory 오류 주입과 실제 로컬 Delta 루트/CLI 검증이다. 서울 API·AWS·Databricks·Discord·LLM 호출, Kubernetes 배포, 예약 관측의 일정/예산 변경은 없다. 실제 장기 관측·S3·운영 metadata DB·실수집 Databricks·순수 pool 대기는 기존 미완료다. 기능·회귀·계약 기록만 main에 커밋·일반 push하고 새 HEAD의 네 CI job 결과를 확인한다.

## 2026-10-13 미사용 코드와 불필요한 인자 정리

사용자가 실제 쓰지 않거나 없어도 되는 부분의 점검을 요청했다. main/origin/main 30eb9b2가 같고 사용자·staged 변경이 없는 상태에서 추적 파일의 DAG·CLI·notebook·Docker/Helm·CI·문서 참조와 Python AST의 함수/인자 사용, Ruff 미사용 항목을 확인했다.

- src/common/get_api.py의 get_env는 전체 코드·문서에 호출처가 없어 제거했다. 두 API key 함수와 실제 수집 호출부는 유지했다.
- 미사용 import 6개를 제거했다: demo의 table_path, 승하차 collector의 datetime, usage_portfolio의 UsageTables, 세 테스트 파일의 Path.
- 내부 함수의 사용하지 않는 인자 3개를 호출부와 함께 제거했다: summarize_arrival의 df_usage, build_prompt의 target_ymd, airflow_offline.run의 pipeline_python. CLI --pipeline-python은 환경 구성과 Bronze 준비에 실제로 사용하므로 유지한다.
- json.parse_constant callback의 value는 내부에서 읽지 않더라도 JSON API 호출 규약의 필수 인자이므로 유지했다.
- Ruff F401/F811/F841을 기존 CI 검사에 추가해 미사용 import·재정의·지역 변수가 다시 들어오는 것을 검사한다.
- 이번 점검에서 통째로 제거할 근거가 확인된 주요 모듈/의존성은 없다. scripts의 수동 재현·증거 수집, Databricks/Kubernetes 실제 실행 증거와 선택적 export는 역할이 있어 유지한다. LLM/Discord도 기본 흐름에 필수는 아니지만 실제 선택 호출 경로가 있으므로 미사용으로 삭제하지 않았다. REPOSITORY_GUIDE에 구분을 기록했다.

### 검증과 범위

UTC 파이프라인 354 passed, 127.54초와 Airflow/Pod 12 passed, 1.64초로 기존 회귀 총 366개가 통과했다. 이전 HEAD에서 추출한 build_prompt와 새 함수의 동일 합성 context 결과가 같음을 별도 로컬 검사로 확인했다. API/LLM 호출은 0회다. 새 테스트를 추가하지 않고 기존 identity/원천 분리 회귀의 호출부를 변경했다.

강화된 Ruff·diff whitespace·문서 상대 링크 183개·공개 JSON·DAG CLI·비밀 값 미포함 감사를 통과했다. private 점검/회귀 결과는 .work/unused-audit-*와 .work/unused-cleanup-{regression,airflow,prompt}.log에 보존한다. 코드·호출부·CI 규칙·기존 회귀·점검 기록을 main에 커밋·일반 push하고 새 HEAD의 네 원격 CI job을 확인한다.

이번 점검은 저장소 내부 참조와 실제 로컬 회귀의 범위이며 동적 플랫폼 외부 소비자를 모두 증명하는 분석은 아니다. private .work·원문·정제 데이터, 공개 증거와 과거 기록을 삭제하지 않았다. 외부 API·AWS·Databricks·Discord·LLM·Kubernetes 배포를 호출하거나 예약 관측을 변경하지 않았다. 장기 실운영 검증은 기존 미완료다.

## 2026-10-13 Slack 운영 알림 전환

사용자가 기존 다른 프로젝트와 같이 Slack을 사용하도록 요청했다. main/origin/main 2f7e945가 같고 사용자·staged 변경이 없는 상태에서 진행했으며, 커밋 전 원격 fetch에서도 추가 변경이 없음을 확인했다.

- src/alerts/notify_pipeline.py를 Slack Incoming Webhook으로 전환하고 ETA 알림 호출부를 함께 변경했다. SLACK_WEBHOOK_URL이 없으면 선택 알림만 비활성화한다. 기존 DISCORD_WEBHOOK_URL을 fallback으로 사용하지 않는다.
- JSON text·10초 timeout·redirect 미추적·HTTP 200/ok 응답 확인을 적용한다. 요청 예외와 실패 응답 본문/URL을 오류 출력에 노출하지 않는다. 별도 SDK나 provider 선택 계층은 추가하지 않았다.
- 실패 callback에 가능한 경우 Airflow 로그 URL을 함께 담는다. 전송 실패는 원래 태스크 오류를 가리지 않으며 Gold 완료 알림과 보고서 트리거의 독립 의존성을 유지한다. ETA의 관측 의미와 보고서/D-4/D-1 날짜 구분을 보존한다.
- .env.example·Kubernetes Secret 생성·오프라인 설정·현재 README/실행 문서를 맞췄다. 합성 Airflow는 상속된 Slack URL을 비운다. 과거 Discord 기반 실행 증거와 SESSION_HANDOFF 기록은 당시 결과로 보존했다.
- 실제 로컬 .env와 환경에는 Slack URL 설정이 없음을 값 출력 없이 확인했다. 원래 .env·기존 비밀 값·예약 관측 계획은 변경하지 않았다. 채널 연결과 배포 환경의 실제 전송은 미검증이다.

### 검증과 남은 작업

새 HTTP mock 회귀 11개를 포함해 UTC 파이프라인 365 passed, 126.98초·Airflow/Pod 12 passed, 1.27초로 서로 다른 회귀 총 377개가 통과했다. 첫 관련 29개 검사는 전체와 중복이며 별도로 더하지 않는다. JSON payload와 성공 응답, timeout/연결/실제 requests HTTP 오류 응답, 예상과 다른 응답과 redirect, traceback/로그의 비밀 값 숨김, 기존 Discord 설정 무시, 실패 callback과 Gold 날짜 계약을 확인했다. Ruff·Secret 셸 구문·diff whitespace·문서 상대 링크/공개 JSON/비밀 값 미포함 검사도 통과했다.

private 결과는 .work/slack-{regression,airflow}.log다. HTTP mock 외 실제 Slack·서울 API·AWS·Databricks·LLM 호출과 Kubernetes 배포는 수행하지 않았다. 실제 사용은 Slack 앱의 채널별 Incoming Webhook을 로컬 SLACK_WEBHOOK_URL에 설정하고 실행 환경에 전달해야 한다. 운영 worker는 새 코드 이미지와 갱신한 Secret을 적용해야 하며 기존 fixture 이미지는 Slack 전환 검증 증거로 소급하지 않는다. 중복 제거/자동 전송 재시도는 제공하지 않으며 Airflow 재시도 시 중복 메시지가 가능하다. 기능·설정·회귀·문서를 main에 커밋·일반 push하고 해당 HEAD의 네 CI job을 확인한다.

## 2026-10-13 승하차 수집의 페이지 완전성

사용자의 다음 고도화 요청으로 main/origin/main 011a200이 같고 사용자·staged 변경이 없는 상태에서 진행했다. 직전 Slack 전환 CI 38410113753의 네 job과 검증 artifact 네 개가 성공한 상태다. 이번 작업은 코드와 로컬 검증에 집중했다.

기존 collector에 동일한 HTTP mock 세 사례를 주입했다. total=4의 두 반복 페이지가 rows=4·고유 자연키=2로 성공했고, total=2.9와 True도 각각 2와 1로 변환돼 성공했다. 총 건수만 맞춘 응답이 원천 내부 누락을 숨기는 문제다.

- total count를 양의 JSON 정수 또는 ASCII 정수 문자열로 제한한다. boolean·float·음수·0·잘못된 문자열을 임의 변환하지 않는다. RESULT와 row의 잘못된 형태도 민감한 본문 없이 SourceError로 분류한다.
- 요청 범위와 total로 페이지별 예상 길이를 계산하고 짧거나 초과한 페이지를 즉시 거부한다. 마지막 페이지의 정상 잔여 건수는 허용한다. 설정된 페이지 예산을 넘는 total은 첫 응답에서 추가 요청 없이 중단한다.
- 한 수집 안에서 이용 날짜·앞뒤 공백을 제거한 노선·역명의 중복을 거부한다. 반복 페이지와 경계 중복, 동일 키의 상충 정정이 Bronze에 저장되지 않게 한다. 서로 다른 노선의 같은 역명과 다른 수집 실행의 정정 이력은 보존하며 반환 원문은 수정하지 않는다.
- CLI가 invalid 수집 뒤 save_to_bronze_delta를 호출하지 않는 연결 검사를 추가했다. 이 검사는 HTTP/저장 mock이며 실제 Delta 쓰기 장애 실험으로 확대하지 않는다.
- DATA_CONTRACTS·RUNBOOK·현재 컨텍스트·로드맵·README 한 줄에 계약을 반영했다. examples/operations/source-pagination.json은 동일 mock 입력의 전후 집계와 범위를 기록하며 원문·키를 포함하지 않는다.

### 검증과 범위

새 회귀 41개를 포함해 UTC 파이프라인 406 passed, 125.10초·Airflow/Pod 12 passed, 1.26초로 서로 다른 회귀 총 418개가 통과했다. 최초 수집/관측 관련 77개와 최종 수집 45개는 전체와 중복이며 별도로 더하지 않는다. 마지막 루프의 불필요한 빈 페이지 조건을 제거한 뒤 수집 45개를 다시 확인했다. 최종 Ruff·diff whitespace·문서 상대 링크 185개·공개 JSON·DAG CLI·비밀 값 미포함 검사도 통과했다.

private 결과는 .work/usage-pages-{before,after,regression,airflow}.log다. 합성 전후 세 사례가 모두 SourceError로 바뀌었고 실제 API GET은 0회다. 원천 API·AWS·Slack·Databricks·LLM 호출, Kubernetes 배포와 예약 관측의 일정/기간/예산 변경은 수행하지 않았다. 기존 실서비스 데이터와 .env는 보존했다.

이번 계약은 응답 내부 페이지 길이와 자연키 완전성이다. 원천이 처음부터 누락한 고유 역, 같은 키의 원천 값이 페이지 사이에 바뀌는 경우, 지속 수집 커버리지까지 증명하지 않는다. 실제 원천이 현재 grain에 대해 중복 키를 제공하면 이를 숨기지 않고 identity 정책을 먼저 검토해야 한다. 기본 5분 도착 수집과 원본 재처리 정책은 유지한다. 기능·회귀·계약·합성 집계 증거를 main에 커밋·일반 push하고 해당 HEAD의 네 CI job을 확인한다.

## 2026-10-13 공개 문서 표현 정리

사용자가 공개 문서의 반복적인 작업 보고 표현을 줄여 달라고 요청했다. README의 기존 섹션·배지·아키텍처를 유지하면서 안내 이름을 샘플·테스트 결과로 정리하고 플랫폼 소개의 반복 강조를 줄였다. 소개 하단은 코드 구조·runbook·데이터 계약으로 연결하며 개발용 작업 지침과 세션 기록은 별도 파일에 보존한다.

examples 목록은 처리 단계별 결과와 재현 명령을 한 표로 모았다. 공개 보고서·JSON·Job/Pod 기록과 해시는 수정하지 않았다. 합성 입력, 서울 API 1일·10분 관측, S3 미검증과 context 상대 경로 정규화의 설명을 유지했다. PROJECT_CONTEXT에 공개 문서 작성 기준을 기록했다.

수집 계약 666fb55의 CI 38413012405는 네 job 성공·artifact 네 개 보존·원격 회귀 406+12개를 확인했다. 이번 변경은 Markdown만 수정하므로 로컬 파이프라인 회귀는 반복하지 않았다. 샘플 보고서 manifest 검증·문서 상대 링크·공개 JSON·비밀 값 미포함·diff whitespace 검사를 통과했다. 문서 정리는 별도 커밋으로 main에 일반 push한다.

## 2026-10-13 README 분량 축약

README를 포트폴리오 소개에 맞게 다듬었다. 기존 섹션·배지·Mermaid 구성과 실행 명령은 유지하고 본문을 약 33% 줄였다. 플랫폼별 상세 수치·반복 설명을 줄이며 핵심 처리·복구 동작과 합성 입력·원천 관측·S3 미검증 범위는 남겼다.

문서 상대 링크 176개·공개 JSON·비밀 값 미포함·diff whitespace와 기존 샘플 보고서 manifest 검사를 통과했다. 코드 변경이 없어 로컬 회귀는 반복하지 않았다. 직전 0d712fd의 CI 38414218489는 네 job 모두 성공이었다. README와 이 기록만 main에 커밋·일반 push한다.

## 2026-10-16 수집·태스크 실행 시간 제한

사용자의 제출 전 마무리 요청으로 main/origin/main 67a30e5와 깨끗한 작업 트리에서 시작했다. fetch 뒤 추가 원격 변경은 없었고 직전 CI 38416294138의 성공을 확인했다.

- 도착 수집 45초·승하차 전체 페이지 수집 120초의 Linux main-thread process timer를 추가했다. socket read timeout과 구분하며 시간 초과 시 부분 입력을 저장하지 않는다. 기존 활성 timer는 덮어쓰지 않고 지원하지 않는 runtime에서는 호출 전에 실패한다.
- 일반 GET의 Retry-After가 10초를 넘으면 조기 재요청 없이 실패한다. 지수 대기 회당 5초·최대 네 시도와 redirect 거부를 적용한다. 주입된 제한 관측 session의 자동 재시도·예산은 변경하지 않았다.
- 모든 태스크·DAG에 실행 상한을 넣었다. 수집/처리 분리·5분 일정·ssdr_delta_writer 1 slot·독립 알림 의존성은 유지한다. 상한은 초기 보호 설정이며 실운영 SLA가 아니다.
- 실제 localhost HTTP의 slow-drip·긴 Retry-After·redirect와 페이지 중단, timer/handler 복원 및 다음 요청 성공을 검사했다. 새 Airflow 실험은 실제 TaskInstance.run에서 1초 상한의 Bash 자식 종료·지연 출력 부재·다음 실행 success를 확인했다. 최종 timeout 실행은 1.132초다. scheduler·Kubernetes에서 timeout을 검증한 것은 아니다.

DAG/Pod 13 passed, 0.80초와 Ruff·diff 검사를 통과했다. 전체 회귀와 새 환경·실제 보고서·운영 점검의 통합 결과는 다음 항목에 기록한다. CI에 timeout 실험과 결과 artifact를 추가했다. private 로그는 .work/finalize-{dags,final-timeout,final-regression}.log이며 실서비스 배포는 수행하지 않았다.

## 2026-10-16 실제 보고서·운영 점검·새 환경 확인

- check_pipeline.py는 완료된 로컬 Delta와 선택적 태스크 로그·캠페인을 대조하고 health JSON/Markdown에 조치 방법을 남긴다. 수집 기록 부재·Bronze 미처리·품질 오류·관측 나이·원천 시각을 구분한다. 과거 실패 시도는 최종 태스크 실패로 단정하지 않고 로그 미제공은 미검증으로 남긴다. process_source_experiment.py에는 처리 후 health 자동 생성을 연결했다.
- 보관된 10/11 실제 원본을 새 lake에서 처리해 7개 테이블 재처리를 확인했다. 이 원본의 10/7 이용량은 10/12 보고서의 D-4 날짜가 아니어서 발행이 차단됐다. 사용자가 별도로 승인한 1 GET으로 10/8 이용량 615행을 확보해 기존 10/11 도착 2,987개 유효 ETA 관측과 함께 10/12 기준 보고서를 생성했다. 실제 생성은 10/16이며 과거 생성으로 소급하지 않는다.
- 해당 원본 재처리에서 7개 테이블 행·전체 해시와 Bronze 버전, 보고서 본문·업무 context가 유지됐고 canonical 발행 검증이 통과했다. Silver/Gold 버전이 바뀌므로 context 전체 bytes 일치로 확대하지 않는다. 예전 도착 원본은 schema 1 미검증 이력을 유지하고 추가 승하차 파일은 수집 시 SHA256을 기록했다.
- 실제 데이터의 점검은 응답 상한·미래 원천 시각·과거 관측 나이로 degraded다. 별도 자동 health 생성까지 실제 저장 원본으로 실행했다. 원문·정제 행·보고서·context는 private .work에만 두고 공개에는 runtime-checks.json/report-check.json의 집계·시각·해시만 남겼다.
- 새 소스 폴더와 새 Python venv에 requirements를 설치하고 private .env 없이 서비스 설정을 제거한 환경에서 README의 합성 데모·보고서 검증·Ruff를 통과했다(79.084초). 첫 준비 시 python3.11이 PATH에 없어 실패했고 기존 설치된 Python 3.11 디렉터리를 PATH에 추가한 새 경로로 재실행했다. 같은 호스트의 Java·pip cache는 공유하며 새 OS/완전한 환경 lock 검증이 아니다.

최종 UTC 파이프라인 425 passed, 124.08초·DAG/Pod 13 passed, 0.80초로 서로 다른 회귀 438개가 통과했다. 이전 전체 424개와 관련 89/18/32개는 겹치는 검사이므로 더하지 않는다. Ruff·diff·문서 상대 링크 182개·공개 JSON·DAG CLI·비밀 값 미포함 검사를 통과했다. 로그와 실제 산출물은 .work/finalize-*에 보관한다.

관측 캠페인은 12~15일 실행 기록 미확인, 16일은 실제 KST의 전용 run CLI에서 missed_start_window/GET 0회다. plan·seal·기간·예산은 변경하지 않았다. 현재 도래 15슬롯 중 미확인 12·시작 놓침 3이며 미확인 날짜의 호출 수를 0으로 단정하지 않는다. 공식 automation_update로 기존 예약의 DTSTART를 Asia/Seoul 10/17 17:50, 남은 두 실행으로 명시했고 설정을 다시 읽어 확인했다. 늦은 기동의 근본 원인과 미래 실제 기동은 미확인이다. 컴퓨터·앱 실행이 필요한 로컬 예약이며 운영 상시 수집 성공으로 처리하지 않는다.

추가 실서비스 GET은 승인된 승하차 한 번뿐이다. AWS·Slack·LLM·Databricks upload·Kubernetes 배포는 없다. 장기 원천 baseline·운영 scheduler 감시·실제 S3·대규모 자원 검증은 미완료로 유지한다. README는 도착 관측 범위를 명확히 한 한 문장만 수정했다. 구현·검증·필수 문서와 공개 집계를 main에 일반 push하고 새 HEAD의 네 CI job을 확인한다.

## 2026-10-16 — 선택적 Prometheus·Grafana

사용자가 요청한 범위로 전용 로컬 ssdr-monitoring namespace에 exporter·Prometheus·Grafana를 배포했다. Terraform은 실제 관리 대상 AWS 인프라가 정해질 때 검토한다. 기존 Airflow 배포와 캠페인 일정은 바꾸지 않았고 서울 API·AWS·Slack·LLM·Databricks upload는 호출하지 않았다.

- task callback·audit_lake의 선택적 숫자 snapshot, Linux 파일 lock·atomic replace, 입력 종류 분리와 읽기 전용 exporter를 추가했다. 키·원문·run ID를 지표에 넣지 않는다.
- 두 5분 DAG의 성공 시각·실패·재시도, 저장 원본 미처리·감사 신선도·마지막 worker cgroup 측정을 대시보드에 연결했다. 누락·손상·exporter 단절을 정상 0과 구분한다.
- 실제 로컬 TaskInstance 첫 실패(up_for_retry), 두 번째 성공을 Kubernetes Job에서 실행했다. 실패 시 SSDRTaskFailed/SSDRProcessingBehind firing, 복구 후 firing 없음, exporter replicas=0 시 SSDRExporterDown firing, replicas=1 후 지표 유지·해제를 확인했다. Grafana provisioning과 datasource proxy 실제 쿼리, 브라우저 차트도 확인했다. 품질 값과 작업 명령은 합성이다.
- 첫 점검에서 Grafana 384Mi 한도의 OOM과 fixture의 Airflow operator 복사 후 재시도 명령 유지 문제를 발견했다. Grafana 한도 1Gi·재시도 operator 재연결로 수정한 뒤 정상 복구를 확인했다. 기록은 private .work/monitoring, 공개 숫자 요약은 examples/operations/monitoring.json이다.
- 파이프라인 전체 **446 passed**, Airflow DAG/Pod **13 passed**. Promtool config 7개 규칙·6개 실패/복구 사례, 실제 컨테이너 retry callback 검증, Helm monitoring overlay worker template 검증, Ruff·actionlint·문서 링크/비밀 값 검사 통과. 처음 DAG 검사는 PYTHONPATH 누락으로 실패해 CI와 같은 .:dags 설정으로 재실행했다.
- CI에 경고 규칙·실제 callback 복구·선택적 Helm overlay 검사를 추가했다. main 직접 커밋·일반 push 경로를 사용하며 원격 CI 결과는 해당 커밋의 Actions 실행 기록으로 확인한다.

남은 범위: 기존 운영 경로에 reviewed 이미지·metrics overlay를 적용하고 완료 구간 감사의 갱신 일정을 운영하는 일, 실수집 장기 임계값 조정과 외부 경고 전달. 이번 fixture는 API 수집·Delta 변환·지속 scheduler 운영의 추가 증거가 아니다. Prometheus 데이터는 2일/512MB, callback 누적은 metrics PVC 보존 범위다. 현재 로컬 대시보드 port-forward는 loopback 13000, Prometheus는 19090이며 종료되면 MONITORING.md 명령으로 다시 연결한다.

## 2026-10-16 — 실제 LLM 보고서와 S3 연결 점검

사용자는 저장된 자료의 집계·기준일을 OpenAI에 전달하는 LLM 1회(출력 상한 1,000토큰·SDK 자동 재요청 없음), 기존 S3 버킷의 새 검증 경로에 네 파일 저장·읽기를 승인했다. 키는 출력하지 않았고 추가 서울 API·Slack·Databricks 호출이나 운영 Kubernetes 배포는 하지 않았다.

- 기존 report_daily가 호출하는 generate_daily_report를 같은 소스로 컨테이너에서 실행했다. 저장 Gold의 2026-10-08 승하차와 2026-10-11 도착 관측으로 2026-10-12 기준 보고서를 실제 생성했다. 실행일과 보고서 기준일은 구분한다.
- 실제 OpenAI 호출은 1회 성공했다. 모델 gpt-5-mini-2025-08-07, prompt 2,000·completion 931·total 2,931토큰, finish_reason=stop. 본문·context·manifest·publication은 private .work/llm-s3-check-20261016-001/llm-report에 보존했다.
- 프롬프트를 집계·기준일 allowlist로 제한해 저장 경로·원문·개별 역 기록을 제외했다. 모델·전송 프롬프트 해시·토큰 사용량을 manifest에 기록하고, 빈/잘린 응답과 요청 실패는 쓰기 전에 차단한다. provider 오류 문자열이 로그에 노출되지 않게 한다.
- S3는 새 v2/validation/llm-report/<임의 UUID> 경로의 첫 PUT 1회에서 403 InvalidAccessKeyId를 반환했다. 성공한 업로드·읽기는 0개이며 재요청하지 않았다. 사용자에게 .env의 AWS 자격 증명 갱신을 요청했다. 갱신 후에는 보존된 발행 객체를 사용하며 LLM을 다시 호출하지 않는다.
- 전체 파이프라인 453 passed(160.14초), 마지막 요청 오류 회귀 추가 후 publication 대상 10 passed, DAG/Pod 13 passed. 서로 다른 검사는 총 467개다. Ruff·diff·문서 링크·공개 파일의 비밀 값 미포함 검사 통과. 원격 CI는 해당 커밋의 Actions 기록으로 확인한다.
- README·맥락 문서에서 LLM 보고서가 핵심 목적임을 바로잡았다. 개발/fixture의 template 기본값과 배포 설정은 유지하고 실제 운영 활성화와 구분했다. OpenAI 공식 API와 GPT-5 매개변수 문서로 token cap·minimal 지원을 확인했다.

남은 작업은 AWS 키 갱신 후 승인된 네 파일의 S3 저장·동일 bytes 읽기 확인, 운영 DAG의 생성/업로드 재시도 분리, LLM 해설의 수치·의미 평가다. 이번 보고서 검증은 정상 종료·발행 해시·기준일 계약이며 문장 전체의 사실성 검증이나 Kubernetes에서의 상시 LLM 운영을 뜻하지 않는다. 공개 메타데이터는 examples/operations/llm-report-check.json이며 원문·본문·context·키·버킷 주소는 공개하지 않는다.

### AWS 자격 증명 갱신 후 재개

사용자가 .env 갱신 완료를 알려 승인된 S3 단계만 재개했다. 기존 llm-report 발행 객체를 사용했으며 OpenAI를 다시 호출하지 않았다. 새 자격 증명으로 첫 PUT 1회에서 404 NoSuchBucket을 받았다. 앞선 403 InvalidAccessKeyId와 구분해 공개 메타데이터에 이력을 보존했다. 누적 S3 HTTP 요청은 2회, 성공한 업로드·GET은 0회다. .env의 실제 기존 버킷 이름·리전 확인을 사용자에게 요청했으며 새 버킷을 만들거나 다른 버킷을 열거하지 않았다. private s3-resume-result.json·예약 파일은 유지한다. 이전 구현 커밋 e7b4d7c의 CI 38918168297은 네 job 모두 success다. 이번 변경은 점검 결과 메타데이터·인계 기록뿐이며 외부 재시도와 전체 회귀 반복은 하지 않았다.

### 사용자 승인으로 버킷 생성·S3 검증 완료

사용자가 버킷 생성부터 직접 진행하도록 명시적으로 승인했다. 현재 .env 자격 증명의 AWS identity를 확인한 뒤 ap-northeast-2에 충돌 방지 이름의 프로젝트 전용 S3 버킷을 생성했다. 퍼블릭 차단 네 설정·AES256 기본 암호화·BucketOwnerEnforced·리전을 실제 조회했고 프로젝트 태그를 적용했다. 버킷 제어/확인 S3 8회·STS identity 1회이며 SDK 자동 재요청 없이 수행했다. IAM 권한·버킷 공개 정책·삭제 일정은 변경하지 않았다.

.env의 SSDR_S3_BUCKET·SSDR_AWS_REGION만 연결하고 다른 값이 보존됐는지 확인했다. 보관된 llm-report의 네 파일을 기존 save_s3_publication으로 저장하고 네 GET의 bytes·해시·AES256 응답과 원격 publication을 검증했다. 이번 객체 요청 8회, 앞선 인증/버킷 실패 두 번을 합친 객체 요청 10회, 전체 S3 요청 18회·STS 1회다. 실제 OpenAI 호출은 이전의 1회 그대로다. 버킷/계정/endpoint·본문·context·인증 정보는 공개하지 않는다. private bucket-provision*·s3-bucket-resume* 결과를 보존하고 공개 집계와 ROADMAP을 갱신했다.

검증 범위는 보고서 객체다. 새 버킷의 Delta lake 수집/변환, Kubernetes 운영 Secret·LLM 모드·상시 일정은 활성화하지 않았다. 코드 변경은 없으며 기존 보고서 회귀와 문서/JSON/비밀 값 미포함 검사를 확인한다.

## 2026-10-17 환경 설정 정리

로컬 .env의 미사용 DISCORD_WEBHOOK_URL·AIRFLOW_GID·SSDR_S3_PREFIX를 제거하고 .env.example과 같은 15개 설정으로 정리했다. 현재 저장 prefix는 SSDR_STORAGE_PREFIX이며 기본 동작 v2를 유지한다. 기존 AWS·서울 API·OpenAI와 실행 계정 값은 전후 비교로 보존을 확인했고 원본은 ignored .work의 private 사본에 보관했다. 예제는 원천·저장소·LLM·선택 Slack·Compose 용도로 구분하고 RUNBOOK의 이전 Discord 전환 안내를 제거했다. 알림의 과거 설정 무시 회귀 테스트는 유지했다.

기존 알림·보고서 발행 회귀 21 passed(0.73초), Compose config --quiet 파싱과 env 키/기존 값 보존·이전/현재 비밀 값의 공개 파일 미포함·문서 링크·diff 검사를 확인했다. 외부 서비스 호출·배포·스케줄 변경은 없고 .env·private 백업은 커밋하지 않는다. 변경은 공개 예제·운영 안내·이 기록뿐이다.

## 2026-10-17 보고서 생성·전송 분리와 해설 검사

report_daily를 날짜 계산→생성→업로드로 분리했다. 생성은 자동 재시도하지 않고 실행 ID 해시별 발행 객체를 보존한다. XCom에는 기준일·경로·발행 객체 해시만 전달하며 upload_report만 두 번 재시도한다. 업로드는 저장된 bytes와 해시를 검사하고 Gold 조회·LLM 호출·날짜별 최신 export fallback을 하지 않는다. 수동 재개 시 완전한 같은 실행 객체를 재사용한다. 모델 응답과 파일 보존 사이의 중단까지 exactly-once를 보장하지 않으며 수동 생성 재실행의 추가 비용 가능성을 문서화했다. S3 SDK 자동 재시도는 제거하고 오류 내용을 노출하지 않는다.

LLM 프롬프트는 지표 참조를 선택해 해설을 작성하도록 변경했다. 참조를 검증된 날짜·지표 이름·수치·단위로 치환하고 자유 문장의 숫자·알 수 없는 참조·단위/배수 덧붙임·기간 비교·ETA의 지연/대기시간 해석 표현은 발행 전에 거부한다. 실패 시 이전 발행을 보존한다. 자유 문장의 전체 의미는 검증되지 않으므로 manifest에도 false를 기록한다. 공식 OpenAI prompt engineering 가이드를 확인했고 새 프롬프트의 실제 모델 준수율은 아직 측정하지 않았다.

Kubernetes의 보고서 경로를 기존 logs PVC에 지정해 서로 다른 생성·업로드 Pod의 파일 공유를 준비했다. llm-values.yaml은 명시적 적용용이며 기본/fixture template 모드를 유지한다. 새 DAG paused 설정은 기존 unpaused DAG를 일시정지하지 않는다. LLM+monitoring·fixture+monitoring Helm 렌더와 실제 DAG 18개 Pod override 병합·직렬화를 통과했다. 신규 합성 실행은 업로드를 포함한 13개 태스크를 요구하며 기존 12개 worker 자원 측정 기록은 계속 읽을 수 있다.

파이프라인 전체 474 passed(168.95초), 마지막 단위/배수 검사 추가 후 관련 회귀 76 passed(2.46초), Airflow/Pod 14 passed(1.11초)다. 중복 검사는 합산하지 않는다. 실제 로컬 TaskInstance 연결 실행은 세 DAG·13개 성공·API 수집 1개 skipped·발행 해시 일치로 완료됐다(78.826832초). 초기 관련 59/49개도 전체·최종 검사와 중복이다. Ruff·복구 CLI help·문서 링크·공개 파일의 비밀 값 미포함·diff 검사를 확인했다. 상세 로그는 private .work/report-reliability-20261017에 있다.

실제 API·AWS·Slack·LLM 호출, Kubernetes 배포·일정 활성화와 원천 관측 캠페인 변경은 없었다. .env와 기존 원본은 보존했다. 남은 작업은 승인된 예산에서 새 프롬프트의 실제 모델 준수율 평가와 Kubernetes 연속 운영이다. 구현·회귀·복구 안내를 main에 직접 커밋하고 일반 push하며 원격 CI는 해당 커밋의 Actions 기록으로 확인한다.
