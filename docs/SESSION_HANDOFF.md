# 세션 인계

## 2026-10-10 완료한 로컬 고도화

사용자는 Kafka가 필수가 아니라면 제거하고, DE 포트폴리오 고도화·커밋 스킬·agent 파일을 자율적으로 진행하도록 요청했다.
브랜치는 codex/de-portfolio-hardening이다. 변경은 로컬 커밋으로 기록했으며 push와 실서비스 배포는 실행하지 않았다.

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

로컬 커밋은 `codex/de-portfolio-hardening`의 git log에서 확인한다. 다음 검증은 live 자료와 운영 환경 접근이 포함되므로 이번 완료 범위와 구분한다.

후속 로컬 커밋: af1b05d(Delta 감사), 050e59c(태스크 관측), 78406d3(복구 실험). 문서·샘플 커밋은 바로 다음 git log 항목에 기록한다.
