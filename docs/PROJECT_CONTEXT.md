# 프로젝트 컨텍스트

## 목적
제공 시점과 갱신 주기가 다른 서울 지하철 공공데이터를 보존·정제·검증하고, 재처리 가능한 일간 분석 결과로 제공한다.
기술 개수보다 데이터 정확성, 장애 후 재처리, 구현 선택의 근거와 실행 증거를 보여주는 DE 포트폴리오다.

## 현재 설계
- 승하차: 보고서 생산일 D-4의 날짜 지정 배치. 자연키는 이용 날짜·노선명·역명이다.
- 도착: 5분 API 폴링으로 얻은 열차·역별 관측값. 수집과 처리 DAG는 독립적이다.
- 보고서: 생산일 D-4 승하차와 D-1 도착 관측을 각각 표시한다. 역명만으로 두 원천을 조인하지 않는다.
- Bronze는 raw_payload·해시·수집 시각·run_id를 append한다. Silver에서 최신 버전을 선택한다.
- Silver/Gold는 Delta 테이블 루트를 읽고 날짜·시간 범위를 replaceWhere로 교체한다.
- 저장 경로는 v2/로 분리한다. 기존 v1 S3 데이터는 이 고도화에서 변경하지 않았다.
- 로컬 모드는 SSDR_DATA_ROOT를 사용한다. fixture는 합성 데이터이며 실제 서울시 분석 결과가 아니다.
- 기본 보고서는 결정적 Markdown이다. LLM은 명시적으로 선택한 추가 해설이며 검증된 수치를 대체하지 않는다.
- S3의 같은 Delta 테이블 동시 driver 쓰기를 피하기 위해 모든 Spark DAG 태스크가 1-slot pool을 공유한다.
- Kafka는 완료 알림을 위해 별도 운영할 이유가 약해 제거했다. Discord는 선택적인 독립 태스크다.

## 관측과 복구
- 로컬 감사 CLI는 Delta 루트·버전을 읽어 품질, 수집 시간 bucket, 처리 누락, 활성 파일 메타데이터를 남긴다.
- 태스크 callback 로그는 실행·시도·executor queue를 기록한다. 순수 pool 대기 시간은 별도로 측정해야 한다.
- 예외 주입 복구 실험은 테이블별 부분 커밋과 품질 오류 후 저장된 원본 재처리를 검증한다.
- 실제 로컬 Airflow 태스크 연결 실험은 AIRFLOW_OFFLINE.md에 있다. 운영 scheduler·executor 대기는 이 실험에서 측정하지 않는다.
- 명령과 해석 범위는 OBSERVABILITY.md에 있다. 공개 operations 샘플은 합성이며 live baseline을 대체하지 않는다.

## 불변 조건
동일 원본 재처리의 합계와 자연키 집합이 같아야 한다. 정정 데이터는 최신 값으로 반영된다.
정각·자정·월말의 집계 누락을 막는다. 누락 ETA를 0초로 바꾸지 않는다.
입력 날짜·키·수치·표본 분모가 잘못됐거나 데이터가 없으면 정상 보고서 출판을 막는다.
실제 운행 지연·승객 대기시간·당일 혼잡도를 관측 ETA나 D-4 이용량으로 단정하지 않는다.

## 검증과 작업 상태
실제 실행 결과는 SESSION_HANDOFF.md, 검증 절차는 RUNBOOK.md, 구현 선택은 adr/에 기록한다.
AWS·서울 API·Discord·LLM·Kubernetes 실배포는 로컬 fixture 검증과 구분한다.
과거 미수집 도착 스냅샷은 복원할 수 없다. 원본 보관 후 처리 실패만 재처리로 복구할 수 있다.

## 플랫폼 실행 경계 (2026-10-11)
- src/platforms/managed_usage.py는 같은 승하차 변환을 Databricks managed Delta에 적용한다. 날짜 제한 MERGE는 인접 날짜를 보존한다. S3 경로 writer의 replaceWhere와 구분한다.
- 기존 Free Edition에서 serverless Job 네 태스크 SUCCESS, Bronze 불변·재처리·업무 컬럼 해시의 로컬 일치를 확인했다. examples/databricks/에는 실제 API로 읽은 실행 증거가 있다.
- KubernetesExecutor는 태스크별 Pod 자원·namespace RBAC·PVC를 사용한다. 실제 Airflow 모델의 태스크 17개 병합/직렬화와 공식 chart 렌더를 확인했다. Spark는 Pod 내부 local[2]다.
- fixture 배포는 schedule=None, 서비스 Secret 미주입, synthetic 보고서·PVC 보존 경로를 강제한다. Docker Desktop Kubernetes에서 실제 scheduler·태스크 12개·worker 강제 삭제 후 새 Pod 재시도·로그 PVC 보존·동일 원본의 7개 테이블 해시 일치를 확인했다. KUBERNETES.md와 examples/kubernetes/에 실행 증거가 있다.
- 이미지의 Airflow·Spark Python 환경을 분리한다. 운영 metadata DB는 ssdr-metadata Secret으로 별도 연결한다. 보관된 PostgreSQL 이미지는 합성 fixture용으로만 제한한다.

## Free Edition 기능 선택 (2026-10-11)

- Databricks는 관리형 실행·변경 감사·SQL 소비 계약을 검증하는 별도 경로다. Airflow/S3와 중복 운영해야 하는 필수 구성으로 설명하지 않는다.
- 새 quality schema에서 음수 최신 정정이 이전 출판 내용/버전을 보존하고 정상 정정·time travel·재처리로 복구되는지 실제 serverless Job에서 확인했다.
- 세 Unity Catalog view는 날짜 의미·합성 표시·품질 PASS·역/노선 identity를 제공한다. 실제 기존 SQL warehouse의 네 쿼리와 notebook 전체 view 행·SQL source 해시가 일치했다.
- 기능 선택·검증 범위와 재현은 DATABRICKS_FREE.md, 실제 증거는 examples/databricks/free-edition*에 있다. enterprise RBAC·managed monitoring·실제 AWS 연동 경험으로 확대하지 않는다.

## 실제 원천 제한 관측 (2026-10-11)

기존 서울 키로 5분 간격 도착 세 번·D-4 승하차 하루를 별도 로컬 실험에서 수집했다. 도착 세 응답 모두 1,000건 상한에 도달했다. 기존 Bronze/Silver/Gold 코드로 처리하고 7개 테이블 전체 행 해시·Bronze 버전 불변을 검증했다. SOURCE_OBSERVATION.md와 examples/observed에 실제 집계 증거를 기록했다. 원문·정제 행은 ignored .work에만 보관한다. 장기 baseline·S3·플랫폼 실수집 운영은 남는다.

## Kubernetes 자원 측정 (2026-10-11)

fixture에서만 SSDR_TASK_RESOURCE_METRICS=true를 켜 worker cgroup v2를 읽는다. terminal callback과 실제 Pod UID·자원 상한을 연결해 scheduler/host 수치를 배제한다. DAG 트리거의 약 463MiB 메모리와 CPU quota 포화를 근거로 control을 CPU 250m/1, 메모리 512Mi/1Gi로 조정했다. 같은 합성 원본의 결과 보존과 전후 수치는 KUBERNETES_RESOURCES.md에 기록한다. Spark/report는 초기 예산을 유지하며 실수집 규모·순수 pool 대기는 미검증이다.

## 저장 파일 유지보수 실험 (2026-10-11)

compaction_experiment는 새로운 .work 합성 lake에서만 36,000행을 만든다. 닫힌 한 시간의 OPTIMIZE 전후 전체 행·인접 파일·Silver/15분 Gold 재처리를 검사한다. source replay의 Bronze 버전 불변과 유지보수의 dataChange=false 새 커밋을 구분한다. STORAGE_MAINTENANCE.md와 examples/operations/compaction.md에 조회 표본·선택 범위를 기록한다. 자동 일정·S3 쓰기·VACUUM은 수행하지 않는다.

## 저장소 실행 경로 정리 (2026-10-11)

현재 코드 탐색은 REPOSITORY_GUIDE.md, 실행 증거 탐색은 examples/README.md를 기준으로 한다. 사용하지 않는 역 Excel 로더·시간 alias·S3 보조 함수, 비활성 Parquet 병합기와 오래된 다운로드 manifest를 제거했다. Secret 생성은 k8s/create-secret.sh 하나로 통합하며 dotenv·표준 입력 전달을 유지한다. 선택적 버전 고정 Bronze export는 scripts/export_usage_snapshot.py, ETA 알림은 src/alerts/notify_arrival_eta_15m.py다. 실험의 전체 행 해시는 src/observability/fingerprints.py로 공유하며 기존 공개 증거의 형식·값을 보존한다. 과거 SESSION_HANDOFF의 경로는 당시 작업 기록이다.

## 표본 계획 감사 (2026-10-13)

summarize_source_observations.py의 --campaign-dir 모드는 고정 plan과 상태·캡처 메타데이터를 대조한다. 예정된 도래 슬롯은 캡처가 없어도 분모에 남고, 실패·시작 놓침·미확인을 구분한다. 모르는 호출량은 null이며 알려진 합계를 전체 호출량으로 단정하지 않는다. --capture-dir의 기존 기록 비교와 운영 DAG의 연속 5분 폴링은 별도다. 실제 원본·처리 품질·재처리는 이 메타데이터 감사가 검증하지 않는다.

## 수집 원본의 무결성 계약 (2026-10-13)

새 제한 관측 capture schema 2는 저장한 UTF-8 원본 파일의 SHA-256을 수집 시점에 기록한다. process_source_experiment.py는 공통 manifest 계약과 파일 체크섬을 새 출력·Spark 실행 전에 검증한다. 행 수와 수집 시각이 같은 값 변경도 거부한다. 기존 schema 1은 원본을 보존하며 unverified_legacy로 표시한다. 처리 시 계산한 해시는 수집 당시 무결성 증거로 소급하지 않는다. 메타데이터 계획 감사는 해시 형식만 검사하며 원본 bytes 검증과 구분한다. 합성 오류 주입의 실행 증거는 examples/observed/source-integrity.json, 계약은 SOURCE_OBSERVATION.md에 있다.

기본 개발 경로는 사용자 요청에 따라 main 직접 검증·커밋·일반 push다. PR은 요청할 때만 만든다. API 관측 자동화의 기간·시간·예산은 그대로 유지한다.

## 보고서 최종 소비 계약 (2026-10-13)

보고서는 Gold의 유한한 정수 건수·비어 있지 않은 identity·유효 ETA 분모와 지표를 독립 검증한다. 관측 범위가 있으면 UTC를 KST로 변환한 뒤 D-1 기준일과 시작/끝 순서를 확인한다. 검증 실패는 쓰기 전에 발생해 기존 보고서·context·manifest를 보존한다. 큰 합계의 int64 wrap과 가중 ETA 계산의 중간 곱 overflow를 피한다. examples/operations/report-contract.{md,json}는 합성 DataFrame 오류 주입의 실행 증거이며 live 원천/Delta 품질과 구분한다.
