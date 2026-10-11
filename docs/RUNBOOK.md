# 실행과 복구

## 로컬 재현

Python 3.11과 Java 11/17을 준비한다. 저장소 루트에서 실행한다.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements/dev.txt
python scripts/demo.py --output-dir .work/demo/run-01
python scripts/verify_report.py .work/demo/run-01/reports/subway_report_20260201.md
pytest -q --ignore=tests/test_dags.py --ignore=tests/test_kubernetes_profiles.py
ruff check src dags scripts tests
```

Maven 의존성은 첫 Spark 실행에 다운로드된다. 데이터와 출력은 .work에만 생성하며 실제 API·S3·webhook을 호출하지 않는다. 이미 데이터가 있는 데모 출력 경로는 새 경로로 바꾼다.

Airflow 검증은 호환성을 위해 별도 환경을 쓴다.

```bash
python3.11 -m venv .venv-airflow
.venv-airflow/bin/pip install "apache-airflow[cncf.kubernetes]==2.7.3" --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.11.txt
.venv-airflow/bin/pip install pytest==8.4.2
AIRFLOW_HOME="$PWD/.work/airflow" PYTHONPATH="$PWD:$PWD/dags" .venv-airflow/bin/pytest -q tests/test_dags.py tests/test_kubernetes_profiles.py
```

## API·S3 실행 준비

.env.example을 참고해 .env를 로컬에서 설정한다. .env를 커밋하거나 출력하지 않는다. 프로그램은 환경 변수를 읽으므로 터미널은 환경을 주입해야 하고 Compose는 env_file을 사용한다. AWS 기본 자격 증명 체인(환경·profile·역할)을 사용하며 단기 자격 증명에는 session token도 필요하다.

필수 값은 SSDR_S3_BUCKET, SEOUL_PASSENGER_API_KEY, SEOUL_ARRIVAL_API_KEY다. prefix 기본값은 v2다. Discord webhook은 비워두면 전송하지 않으며 기본 보고서는 OpenAI 키를 사용하지 않는다. LLM은 requirements/llm.txt 설치와 SSDR_REPORT_MODE=llm 설정을 별도로 요구한다.

```bash
docker compose -f docker/docker-compose.yaml build
docker compose -f docker/docker-compose.yaml up airflow-init
docker compose -f docker/docker-compose.yaml up -d airflow-webserver airflow-scheduler
```

Compose init은 ssdr_delta_writer pool을 1 slot으로 생성한다. 모든 Spark 작업이 이 pool을 공유하므로 동시에 Delta를 쓰지 않는다.

웹 UI는 localhost:8080, 로컬 기본 계정은 airflow/airflow다. 기본 DAG는 paused다. 먼저 arrival_collect_5m을 활성화하고 실제 Bronze snapshot 저장을 확인한 뒤 pipeline_arrival_5m과 usage_daily를 활성화한다. 최초 15분 구간이나 도착 D-1 관측이 없는 첫날은 처리/보고서가 실패할 수 있다. 실제 관측을 수집한 다음 재실행하며 누락을 정상 데이터로 대체하지 않는다.

Compose는 로컬 개발용 설정이다. 설정 파싱과 후속 Docker 이미지 빌드는 통과했다. Compose scheduler 전체 실행은 별도 미검증이다. 컨테이너 Spark 명령은 /opt/pipeline/bin/spark-submit을 사용한다.

## 재처리

usage_daily 수동 실행 conf 예:

```json
{"target_ymd": "20260201", "reprocess_only": true}
```

생산일 20260201은 승하차 20260128, 도착 20260131을 뜻한다. reprocess_only=true면 usage Bronze 수집을 생략한다. 저장된 원본으로 Silver→Gold→도착 일간 Gold→보고서를 다시 만든다. Airflow 밖의 수동 Spark/CLI는 pool 보호를 받지 않는다. 동일 prefix의 다른 writer를 중지한 뒤 실행한다. S3에서는 서로 다른 파티션이라도 같은 테이블의 driver 동시 쓰기를 피한다.

승하차 범위 재처리는 기본적으로 API를 호출하지 않는다.

```bash
PYTHONPATH="$PWD" spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 src/jobs/backfill_usage.py --start 20260128 --end 20260129
```

로컬 저장을 재처리할 때는 SSDR_DATA_ROOT에 lake 디렉터리를 설정한다. 새 원천 조회가 필요할 때만 --fetch를 추가한다. 범위는 최대 366일이다.

도착 원본은 수집된 기간만 재처리할 수 있다. Spark 패키지·PYTHONPATH를 위와 같이 제공하고 build_fact_subway_arrival_spark.py --day YYYYMMDD, build_gold_subway_arrival_spark.py YYYYMMDD 순서로 실행한다. 15분 처리는 두 작업에 동일한 --as_of "YYYY-MM-DD HH:MM:SS"를 사용한다. API로 과거 관측을 생성할 수 없다.

## 장애 판단

| 증상 | 확인·조치 |
|---|---|
| 승하차 미제공·페이지 불완전 | API 코드·응답 건수 확인; 날짜 변경 없이 원천 준비 후 재수집 |
| 품질 검증 실패 | raw_payload·run_id·최신 정정 확인; NULL을 0으로 바꾸지 않고 원천 수정/정책 결정 |
| 도착 Bronze 없음 | 실제 수집 이력 확인; 수집 누락으로 기록; 과거 수집 시각을 위조하지 않음 |
| Delta 쓰기 충돌 | 같은 파티션 동시 쓰기를 중지하고 저장된 원본으로 태스크 재시도 |
| 선택적 webhook 실패 | 해당 알림 태스크만 재시도; 보고서 트리거의 상태는 따로 확인 |
| 보고서 Gold 날짜 없음 | 해당 날짜 Silver·Gold부터 복구; 다른 날짜 데이터로 대체하지 않음 |
| manifest 해시 불일치 | 게시 세트를 사용하지 않고 동일 입력으로 다시 발행 |

스케줄러가 Bronze 수집 성공 후 실패해도 usage 자연키와 arrival snapshot 중복 제거가 집계의 중복을 막는다. API를 다시 호출해 새로운 snapshot을 얻은 것은 과거 snapshot의 복구와 다르다.

## Kubernetes 후보 설정

k8s/airflow-values.yaml은 src와 DAG를 같은 이미지에 포함하고 git-sync/init clone을 사용하지 않는다. 이미지 태그는 ssdr-airflow:portfolio-v4다. 저장소 루트의 docker build -f docker/dockerfile -t ssdr-airflow:portfolio-v4 . 명령으로 빌드할 수 있다. 로컬 클러스터에 이미지를 제공한 후 namespace, Secret, Airflow Helm 릴리스를 준비해야 한다. Secret 생성은 프로젝트 상대 경로로 .env를 읽는 k8s/create-secret.sh 한 경로를 사용한다.

DAG 활성화 전에 Airflow UI의 Admin → Pools에서 ssdr_delta_writer를 1 slot으로 생성한다. pool이 없으면 Spark 태스크는 대기한다. CLI는 airflow pools set ssdr_delta_writer 1 serialize_delta_writers를 사용할 수 있다.

합성 fixture는 실제 Docker Desktop 클러스터에서 Helm 설치·PVC binding·scheduler 실행·worker 복구를 검증했다. 운영 배포 전에는 metadata DB·다중 node storage class·작업 Pod 이미지·S3 자격 증명을 별도로 검증한다. 과거 Kafka namespace나 릴리스는 이 코드 변경만으로 삭제되지 않으며 기존 리소스 정리는 별도 운영 작업이다.

## v1 데이터

v2 prefix의 새 계약과 기존 테이블 스키마는 다르다. 사용 가능한 승하차 날짜는 API 재수집으로 v2를 생성할 수 있다. 과거 arrival은 기존 원문의 필드·수집 시각을 확인하는 명시적인 migration이 필요하며 아직 자동 migration을 제공하지 않는다. 기존 테이블을 v2 경로로 복사하거나 overwrite하지 않는다.

## Databricks와 Kubernetes 재현

Databricks는 [전용 문서](../databricks/README.md)의 공식 CLI OAuth와 bundle을 사용한다. 기본 Job은 namespace=workspace.ssdr_v2_fixture, max_concurrent_runs=1, 일정 없음이다. 같은 managed namespace를 다른 notebook/Job에서 동시에 쓰지 않는다. Bronze/Silver의 입력 버전을 고정하고 날짜 제한 MERGE로 결과를 교체한다. Gold 두 테이블은 별도 트랜잭션이며 부분 성공 시 같은 원본을 재처리한다.

Kubernetes의 chart/provider·태스크 자원·RBAC·PVC·합성 배포와 worker 삭제·재시도 실험은 [KUBERNETES](KUBERNETES.md)를 따른다. 운영 profile은 관리되는 external metadata DB의 ssdr-metadata Secret과 검증된 storage class가 필요하다. 합성 namespace는 시간 기반 수집을 비활성화하며 live Secret을 사용하지 않는다. S3 writer pool 1 slot은 Pod 자원 프로필과 별도로 유지한다.

## 운영 저장소와 분리한 실제 원천 실험

서울 API만 허용된 경우 [SOURCE_OBSERVATION](SOURCE_OBSERVATION.md)의 명시적 --fetch 캡처와 네트워크 없는 저장 원본 처리 명령을 사용한다. 기존 경로를 덮어쓰지 않으며 .work 새 디렉터리만 허용한다. 실패·빈 응답·불완전 페이지는 정상 출판을 막는다. 실시간 D-1 관측이 없으면 과거 도착을 생성하거나 정상 일간 보고서로 보완하지 않는다.

## Kubernetes 자원 관측

fixture의 worker 계측과 실제 Pod/로그 연결은 [KUBERNETES_RESOURCES](KUBERNETES_RESOURCES.md)를 따른다. CPU는 표본 구간 평균이며 executor queue에는 Pod 준비 시간이 포함된다. missing/partial/다른 cgroup 상한은 검증 성공으로 기록하지 않는다. 운영 환경 계측 활성화와 실수집 부하 시험은 별도로 판단한다.

## Delta 파일 유지보수 검증

[STORAGE_MAINTENANCE](STORAGE_MAINTENANCE.md)는 별도 합성 lake의 닫힌 시간 compaction과 버전 고정 조회를 재현한다. 기존 lake를 받는 운영 CLI는 제공하지 않는다. 원본 재처리와 물리 파일 유지보수의 커밋 의미를 구분하며, 운영 S3 유지보수 도입 전에는 같은 writer pool 직렬화·실측 비용·보존 정책 검증이 필요하다.
