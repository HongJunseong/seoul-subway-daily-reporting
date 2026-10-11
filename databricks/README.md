# Databricks의 승하차 처리

Databricks는 승하차 Bronze → Silver → Gold의 대체 실행 환경이다. Free Edition serverless와 Unity Catalog 관리형 Delta 테이블을 사용한다. API 수집·도착 관측·일간 보고서의 현재 Airflow 경로와 역할을 구분한다. S3와 Unity Catalog 테이블을 자동 복제하거나 서로 같은 저장소로 취급하지 않는다.

## 구현

노트북은 src의 운영 승하차 변환 함수를 그대로 호출한다. NULL/잘못된 수치를 0으로 바꾸지 않으며 자연키는 이용 날짜·노선·역이다. 최신 REG_YMD·수집 시각·record hash로 원천 정정을 선택하고 노선을 보존한 역별/노선별 Gold를 만든다.

src/platforms/managed_usage.py는 저장 대상만 관리형 테이블로 바꾼다. 읽기 버전을 먼저 고정하고, 자연키 MERGE와 해당 날짜의 whenNotMatchedBySourceDelete를 하나의 테이블 커밋으로 처리한다. 다른 날짜는 삭제하지 않는다. Silver·두 Gold의 커밋은 각각 별도이며 부분 실패는 같은 Bronze에서 재처리한다. S3 운영의 replaceWhere와 업무 결과가 같은지 합성 검사로 비교한다.

Serverless 세션을 그대로 사용한다. 자체 SparkSession/JVM/JAR 설정, S3 키, FileStore/DBFS mount를 사용하지 않는다. KST와 ANSI 모드의 잘못된 입력을 고려해 공통 변환에서 try_cast/try_to_timestamp로 품질 오류를 명시적으로 차단한다.

## 실제 workspace Job

저장소 루트 databricks.yml은 seed_fixture → silver → gold → verify_replay의 serverless Job을 정의한다. 동시에 한 실행만 허용하고 schedule은 만들지 않는다. 기본 namespace workspace.ssdr_v2_fixture에서 *_v2 테이블만 사용한다. 기존 workspace.default의 학습 테이블과 원본 notebook은 변경하지 않는다.

```bash
databricks auth login --host YOUR_WORKSPACE_URL --profile SSDR_PORTFOLIO
databricks bundle validate --profile SSDR_PORTFOLIO -t free
databricks bundle deploy --profile SSDR_PORTFOLIO -t free
databricks bundle run usage_fixture --profile SSDR_PORTFOLIO -t free
```

CLI OAuth는 사용자가 직접 인증한다. 토큰을 채팅·코드·증거 파일에 넣지 않는다. sync.paths는 src, fixtures, databricks/notebooks로 한정하며 pycache/.env를 제외한다. Job 인자의 project_root는 bundle workspace.file_path로 전달한다. UI에서 개별 notebook을 실행할 때도 전체 소스를 upload한 절대 workspace 경로를 이 widget에 지정한다.

fixture seed는 합성 원문·중복·정정과 인접 날짜만 append한다. 기존 fixture Bronze는 삭제/overwrite하지 않으며 비합성 run_id가 있으면 거부한다. 검증은 업무 컬럼 전체 hash, 날짜별 합계 680/660, Bronze 불변과 관측한 Delta 버전을 남긴다. 결과가 서울 실제 이용량을 뜻하지 않는다.

## 로컬 계약 검사

```bash
python scripts/managed_usage_experiment.py --output-dir .work/managed-usage/my-run
pytest -q tests/test_managed_usage.py
```

로컬 Spark/Delta managed table의 실제 커밋을 사용하며 Databricks 실행 증거와 구분한다. ANSI 모드에서 잘못된 문자열·정수 overflow·잘못된 revision 날짜·빈 입력이 이전 Silver 내용과 버전을 보존하는지 검사한다.

## 선택적 Bronze snapshot export

현재 Job은 합성 fixture를 managed Delta에 직접 만든다. 별도 저장 원본을 검토할 때만 [snapshot export CLI](../scripts/export_usage_snapshot.py)를 사용한다. Delta 루트와 versionAsOf로 읽고 이용 날짜를 필터링해 독립된 Parquet snapshot을 만든다. raw_payload·record hash·수집 시각·run ID와 입력 버전·행 수 manifest를 보존한다. Delta 내부 파일을 병합하지 않는다. S3 입력은 --allow-s3로 명시적으로 선택한다.

```bash
PYTHONPATH="$PWD" SSDR_DATA_ROOT=/path/to/local/lake spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  scripts/export_usage_snapshot.py \
  --delta-root /path/to/local/lake/v2/bronze/subway_usage_delta \
  --use-ymd 20260128 --version 0 --output .work/exports/usage-01
```

실데이터 snapshot의 workspace upload는 별도 작업이다. 현재 Job은 API/AWS/Discord/LLM을 호출하지 않으며 다른 notebook/Job과 같은 managed namespace에 동시에 쓰지 않는다.

공식 기준: [Free Edition 제한](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), [serverless API/저장 제약](https://docs.databricks.com/aws/en/compute/serverless/limitations), [OAuth CLI 인증](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m), [bundle sync 범위](https://docs.databricks.com/aws/en/dev-tools/bundles/reference#sync).

## 확인한 실제 실행

2026-10-11 기존 Free Edition에서 serverless Job 네 태스크가 모두 SUCCESS였다. Run 44336808137994, 전체 197.11초, 정정 날짜 680·인접 날짜 660, Bronze 불변, 로컬 Spark 3.5.7과 Databricks 4.2.0의 업무 컬럼 해시 일치를 확인했다. [공개 결과와 실제 Job 링크](../examples/databricks/README.md)에 실패 후 수정 기록과 측정 범위를 함께 보존했다.

실제 결과를 새 경로에 기록하는 예:

```bash
python scripts/record_databricks_run.py --profile SSDR_PORTFOLIO \
  --run-id 44336808137994 --local-evidence .work/managed-usage/my-run/evidence.json \
  --output-dir .work/databricks-native/my-run
```

기존 출력은 덮어쓰지 않는다. 개인 인증 파일은 .work 등 ignored 경로에 보관하고 Job ID 외 사용자 이메일·토큰은 공개 증거에 넣지 않는다.

## Free Edition의 품질·SQL 제공 실험

usage_quality Job은 새 ssdr_v2_fixture_quality_* schema에서 잘못된 최신 정정 차단·정상 정정·Delta history/time travel·원본 재처리를 확인하고 SQL view를 만든다. 기존 usage_fixture Job과 테이블은 보존한다. SQL warehouse는 별도의 opt-in 기록 도구로 실제 조회하며 notebook 전체 view 행 및 SQL source 해시와 비교한다.

[Free Edition 재현 절차](../docs/DATABRICKS_FREE.md)와 [실제 결과](../examples/databricks/free-edition.md)를 확인한다. SQL 모델의 품질 실패 날짜는 분석 view에서 제외하고 품질 상태에서 FAIL로 남긴다. 관리형 모니터링이나 enterprise 권한 검증과 구분한다.
