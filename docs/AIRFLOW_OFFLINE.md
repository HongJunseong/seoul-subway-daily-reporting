# 로컬 Airflow 연결 실행 검증

DagBag 검사에 더해 실제 TaskInstance 실행으로 날짜 XCom, 재처리 분기, Bash의 Spark 제출, 다음 DAG 트리거, 보고서와 callback 로그를 검증한다. 합성 원본과 별도 SQLite DB를 사용하며 실제 API·AWS·Discord·LLM은 호출하지 않는다.

## 실행

Linux/WSL, Python 3.11, Java 11/17 환경에서 파이프라인과 Airflow를 서로 다른 가상환경에 설치한다. 기존 환경이 준비돼 있으면 실행 명령부터 시작한다.

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements/dev.txt
python3.11 -m venv .venv-airflow
.venv-airflow/bin/python -m pip install apache-airflow==2.7.3 \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.11.txt
.venv-airflow/bin/python -m pip install -r requirements/airflow-verification.txt
.venv-airflow/bin/python scripts/airflow_offline.py \
  --output-dir .work/airflow-offline/my-run \
  --pipeline-python .venv/bin/python
```

출력 디렉터리는 새 경로 또는 빈 경로여야 한다. 기존 실행 결과·운영 DB·원본은 덮어쓰지 않는다. 최초 실행은 Python/Maven 의존성 다운로드가 필요할 수 있다. 입력 데이터는 저장소의 fixture만 사용한다.

별도 Airflow 환경은 보고서 읽기에 필요한 pandas·PyArrow·deltalake만 추가한다. Spark driver/worker는 pipeline-python이 있는 가상환경을 사용한다. spark-submit도 같은 bin 디렉터리에서 찾으므로 가상환경 Python symlink를 실제 실행 파일의 디렉터리로 바꾸지 않는다.

## 실제 실행 경로

1. `seed_bronze.py`가 원천 정정과 반복 수집 이력이 있는 합성 Bronze만 저장한다. Silver·Gold·보고서는 미리 생성하지 않는다.
2. Airflow 설정을 import 전에 출력 디렉터리의 SQLite DB로 고정한다. 상속된 Airflow 서비스 설정을 제거하고 로컬 저장·템플릿·합성 표시를 지정한다. Discord·LLM·AWS 자격 증명 변수를 비우고 HTTP 요청을 차단한다.
3. 현재 다섯 DAG를 읽고 별도 DB에 등록한다. 로컬 `ssdr_delta_writer` pool을 1-slot으로 만든다.
4. `usage_daily`의 실제 수동 DagRun을 `target_ymd=20260201`, `reprocess_only=true`로 시작한다. 날짜 XCom과 branch를 실행하며 `bronze_usage`가 skipped인지 확인한다.
5. `TaskInstance.run()`이 의존성 검사·시도 번호·시작/종료 시각·템플릿 렌더·실제 Spark BashOperator·callback을 수행한다. 변환 코드·보고서 callable·트리거를 대체하지 않는다.
6. `TriggerDagRunOperator`가 만든 queued DagRun의 ID와 conf를 확인하고 그 실행 자체를 이어서 처리한다. `gold_daily`와 `report_daily`에 다른 테스트 실행을 대신 만들지 않는다.
7. 세 DAG의 성공 상태, 성공 태스크 12개, API 수집 skipped 1개, lifecycle 로그 완결성, 태스크 시간 표본, 보고서 수치·해시를 검증한다. 조건이 어긋나면 실행은 실패한다.

의존성 때문에 실행할 태스크가 없거나 재시도 대기가 필요하면 실패로 종료한다. 이 로컬 루프는 운영 재시도·scheduler·executor 대기를 모사하지 않는다. 실제 재시도 callback의 시도 번호와 집계는 별도 회귀 검사로 검증한다.

## 날짜와 결과

보고서 생산 기준일은 2026-02-01, 승하차 이용 날짜는 D-4인 2026-01-28, 도착 관측은 D-1인 2026-01-31이다. 승하차 정정 합계는 680이다.

도착 fixture 전체에는 월말 자정을 넘는 6건 중 유효 ETA 5건이 있다. 이번 **일간 보고서**는 1월 31일 관측만 읽으므로 전체 4건·유효 4건이다. NULL ETA가 있는 2월 1일 00:00 관측은 다음 날짜에 속하며 일간 결과에 넣지 않는다.

출력 파일:

- `evidence.json`, `evidence.md`: 실제 DAG/run/task 상태, 표본 수치, 경로 검증과 실행 제한.
- `task-events.log`, `task-summary.json`: 실제 callback 로그와 집계. 실행 시간을 임의의 합성 로그로 만들지 않는다.
- `reports/`: 실제 report DAG가 생성한 Markdown/context/manifest.
- `input/`: 합성 Bronze와 입력 기대값, `airflow/`: 이 실행의 별도 DB·설정, `seed.log`: 입력 준비 로그.

[공개 실행 증거](../examples/airflow/README.md)에 실제 로컬 결과를 보존한다. 로컬 lake·DB·원본 로그는 `.work`에 두며 Git에는 요약만 넣는다. CI의 `airflow-integration` job은 같은 경로를 실행하고 실패 시에도 디버깅 artifact를 보존한다. 원격 CI 실행은 push 이후 별도로 확인해야 한다.

## 측정 범위

순차 실행의 실제 태스크 시작→종료 시간은 측정한다. executor queued_dttm이 없으므로 queue는 NULL이고 집계 samples=0이다. 수동 실행의 scheduled interval→start도 NULL이다. 이를 운영 queue/pool 대기 0초로 바꾸지 않는다.

전체 처리 시간은 Bronze 준비와 Airflow DB 초기화를 제외하고 DAG 실행·최종 DAG 상태 확인을 포함한다. Spark 태스크 시간에는 JVM 시작·제출이 포함된다. 소량 합성 입력의 로컬 시간이며 실제 서비스 SLA·처리량으로 주장하지 않는다.

운영 scheduler·분산 executor·Docker/Kubernetes·S3 writer의 동시성·지속 API 수집을 검증한 결과는 아니다. 실수집 결측·신선도 baseline과 운영 환경 대기·복구 측정은 [로드맵](ROADMAP.md)의 미완료 항목으로 남긴다.

## 실행 경로 설정

`dags/common.py`의 `SSDR_PROJECT_DIR`(기본 `/opt/airflow`)와 `SSDR_SPARK_BINARY`(기본 `spark-submit`)로 같은 DAG를 로컬 환경에서도 실행한다. 경로는 shell quoting을 적용해 공백·따옴표·달러 기호가 실행 코드로 해석되지 않게 한다.

`SSDR_DATA_ROOT`가 설정된 로컬 모드에는 Delta 패키지만 제출한다. S3 모드에는 기존 Hadoop AWS/SDK 패키지를 유지한다. 로컬 설정을 이용한 검증은 실제 S3 writer나 외부 scheduler와 공유하지 않는다.
