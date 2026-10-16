# 품질 감사와 복구 증거

이 도구는 저장된 원본과 처리 결과에서 관측 가능한 사실을 남긴다. API 호출·AWS 접근·Slack 전송 없이 로컬 Delta 테이블을 읽는다. examples/operations는 합성 fixture이며 examples/observed는 별도 소량 실수집 실험이다. 실운영 baseline은 실제 수집 기간과 태스크 로그를 확보한 뒤 별도로 기록한다.

태스크와 감사 결과를 대시보드로 보는 선택적 경로는 [Prometheus·Grafana](MONITORING.md)에 있다.

## 로컬 데이터 감사

`requirements/dev.txt` 환경에서 다음 명령을 실행한다. 아래 lake는 복구 실험으로 만든다.

```bash
python scripts/recovery_experiment.py --output-dir .work/recovery/my-run
python scripts/audit_lake.py \
  --local-root .work/recovery/my-run/lake \
  --usage-day 20260128 \
  --start '2026-01-31 23:50:00' \
  --end '2026-02-01 00:10:00' \
  --as-of '2026-02-01 00:20:00' \
  --data-kind synthetic \
  --output-dir .work/audit/my-run
```

감사 명령은 이 샘플에서 경고를 발견하므로 exit code 1이 정상적인 기대 결과다. 복구 실험 자체는 기대한 경고와 복구 조건을 검증하고 0을 반환한다.

- `--local-root` 아래 `--prefix`(기본 v2)의 Delta 루트를 읽는다. 내부 Parquet를 직접 열지 않는다.
- 날짜·시각은 KST로 해석한다. 조회 구간은 시작 포함·끝 제외이며, 이미 종료된 양의 구간과 최대 24시간을 허용한다. 긴 기간은 하루씩 감사한다.
- 승하차 이용 날짜는 관측 구간과 독립적으로 지정한다. D-4 자료를 도착 관측 날짜의 이용량으로 표시하지 않는다.
- `--as-of`는 조회 종료 이후의 명시적 기준 시각이다. `freshness_seconds`는 **조회 구간 내 최신 관측**과 이 시각의 차이다. 과거 자료 감사에는 구간 종료 시각을 주면 된다. 현재 lake 전체의 최신성을 뜻하지 않는다.
- `--data-kind`는 synthetic/observed를 반드시 지정한다. observed 표시는 사용자가 입력 출처를 확인한 경우에만 사용한다.
- 결과는 `quality.json`, `quality.md`다. exit code는 healthy=0, degraded=1, failed=2다. 누락 테이블·빈 날짜·잘못된 키/수치·계층 불일치는 정상 0건으로 처리하지 않는다.

각 테이블은 읽은 Delta 버전을 기록한다. 여러 테이블 전체의 단일 트랜잭션 snapshot은 아니다. 동시 변환 중 일시적인 Silver/Gold 차이가 보일 수 있으므로 완료된 실행의 결과를 감사하고 필요하면 재확인한다.

## 지표의 의미

| 지표 | 계산·해석 |
|---|---|
| 승하차 Bronze 반복 | 자연키 반복 행, 키+원문 해시 반복 행, 여러 payload가 있는 키를 구분한다. append 원본의 재수집 이력을 오류로 단정하지 않는다. |
| Silver/Gold 일치 | 날짜·노선·역·승차·하차·합계 전체를 비교한다. 전체 합계만 같은 잘못된 역별 수치를 놓치지 않는다. |
| 시간 구간 관측률 | 시작 시각부터 cadence(기본 300초)별 bucket에 실제 snapshot이 하나라도 있는지 계산한다. 수집 지터·동일 snapshot 중복은 관측률을 부풀리지 않는다. |
| 누락 bucket | Bronze에서 관측이 없는 시간 구간이다. API 실패, 열차 부재, 전체 역·노선 커버리지를 증명하지 않는다. |
| 미처리 snapshot | Bronze 시각 중 Silver에 없는 시각 수다. 행 단위 완전성이나 작업 실패 원인까지 증명하지 않는다. |
| ETA | 전체 Silver 관측, 유효 ETA, NULL ETA, 잘못된 ETA를 구분한다. 음수·무한대·소수·잘못된 문자열을 유효 표본에 넣지 않는다. |
| 원천 시각 차이 | recptn_dt가 있는 관측만 source_age_sec의 중앙값·95백분위수·최댓값을 집계한다. 원천 시각이 없어 대체된 0초는 최신성의 증거가 아니다. |
| 표본 identity | 노선 수와 (노선 ID, 역 ID) 고유 수다. 역명이 같아도 다른 노선 identity를 합치지 않으며 전체 네트워크 커버리지로 해석하지 않는다. |
| 응답 상한 | response_at_limit가 표시된 고유 snapshot 수다. 누락량이나 전체 네트워크 관측 여부를 추정하지 않는다. |
| 파일·읽기 | Delta 활성 add action의 파일 수·크기와 날짜 후보 파일, 실제 로컬 읽기 시간을 남긴다. 후보 bytes는 해당 날짜 전체이며 구간의 실제 IO bytes·AWS 비용이 아니다. |

신선도 경고 기본값 600초는 도구 설정값이며 측정된 SLA가 아니다. 도구는 pandas로 선택 날짜의 컬럼을 읽는 로컬 감사다. 대규모 전체 lake 감사·분산 모니터링·S3 비용 측정의 검증 근거로 사용하지 않는다.

## Airflow 태스크 실행 로그

다섯 DAG의 모든 태스크에 시작·성공·재시도 예정 실패·최종 실패 callback을 연결했다. 기존 실패 알림도 유지한다. 로그의 `SSDR_TASK_EVENT ` 뒤 JSON에는 DAG/태스크/run/실제 시도 번호, pool, 시각·기간, 예외 클래스만 기록한다. conf·환경 변수·명령·URL·예외 메시지를 덤프하지 않는다.

태스크 로그와 callback이 실행된 worker/scheduler 로그를 함께 로컬의 일반 텍스트 파일로 확보한 뒤 다음 명령으로 집계한다.

```bash
python scripts/summarize_tasks.py \
  --log-file .work/logs/task-1.log .work/logs/task-2.log \
  --output .work/audit/task-summary.json
```

- 실행 시간은 태스크 start→end다. 성공·실패 건수와 종료가 확인된 시도의 시간 표본 수·평균·최댓값을 남긴다.
- executor queue는 queued_dttm→start다. pool 배정을 기다린 전체 시간이 아니다.
- scheduled interval→start는 의존성·scheduler·pool·executor 지연을 함께 포함한다. 수동·trigger 실행에는 계산하지 않는다.
- 재시도 예정 실패는 on_retry_callback으로 실패한 시도를 기록하며, 최종 실패 알림은 on_failure_callback에만 유지한다.
- Airflow 2.7은 실패 후 try_number가 다음 시도 번호가 되므로 prev_attempted_tries로 실제 시도를 연결한다.
- 같은 이벤트의 중복 전달은 하나로 집계한다. 재시도는 별도 시도다. 종료 이벤트가 없으면 incomplete, 성공·실패가 함께 있으면 conflicting으로 기록한다.
- 로그가 없거나 malformed 이벤트가 있으면 exit code 2다. 집계 성공 0은 모든 태스크가 성공했다는 의미가 아니다. 결과의 failed/incomplete/conflicting 수를 확인한다.
- callback 로그는 scheduler DB의 최종 상태를 대체하지 않는다. 로그 누락·잘림·강제 종료는 별도로 확인한다. 독립된 pool 대기 시간은 측정하지 않았다.

콜백 연결과 실제 Airflow TaskInstance의 시도 번호는 로컬에서 검사했다. [로컬 Airflow 연결 실행](AIRFLOW_OFFLINE.md)으로 실제 태스크 시간 표본을 확보했다. 실제 Kubernetes 합성 실행의 executor queue와 worker 자원은 [별도 계측](KUBERNETES_RESOURCES.md)으로 확인했다. 실수집 운영 기간의 baseline과 순수 pool 대기는 남는다.

## 예외 주입 복구 실험

`recovery_experiment.py`는 새 디렉터리 또는 빈 디렉터리만 허용한다. 출력 경로의 lake에만 fixture를 저장하고 외부 서비스에 접근하지 않는다.

1. 승하차 원문 3행을 두 번 저장해 중복 이력을 남긴다. 정상 결과 660을 만든 뒤 최신 원천 정정으로 680을 계산한다.
2. Silver 커밋 직후 Gold 실행 전에 의도적으로 예외를 발생시킨다. Silver=680, 기존 Gold=660이 보존되는지 검사한다.
3. API 재수집 없이 저장된 Bronze를 재처리해 Gold=680을 복구한다. 다시 재처리해 Silver·역 Gold·노선 Gold의 **업무 컬럼 전체 SHA-256**이 같은지 검사한다. Bronze 내용·버전도 유지한다.
4. 다른 이용 날짜의 최신 원천 값에 음수를 넣는다. 품질 검증이 Silver 쓰기 전에 실패하고 이전 Silver·Gold의 내용·버전이 유지되는지 검사한다. 올바른 원천 정정을 추가해 정상 결과를 복원한다.
5. 월말 자정 경계의 중복 도착 관측을 처리하고, 의도적으로 비어 있는 bucket 1개와 NULL ETA 1개를 감사 도구가 발견하는지 확인한다.

`recovery.json`, `recovery.md`, `audit/quality.json`, `audit/quality.md`와 실제 Delta lake가 출력된다. [공개 복구 기록](../examples/operations/recovery.md)과 [품질 감사](../examples/operations/quality.md)는 이 실행의 결과다. CI에도 동일 실험과 artifact 보존을 추가했다.

여러 Delta 테이블의 커밋은 원자적으로 묶이지 않는다. 중간 실패 후 저장된 원본 재처리로 복구하는 경로를 증명했다. 재처리는 파일·Delta 버전을 새로 만들 수 있지만 업무 결과는 같아야 한다.

측정 시간은 소량 로컬 fixture의 복구 처리 시간으로 Spark 시작·검증 읽기를 제외한다. 예외 주입은 실제 프로세스 종료·Pod 장애·S3 장애 시험이 아니다. 운영 RTO·SLA로 주장하지 않는다. 과거 미수집 실시간 관측은 이 방식으로 복원할 수 없다.

## 다음 운영 검증

### 처리 후 점검과 조치

저장 원본의 process_source_experiment.py는 처리·재처리 뒤 health.json/health.md를 자동으로 남긴다. 아래 CLI는 완료된 로컬 lake를 점검하고 선택적으로 복사한 태스크 로그(--task-log 반복)와 고정 캠페인(--campaign-dir)을 함께 대조한다. 별도 서비스 호출이나 자동 복구는 하지 않는다.

```bash
python scripts/check_pipeline.py \
  --local-root .work/recovery/my-run/lake --usage-day 20260128 \
  --start '2026-01-31 23:50:00' --end '2026-02-01 00:10:00' \
  --as-of '2026-02-01 00:20:00' --data-kind synthetic \
  --output-dir .work/health/my-run
```

기존 감사의 healthy=0/degraded=1/failed=2 종료 코드를 사용하며 출력은 새 .work 경로만 허용한다. 위 fixture에서는 degraded가 예상된다. 관측 부재는 수집 기록 확인, Bronze 미처리는 저장 원본 재처리, 타입·키 오류는 품질 원인 확인으로 안내한다. 원천 시각 차이와 감사 시점의 관측 나이를 구분한다.

태스크 로그를 주지 않으면 미검증이며 성공으로 간주하지 않는다. 실패 후 성공한 재시도가 있을 수 있어 실패 시도 기록은 Airflow 최종 상태 확인 대상으로 남긴다. 캠페인 기록은 observed 입력에만 연결한다. 기록 없는 날짜를 API 실패나 GET 0회로 단정하지 않는다. 이 CLI는 로컬 점검이며 S3의 운영 scheduler에 자동 배포한 모니터링은 아니다.

실수집 자료·태스크 로그의 코드 commit, 실행 환경, 수집 시작·종료 시각을 함께 기록한다. 날짜별 감사 결과와 실행 로그를 보존해 누락 bucket·응답 상한·ETA 결측·최신성·처리 시간을 비교한다. 실제 파일 수·읽기 시간이 확인된 뒤 compaction 또는 파티션 변경의 전후를 측정한다. 실서비스 접근과 배포는 별도 작업 범위다.

## 파일 수와 조회 시간에 근거한 유지보수

실수집 도착 3,000행의 활성 파일 6개를 계기로 더 큰 합성 입력에서 [닫힌 시간 compaction](STORAGE_MAINTENANCE.md)을 실험했다. 활성 파일 수/bytes와 버전 고정 root 조회를 비교하고 전체 행·인접 파일·다운스트림 재처리 일치를 확인한다. bytes는 실제 스캔 IO나 S3 비용으로 바꾸어 표시하지 않는다. source replay는 Bronze 버전을 보존하지만 OPTIMIZE 유지보수는 dataChange=false 커밋을 만든다.

### 승하차 감사의 타입과 합계

Silver/Gold의 날짜·노선·역 키는 비어 있지 않은 문자열이어야 한다. 건수는 보고서 소비 단계와 같은 공통 숫자 계약으로 검증한다. 유한한 비음수 정수 값만 허용하며, 문자열 숫자와 boolean을 변환해서 받아들이지 않는다. 정상 정수 값으로 표현된 float는 정수로 정규화해 계층 간 업무 값을 비교한다.

행별 승차+하차=합계와 전체 합계는 Python 정수로 계산한다. signed int64 범위를 넘는 여러 행의 합계가 음수로 뒤집히거나, 불량 행 옆의 큰 정수가 float 추론으로 반올림되지 않게 한다. 불량 행이 있으면 부분 합계를 출판하지 않고 total_count=null, silver_gold_business_match=null로 표시한다. 이 null은 계층 일치 여부를 검증할 수 없다는 뜻이며, invalid_rows와 failed 판정은 유지된다.

tests/test_audit_usage_contract.py는 합성 타입 오류·identity·큰 정수·입력 불변성과 실제 로컬 Delta 루트 읽기/별도 CLI 프로세스의 healthy/failed exit code를 검증한다. 이 검증은 예약 실행이나 live 원천 운영의 증거가 아니다.
