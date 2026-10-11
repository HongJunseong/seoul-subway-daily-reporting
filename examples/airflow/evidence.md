# 로컬 Airflow 연결 실행 결과

> 합성 원본을 실제 Airflow TaskInstance와 Spark 태스크로 처리한 결과입니다.

- 검증 성공: True
- 성공 태스크: 12, API 수집 skipped: 1
- DAG 실행 전체 시간: 92.935초
- 승하차 정정 합계: 680
- D-1 도착 관측 / 유효 ETA: 4 / 4
- 보고서/context 해시 검증: True

## 태스크 상태와 실제 로컬 실행 시간

| DAG | 태스크 | 상태 | 실행 시간(초) |
|---|---|---|---:|
| usage_daily | bronze_usage | skipped | 미실행 |
| usage_daily | choose_input | success | 0.099 |
| usage_daily | compute_dates | success | 0.099 |
| usage_daily | gold_usage | success | 21.303 |
| usage_daily | silver_usage | success | 20.262 |
| usage_daily | trigger_gold_daily | success | 0.134 |
| gold_daily | compute_dates | success | 0.075 |
| gold_daily | gold_arrival | success | 22.302 |
| gold_daily | notify_gold_ready | success | 0.099 |
| gold_daily | silver_arrival_day | success | 27.093 |
| gold_daily | trigger_report_daily | success | 0.108 |
| report_daily | compute_target_ymd | success | 0.095 |
| report_daily | generate_report | success | 0.618 |

## 검증과 해석

- 실제 trigger가 만든 DagRun ID와 target_ymd를 확인하고 해당 실행을 이어 처리했습니다.
- 날짜 XCom·branch·Bash Spark 제출·보고서 callable·callback은 실제 DAG 코드입니다.
- 일간 도착은 1월 31일만 포함합니다. NULL ETA가 있는 다음 날 자정 관측은 제외합니다.
- executor queue와 scheduled 시작 지연 표본은 없습니다. 운영 대기 0초라는 뜻이 아닙니다.
- 전체 시간은 원본 준비·DB 초기화를 제외합니다. 태스크 시간에는 Spark 제출·JVM 시작이 포함됩니다.
- 순차 로컬 실행이며 실제 scheduler·분산 executor·S3·실수집 운영 검증은 별도로 필요합니다.
