# 실제 로컬 Airflow 실행 증거

이 폴더는 **합성 Bronze로 실제 Airflow 2.7.3 TaskInstance와 Spark 태스크를 실행한 결과**다. scheduler/분산 executor의 운영 시간이나 실제 서울 API 자료를 측정한 결과는 아니다.

- [evidence.md](evidence.md), [evidence.json](evidence.json): 세 DAG 성공, 실제 트리거가 만든 DagRun 연결, 태스크 12개 성공·API 수집 1개 skipped·보고서 해시 검증.
- [task-summary.json](task-summary.json): 실제 callback 이벤트 24개, 종료된 시도 12개, 실행 시간 표본 12개. executor queue와 scheduled 시작 지연의 표본은 0개이며 미측정이다.
- 승하차 정정 합계는 680이다. 일간 도착은 1월 31일 4건·유효 ETA 4건이다. 기존 월말 window 실험의 6건·유효 5건은 다음 날 자정까지 포함한 별도 범위다.
- DAG 실행 시간은 92.934660초다. 원본 준비·Airflow DB 초기화를 제외하며, 실제 Spark 태스크 제출과 JVM 시작은 포함한다. 소량 로컬 입력의 측정이며 운영 SLA·처리량이 아니다.
- 공개 JSON은 `.work/airflow-offline/verified-03`에서 생성된 실제 결과를 그대로 복사했다. 원본 lake·DB·로그·report/context/manifest는 `.work` 아래에 보존한다.

[실행 절차와 제한](../../docs/AIRFLOW_OFFLINE.md)에 재현 명령을 기록했다. 실수집 baseline·S3·scheduler/분산 executor의 대기와 복구는 별도 검증이 필요하다.
