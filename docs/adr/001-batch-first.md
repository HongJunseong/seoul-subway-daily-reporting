# ADR 001: API 배치와 관측 스냅샷 중심 구조

- 상태: 채택
- 날짜: 2026-10-10

## 문제

원천은 일간 승하차 API와 주기적으로 조회하는 도착 ETA API다. 기존 Kafka 경로는 Gold 완료 이벤트를 Discord 소비자로 전달했으며 핵심 수집·처리는 Kafka를 거치지 않았다. 현재 요구에서 이벤트 보존·consumer offset·독립 소비자 확장으로 해결할 구체적인 문제가 없었다.

## 결정

Kafka broker·완료 이벤트 producer·알림 consumer를 제거한다. 일간 후속 작업은 Airflow TriggerDagRunOperator로 연결하고 선택적인 알림은 webhook으로 전달한다. 알림 태스크와 보고서 트리거는 Gold의 독립적인 하위 태스크로 둔다. webhook 미설정은 skip, 실패는 Airflow 태스크 실패로 보이지만 보고서 의존성을 막지 않는다.

기본 보고서는 결정적인 템플릿이다. LLM 해설은 별도 선택 기능으로, 정형 수치와 자동 생성 해설을 구분한다. 현재 데이터의 시간 기준·표본 한계를 먼저 해결한다.

## 데이터와 복구

Bronze는 원문·수집 시각을 append 보존한다. Silver·Gold는 검증 후 해당 파티션을 교체한다. API의 재수집과 저장된 원문의 재처리를 분리한다. 승하차 최신 정정은 자연키별 REG_YMD, ingested_at, record_hash 순으로 결정한다. 최종 hash는 동일 시각의 충돌에서 결정적인 선택을 제공하며 원천 정정 시각을 추가로 추론하지 않는다.

시간은 KST로 고정한다. 15분 window는 실행 구간 끝을 5분 경계로 내린 시각을 기준으로 [start, end)다. 도착 수집 시각은 API 응답을 받은 실제 시각이며, 스케줄러 날짜를 원천 시각으로 위조하지 않는다. 수집 DAG와 처리 DAG는 분리한다.

Delta는 루트에서 읽고 파티션 조건으로 범위를 제한한다. 테이블별 커밋만 원자적이다. station Gold와 line Gold를 한 트랜잭션으로 발행하지 않는다. 보고서는 station Gold 두 출처의 실제 읽은 Delta 버전을 기록한다.

## S3 writer 직렬화

기본 S3 LogStore에서 서로 다른 Spark driver가 같은 Delta 테이블을 동시에 쓰지 않도록 모든 DAG의 Spark 태스크는 ssdr_delta_writer pool(1 slot)을 공유한다. 일간 arrival Silver와 5분 Silver가 겹치는 문제도 이 범위에서 방지한다. Compose init은 pool을 만들고 Kubernetes 준비 단계에서 같은 pool을 생성한다. CLI와 다른 Airflow 인스턴스는 이 pool 밖이므로 운영자가 writer를 중지한 뒤 재처리한다.

처리량이 제한되는 비용이 있다. 다중 driver 쓰기가 필요해지면 모든 writer에서 동일한 DynamoDB LogStore 등 저장소의 조정 규약을 설정하고 별도 검증한다. 현재 구현이 분산 writer 안전성을 제공한다고 주장하지 않는다.

## 비용과 제약

Airflow/Spark는 이 작은 데이터에 꼭 필요한 최소 도구라고 주장하지 않는다. 이 프로젝트에서는 재실행·정정·분리된 데이터 계층을 구현하고 검증하는 학습 목적이 있다. 단일 머신의 Spark 실행은 분산 처리량 증거가 아니다.

날짜·시간·window 파티션은 범위 교체를 단순하게 하지만 작은 파일을 만들 수 있다. 운영 관측 후 파일 크기·개수·읽기 시간을 비교해 compaction 또는 파티션 축소를 결정한다. 아직 실행하지 않은 최적화 효과는 성과로 기록하지 않는다.

## 재검토 조건

실제 연속 이벤트 원천, 서로 다른 속도로 소비하는 여러 독립 시스템, 브로커 보존·offset 재생·역압 제어가 필요해지면 Kafka 도입을 재검토한다. API를 짧게 polling하는 것만으로 스트리밍 요구가 성립했다고 판단하지 않는다.

## 참고

- [Airflow 재실행과 태스크 설계](https://airflow.apache.org/docs/apache-airflow/2.7.3/best-practices.html)
- [Delta Lake 파티션·작은 파일 관리](https://docs.delta.io/best-practices/)

- [Delta S3 저장소와 writer 제한](https://docs.delta.io/delta-storage/)
