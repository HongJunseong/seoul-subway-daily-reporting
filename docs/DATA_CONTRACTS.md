# 데이터 계약 v2

시간대는 Asia/Seoul, 날짜 입력은 엄격한 YYYYMMDD다. 저장 루트는 기본 `v2/`이며 `SSDR_STORAGE_PREFIX`로 변경할 수 있다. 로컬 검증은 `SSDR_DATA_ROOT`, 클라우드는 `SSDR_S3_BUCKET`을 사용한다.

## 테이블과 grain

| 경로 | 한 행의 의미 | 파티션 |
|---|---|---|
| bronze/subway_usage_delta | API 응답의 한 원천 행·수집 이력 | year/month/day |
| bronze/subway_arrival_delta | 특정 실제 수집 snapshot의 도착 응답 행 | year/month/day/hour |
| silver/fact_subway_usage_delta | use_ymd·line_name·station_name별 최신 정정 | year/month/day |
| silver/fact_subway_arrival_delta | snapshot_ts·line_id·station_id·direction·train_no·dest_station_id별 관측 | year/month/day/hour |
| gold/subway_usage/station_daily | 날짜·노선·역 이용량 | year/month/day |
| gold/subway_usage/line_daily | 날짜·노선 이용량 | year/month/day |
| gold/arrival_metrics_15m_delta | window 종료 시각·노선·역·방향별 ETA 표본 | year/month/day/hour/window_end_ymdhm |
| gold/subway_arrival/station_daily_kpi | 관측 날짜·노선 ID·역 ID·역명별 일간 표본 | year/month/day |

Bronze는 모든 원문 필드를 raw_payload에 보존한다. 알려진 필드는 nullable string 스키마로 유지하고 run_id, ingested_at, record_hash를 추가한다. 같은 응답을 다시 수집해 Bronze 행 수가 늘어나는 것은 허용하며 Silver에서 처리한다. 프로세스가 append 성공 뒤 실패해도 다음 재시도가 Gold 합계를 증가시키지 않아야 한다.

## 승하차 품질

수집은 모든 페이지의 응답 코드·total count 일관성을 검사한다. 완료 건수가 예고 건수와 다르거나 조회 날짜가 다르면 실패한다. 빈 응답과 페이지 상한 도달은 정상 0건으로 발행하지 않는다. 일간 이용량이 API에서 준비되지 않으면 DAG가 실패·재시도하고 날짜를 임의로 바꾸지 않는다.

날짜·노선·역 키는 비어 있을 수 없다. 최신 정정의 승차·하차는 0 이상의 정수이며 NULL·음수·파싱 불가 값은 발행을 차단한다. total_count는 승차+하차다. 고유 이용자 수를 뜻하지 않는다.

S3의 기본 LogStore에서 여러 Spark driver의 같은 테이블 동시 쓰기를 허용하지 않는다. Airflow Spark 태스크는 공유 1-slot pool로 직렬화하고 외부 CLI writer는 중지 후 실행한다.

Silver/Gold의 날짜 파티션을 검증 후 replaceWhere로 교체한다. 다른 날짜는 유지한다. 서로 다른 REG_YMD는 큰 날짜를 우선하고 동일 날짜는 ingested_at, record_hash로 결정한다. 원문을 다시 수집하지 않는 재처리는 같은 입력에서 같은 결과를 만든다. 역명·노선 키가 바뀐 정정이나 원천에서 사라진 행은 자동 삭제로 해석하지 않는다. 이러한 원천 변경은 자연키 정책과 기존 이력을 별도로 검토해야 한다.

## 도착 관측 품질

수집 API의 ALL 범위는 호출당 최대 1,000행이다. response_at_limit은 상한 도달 가능성을 나타내며 전체 네트워크의 완전성을 증명하지 않는다. 원천의 모든 열차가 모든 snapshot에 존재한다고 가정하지 않는다.

snapshot_ts는 API 응답 이후의 실제 수집 시각이다. recptn_dt는 원천 생성 시각, event_time은 사용 가능한 원천 시각 또는 snapshot 시각이다. 집계 구간 선택은 snapshot_ts로 한다. 누락된 과거 도착 관측은 API 재조회로 복구할 수 없다.

같은 snapshot·자연키가 반복되면 최신 ingested_at, 최신 recptn_dt, record_hash 순서로 하나를 선택한다. 같은 수집의 서로 다른 원천 세대를 해시 순서로만 결정하지 않는다. NULL 원천 시각은 확인된 원천 시각 뒤에 둔다. 원천 시각이 수집보다 미래이면 시각을 임의 보정하지 않고 감사 경고로 드러낸다.

잘못된 ETA는 NULL로 유지한다. 전체 관측 n과 valid_eta_n을 분리한다. 평균·분위수·긴 ETA 비율의 분모는 유효 ETA 표본이다. 같은 열차를 여러 snapshot에서 관측한 값은 서로 다른 관측이며 독립적인 열차 여행 수나 승객 대기시간이 아니다.

도착 Silver는 처리 구간에 걸친 시간 파티션 전체를 다시 구성한다. 과거 window 재처리가 같은 시간의 후속 snapshot을 지우지 않게 한다. 15분 window는 KST [end-15분, end)로 날짜·월 경계를 넘는 관측도 포함한다. 빈 입력이나 유효 ETA가 전혀 없으면 발행하지 않는다.

역명이 동일해도 노선·역 ID가 다르면 유지한다. 역명 변경으로 일간 Gold의 ID 키가 여러 행으로 나뉘면 보고서 중복 키 검사가 실패하며 표준화 정책을 먼저 결정해야 한다.

## 보고서와 발행

생산일 D의 이용량은 D-4, 도착 관측은 D-1이다. 출처별 기준일을 별도로 표기한다. 승하차와 ETA를 역명으로 조인하지 않는다. 노선 평균·비율은 역별 valid_eta_n으로 가중하고 역별 p95 평균을 노선 p95라고 표시하지 않는다.

필수 날짜·건수·키·분모 검증에 실패하면 보고서 생성도 실패한다. 기본 Markdown, 정렬된 context JSON, manifest를 함께 만든다. context는 실제 읽은 테이블 경로와 Delta 버전을 포함한다. manifest의 SHA-256은 게시된 입력·출력의 바이트 일치만 검증하며 원천의 사실성을 보장하는 서명은 아니다.

발행 순서는 Markdown → context → manifest다. 로컬 파일 교체와 S3 객체 쓰기는 파일/객체별 처리이며 다중 파일 트랜잭션이 아니다. 소비자는 manifest의 해시 검증을 통과한 세트만 사용한다. 같은 날짜의 동시 수동 발행은 피한다.

## 운영 측정 계약

실데이터 검증에서 최소 다음을 기록한다: 수집 시작/끝, 응답 건수·상한 도달 여부, 원천/수집 시각 차이, 정상 snapshot 수, 키별 결측 ETA 비율, 처리 실행 시간, 최신 Gold 날짜·window, Delta 버전, 실패·재시도·복구 시간. 현재 샘플은 이 운영 baseline의 대체물이 아니다.

Bronze 보존·VACUUM·S3 lifecycle 기간은 아직 확정하지 않았다. 운영 기간·복구 요구·저장 비용을 측정한 뒤 정한다. 원문과 Delta 로그를 무작정 삭제하면 재처리와 time travel이 손상될 수 있다.

## 원본 재처리와 파일 유지보수

원본 재처리는 Bronze에 쓰지 않으며 같은 입력의 Silver/Gold 업무 결과를 유지한다. 별도 합성 [파일 유지보수 실험](STORAGE_MAINTENANCE.md)의 OPTIMIZE는 dataChange=false의 새 Bronze 커밋을 만든다. 물리 버전은 증가하지만 raw_payload·lineage·중복·NULL을 포함한 전체 행과 인접 파티션 파일을 보존해야 한다. 이 실험은 운영 자동 유지보수나 VACUUM을 활성화하지 않는다.
