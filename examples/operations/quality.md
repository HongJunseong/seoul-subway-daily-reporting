# 데이터 품질·수집 현황

- 상태: degraded
- 데이터 종류: synthetic
- 승하차 기준일: 20260128
- 관측 구간(KST): [2026-01-31T23:50:00+09:00, 2026-02-01T00:10:00+09:00)

- 감사 기준 시각(KST): 2026-02-01T00:20:00+09:00

## 수집·처리

- 관측한 시간 구간: 3 / 4
- 관측이 없는 시간 구간: 1
- 유효 ETA / Silver 관측: 5 / 6
- 구간 내 최신 관측의 나이(초): 1200.0
- 아직 Silver에 없는 snapshot: 0
- 승하차 Silver/Gold 일치: True

## 읽은 Delta snapshot

| 테이블 | 상태 | 버전 | 조회 행 | 해당 날짜 후보 파일 | 후보 bytes |
|---|---|---:|---:|---:|---:|
| bronze/subway_usage_delta | available | 5 | 7 | 5 | 20318 |
| silver/fact_subway_usage_delta | available | 5 | 3 | 1 | 3522 |
| gold/subway_usage/station_daily | available | 4 | 3 | 1 | 2037 |
| bronze/subway_arrival_delta | available | 5 | 12 | 12 | 98642 |
| silver/fact_subway_arrival_delta | available | 0 | 6 | 2 | 11088 |

## 판정

- warning: arrival_unobserved_time_buckets
- warning: arrival_stale

## 해석 한계

- 관측이 없는 5분 구간이 API 실패를 증명하지는 않습니다.
- 시간 구간 관측률은 전체 역·열차 커버리지가 아닙니다.
- 파일 수·bytes는 Delta 활성 파일 메타데이터이며 실제 IO 비용이 아닙니다.
- 최신 관측 나이는 이 조회 구간 안에서 계산합니다.
- 처리 시간·pool 대기는 Delta 시각으로 추정하지 않습니다.
