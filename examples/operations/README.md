# 로컬 운영 검증 샘플

이 폴더는 **합성 입력으로 수행한 로컬 처리·품질·복구 검증 결과**다. 각 증거에 실제 Delta 쓰기, in-memory 검증, HTTP mock의 범위를 명시한다. 실데이터·운영·S3 검증은 별도다.

- [recovery.md](recovery.md), [recovery.json](recovery.json): Silver 커밋 후 중단과 최신 원천 품질 오류의 복구. 단계별 합계·테이블 버전·업무 컬럼 SHA-256을 보존한다.
- [quality.md](quality.md), [quality.json](quality.json): 저장된 최종 테이블의 읽기 전용 감사. 이용 날짜 2026-01-28과 KST 2026-01-31 23:50~2026-02-01 00:10을 독립적으로 지정했다.
- 관측 시간 bucket은 4개 중 3개, Silver 관측 6개 중 유효 ETA 5개다. 의도적인 미관측 bucket 1개와 감사 기준 시각 00:20의 신선도 경고로 degraded 판정이 기대된다.
- 복구 처리 시간은 약 4.032초와 4.273초다. 소량 로컬 입력의 측정이며 Spark 시작·검증 읽기를 제외한다. 운영 SLA/RTO가 아니다.
- Delta 버전은 테이블 전체 버전이다. quality의 최종 버전은 두 이용 날짜 실험 모두 끝난 뒤의 버전이므로 앞 단계 기록의 버전과 다를 수 있다.
- 파일 크기·해시는 이번 실행 결과다. 재실행 시 파일 구성·시간·버전이 달라질 수 있으나 업무 결과와 검증 조건은 같아야 한다.

[실행 절차와 지표 해석](../../docs/OBSERVABILITY.md)에 명령과 제한을 기록했다. 원본 lake는 .work 아래에만 보존하며 Git에는 이 요약 파일만 넣는다.

[compaction.md](compaction.md)와 [compaction.json](compaction.json)은 더 큰 합성 36,000행의 닫힌 시간 파일 유지보수 결과다. 파일 수·버전·조회 표본과 전체 행·인접 파일·원본 재처리 보존을 확인했다. 기존 recovery/quality 실험의 lake와 분리한다.

[source-pagination.json](source-pagination.json)은 같은 HTTP mock 응답의 수집 전후 비교다. 반복 페이지가 총 행 수를 채우는 경우와 소수/boolean 예고 건수가 기존에 통과했고 새 collector에서는 SourceError로 차단됐다. 실제 API 커버리지나 Delta 쓰기 실험과 구분한다.
