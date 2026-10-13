# 보고서 소비 계약의 오류 주입

2026-10-13 17:15 KST에 합성 Gold DataFrame을 기존 보고서 context loader에 공급하고 실제 Markdown·context·manifest를 로컬에 생성했다. [실행 집계](report-contract.json)는 실제 외부 호출 0회이며 live Delta 읽기나 서울시 데이터 품질의 증거가 아니다.

기존 검사는 소수 승차·하차 건수나 빈 역명을 통과시킬 수 있었다. 최종 소비 단계에서 검증하지 않으면 잘못된 Gold가 정수로 잘린 합계나 불완전한 식별자로 보고서에 표시될 수 있다. 원천 수집·변환의 품질 검사에 더해 보고서가 읽는 Gold도 검사한다.

| 주입한 불량 입력 | 결과 | 이전 발행 |
|---|---|---|
| 합계가 맞지만 승차·하차가 소수 | 발행 거부 | 세 파일 동일 |
| 공백 역명 | 발행 거부 | 세 파일 동일 |
| 소수 유효 ETA 분모 | 발행 거부 | 세 파일 동일 |
| 음수 p95 ETA | 발행 거부 | 세 파일 동일 |
| UTC를 KST로 변환하면 다음 날인 관측 | 발행 거부 | 세 파일 동일 |

각 실패 후 세 파일 전체의 SHA-256을 처음 발행과 비교하고 기존 verify_report 검증을 통과했다. 데이터 검증은 출력 파일 쓰기 전에 완료된다. 쓰기 도중 실패·여러 파일의 원자적 교체·동시 발행은 이 실험의 검증 범위가 아니다.

건수 합계는 Python 정수로 계산해 signed int64 덧셈의 wrap을 막고, 노선별 ETA는 먼저 표본 가중치를 계산해 큰 분모의 곱셈 overflow를 피한다. 이 경계는 별도 회귀로 확인했다. 유효 ETA가 없는 역은 전체 관측 분모에 남으며 ETA 지표는 NULL이어야 한다. 음수 원천 시각 차이는 미래 원천 시각을 드러내므로 0으로 보정하지 않는다.

재현은 키 없이 아래 검사로 실행할 수 있다.

```bash
TZ=UTC .venv/bin/pytest -q tests/test_report.py tests/test_publication.py tests/test_report_contract.py
```

관련 구현은 [보고서 context](../../src/report/build_report_context_daily.py), 계약은 [DATA_CONTRACTS](../../docs/DATA_CONTRACTS.md)다. 기존 실서비스 원본이나 발행물을 실험에 사용하지 않았다.
