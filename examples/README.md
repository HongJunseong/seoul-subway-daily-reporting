# 합성 데이터 실행 예시

이 디렉터리는 fixtures/의 가상 역·노선·ETA 데이터로 실제 Spark/Delta와 보고서 코드를 실행한 결과다. 서울 실제 수집 결과나 성능 벤치마크가 아니다. 리포트에도 합성 데이터임을 표시한다.

- subway_report_20260201.md: 생산일 20260201, 승하차 20260128, 도착 20260131.
- context.json: 수치·실제 읽은 Delta 버전·관측 범위.
- manifest.json: 입력·출력 SHA-256.
- evidence.json: 중복 재수집·정정·자정 경계 실험의 실행 결과.

공개 샘플 context의 source path만 로컬 절대 경로에서 v2 상대 경로로 정규화하고 manifest의 context 해시를 다시 계산했다. 원래 실행 context의 해시는 evidence.runtime_context_sha256에 기록했다. 보고서 수치·Delta 버전·본문은 실행 결과 그대로다. Delta 테이블 자체는 이 디렉터리에 포함하지 않는다.

재현:

```bash
python scripts/demo.py --output-dir .work/demo/new-run
python scripts/verify_report.py .work/demo/new-run/reports/subway_report_20260201.md
python scripts/verify_report.py examples/subway_report_20260201.md
```

데모 출력의 context는 실행한 머신의 실제 저장 경로를 기록하므로 공개 샘플과 context 해시는 다를 수 있다.
