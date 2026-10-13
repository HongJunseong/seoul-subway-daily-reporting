# 실행 증거 안내

합성 입력의 회귀 결과와 실제 서울 API 관측을 구분한다. Databricks·Kubernetes에서는 실제 플랫폼을 실행했지만 입력은 합성 데이터다.

| 증거 | 입력과 검증 범위 |
|---|---|
| [기본 보고서](subway_report_20260201.md), [실험 JSON](evidence.json) | 합성 역·노선·ETA. 중복 재수집·정정·자정 경계와 결정적 보고서 |
| [품질·복구](operations/README.md), [compaction](operations/compaction.md) | 로컬 합성 Delta. 잘못된 입력 차단·부분 커밋 복구·닫힌 시간 파일 병합의 결과 보존 |
| [Airflow](airflow/README.md) | 합성 원본의 실제 로컬 태스크·XCom·DAG 트리거. 운영 scheduler 대기와 구분 |
| [컨테이너](container/README.md) | 합성 원본을 분리된 Airflow/Spark 환경에서 처리 |
| [Kubernetes](kubernetes/README.md), [worker 자원](kubernetes/resources.md) | 실제 scheduler→worker Pod·삭제 후 새 Pod 재시도·PVC 로그 보존·자원 관측. 단일 node 합성 입력 |
| [Databricks Job](databricks/README.md), [Free Edition 품질·SQL](databricks/free-edition.md) | 실제 serverless Job·managed Delta·time travel·Unity Catalog view·SQL warehouse. 합성 승하차 |
| [서울 API 제한 관측](observed/evidence.md) | 실제 도착 3회·D-4 승하차 하루의 소량 수집과 7개 테이블 재처리 일치. 장기 운영·전체 커버리지의 증거로 해석하지 않음 |

## 기본 보고서 파일

- [subway_report_20260201.md](subway_report_20260201.md): 생산일 20260201, 승하차 이용 날짜 20260128, 도착 관측 날짜 20260131.
- [subway_report_20260201.context.json](subway_report_20260201.context.json): 수치·읽은 Delta 버전·관측 범위.
- [subway_report_20260201.manifest.json](subway_report_20260201.manifest.json): 입력·출력 SHA-256.
- [evidence.json](evidence.json): 중복 재수집·정정·자정 경계 실험 결과.

공개 context의 source path만 로컬 절대 경로에서 v2 상대 경로로 정규화하고 manifest의 context 해시를 다시 계산했다. 원래 실행 context 해시는 evidence.runtime_context_sha256에 기록했다. 보고서 수치·Delta 버전·본문은 실행 결과 그대로다. Delta 테이블과 실수집 원문·정제 행은 공개하지 않는다.

재현:

```bash
python scripts/demo.py --output-dir .work/demo/new-run
python scripts/verify_report.py .work/demo/new-run/reports/subway_report_20260201.md
python scripts/verify_report.py examples/subway_report_20260201.md
```

데모 출력 context는 실행 머신의 실제 저장 경로를 기록하므로 공개 샘플과 context 해시는 다를 수 있다. 각 플랫폼의 재현 조건·실패 기록·해석 범위는 해당 증거 문서에 적혀 있다.

[표본 계획 감사](observed/campaign-plan-audit.md)는 실제 고정 계획과 현재 상태·캡처 파일만 대조한다. 예정 수집 기록이 없는 날을 API 실패나 GET 0회로 확대하지 않는다. 미래 슬롯을 도래 분모에 포함하지 않으며, 원문·정제 품질·전체 하루 운영 성공률은 이 감사의 범위가 아니다.

[원본 무결성 증거](observed/source-integrity.json)는 합성 파일의 동일 건수·시각 값 변경이 변환 전에 차단되는 실험이다. 서울 API를 호출하지 않았고, 기존 실제 10/11 원본은 legacy 무결성 상태로 읽기만 했다. 수집 체크섬 검증·현재 해시·출처 인증을 구분한다.
