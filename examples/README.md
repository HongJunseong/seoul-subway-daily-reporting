# 샘플과 테스트 결과

샘플 보고서부터 읽고, 관심 있는 처리 단계의 결과와 재현 방법을 확인할 수 있다. Databricks와 Kubernetes 실험은 합성 데이터로 실행했다. 서울 API에서 수집한 결과는 별도로 표시했다.

| 항목 | 확인할 내용 |
|---|---|
| [샘플 보고서](subway_report_20260201.md) | 합성 이용량·ETA를 기준일별로 표시한 일간 보고서 |
| [정정·재처리](evidence.json) | 같은 원본의 반복 처리와 정정 반영, 자정 경계 |
| [품질·복구](operations/README.md), [파일 병합](operations/compaction.md) | 품질 오류와 부분 실패 후 복구, compaction 전후 결과 비교 |
| [Airflow](airflow/README.md), [컨테이너](container/README.md) | 원본 처리부터 XCom·DAG 트리거·보고서까지 연결 |
| [Kubernetes](kubernetes/README.md), [worker 자원](kubernetes/resources.md) | worker 삭제 후 재시도, PVC 로그 보존과 자원 사용량 |
| [Databricks Job](databricks/README.md), [품질·SQL](databricks/free-edition.md) | managed Delta 정정·time travel과 SQL view 조회 |
| [서울 API 수집](observed/evidence.md) | 도착 3회·D-4 승하차 하루 수집과 7개 테이블 재처리 |
| [수집 계획 점검](observed/campaign-plan-audit.md) | 예정 슬롯과 저장된 기록 비교; 기록 없는 날짜는 미확인으로 표시 |
| [원본 무결성](observed/source-integrity.json), [페이지 수집](operations/source-pagination.json) | 파일 변조와 반복·불완전 페이지를 합성 입력으로 재현 |
| [보고서 품질](operations/report-contract.md), [발행 중단·재시도](operations/publication-commit.md) | 잘못된 Gold 거부, writer 중단 후 이전 보고서 읽기와 재발행 |

## 샘플 보고서 실행

```bash
python scripts/demo.py --output-dir .work/demo/new-run
python scripts/verify_report.py .work/demo/new-run/reports/subway_report_20260201.publication.json
python scripts/verify_report.py examples/subway_report_20260201.md
```

새 출력 디렉터리에서 실행한다. 데모는 합성 원본을 사용하며 외부 API를 호출하지 않는다.

공개 보고서의 생산일은 20260201, 승하차 이용 날짜는 20260128, 도착 관측 날짜는 20260131이다. 함께 있는 [context](subway_report_20260201.context.json)는 수치·Delta 버전·관측 범위, [manifest](subway_report_20260201.manifest.json)는 입력·출력 해시를 담는다.

공개 context의 저장 경로는 v2 상대 경로로 바꾸고 manifest의 context 해시를 다시 계산했다. 원래 해시는 evidence.json의 runtime_context_sha256에 남겼다. 보고서 수치·Delta 버전·본문은 그대로다. 실행 머신과 경로가 다르면 데모의 context 해시도 달라질 수 있다.

서울 API 관측은 1일·10분 규모이며 전체 네트워크 커버리지나 장기 가용성을 판단하기에는 짧다. 각 실험의 입력·환경·한계와 재현 명령은 링크한 문서에서 확인할 수 있다. JSON 결과는 실행 기록이며, 원본 lake와 서울 API 원문·정제 행은 로컬에 보관한다.
