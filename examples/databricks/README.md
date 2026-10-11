# 실제 Databricks 실행 증거

2026-10-11 기존 Free Edition workspace에서 공식 CLI OAuth 인증 후 serverless Job을 실행했다. 입력은 공개 합성 fixture이며 서울시 API·S3·Discord·LLM을 호출하지 않았다.

- Job 1103992482650397, Run 44336808137994: seed_fixture → silver → gold → verify_replay 네 태스크 모두 SUCCESS.
- Job 전체 시간 197.11초. 작은 합성 입력의 실행 시간이며 운영 SLA나 처리량 지표가 아니다.
- Spark가 보고한 버전: Databricks 4.2.0, 로컬 3.5.7.
- 정정 적용 날짜 20260128 합계 680, 인접 날짜 20260129 합계 660 유지.
- 재실행 후 Bronze 버전 0 유지. Silver·역 Gold·노선 Gold 업무 컬럼 전체 해시가 로컬 실행과 일치.
- [native evidence](evidence.json), [로컬 비교 증거](local-evidence.json), [실제 Job 화면](https://dbc-e26a3e87-9542.cloud.databricks.com/jobs/1103992482650397/runs/44336808137994?o=7474660778526890). Job 화면은 workspace 접근 권한이 필요하다.

증거 JSON은 jobs get-run/get-run-output의 실제 결과에서 scripts/record_databricks_run.py로 생성했다. 이메일·인증 값·원본 API 응답은 포함하지 않는다. 합계만 같은 결과를 통과시키지 않고 모든 업무 컬럼 해시를 비교한다.

첫 Run 471413789256360은 serverless에서 errorifexists 쓰기 모드를 지원하지 않아 실패했다. 첫 테이블 생성에 지원되는 error 모드를 사용한 후 성공했다. 이전 실패 기록은 덮어쓰지 않았다.

소스 동기화는 src·fixtures·databricks/notebooks 세 디렉터리로 제한한다. 첫 배포의 넓은 glob이 캐시 파일까지 포함한 문제를 고쳤고 해당 bundle 경로의 불필요한 파일을 제거했다. 비밀 파일이 업로드되지 않았음을 확인했다. 기존 workspace.default 테이블과 다른 notebook은 수정하지 않았다.

재현 절차와 managed Delta 쓰기 정책은 [Databricks 문서](../../databricks/README.md), [ADR](../../docs/adr/002-execution-platforms.md)에 있다.

## Free Edition 추가 검증

[품질·변경 감사·SQL 제공](free-edition.md)은 별도의 새 schema를 사용한다. 정상 정정 후 700/660과 과거 680/660, 실제 serverless warehouse 쿼리·Catalog view 메타데이터를 검증했다. [새 공개 JSON](free-edition-evidence.json)을 기존 680/660 replay 증거와 분리해 보존한다.
