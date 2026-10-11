# 작업 지침

- 시작할 때 docs/PROJECT_CONTEXT.md, docs/ROADMAP.md, docs/SESSION_HANDOFF.md를 읽는다. 사용자의 현재 요청이 우선한다.
- 기존 사용자 변경과 비밀 값을 보존한다. .env와 실서비스 데이터를 커밋하거나 로그에 출력하지 않는다.
- 동일 Bronze 입력과 처리 구간의 재실행 결과는 같아야 한다.
- Delta 테이블은 루트를 format("delta")로 읽고 필터링한다. 내부 Parquet를 직접 읽지 않는다.
- KST 수집 시각, 원천 시각, 실행 구간, 이용 날짜를 구분한다. 정각·자정·월말도 검증한다.
- ETA를 실제 운행 지연이나 승객 대기시간으로 단정하지 않는다. D-4 이용량은 해당 날짜의 값으로 표시한다.
- 과거 미수집 실시간 관측값은 복원할 수 없다. API backfill과 원본 재처리를 구분한다.
- 빈 입력·불완전 페이지·잘못된 키와 수치가 정상 데이터처럼 출판되지 않게 한다.
- S3의 Delta writer는 ssdr_delta_writer 1-slot pool로 직렬화한다. 외부 CLI·다른 scheduler의 동시 writer를 허용하지 않는다.
- 수집·변환·저장·알림을 분리한다. 외부 API·AWS·Discord·LLM 검증은 별도 opt-in이다.
- 회귀 테스트를 실행하고 실제 결과·남은 작업을 docs/SESSION_HANDOFF.md에 기록한다.
- 현재 운영 경로에는 Kafka가 필요하지 않다. 새 기술은 문제와 검증 방법으로 선택한다.
- .agents/skills/write-commit/SKILL.md를 따른다. 기능과 테스트를 함께 커밋한다.
- 브랜치는 작업 목적에 맞는 feature/, fix/, refactor/ 이름을 사용한다. 현재 개발 브랜치는 feature/de-portfolio다.
- 승인된 자율 개발 범위의 로컬 커밋은 재확인 없이 진행한다. push·배포는 해당 요청 범위를 따른다.
- 사용자 변경을 함께 스테이징하거나 Git 저자를 바꾸거나 AI 공동저자를 추가하지 않는다.
