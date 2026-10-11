---
name: write-commit
description: "서울 지하철 DE 프로젝트의 실제 diff를 기능 단위로 스테이징하고 한국어 Conventional Commit 초안 또는 승인된 로컬 커밋을 만든다. 커밋 요청과 승인된 자율 개발의 변경 기록에 사용한다. push·배포는 포함하지 않는다."
---

# 커밋 작성

이전 DE 프로젝트의 write-commit 규칙을 이 저장소의 자율 개발 흐름에 맞게 적용한다.

- git branch --show-current, git status --short, git diff, git diff --cached를 확인한다.
- 기존 staged 변경과 사용자 변경을 보존한다. git add -A 대신 작업의 명시적 경로·hunk를 스테이징한다.
- main/develop 직접 커밋은 피하고 작업 목적에 맞는 feature/, fix/, refactor/ 브랜치를 사용한다. 승인된 자율 작업의 브랜치명·커밋 분할은 다시 묻지 않는다.
- 목적 하나를 기준으로 구현·호출부·회귀 테스트·필수 설정을 묶는다. 독립된 개선은 별도 커밋한다.
- 적절한 검증을 실행한다. 미실행·실패 검증을 통과로 적지 않는다.
- 메시지 초안만 요청한 경우 커밋하지 않는다. 로컬 커밋이 요청되었거나 승인된 자율 개발에 포함되면 진행한다.
- 자동 승인 검토나 접근 권한이 막으면 제약을 보고한다. 스킬은 권한을 확대하지 않는다.

## 형식

type(scope): subject. type은 feat/fix/docs/refactor/test/chore/build/ci/perf/revert 중 고른다.
scope는 ingestion/usage/arrival/report/airflow/docker/agents 등 실제 변경 영역이다.
subject는 한국어, 50자 이내, 마침표 없이 명사형으로 끝낸다.
body는 이유가 필요할 때 한 문단으로 쓰며 문단 내부 하드랩은 하지 않는다.
확인된 이슈 번호만 연결한다. 브랜치 숫자로 Closes를 추정하지 않는다.
AI Co-Authored-By, Generated with, Assisted-by를 넣지 않는다. --author와 Git 저자 설정을 바꾸지 않는다.

## 실행

UTF-8 메시지 파일과 git commit -F를 사용한다.
--no-verify, 공유 히스토리 재작성, 자동 push를 하지 않는다.
git log -1 --format=%B와 git status --short로 결과를 확인한다.
