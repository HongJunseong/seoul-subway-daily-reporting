# 보고서 발행 중단과 복구

[실행 집계 JSON](publication-commit.json)은 2026-10-13 18:14 KST의 합성 보고서 실험이다. 실제 서울 API·AWS 호출은 0회다.

기존 Markdown·context·manifest의 순차 교체는 중간 실패 때 서로 다른 버전을 만들 수 있었다. 새 단일 발행 객체는 세 내용을 포함하고 마지막에 교체된다. 실패 중 독자는 이전 객체 한 개를 읽는다.

| 실행한 오류 | 검증 결과 |
|---|---|
| 별도 local writer를 두 export 교체 뒤 SIGKILL | exit -9, 이전 발행 객체 bytes 보존 |
| 본문/context가 바뀌고 manifest가 남은 export | 기존 CLI가 혼합 세트를 거부 |
| 같은 출력으로 재시도 | 새 발행·세 export 검증 성공, 반복 게시 bytes 동일 |
| mock S3의 각 1~4번째 PUT 전 실패 | 이전 발행 객체 유지, 재시도 후 완전한 새 객체 |
| mock S3의 커밋 후 응답 유실 | 새 객체가 이미 존재, 재시도 bytes 동일 |

SIGKILL은 실제 합성 자식 프로세스만 대상으로 했다. 커밋 이전 오류와 커밋 후 응답 유실을 구분한다. 순조로운 재시도에 다른 원천 수집은 필요하지 않다.

```bash
TZ=UTC .venv/bin/pytest -q tests/test_publication_commit.py
```

회귀는 네 단계 local/S3 오류, 첫 발행 실패, 실제 프로세스 강제 종료, 커밋 후 응답 유실, 교체 도중 독자의 한 객체 snapshot, 변조·schema·NaN 거부, PYTHONPATH 없는 CLI 실행을 포함한다.

S3는 메모리 mock, local은 합성 보고서 파일을 사용했다. Kubernetes·전원 장애·디스크 장애·실제 S3 내구성·동시 writer의 시각 순서를 증명하지 않는다. 호환 export 세 파일의 교체는 여전히 개별 작업이며 가용성 계약은 단일 발행 객체를 읽는 소비자에 적용된다. [발행 계약과 재현](../../docs/REPORT_PUBLICATION.md)에 범위를 기록했다.
