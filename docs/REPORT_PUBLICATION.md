# 일간 보고서의 발행과 재시도

보고서 내용·입력 context·manifest를 한 JSON 객체로 함께 보관한다. 소비자는 subway_report_YYYYMMDD.publication.json을 한 번 읽고 내부 두 해시와 D-4/D-1 날짜를 검증한다. 이전 세 파일은 사람이 보기 위한 호환 export이며 새 소비 기준은 발행 객체다.

| 파일 | 역할 |
|---|---|
| .md | Markdown 호환 export |
| .context.json | 입력 context 호환 export |
| .manifest.json | export 검증용 manifest schema 2 |
| .publication.json | 본문·정확한 context 문자열·manifest를 함께 담은 최종 발행 객체, bundle schema 1 |

## 선택 이유

작은 일간 보고서에 세 파일 트랜잭션이나 별도 DB를 도입하면 복구·정리 절차가 늘어난다. 단일 객체는 내용이 일부 중복 저장되지만, 소비자가 서로 다른 버전의 파일을 조합하지 않게 한다. 날짜별 버전 history 보관은 현재 요구와 분리한다.

Gold 소비 계약을 통과한 본문/context의 정확한 UTF-8 bytes로 SHA-256을 계산한다. metadata와 JSON은 결정적으로 직렬화한다. 서로 다른 bundle/manifest schema와 보고서 mode를 검사하며 non-finite JSON은 거부한다. 해시는 출처 인증이나 Gold 사실성의 서명이 아니다.

## 쓰기와 읽기

호환 export 세 개를 먼저 쓰고 발행 객체를 마지막에 교체한다. 로컬은 같은 디렉터리의 고유 임시 파일에 쓰고 flush·file fsync 뒤 replace한다. 발행 객체 교체 전 실패/프로세스 종료에서는 기존 발행을 읽는다. 소비자는 한 객체의 bytes를 한 번만 읽어 본문과 context를 검증한다.

S3 adapter도 네 PUT 중 발행 객체를 마지막에 보낸다. 단일 key 갱신과 여러 key의 트랜잭션은 구분해야 한다. 이 선택은 [AWS S3 consistency 문서](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)의 단일 key 원자적 갱신 계약을 사용한다. S3 실패·응답 유실 주입은 메모리 mock으로 검사했다. 정상 저장·읽기는 이후 [실제 LLM 보고서 네 파일](../examples/operations/llm-report-check.json)로 별도 확인했으며 로컬과 S3의 전체 bytes·발행 해시가 일치했다.

커밋은 성공했지만 응답이 유실되면 새 완전한 객체가 이미 존재할 수 있다. 실패 응답만으로 이전 값이라고 단정하지 않는다. 같은 출력/context로 재시도하면 동일 bytes를 게시한다. local/S3는 독립적으로 커밋되며 분산 트랜잭션이 아니다. 동시 writer의 시각 순서·최신성, 전원/디스크 장애 내구성, S3 version history는 이 구현의 검증 범위가 아니다.

첫 성공 이전에는 발행 객체가 없다. 예전 세 파일만 존재하는 보고서는 기존 CLI로 검증할 수 있으며, 새 발행 객체를 소급 생성하지 않는다. 발행 객체가 있으면 export 검증도 그 manifest와 일치해야 한다. 손상된 발행 객체를 정상 export로 fallback하지 않는다.

## 재현과 소비

```bash
python scripts/demo.py --output-dir .work/demo/new-publication-run
python scripts/verify_report.py .work/demo/new-publication-run/reports/subway_report_20260201.publication.json
TZ=UTC .venv/bin/pytest -q tests/test_publication_commit.py
```

```python
from src.report.publication import read_publication

snapshot = read_publication("reports/subway_report_20260201.publication.json")
report_text = snapshot["report"]
context = snapshot["context"]
manifest = snapshot["manifest"]
```

S3 객체를 다운로드한 bytes에는 decode_publication을 적용한다. 기존 .md CLI는 세 export 해시와 현재 발행 manifest의 일치 여부를 확인한다. 실패 후 부분 export는 거부되지만 .publication.json은 이전 완전한 보고서를 계속 제공한다. 같은 날짜의 writer는 기존 정책대로 직렬 실행한다.

[실제 합성 오류 주입](../examples/operations/publication-commit.md)은 writer 자식 프로세스 SIGKILL·재시도·mock S3 실패/응답 유실을 구분한다. 로컬 Airflow·컨테이너 재현 소비도 read_publication으로 context를 읽는다. 예전 공개 샘플의 bytes·실행 증거는 그대로 보존했다.

## 생성과 전송의 재시도

report_daily는 compute_target_ymd → generate_report → upload_report로 실행한다. 생성 태스크는 S3에 쓰지 않고 `SSDR_REPORT_OUTPUT_DIR/runs/<run ID의 SHA-256>/`에 발행 객체를 남긴다. XCom에는 기준일·파일 경로·발행 객체 해시만 전달한다. 업로드 태스크는 해당 파일을 검증한 뒤 네 객체를 전송하며 날짜별 최신 export로 대체하지 않는다. 전송 실패·파일 손상·파일 누락은 LLM 재호출로 복구하지 않는다. 로컬 저장 모드는 파일만 검증하고 S3 전송을 생략한다.

생성은 자동 재시도 0회, 업로드는 Airflow에서 최대 2회 재시도한다. S3 SDK 자체 재시도는 비활성화했다. 같은 실행을 수동 재개할 때 완전한 발행 객체가 있으면 그대로 사용한다. 모델 응답을 받았지만 파일을 남기기 전에 중단된 경우까지 exactly-once를 보장하지 않으며, 그때 생성 태스크의 수동 재실행은 추가 모델 호출이 될 수 있다. 새 입력으로 새 보고서를 만들려면 새 DAG run을 사용한다.

S3 단계만 수동 복구할 때도 저장 파일과 생성 당시 해시를 지정한다. 이 명령은 LLM을 호출하지 않는다. 실제 S3 실행에는 외부 호출 승인이 필요하다.

```bash
python -m src.report.delivery <saved.publication.json> --sha256 <publication_sha256>
```

Kubernetes에서는 기존 logs PVC의 `/opt/airflow/logs/report-publications`를 사용해 서로 다른 생성·업로드 Pod가 파일을 공유한다. Compose는 기존 data 볼륨을 사용한다. 재시도·수동 복구가 끝나기 전에 실행별 파일을 지우지 않는다. logs PVC에 대한 보존·정리 정책도 이 파일에 적용될 수 있다.

## LLM 해설의 근거와 검사 범위

LLM은 프롬프트의 report_facts에서 지표를 선택하고 `{{usage.total_count}}` 같은 참조로 해설을 작성한다. 참조는 검증된 Gold의 지표 이름·값·단위로 치환한다. 자유 문장의 숫자·잘못된 날짜·알 수 없는 참조와 기간 증감·실제 지연·혼잡·승객 대기시간 관련 표현은 발행 전에 거부한다. 해석 한계와 기준일은 고정 본문에서 제공한다. 검사 실패 시 이전 발행을 보존하고 template로 조용히 대체하지 않는다.

이 검사는 수치 참조 계약과 보수적인 표현 검사다. 자유 문장의 전체 의미, 동의어·우회 표현의 사실성까지 증명하지 않으며 manifest에 `free_text_semantics_verified=false`를 남긴다. 새 프롬프트는 합성 응답 회귀로 검사했고 실제 모델의 준수율은 아직 평가하지 않았다. 프롬프트 변경과 평가의 근거는 [OpenAI 공식 가이드](https://developers.openai.com/api/docs/guides/prompt-engineering)를 참고했다.
