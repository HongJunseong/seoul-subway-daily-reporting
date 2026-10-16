# 실제 원천의 제한 수집과 재처리

합성 장애 복구는 처리 정확성을 보여주지만 원천 API의 상한·누락·생성 시각을 설명하지 못한다. 이 단계는 외부 호출을 명시적으로 허용한 뒤 5분 간격 도착 스냅샷 세 번과 날짜 지정 승하차 하루를 별도 로컬 실험으로 수집한다. AWS·Databricks·Kubernetes·Slack·LLM은 호출하지 않는다.

## 재현

기존 로컬 .env 또는 환경변수의 SEOUL_ARRIVAL_API_KEY, SEOUL_PASSENGER_API_KEY를 사용한다. 키를 명령 인자·로그·문서에 넣지 않는다. 저장 경로는 저장소의 ignored .work 하위에 있는 새 디렉터리로 제한한다. 기존 디렉터리가 있으면 중단한다.

```bash
.venv/bin/python scripts/capture_sources.py \
  --fetch --output-dir .work/source-capture-new \
  --snapshots 3 --cadence-seconds 300

.venv/bin/python scripts/process_source_experiment.py \
  --capture-dir .work/source-capture-new \
  --output-dir .work/source-processing-new
```

첫 명령만 외부 API에 접근한다. 두 번째 명령은 저장된 원본을 새 로컬 Delta lake로 옮기고 기존 Bronze·Silver·Gold 코드를 호출한다. 다시 처리해 7개 테이블 전체 행 해시가 같은지, Bronze 버전이 바뀌지 않았는지 확인한다. Delta는 루트의 transaction log를 통해 읽는다. 원천을 재수집하는 API backfill과 보관된 원본 재처리를 구분한다.

캡처는 재시도 없는 논리 GET을 센다. 도착은 슬롯별 한 번, 승하차는 완전한 페이지 수집을 최대 10번으로 제한한다. HTTP redirect도 따르지 않는다. 간격은 단조 시계로 유지하며 실제 KST 시각을 그대로 기록한다. 표시 시계와의 차이는 검증에서 최대 1초까지만 허용한다. 늦어진 슬롯은 몰아서 호출하지 않고 missed_schedule로 남긴다. 빈 행·오류·불완전 승하차 페이지에는 raw 성공 파일을 쓰지 않는다. 캡처가 완결되지 않으면 정제 출판을 중단한다. 실패 시 예외 종류만 기록하며 URL·원천 오류 문장은 공개하지 않는다.

## 측정과 해석

- 슬롯별 계획 시각·요청 시작/완료·관측 저장 시각·응답 행·논리 요청 수를 구분한다. 원천 생성 시각 recptnDt는 Silver의 별도 컬럼이다.
- 응답 상한 도달, 관측한 노선과 역/노선 identity 수, 관측 시간 bucket, 결측 ETA와 원천 시각, 원천 시각 나이의 중앙값·95백분위수·최댓값을 측정한다.
- 같은 snapshot·자연키의 서로 다른 원천 세대는 최신 수집→최신 원천 생성 시각→해시 순서로 선택한다. 원천 미래 시각은 보정하지 않고 별도 경고한다. 일간 도착 Gold는 이 세 snapshot만의 요약이며 완전한 하루 관측이 아니다.
- 승하차는 이용 날짜·자연키 수·중복·Silver/역 Gold 전체 업무 행 일치를 확인한다. 수집한 날의 혼잡도라고 설명하지 않는다.
- 실제 API 실패는 capture 슬롯으로 확인한다. Delta 감사의 관측 없는 bucket만으로 API 실패라고 추론하지 않는다.
- 파일 수·bytes는 Delta 활성 파일 메타데이터다. 실제 읽기 비용이나 compaction 효과를 측정한 값이 아니다.
- 공개 evidence에는 캡처 메타데이터·집계 수치·해시·Delta 버전만 남긴다. 원문 JSON, Bronze raw_payload, 정제 행, 인증 파일은 .work에 보존하며 커밋하지 않는다.

서울시 [실시간 도착 명세](https://data.seoul.go.kr/dataList/openApiView.do?infId=OA-12764&srvType=A)는 요청당 최대 1,000건과 recptnDt의 생성 시각 의미를 설명한다. [API 사용 안내](https://data.seoul.go.kr/together/guide/useGuide.do)는 실시간 지하철 일일 최대 1,000회 요청을 안내한다. 한 collector의 5분 주기는 하루 288번의 논리 호출이지만 운영 HTTP 재시도·다른 실행·같은 키 사용량은 추가된다. 운영 키 전체의 일일 예산은 이 짧은 실험으로 검증하지 않았다.

ALL은 요청 범위다. 응답이 1,000건이면 전체 역·열차를 확보했다고 볼 수 없다. 스냅샷 세 번의 identity 수와 수집 성공률을 전체 네트워크 커버리지나 운영 가용성으로 표현하지 않는다. 과거에 수집하지 않은 실시간 관측을 이 API로 복원할 수 없다는 점은 [서울시 데이터 안내](https://data.seoul.go.kr/dataList/OA-12764/A/1/datasetView.do)와 같다.

## 도입 판단

현재 요구가 제한된 표본의 ETA 관측 분석이면 상한과 표본 범위를 보고서에 드러내고 유지할 수 있다. 전체 역 커버리지가 요구되면 실제 관측 identity의 변동과 API 예산을 더 측정한 뒤 대상 역·시간대 정책을 정해야 한다. 실시간 원천은 페이지마다 변할 수 있으므로 페이지를 추가했다고 같은 시점의 완전한 snapshot이 되는 것은 아니다. 기술 추가보다 원천 계약과 수집 범위를 먼저 정한다.

실제 실행 결과는 [관측 증거](../examples/observed/evidence.md)에 기록한다. 10분 규모 실험은 장기 baseline의 시작이며 [로드맵](ROADMAP.md)의 지속 수집·배포 운영 측정은 남는다.

## 여러 날짜의 수집 기록 비교

[메타데이터 비교 CLI](../scripts/summarize_source_observations.py)는 저장된 capture.json만 읽는다. API·인증·원문·Delta에 접근하지 않는다. 실패했거나 중단된 캡처도 비교 대상이며, 출력은 새로운 .work 경로에 만든다. 같은 캡처 파일의 사본은 canonical JSON SHA-256으로 제외한다. 시작 시각이 같지만 내용이 다른 중간/최종 파일을 함께 넣으면 어느 버전이 맞는지 자동 선택하지 않고 중단한다.

```bash
.venv/bin/python scripts/summarize_source_observations.py \
  --capture-dir .work/source-capture-day1 \
  --capture-dir .work/source-capture-day2 \
  --start-day 20261011 --end-day 20261012 \
  --as-of 2026-10-12T23:00:00+09:00 \
  --output-dir .work/source-baseline-new
```

날짜 구간은 KST 달력 날짜이며 최대 31일이다. 분석 기준 시각은 명시적인 +09:00을 요구한다. 완료된 요청이 이 시각보다 미래이면 분석을 중단한다. 자정을 넘긴 도착 슬롯은 계획 날짜별로 나누고, 승하차 시도는 캡처 시작 날짜에 귀속하면서 실제 이용 날짜 usage_day를 별도로 유지한다.

summary.json과 summary.md에는 다음을 기록한다.

- collected: 비어 있지 않은 응답을 저장했다고 기록된 슬롯. 이 비교는 원문과 수치 품질을 다시 검증하지 않는다.
- failed: collector의 요청·저장 시도 실패 기록. API GET 전에 실패하면 실제 호출 수는 0이다.
- missed_schedule: collector가 계획 시각을 놓쳐 요청하지 않았다고 기록한 슬롯.
- not_recorded_due / future: 실행 중 캡처에 아직 기록이 없는 도래/향후 슬롯. 기록 없음만으로 요청 실패를 단정하지 않는다.
- 날짜별 응답 행 수·1,000건 상한 도달 횟수·실제 논리 GET 수·마지막 관측 시각.
- 요청·로컬 저장 시도 시간의 최소/중앙값/95백분위수/최대와 표본 수. 95백분위수는 정렬 표본의 선형 보간이며, 표본 3개의 수치를 운영 SLO로 해석하지 않는다.
- 분석 기준 시점의 마지막 관측 나이. 원천 생성 시각의 나이나 처리 지연과 구분한다. recptnDt와 Silver 품질은 기존 process_source_experiment.py의 감사 결과에서 확인한다.

request_outcome_success_rate의 분모는 기록된 collected + failed 시도이며, HTTP 응답 성공률과 같지 않다. declared_due_slot_collection_rate의 분모는 supplied capture 계획의 도래 슬롯이며, 일정 놓침과 기록 누락도 포함한다. 향후 슬롯은 제외한다. 계획 자체가 없는 날짜는 수집 범위 미확인으로 표시하고 실패율의 분모에 넣지 않는다. CLI 종료 코드 0은 분석 파일 생성 성공이며 원천이나 운영 파이프라인의 건강 판정이 아니다.

이 도구는 장기 관측을 비교할 준비다. 한 날짜의 세 슬롯을 비교한다고 하루 전체 계획이나 여러 날 baseline이 생기지 않는다. 여러 날 실제 호출은 기간·횟수를 별도로 허용받은 뒤 기존 capture_sources.py를 재사용한다. 원천 일간 예산·다른 collector·전체 역 커버리지는 별도 관측이 필요하다.

## 승인된 기간의 하루 한 번 관측

[run_source_observation_day.py](../scripts/run_source_observation_day.py)는 기존 collector를 호출하는 기간·예산 제어 진입점이다. 새 캠페인은 최대 7일, 매일 도착 세 번(5분 간격)·승하차 D-4 한 날짜, GET 최대 13회로 제한한다. 원천 수집 코드를 복제하지 않는다.

```bash
python scripts/run_source_observation_day.py init \
  --campaign-dir .work/source-campaign-new --start-day 20261012 --days 7 --daily-time 17:50
python scripts/run_source_observation_day.py status --campaign-dir .work/source-campaign-new
python scripts/run_source_observation_day.py run --fetch --campaign-dir .work/source-campaign-new
```

init/status는 서비스에 접근하지 않는다. run은 명시적 --fetch를 요구하고 실제 KST 오늘이 계획 기간에 속하며 계획 시각부터 5분 안에 시작할 때만 수집한다. 날짜나 현재 시각을 임의로 바꾸는 실행 인자를 제공하지 않는다. 기간 밖·시각 이전·늦은 시작에는 GET이 없다. 계획의 SHA-256을 별도로 고정하고 내용이 바뀌면 수집을 중단한다. 기간·예산·cadence 변경이나 새 캠페인은 새 승인 범위다.

하루 디렉터리의 원자적 생성으로 예산을 먼저 예약한다. 중복/동시 실행·실패·프로세스 중단 뒤에는 같은 날 다시 호출하지 않는다. 예약 디렉터리를 삭제하거나 다른 캠페인으로 우회해 재수집하지 않는다. session 자체가 14번째 GET과 KST 날짜를 넘는 호출도 차단한다. 예약 기록이 남았지만 완료 기록이 없으면 실제 호출 수는 미확인으로 취급한다.

campaign status의 계획 날짜와 실행 기록을 capture 비교 결과와 함께 본다. state가 없는 도래 날짜는 unrecorded_due이며 API 실패라고 단정하지 않는다. 수집이 불완전하면 process_source_experiment의 출판 gate를 통과하지 못한다. 완료된 날짜만 저장 원본에서 별도로 처리한다.

현재 사용자가 허용한 계획은 **2026-10-12~18, 매일 17:50 KST, 최대 91 GET/7일**이다. .work/source-campaign-20261012-18/plan.json에 고정했으며, 20261011의 기존 10분 실험과 구분한다. 원문은 로컬에만 두며 AWS·Databricks upload·Slack·LLM은 이 계획에서 호출하지 않는다. API 키 전체 예산이나 다른 collector의 동시 호출을 제어한 것은 아니다.

예약은 이 채팅의 앱 자동화로 실행한다. [공식 예약 작업 안내](https://learn.chatgpt.com/docs/automations?surface=app)에 따라 로컬 파일 작업에는 컴퓨터와 앱이 실행 중이어야 한다. 꺼져 있거나 실행이 늦어 놓친 관측은 소급 호출하지 않는다. 기간 종료 후 호출을 종료하고 확보한 날짜·미확인 날짜·처리 품질·응답 상한을 보고한다.

10/16 점검에서 12~15일 실행 기록은 미확인, 16일은 실제 CLI의 missed_start_window/GET 0회로 남았다. 예약 설정은 17·18일 17:50 KST를 명시하도록 갱신했으며 plan의 기간·예산은 보존했다. 늦은 실행의 원인은 기록만으로 확정하지 않았고 남은 예약의 실제 기동은 별도 확인 대상이다.

10/16의 추가 승인된 승하차 1 GET은 이 캠페인과 별도다. 10/8 이용량과 기존 10/11 도착으로 10/12 기준 보고서를 재처리했고, 날짜·생성 시각·범위는 [보고서 점검](../examples/observed/report-check.json)에 남겼다. 로컬 lake의 보고서는 SSDR_DATA_ROOT·SSDR_REPORT_OUTPUT_DIR·SSDR_DATA_KIND=observed·SSDR_REPORT_MODE=template을 설정한 뒤 `python -m src.report.generate_daily_report 20261012 --local-only`로 생성하고 verify_report.py로 확인한다. 날짜가 없는 경우 다른 날짜로 대체하지 않는다.

## 고정 계획과 실제 기록 대조

기존 capture 모드는 캡처 파일에 선언된 슬롯만 비교한다. 캠페인 모드는 SHA-256으로 고정한 plan.json과 날짜별 state.json을 함께 읽어, 캡처가 없는 예정 날짜도 분모에 포함한다.

```bash
.venv/bin/python scripts/summarize_source_observations.py \
  --campaign-dir .work/source-campaign-20261012-18 \
  --as-of '2026-10-13T01:10:00+09:00' \
  --output-dir .work/source-campaign-20261012-18/summaries/20261013-0110
```

--as-of는 실제 분석 시점의 명시적 KST로 지정한다. 위 시각은 사용법 예시다. 기존 출력은 덮어쓰지 않는다. 이 명령은 API·원문·Delta·인증 정보를 읽지 않고, 계획 seal 및 상태·캡처 메타데이터만 검증한다.

- due_slots는 고정 계획의 도래 슬롯, future_slots는 아직 예정 시각 전인 슬롯이다.
- failed_request_slots는 캡처에 실패가 기록된 요청이다.
- missed_start_window_slots는 CLI가 시작 시각을 놓쳐 호출하지 않았다고 명시한 슬롯이다.
- unconfirmed_due_slots는 도래했지만 결과를 확인할 기록이 없는 슬롯이다. 호출 실패나 0회로 해석하지 않는다.
- recorded_get_attempts가 null이면 호출량 미확인이다. 알려진 합계는 이러한 날짜가 있으면 전체 실제 호출량의 하한이다.
- planned_due_slot_collection_rate의 분모는 고정된 하루 세 번 표본 계획이다. 운영 DAG의 24시간 5분 폴링 성공률이 아니다.

완료 상태인데 capture.json이 없거나, D-4 날짜·호출 수·수집 날짜·간격이 서로 다르면 집계를 거부한다. 예약 디렉터리의 외부 경로 이탈도 차단한다. 늦게 시작한 실제 캡처의 시각은 원본 비교 결과에 유지하고, 고정 시작 시각과 구분한다. 출판 전 정제 품질과 재처리는 기존 process_source_experiment.py에서 별도로 검증한다.

[실제 계획 감사 예시](../examples/observed/campaign-plan-audit.md)는 API 실패를 추정하지 않고 확보된 실행 기록의 범위를 보여준다.

## 저장 원본의 무결성 계약

새 수집은 capture schema 2로 성공한 파일마다 raw_sha256을 기록한다. SHA-256은 저장한 UTF-8 JSON의 정확한 바이트를 대상으로 하며, Bronze의 행 단위 raw_payload 해시와 구분한다. JSON 저장은 임시 파일의 원자적 교체를 유지하고 private 파일 권한은 0600이다.

process_source_experiment.py는 schema·수집 메타데이터를 검증한 뒤 각 파일의 바이트 해시를 수집 당시 기록과 대조한다. 행 수·수집 시각이 같아도 값이 달라졌거나 바이트가 손상되면 출력 디렉터리 생성과 Spark 시작 전에 중단한다. 성공 파일의 해시 누락·잘못된 형식·미지원 schema·예약 밖 파일 경로도 허용하지 않는다. capture.json과 원본은 변경하지 않으며 불일치를 새 해시로 덮어 봉합하지 않는다.

처리 evidence.json의 source_integrity에는 다음 범위를 기록한다.

| 상태 | 의미 |
|---|---|
| verified_against_capture | schema 2 원본의 모든 바이트가 수집 당시 체크섬과 일치 |
| unverified_legacy | schema 1 원본을 현재 해시했지만 수집 당시 체크섬은 없어 무결성을 소급 입증하지 못함 |

기존 schema 1 원본과 공개 증거는 보존한다. 현재 해시를 과거 수집 시점의 체크섬으로 추가하지 않는다. 날짜별 메타데이터 비교는 schema 1/2 모두 지원하지만 해시 형식만 확인하며 원본 바이트 검증으로 표현하지 않는다. 고정 관측 plan schema 1과 capture schema 2는 별도 계약이다. 수집 간격·기간·GET 예산을 바꾸지 않는다.

체크섬은 원본과 수집 기록 사이의 변경을 탐지한다. 원본과 메타데이터를 함께 바꾼 경우의 출처 인증을 제공하지 않으며, 정제 데이터 품질과 Delta 재처리 일치도 별도 검사다. [합성 손상 주입 증거](../examples/observed/source-integrity.json)는 네 파일 검증 후 동일 건수·시각의 값 변경을 차단한 실제 실행 결과다. 10/11 실제 원본은 읽기만 했으며 legacy 상태로 남겼다. 이번 검증에는 서울 API 호출이 없다.
