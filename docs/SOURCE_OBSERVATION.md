# 실제 원천의 제한 수집과 재처리

합성 장애 복구는 처리 정확성을 보여주지만 원천 API의 상한·누락·생성 시각을 설명하지 못한다. 이 단계는 외부 호출을 명시적으로 허용한 뒤 5분 간격 도착 스냅샷 세 번과 날짜 지정 승하차 하루를 별도 로컬 실험으로 수집한다. AWS·Databricks·Kubernetes·Discord·LLM은 호출하지 않는다.

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
