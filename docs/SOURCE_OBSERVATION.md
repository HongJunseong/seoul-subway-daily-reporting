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
