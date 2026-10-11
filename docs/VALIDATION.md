# 검증 범위와 증거

모든 수치 결과는 검증 입력과 함께 해석한다.

## 로컬에서 실행한 검증

- Python 3.11.15 / Spark 3.5.7 / Delta 3.2.0 / Java 11 환경에서 실제 Delta 테이블 읽기·쓰기.
- 합성 fixture 재수집, 같은 날짜 재처리, 정정 반영, 인접 날짜 보존.
- KST 월말 자정을 넘는 15분 window, 중복 snapshot, 결측 ETA의 유효 분모.
- 불완전 API pagination과 key가 포함된 URL 오류의 정제.
- 환승역 노선 구분, 노선 가중 평균, 분위수 해석, 보고서 기준일.
- 보고서/context 변조에 대한 manifest 검증 실패.
- 별도 Airflow 2.7.3 환경의 DagBag import, DAG 의존성, 수동 날짜·branch, 실제 Bash 템플릿 렌더.
- 로컬 감사 CLI: 월말 반개구간·수집 지터·중복 관측·처리 누락·계층별 날짜/값 불일치·잘못된 키/수치·누락 테이블의 실패 판정.
- 실제 Delta Silver 커밋 후 예외 주입: 기존 Gold 유지, 저장된 Bronze 재처리, 전체 업무 컬럼 해시 일치.
- 최신 원천의 음수 값 차단: 기존 Silver/Gold 내용·버전 유지, 올바른 원천 정정 후 복구.
- 태스크 callback의 정보 제한·실제 시도 번호, 중복/재시도/불완전·충돌 이벤트 집계와 로그 CLI 종료 코드.
- 실제 로컬 Airflow TaskInstance.run 실행: 날짜 XCom·재처리 분기·Spark BashOperator·queued DagRun 두 번 연결·실제 보고서/context/manifest 검증.
- 프로젝트 경로·Spark 실행 파일의 shell quoting, 로컬/S3 패키지 분리, 가상환경 symlink·DB 격리·기존 출력 보존.
- Ruff correctness 검사, Python 컴파일 검사, Compose 설정 파싱, Secret 셸 구문 검사.

현재 실행 결과의 테스트 수와 명령은 SESSION_HANDOFF에 기록한다. examples/evidence.json은 저장된 합성 데모 실행 결과다. 처리 시간은 소량 로컬 입력의 전체 데모 시간으로, 서비스 SLA나 대용량 성능으로 사용하지 않는다.

[관측·복구 절차](OBSERVABILITY.md)와 [operations 공개 샘플](../examples/operations/README.md)에 이번 실행 증거를 남겼다. 예외 주입은 실제 프로세스 강제 종료·인프라 장애 시험과 구분한다. callback 지표는 실제 로컬 태스크 실행으로 시간 표본을 확보했다. [Airflow 연결 실행](AIRFLOW_OFFLINE.md)은 순차 TaskInstance 실행이다. 실제 scheduler와 KubernetesExecutor의 별도 실행 증거는 아래에 기록했다. 순수 pool 대기·peak 자원은 아직 측정하지 않았다.

## 아직 검증하지 않은 범위

- 실제 서울 API의 지속 수집, 사용 가능한 날짜 지연의 분포, 응답 상한·결측·네트워크 커버리지.
- 실제 S3 자격 증명·권한·Delta 쓰기·읽기와 장애 복구.
- Compose scheduler 전체 실행. Docker 이미지 빌드는 2026-10-11 후속 검증에서 통과했다.
- 운영 Kubernetes의 다중 node PVC·remote logging, Pod peak CPU/메모리·queue/pool 대기, 유지보수 버전과 metadata DB 조합.
- Discord 전달과 OpenAI 해설. 기본 실행·테스트에서 외부 메시지/LLM을 호출하지 않았다.
- 정의된 GitHub Actions의 원격 실행. 로컬에서 동일 검증 명령을 실행했으나 push하지 않았다.

## 실데이터 검증 절차

운영 검증은 baseline 측정 결과를 파일로 남긴다. 최소 일간 보고서의 D-1 관측이 가능한 기간을 수집하며, 더 긴 기간의 커버리지와 오류 패턴을 비교한다.

1. 사용한 코드 commit, prefix, 실행 환경, 수집 시작·종료 시각을 기록한다.
2. snapshot별 응답 수·response_at_limit, 정상 수집 횟수, 소스 시각 차이, ETA 결측률을 집계한다.
3. report의 출처별 Delta 버전과 manifest 해시를 보존한다.
4. 저장된 원본으로 같은 날짜를 두 번 재처리하고 자연키 수·합계·유효 표본이 같은지 확인한다.
5. 외부 API 호출 없이 복구하는 경로의 실행 시간, 실패 구간·재시도 기록을 남긴다.
6. parquet 파일 수·크기·읽기 시간을 측정한 다음 compaction/파티션 변경을 비교한다.

관측하지 못한 날짜나 역을 정상 0건으로 바꾸지 않는다. 장애 검증은 운영 원본을 지우는 방식으로 만들지 않고 별도 prefix에서 수행한다.

## Databricks와 Kubernetes 실제 실행 (2026-10-11)

기존 Free Edition에서 serverless Job 1103992482650397, Run 44336808137994의 네 태스크가 SUCCESS다. Spark.version=4.2.0, 전체 197.11초, 날짜별 680/660, Bronze 버전 0 불변, Silver·역/노선 Gold 전체 업무 컬럼 해시가 실제 로컬 Spark 3.5.7 실행과 일치한다. [공개 증거](../examples/databricks/README.md)는 성공한 실제 Job 응답에서 생성했고 실패/누락/다른 엔진/변경된 해시를 거부한다.

ANSI 모드의 잘못된 문자열·overflow·revision 날짜·빈 입력이 이전 정상 Silver를 보존하는 회귀 검사도 통과했다. 버전을 고정한 Delta 루트 export는 이후 원본 변경을 포함하지 않으며 lineage를 보존한다.

공식 Helm chart 1.11.0 렌더, Airflow 2.7.3 PodGenerator 병합과 DAG 직렬화에서 태스크 17개를 확인했다. 해당 구성 JSON은 actual_pod_execution=false를 유지한다. 별도의 [실제 실행 증거](../examples/kubernetes/README.md)는 Docker Desktop Kubernetes 1.32.2에서 scheduler→worker Pod→보고서를 실행한 결과이며 actual_pod_execution=true다.

첫 실행은 태스크 12개 success·API 수집 1개 skipped, worker Pod 13개, 376.452561초다. Silver 성공 후 실행 중인 Gold worker를 UID 조건으로 삭제했고 다른 UID의 두 번째 시도가 성공했다. 종료·삭제된 worker 로그 13개가 PVC에서 읽혔다. 최종 이미지의 두 번째 실행은 태스크 12개 success·worker Pod 12개, 240.209483초다. 원본 버전 불변과 7개 Delta 테이블 전체 데이터 해시 일치, 로그 12개·보고서 manifest를 검증했다. 후속 Gold 커밋 버전의 증가는 데이터 변경과 구분한다. RWO PVC는 단일 node 합성 저장이며 S3·운영 다중 node 증거가 아니다.

최종 회귀: 파이프라인 81 passed(123.15초), 이후 추가한 PVC 재처리 비교 검사 2 passed(0.01초), Airflow/Pod 12 passed(1.20초), 서로 다른 검사 총 95개. 기존 Airflow/Pydantic 경고 8개. 공식 provider/client constraints를 사용했고 Airflow pip check가 통과했다. 원격 CI는 실행하지 않았다.

실제 Docker 이미지 빌드와 두 Python 환경의 pip check도 통과했다. 이미지 안에서 합성 원본 준비·실제 Airflow 태스크 12개·보고서 해시를 검증했다. [컨테이너 증거](../examples/container/README.md)는 최종 이미지 실행 79.324771초와 측정 범위를 기록한다. 순차 TaskInstance 실행이며 실제 Kubernetes scheduler/Pod 실험과 구분한다.
