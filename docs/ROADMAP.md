# 고도화 로드맵

작업은 의존성과 완료 증거를 기준으로 진행한다. 테스트 수나 기술 수를 성과로 대체하지 않는다.

## Phase 1 — 데이터 정확성
- [x] 명시적 Bronze 스키마·원문·수집 lineage
- [x] 승하차 재수집 중복 제거·최신 정정 반영
- [x] Delta 루트 읽기와 날짜 단위 교체
- [x] 15분 집계의 시간·날짜 경계 수정
- [x] NULL ETA 보존·유효 표본 분모 분리
- [x] 환승역 역명 조인 제거·가중 평균·분위수 의미 정리
- 완료 증거: Spark/Delta 회귀 테스트, fixture 정정·재처리 실험

## Phase 2 — 자동화·복구
- [x] 수집 DAG와 원본 처리 DAG 분리
- [x] 일간 자동 수집에서 Gold·보고서로 연결
- [x] 공통 실행 구간과 수동 날짜 검증
- [x] 원본 재처리 CLI·API 재수집 opt-in
- [x] S3 writer 공유 pool 직렬화·수동 실행 제약 문서
- [x] Kafka 제거·알림 실패와 보고서 실행 분리
- 완료 증거: 실제 Airflow DagBag·템플릿·의존성 검사

## Phase 3 — 재현 가능한 결과
- [x] 키 없는 합성 데이터 end-to-end 데모
- [x] 결정적 보고서·입력 context·publication hash
- [x] 의미 있는 회귀 테스트와 CI 정의
- [x] 로컬 Delta 품질·시간 구간·처리 누락·파일 감사 CLI
- [x] 실제 Delta 커밋 후 예외 주입·품질 실패·원본 재처리 복구 실험
- [x] Airflow lifecycle 로그와 시도별 실행·executor 대기 집계
- [x] 저장된 합성 Bronze에서 실제 Airflow 태스크·XCom·DAG 트리거·보고서 연결 실행
- [ ] 실제 수집 데이터의 결측·건수·커버리지 baseline 수립
- [ ] 배포 환경에서 수집 신선도·실행 시간·복구 시간 측정
- Phase 3의 실운영 항목은 live 서비스 검증 증거가 있어야 완료한다.

## Phase 4 — 포트폴리오 평가 근거
- [x] 문제→설계→오류 개선→검증→결과의 README
- [x] 데이터 계약·ADR·복구 runbook·검증 제한 문서
- [ ] 샘플 외 실제 데이터 관측 결과와 운영 기간 기록
- [ ] 실측 파일 수·읽기 비용을 기준으로 파티션/compaction 개선
- [ ] 보존 기간·API 페이지 상한·운영 시간대 정책을 실측으로 확정

## 작업 선택
미완료 live 항목을 문서·합성 결과로 완료 처리하지 않는다. 외부 접근이 없으면 독립적인 코드·재현·검증 작업을 진행한다.
새 기능이 검증 목표를 늘리지 않으면 추가하지 않는다.

## Phase 5 — 플랫폼과 실행 증거
- [x] 공통 승하차 변환의 Databricks managed Delta adapter
- [x] ANSI 불량 수치·날짜 차단, 날짜별 MERGE와 인접 날짜 보존
- [x] 기존 Free Edition serverless Job 네 태스크 실제 성공과 재처리 해시 비교
- [x] KubernetesExecutor 자원 프로필, 고정 chart/provider, namespace RBAC, lake/log PVC 설정
- [x] 공식 Helm 렌더·실제 Airflow 태스크 17개 Pod 병합과 직렬화 검사
- [x] 실제 Kubernetes scheduler→worker Pod→보고서 실행 및 PVC binding
- [x] worker 실패 후 다른 Pod 재시도, 삭제된 Pod의 로그 보존 확인
- [x] 동일 Bronze의 실제 Pod 재처리에서 7개 Delta 테이블 전체 해시·원본 버전 불변
- [ ] 실제 Pod peak 메모리/CPU·queue/pool 대기로 자원 설정 조정
- [ ] 유지보수 중인 Airflow/chart/provider 조합과 운영 metadata DB 검증

Databricks 성공은 합성 승하차 fixture, Kubernetes 성공은 단일 node 합성 lake를 대상으로 한다. 실수집·S3·실시간 관측·대규모 성능의 증거로 확대하지 않는다.
