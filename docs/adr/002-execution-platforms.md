# ADR 002 — 실행 환경별 책임과 검증

날짜별 정확성·품질·재처리 규칙은 src의 공통 함수에 둔다. 기술 도입의 판단 기준은 동일 업무 결과와 실제 실행 증거다.

Kubernetes는 Airflow scheduler와 태스크 프로세스의 자원을 분리하고 태스크별 재시도를 관리한다. local[2] Spark를 태스크 Pod에서 실행하므로 분산 Spark 운영 경험으로 주장하지 않는다. 공식 chart/이미지/provider 버전을 함께 고정하고 자원 override가 이미지·설정·PVC·service account를 잃지 않는지 실제 Airflow 모델로 검사한다.

Databricks는 승하차 처리를 관리형 Spark와 Unity Catalog Delta 테이블에서 실행하는 경로다. Free Edition의 serverless 세션을 사용하고 RDD/JVM 설정, 직접 S3 자격 증명, legacy DBFS/FileStore에 의존하지 않는다. 저장 adapter는 관리형 테이블 이름을 사용하며 업무 변환은 Airflow/S3 경로와 공유한다. 날짜 단위 결과 교체를 자연키 MERGE와 해당 날짜의 source에 없는 행 삭제로 구현한다.

로컬 OSS Delta 3.2의 managed saveAsTable overwrite는 truncate 미지원 오류가 있어 관리형 쓰기는 MERGE로 검증했다. 실제 serverless 실행에서는 저장 mode 별칭 errorifexists가 거부돼 error로 수정했다. 운영 S3의 replaceWhere 쓰기는 유지한다. 다른 엔진의 구현 차이를 목격하고 재현 검사로 확인한 선택이다.

현재 workspace 검증 Job은 전용 합성 namespace와 *_v2 테이블을 사용하며 기존 사용자의 legacy 학습 테이블은 보존한다. 자동 API 수집이나 운영 S3와의 동기화를 만들지 않는다. 단일 Job 동시 실행 제한을 사용하되 다른 notebook/Job의 writer는 별도로 직렬화해야 한다.

각 테이블의 커밋은 원자적이지만 Silver/Gold 전체의 단일 트랜잭션은 없다. API 원문 보존, 명시적인 읽기 Delta 버전, 멱등 재처리로 중간 실패를 복구한다. 성능 최적화·파일 compaction과 운영 scale-up은 실측 이후 선택한다.

채택 증거는 examples/databricks, examples/kubernetes와 SESSION_HANDOFF에 기록한다. 관리형 Job 실행, Helm 렌더링, 실제 worker Pod 실행을 서로 다른 검증 범위로 표시한다.

실제 Kubernetes 검증에서는 실행 중인 Gold Pod를 종료하고 scheduler가 다른 Pod에서 재시도해 보고서를 완성했다. 로그 PVC와 동일 원본의 전체 테이블 해시 비교로 복구를 확인했다. 자원 프로필의 성능 적정성과 운영 S3 writer 복구는 별도로 측정해야 한다.
