# 실제 컨테이너 실행 증거

2026-10-11 Docker Linux engine 28.3.0에서 ssdr-airflow:portfolio-v2를 빌드하고 서비스 키·호스트 볼륨 없이 합성 fixture를 실행했다.

- 이미지 ID: sha256:15fbf2798c006efef73add9d61f747b0f6e1eaabe28e3135f9ed314fa10f7d4c
- 컨테이너 ssdr-airflow-fixture-20261011-b, exit 0, OOMKilled=false.
- Airflow 2.7.3, Spark 3.5.7, Delta 3.2.0. Airflow와 pipeline Python을 분리하고 두 pip check 통과.
- 실제 태스크 12개 success, API 수집 1개 skipped, 각 실행 시도 1회.
- DAG 연결 실행 시간 79.324771초. 원본 준비/DB 초기화를 제외하고 Spark 제출/JVM 시작을 포함한다.
- 이용량 680, D-1 도착 4건/유효 ETA 4건, 보고서·context 해시 검증 통과.
- [실행 결과](evidence.json), [실제 callback 집계](task-summary.json).

이 실험은 컨테이너 내부 순차 TaskInstance.run 실행이다. 실제 Kubernetes scheduler·Pod·PVC·복구는 [별도 클러스터 실험](../kubernetes/README.md)에서 검증했다. Compose scheduler와 순수 queue/pool 대기는 미검증이다. 소량 합성 데이터 실행 시간이며 SLA·처리량 지표가 아니다.

새 컨테이너와 새 출력 경로에서 재현한다:

```bash
docker build -f docker/dockerfile -t ssdr-airflow:portfolio-v2 .
docker run --name ssdr-fixture-demo --entrypoint python \
  -e PYTHONPATH=/opt/airflow:/opt/airflow/dags -e SSDR_TASK_POD_PROFILES=false \
  ssdr-airflow:portfolio-v2 /opt/airflow/scripts/airflow_offline.py \
  --output-dir /opt/airflow/.work/container/demo --pipeline-python /opt/pipeline/bin/python
docker cp ssdr-fixture-demo:/opt/airflow/.work/container/demo/evidence.json ./container-evidence.json
```

.env·운영 Secret·live 데이터를 컨테이너에 넘기지 않는다. Dockerfile은 JRE와 Airflow 기반 이미지 digest를 고정했다. 기존 apt repository 종료/패키지 404와 단일 Python 환경의 boto 의존성 충돌을 실제 빌드에서 발견하고 고쳤다. 보관된 기본 이미지의 모든 보안 항목을 검증한 것은 아니며 유지보수 중인 Airflow 조합으로의 migration은 로드맵에 남겼다.

최종 이미지에서 Airflow NumPy 1.26.4와 pipeline NumPy 2.4.6을 분리했다. 기본 image의 compiled Shapely/BigQuery provider import도 빌드에서 확인한다. 첫 실행에 나타난 NumPy ABI 오류를 수정한 뒤 전체 컨테이너 실행을 재검증했다.
