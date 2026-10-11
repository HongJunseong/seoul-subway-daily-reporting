# Delta 작은 파일 실험과 유지보수 선택

5분마다 append하는 도착 원본은 데이터가 적어도 파일을 계속 만든다. 기존 소량 실수집의 도착 3,000행은 활성 파일 6개·약 0.8MB였다. 이것만으로 운영 compaction이 필수라고 판단하지 않고 더 큰 별도 합성 입력에서 물리 파일과 조회 시간을 비교했다.

## 문제와 선택 범위

기존 year/month/day/hour 파티션과 수집 함수를 유지한다. 합성 36개 snapshot을 5분 간격으로 생성해 36,000행을 저장한다. 닫힌 KST 2026-01-31 23시 파티션만 DeltaTable.optimize().where(...).executeCompaction()으로 합친다. 22시와 월말을 넘은 2026-02-01 00시 파일은 그대로 보존한다.

[공식 Delta 문서](https://docs.delta.io/optimizations-oss/)의 partition predicate·bin-packing API를 사용하고 실제 설치된 Delta Spark 3.2.0에서도 검증했다. 이 유지보수는 새로운 dataChange=false 커밋을 만든다. 수집 lineage·원문·행 해시가 바뀌지 않는다는 의미이며 Bronze 물리 버전이 그대로라는 의미는 아니다. 저장 원본을 Silver/Gold로 재처리할 때는 Bronze 버전이 유지돼야 한다.

실험 도구는 기존 lake를 받지 않는다. 새 저장소 .work 하위에 별도 합성 lake만 만들고 이미 존재하는 출력 경로를 거부한다. 외부 API·S3·Databricks·Kubernetes·알림은 호출하지 않는다. fixture timestamp는 합성 계획 시각이며 실제 수집 시각으로 설명하지 않는다.

## 재현

Python 3.11·Java·requirements/dev.txt 환경을 사용한다. 기존 다른 writer가 접근하는 lake를 지정하지 않는다.

```bash
python scripts/compaction_experiment.py --output-dir .work/compaction/my-run
pytest -q tests/test_compaction.py
```

입력은 --snapshots 25..48, --rows-per-snapshot 1..1000으로 제한한다. 기본은 36×1000이며 전체 fingerprint 수집도 최대 48,000행으로 제한한다. 대용량 fingerprint를 driver에 모으는 운영 도구로 사용하지 않는다.

검증은 다음을 포함한다.

1. 명시적 KST·정각 시작·이미 닫힌 한 시간만 선택한다. 자정·월말·미래/열린 시간·naive 시각·잘못된 predicate를 검사한다.
2. Delta 루트와 versionAsOf로 읽어 모든 컬럼의 정렬된 JSON SHA-256을 비교한다. 원문·NULL·중복·마이크로초를 포함한다. 내부 Parquet를 직접 읽지 않는다.
3. 활성 add metadata에서 대상 파일 수/크기와 인접 파티션 파일 path/size 해시를 비교한다. 유지보수 커밋의 add/remove가 모두 dataChange=false인지 확인한다.
4. 유지보수 후 과거 전체 snapshot을 다시 읽는다. 월말 자정과 00:15의 기존 Silver·15분 Gold를 재처리해 전체 행 해시가 같은지 확인한다. 재처리 중 Bronze 버전도 유지한다.
5. 같은 파티션의 두 번째 compaction이 파일 추가/제거 0과 버전 불변으로 끝나는지 확인한다.

evidence.json은 파일·버전·전체 행 해시·조회 표본·검증 조건을 제공한다. [실제 실행 결과](../examples/operations/compaction.md)는 생성 원본을 그대로 복사했다. 합성 lake와 실행 로그는 ignored .work에 보존한다. CI에도 같은 실험과 evidence artifact를 정의했으며 원격 실행 여부는 SESSION_HANDOFF에 기록한다.

## 조회 시간의 해석

같은 Spark 세션에서 이전/이후 Delta 버전을 고정하고 두 쿼리를 비교한다. 한 시간 전체 조회와 마지막 15분 조회에서 행 수·ETA 정수 합·원문 문자 수를 확인해 count-only metadata shortcut에 의존하지 않는다. 두 버전을 각각 한 번 warm-up한 뒤 다섯 쌍을 번갈아 측정하고 표본을 모두 저장한다.

시간에는 Spark root 읽기 계획·실행이 포함된다. 별도 persist/cache를 만들지 않았지만 OS·JVM·Delta metadata cache 효과는 남는다. cold 조회나 S3 비용의 측정이 아니다. 활성 대상 파일 bytes는 실제 읽은 bytes와 구분하며 좁은 시간 조회가 모든 후보 파일을 읽었다고 단정하지 않는다.

## 운영 도입 판단

이번 결과는 파일 누적에 대응할 수 있는 검증된 방법을 제공한다. 현재 자동 일정과 운영 S3 writer는 추가하지 않았다. 운영 도입은 다음 실측으로 정한다.

- 실제 닫힌 시간/날짜의 파일 수·크기 분포와 빈번한 조회 범위.
- compaction 실행/추가 쓰기 비용과 반복 조회에서 절약되는 읽기·요청 비용.
- 1-slot writer pool에서 수집·변환과 경합하는 시간 및 신선도 영향.
- 작은 시간 파티션을 계속 유지할지, 더 큰 날짜 파티션으로 옮길지의 조회/재처리 비교.

예를 들어 닫힌 시간에 여러 작은 파일이 있고 반복 조회의 비용이 유지보수보다 큰 경우에만 대상으로 삼을 수 있다. 이번 synthetic 결과로 보편적 최소 파일 수나 자동 실행 주기를 확정하지 않는다. 시간 파티션을 합쳐도 출력 약 1MB 수준이면 여전히 작으므로 장기 입력과 비용을 확인한 뒤 파티션 변경까지 판단해야 한다.

운영 S3에 도입한다면 모든 유지보수 writer도 ssdr_delta_writer 1-slot pool에 포함하고 외부 CLI·다른 scheduler와 동시 쓰기를 금지한다. 로그·원문 보존 기간과 VACUUM/S3 lifecycle은 복구 요구에 맞춰 별도 검증한다. 이번 실험은 VACUUM을 호출하거나 과거 파일을 삭제하지 않는다.
