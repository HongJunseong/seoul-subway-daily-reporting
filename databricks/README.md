## Databricks (Free Edition)

이 디렉터리는 서울 지하철 일간 리포팅 파이프라인이 Databricks 환경에서도  
Spark + Delta Lake 기반으로 재현 및 실행 가능함을 확인하기 위한 목적을 가집니다.

---

## 실행 환경

- Databricks Free Edition
- Databricks Managed Spark
- Delta Lake (Managed Table)

### 제약 사항 (Limitation)

- Databricks Free Edition에서는 s3:// 경로에 대한 직접 접근이 제한됨
- 파일 업로드 개수에 제한이 존재함

### 대응 방식 (Approach)

- 운영 환경의 S3 Bronze 레이어에서 일 단위 샘플 데이터 추출
- 다수의 Parquet 파일을 사전 병합(compaction) 하여 업로드 개수 제한 대응
- Databricks FileStore에 샘플 데이터 업로드
- Create Table 기능을 활용해 Parquet → Delta 테이블로 변환

---

## Layer

### Bronze
- 원천 데이터를 최대한 가공 없이 적재
- Databricks에 upload된 일 단위 지하철 이용 데이터 sample

Table : `bronze_subway_usage`

### Silver
- 데이터 정제 및 표준화 수행
- 수치 컬럼에 대한 명시적 타입 캐스팅 (string → numeric)
- 컬럼명 표준화
- REG_YMD 기준 최신 데이터만 유지하여 중복 제거
- 파생 지표 생성
    - total_cnt = 승차 인원 + 하차 인원

Table : `silver_subway_usage`
Notebook : `01_usage_bronze_to_silver`

### Gold
- 분석 및 리포팅에 바로 활용 가능한 집계 테이블 생성
- 역 단위 일간 이용량 집계
- 노선 단위 일간 이용량 집계

Tables : `gold_subway_usage_daily_station`, `gold_subway_usage_daily_line`
Notebook : `02_usage_silver_to_gold`

---

## Notebook 구성
Databricks Notebook은 Databricks UI에서 직접 작성 및 실행되었으며,
버전 관리 및 재현성을 위해 Source 파일(.py) 형태로 Export하여 관리하였습니다.

> 참고: Export된 .py 파일은 로컬 실행을 목적으로 하지 않으며,
Databricks 전용 MAGIC 명령어 및 셀 구조를 보존하기 위한 소스 파일이다.

## 핵심
- Databricks는 Spark + Delta 기반 파이프라인의 대체 실행 환경으로 활용 가능함
- Notebook을 기반으로 하여 Bronze / Silver / Gold 레이어의 책임을 명확히 분리함
- Delta 테이블을 통해 Notebook 실행과 무관하게 결과를 영속화함
- Free Edition의 제약 사항을 고려하여 샘플링, 파일 병합, 업로드 기반 검증 방식으로 실무적인 대응 전략을 적용함