# Seoul Subway Daily Reporting

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.3-017CEE)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Event--Driven-231F20)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.7-red)
![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900)
![Docker](https://img.shields.io/badge/Docker-2496ED)
![OpenAI](https://img.shields.io/badge/OpenAI-LLM%20API-412991)

> End-to-end data platform for collecting, processing, and daily reporting of Seoul subway operations using AWS, Spark, Kafka, Airflow, and LLM Auto-Reporting.

## Project Overview

본 프로젝트는 **서울 지하철 운영 데이터를 수집 → 정제 → 분석 → 리포트 자동 생성**까지  
전 과정을 아우르는 **End-to-End 데이터 플랫폼**을 구축하는 것을 목표로 합니다.

서울 열린데이터 API 기반의 지하철 데이터를 대상으로 다음을 구현했습니다.

- Spark 기반 Batch 데이터 파이프라인
- Bronze / Silver / Gold 계층 기반 데이터 관리
- Apache Airflow 기반 워크플로우 오케스트레이션
- Kafka 이벤트 기반 알림 구조
- LLM을 활용한 일간 분석 리포트 자동 생성

## System Architecture


## Data Layer Design (Bronze / Silver / Gold)

| Layer | Description |
|------|------------|
| **Bronze** | 원천 API 데이터를 가공 없이 저장 (Raw JSON / Parquet) |
| **Silver** | 결측치 제거, 타입 정합성 보정, 시간 기준 정규화 |
| **Gold** | 분석 및 리포트에 최적화된 집계 테이블 (Daily KPI, Top Stations 등) |

> medallion architecture 구조를 참고하여, Bronze / Silver / Gold 계층으로 데이터를 단계적으로 관리하도록 설계했습니다.

---

## Data Pipelines

### 1. Ingestion (Bronze)
- 서울 지하철 Open API 호출 (일 / 시간 단위)
- 장애 대비 재시도 및 로깅 처리
- 수집된 Raw 데이터를 **AWS S3 (Bronze Layer)** 에 저장
- 원본 데이터 보존을 통해 재처리 및 데이터 추적 가능 구조 설계

### 2. Processing (Silver / Gold)
- **Apache Spark** 기반 데이터 처리 (대용량 데이터 집계 및 확장성 고려)
- AWS S3에 저장된 Bronze 데이터를 읽어 정제 및 집계 수행
- 결측치 제거, 타입 정합성 보정 등 데이터 품질 관리
- 처리 결과를 **AWS S3 (Silver / Gold Layer)** 에 Parquet 형식으로 저장
- 날짜 지연(Lag Day)을 고려한 안정적인 집계 로직 구현
- 역 / 노선 / 시간 단위 분석 테이블 생성

### 3. Orchestration
- **Apache Airflow**
  - AWS S3 기반 데이터 파이프라인 전체 오케스트레이션
  - 수집, 처리, 리포트 생성 DAG 분리
  - 각 파이프라인의 실행 주기 독립 관리
  - 실패 시 재시도, 태스크 의존성 및 실행 순서 제어

---

## Streaming & Alerting

- **Apache Kafka**
  - Gold Layer 데이터 생성 완료 이벤트 발행
  - Consumer가 이벤트 수신 후 Discord 알림 전송
- **Discord Alert**
  - 일간 데이터 준비 완료 시 자동 알림
  - 운영 관점의 파이프라인 모니터링 구현

---

## LLM Auto-Reporting

- **LLM 기반 일간 분석 리포트 자동 생성**
- Gold Layer 집계 결과를 구조화된 Prompt로 변환
- 단순 수치 나열이 아닌 **해석 중심 요약 리포트** 생성
- 정형 데이터 → 자연어 인사이트로 변환하는 자동화 흐름 구현

### Report Contents
- 오늘의 한 줄 요약
- 가장 혼잡한 역 / 노선
- 전일 대비 변화 포인트
- 운영 관점 인사이트

---
## Limitations

### API Traffic Limitation
서울 열린데이터 API는 **일일 호출 트래픽 제한**이 존재하여, 도착 정보(arrival), 열차 위치(position)와 같은 데이터는  
짧은 주기로 **부분적인 실시간 수집**은 가능하지만, 지속적이거나 고빈도의 연속 수집에는 제약이 있습니다.
