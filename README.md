# Seoul Subway Daily Reporting

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.3-017CEE)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Event--Driven-231F20)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.7-red)
![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900)
![Docker](https://img.shields.io/badge/Docker-2496ED)
![OpenAI](https://img.shields.io/badge/OpenAI-LLM%20API-412991)

> End-to-end data platform for collecting, processing, and daily reporting of Seoul subway operations using AWS, Spark, Kafka, Airflow, and LLM Auto-Reporting.

---

## Project Overview

본 프로젝트는 **서울 지하철 운영 데이터를 수집 → 정제 → 분석 → 리포트 자동 생성**까지  
전 과정을 아우르는 **End-to-End 데이터 플랫폼**을 구축하는 것을 목표로 합니다.

서울 열린데이터 API 기반의 지하철 데이터를 대상으로 다음을 구현했습니다.

- Spark 기반 Batch 데이터 파이프라인
- Bronze / Silver / Gold 계층 기반 데이터 관리
- Apache Airflow 기반 워크플로우 오케스트레이션
- Kafka 이벤트 기반 알림 구조
- LLM을 활용한 일간 분석 리포트 자동 생성

---

## System Architecture
전체 시스템 구조는 다음과 같습니다.

<img width="600" height="380" alt="image" src="https://github.com/user-attachments/assets/4d173ece-bed6-4e2f-8043-9471ba9a7fa4" />

---

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

<img width="550" height="230" alt="image" src="https://github.com/user-attachments/assets/191f0283-e660-4382-bf25-1b44a644f0a2" />

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

### Data Availability Delay (Usage Data)
서울 지하철 승하차 인원(usage) 데이터는 API 특성상 **실시간 또는 당일 데이터가 제공되지 않으며**,  
일반적으로 **3~4일 이전의 데이터만 조회 가능합니다**.

이에 따라 usage 데이터는 arrival 및 position 데이터와 달리 지연된(Lagged) 데이터 기반으로 수집 및 분석됩니다.

---
## Execution Flow (Overview)

본 프로젝트는 Apache Airflow를 중심으로
데이터 수집, 처리, 리포트 생성 파이프라인이 자동으로 실행됩니다.

### 1. Environment Setup
- Python 3.11
- Docker / Docker Compose
- Apache Airflow
- Apache Kafka
- Apache Spark
- AWS S3 접근 권한 설정

> AWS 자격 증명 및 API Key, Discord Webhook URL 등은 `.env` 파일을 통해 관리됩니다.

### 2. Start Core Services
Docker Compose를 통해 Airflow, Kafka 등 주요 서비스를 실행합니다.

```bash
docker compose up -d
```

### 3. Airflow DAG Execution
본 프로젝트는 여러 Airflow DAG들이
각각의 책임을 가지고 독립적으로 실행되며,
전체적으로는 다음과 같은 흐름을 가집니다.

- **bronze_arrival** : 서울 지하철 도착(arrival) 데이터를 API로 수집하여 Bronze Layer에 저장
- **bronze_position** : 열차 위치(position) 데이터를 주기적으로 수집하여 Bronze Layer에 저장
- **usage_daily** : 일 단위 승하차 인원 데이터를 수집 및 집계하여 분석용 데이터 생성
- **silver_hourly** : Bronze 데이터를 정제하여 시간 단위로 정규화된 Silver Layer 데이터 생성
- **gold_daily** : Silver 데이터를 기반으로 일간 지표 및 집계 결과를 Gold Layer에 생성
- **report_daily** : Gold Layer 집계 결과를 기반으로 일간 분석 리포트를 자동 생성

### 4. Event-driven Alert

Gold Layer 데이터 생성이 완료되면,
Kafka 이벤트가 발행되고 Consumer가 이를 수신하여
Discord 알림을 전송합니다.

### 5. LLM Daily Report

집계된 Gold Layer 데이터를 기반으로
LLM이 자동으로 일간 분석 리포트를 생성합니다.
