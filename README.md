# Clinical Search Data Pipeline

TripClick 임상 데이터 검색 로그를 처리하는 엔드-투-엔드 데이터 파이프라인

## 프로젝트 개요

| 항목 | 내용 |
|------|------|
| 데이터 소스 | TripClick 임상 검색 로그 (JSON) |
| 파이프라인 | Kafka → Spark → S3 (Bronze/Silver/Gold) |
| 아키텍처 | Lambda Architecture (배치 + 스트림) |
| 인프라 | Docker Compose 기반 |
| 시각화 | Apache Superset |

---

## 기술 스택

| 구분 | 기술 | 버전 |
|------|------|------|
| 메시징 | Apache Kafka | 7.5.1 (Confluent) |
| 처리 | Apache Spark | 3.4.1 |
| 오케스트레이션 | Apache Airflow | 2.10.5 |
| 저장소 | AWS S3 | - |
| 데이터베이스 | PostgreSQL | 15 |
| 시각화 | Apache Superset | 3.1.0 |
| 컨테이너 | Docker Compose | 3.8 |

---

## 디렉터리 구조

```
clinical-search-data-pipeline/
│
├── ingestion/                    # 데이터 수집 레이어
│   ├── ingestion.md              # Ingestion 상세 문서
│   ├── producer/                 # Kafka Producer
│   ├── config/                   # 설정 파일
│   ├── utils/                    # 유틸리티
│   ├── data/                     # 로그 데이터 (server0, server1)
│   └── sample_data/              # 샘플 데이터
│
├── messaging/                    # 메시징 인프라 레이어
│   ├── messaging.md              # Messaging 상세 문서
│   └── kafka-compose.yaml        # Kafka 클러스터 구성
│
├── processing/                   # 데이터 처리 레이어
│   ├── processing.md             # Processing 상세 문서
│   ├── spark/                    # Spark 작업
│   │   └── jobs/
│   │       ├── streaming_to_silver.py   # 실시간 → Silver
│   │       ├── batch_to_bronze.py       # 배치 → Bronze
│   │       ├── etl_to_gold.py           # Silver → Gold 마트
│   │       ├── load_to_postgres.py      # Gold → PostgreSQL
│   │       └── consumer_batch.py        # 분석용 (테스트)
│   └── spark-compose.yaml        # Spark 클러스터
│
├── orchestration/                # 오케스트레이션 레이어 (별도 서버)
│   ├── orchestration.md          # Orchestration 상세 문서
│   ├── dags/                     # Airflow DAG
│   │   └── tripclick_daily_pipeline.py
│   ├── docker-compose.yaml       # Airflow 클러스터
│   ├── Dockerfile
│   └── requirements.txt
│
├── gold/                         # Gold Mart 레이어 (서빙)
│   ├── gold.md                   # Gold 상세 문서
│   ├── docker-compose.yaml       # PostgreSQL + Superset 통합 구성
│   ├── postgres/                 # PostgreSQL 설정
│   │   └── init/                 # 초기화 SQL
│   └── superset/                 # Superset 설정
│       └── superset_config.py
│
└── infrastructure/               # Technical Architecture
    └── infrastructure.md         # 인프라 기술 아키텍처 문서
```

---

## 전체 파이프라인 흐름

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATION                               │
│                    (Airflow on Separate Server)                     │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│  Web Server 0 │       │  Web Server 1 │       │ Spark Cluster │
│  (Producer)   │       │  (Producer)   │       │               │
└───────┬───────┘       └───────┬───────┘       └───────┬───────┘
        │                       │                       │
        └───────────────────────┼───────────────────────┘
                                ▼
                    ┌───────────────────────┐
                    │    Kafka Cluster      │
                    │    (3 Brokers)        │
                    └───────────┬───────────┘
                                │
            ┌───────────────────┼───────────────────┐
            ▼                                       ▼
    ┌───────────────┐                       ┌───────────────┐
    │ Spark Batch   │                       │Spark Streaming│
    │ → S3 Bronze   │                       │ → S3 Silver   │
    └───────┬───────┘                       └───────┬───────┘
            │                                       │
            └───────────────────┬───────────────────┘
                                ▼
                    ┌───────────────────────┐
                    │     Spark ETL         │
                    │     → S3 Gold         │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │     PostgreSQL        │
                    │     (Gold Mart)       │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   Apache Superset     │
                    │   (Dashboard)         │
                    └───────────────────────┘
```

---

## 레이어별 상세 문서

| 레이어 | 문서 | 설명 |
|--------|------|------|
| Ingestion | [ingestion/ingestion.md](ingestion/ingestion.md) | Kafka Producer, 실시간 전송, dedup 키 |
| Messaging | [messaging/messaging.md](messaging/messaging.md) | Kafka 클러스터, 브로커 구성, 리스너 설정 |
| Processing | [processing/processing.md](processing/processing.md) | Spark Streaming/Batch, Bronze/Silver |
| Orchestration | [orchestration/orchestration.md](orchestration/orchestration.md) | Airflow DAG, Remote Docker API |
| Gold | [gold/gold.md](gold/gold.md) | 데이터 마트, PostgreSQL, Superset |
| Infrastructure | [infrastructure/infrastructure.md](infrastructure/infrastructure.md) | Technical Architecture, EC2, Network |

---

## 데이터 레이어 정의

| 레이어 | 경로 | 설명 |
|--------|------|------|
| **Bronze** | `s3://tripclick-lake/bronze/` | 원시 데이터, Kafka 메타데이터 포함, 중복 허용 |
| **Silver** | `s3://tripclick-lake/silver/` | 정제 데이터, dedup_key 기반 중복 제거 |
| **Gold** | `s3://tripclick-lake/gold/` | 분석 마트, PostgreSQL + Superset 연동 |

---

## 일일 실행 흐름

```
15:00 ─┬─▶ Producer 시작 (server0, server1) - 실시간 전송
       └─▶ Spark Streaming 시작 - Kafka → Silver
              ↓
16:00 ─────▶ Producer/Streaming 종료
              ↓
17:00 ─────▶ Spark Batch - Kafka 전체 → Bronze
              ↓
18:00 ─────▶ Spark ETL - Silver → Gold → PostgreSQL
              ↓
       ─────▶ 완료
```

---

## 빠른 시작

```bash
# 1. Kafka 클러스터
docker-compose -f messaging/kafka-compose.yaml up -d

# 2. Spark 클러스터
docker-compose -f processing/spark-compose.yaml up -d

# 3. PostgreSQL + Superset (통합)
docker-compose -f gold/docker-compose.yaml up -d

# 4. Airflow (Orchestration)
docker-compose -f orchestration/docker-compose.yaml up -d
```

| 서비스 | URL | 계정 |
|--------|-----|------|
| Kafka UI | http://localhost:8080 | - |
| Airflow | http://localhost:8080 | admin/admin |
| Superset | http://localhost:8088 | admin/admin |

---

## 설계 원칙

| 원칙 | 설명 |
|------|------|
| 역할 기반 모듈화 | 물리 서버가 아닌 역할로 디렉토리 분리 |
| At-Least-Once | dedup_key로 중복 제거 보장 |
| Lambda Architecture | 배치 + 스트림 동시 처리 |
| 환경 분리 | 설정 파일 Git 제외, 환경변수 오버라이드 |

---

## TODO

- [x] infrastructure/ Technical Architecture 문서 작성
- [ ] Terraform/CloudFormation 템플릿 작성
- [ ] CI/CD 파이프라인 (GitHub Actions)
- [ ] 모니터링 (Prometheus + Grafana)
- [ ] 알림 설정 (Slack/Email)
- [ ] 데이터 품질 검증 (Great Expectations)

---

## 라이선스

MIT License
