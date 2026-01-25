# Infrastructure - Technical Architecture

TripClick 임상 데이터 파이프라인 인프라 기술 아키텍처 문서

---

## 전체 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              AWS VPC (ap-northeast-2)                           │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                         Public Subnet (10.0.1.0/24)                     │   │
│  │                                                                          │   │
│  │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                 │   │
│  │   │  WebServer  │    │  WebServer  │    │   Airflow   │                 │   │
│  │   │  (server0)  │    │  (server1)  │    │   Server    │                 │   │
│  │   │  m5.large   │    │  m5.large   │    │  m5.large   │                 │   │
│  │   │  :2375 TCP  │    │  :2375 TCP  │    │  :8080 HTTP │                 │   │
│  │   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                 │   │
│  │          │                  │                  │                         │   │
│  └──────────┼──────────────────┼──────────────────┼─────────────────────────┘   │
│             │                  │                  │                             │
│             │   DockerOperator │   DockerOperator │                             │
│             │   (tcp:2375)     │   (tcp:2375)     │                             │
│             │                  │                  │                             │
│  ┌──────────┼──────────────────┼──────────────────┼─────────────────────────┐   │
│  │          ▼                  ▼                  │      Private Subnet     │   │
│  │   ┌─────────────────────────────────────┐     │      (10.0.2.0/24)      │   │
│  │   │         Kafka Cluster               │     │                          │   │
│  │   │  ┌─────────┐┌─────────┐┌─────────┐  │     │                          │   │
│  │   │  │kafka-1  ││kafka-2  ││kafka-3  │  │     │                          │   │
│  │   │  │t3.large ││         ││         │  │     │                          │   │
│  │   │  │:9092    ││:9093    ││:9094    │  │     │                          │   │
│  │   │  └─────────┘└─────────┘└─────────┘  │     │                          │   │
│  │   └─────────────────────────────────────┘     │                          │   │
│  │                      │                        │                          │   │
│  │                      ▼                        │                          │   │
│  │   ┌─────────────────────────────────────┐     │                          │   │
│  │   │         Spark Cluster               │     │                          │   │
│  │   │  ┌─────────┐  ┌─────────────────┐   │     │                          │   │
│  │   │  │ Master  │  │    Workers      │   │     │                          │   │
│  │   │  │m5.large │  │ m5.large x 2    │   │     │                          │   │
│  │   │  │:7077    │  │                 │   │     │                          │   │
│  │   │  └─────────┘  └─────────────────┘   │     │                          │   │
│  │   └─────────────────────────────────────┘     │                          │   │
│  │                      │                        │                          │   │
│  │                      ▼                        │                          │   │
│  │   ┌─────────────────────────────────────┐     │                          │   │
│  │   │    Gold Layer (Data Mart)           │     │                          │   │
│  │   │  ┌─────────┐  ┌─────────┐           │     │                          │   │
│  │   │  │PostgreSQL│  │ Superset│           │     │                          │   │
│  │   │  │t3.small │  │         │           │     │                          │   │
│  │   │  │:5432    │  │:8088    │           │     │                          │   │
│  │   │  └─────────┘  └─────────┘           │     │                          │   │
│  │   └─────────────────────────────────────┘     │                          │   │
│  │                                               │                          │   │
│  └───────────────────────────────────────────────┘                          │   │
│                                                                             │   │
│                         ┌─────────────────┐                                 │   │
│                         │    S3 Bucket    │                                 │   │
│                         │ tripclick-lake  │                                 │   │
│                         │ /bronze/silver/ │                                 │   │
│                         │ /gold/          │                                 │   │
│                         └─────────────────┘                                 │   │
│                                                                             │   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 데이터 파이프라인 흐름

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATION                               │
│                    (Airflow on Separate Server)                     │
│                                                                     │
│  DAGs:                                                              │
│  ├─ tripclick_producer_realtime / batch                             │
│  ├─ tripclick_streaming_silver                                      │
│  ├─ tripclick_batch_bronze                                          │
│  ├─ tripclick_gold_etl                                              │
│  ├─ tripclick_load_postgres                                         │
│  └─ tripclick_daily_pipeline (Main Orchestrator)                    │
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
                    │    Replication: 2     │
                    │    Partitions: 3      │
                    └───────────┬───────────┘
                                │
            ┌───────────────────┼───────────────────┐
            ▼                                       ▼
    ┌───────────────┐                       ┌───────────────┐
    │ Spark Batch   │                       │Spark Streaming│
    │ → S3 Bronze   │                       │ → S3 Silver   │
    │ (전체 적재)   │                       │ (dedup 처리)  │
    └───────┬───────┘                       └───────┬───────┘
            │                                       │
            └───────────────────┬───────────────────┘
                                ▼
                    ┌───────────────────────┐
                    │     Spark ETL         │
                    │  Silver → PostgreSQL  │
                    │  (집계/마트 테이블)   │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │     PostgreSQL        │
                    │     (Gold Mart)       │
                    │  - mart_session       │
                    │  - mart_daily_traffic │
                    │  - mart_clinical_areas│
                    │  - mart_popular_docs  │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   Apache Superset     │
                    │   (Dashboard)         │
                    └───────────────────────┘
```

---

## 레이어별 기술 구성

### 1. Ingestion Layer

| 항목 | 내용 |
|------|------|
| 역할 | 원천 데이터 수집 및 Kafka 전송 |
| 기술 | Python Kafka Producer |
| 실행 | DockerOperator (Remote Docker API) |
| 모드 | realtime (스트리밍 시뮬레이션), batch (백필) |

**Producer 구성:**
- `producer_base.py`: 공통 유틸리티 (config, dedup, kafka)
- `producer_realtime.py`: event_ts 기반 실시간 전송
- `producer_batch.py`: 대기 없이 즉시 전송

**Dedup Key 생성:**
```
xxhash64(session_id | document_id | event_ts)
```

### 2. Messaging Layer

| 항목 | 내용 |
|------|------|
| 플랫폼 | Apache Kafka (Confluent 7.5.1) |
| 브로커 | 3개 (9092, 9093, 9094) |
| Zookeeper | 1개 (:2181) |
| 모니터링 | Kafka UI (:8080) |

**토픽 설정:**
| 토픽 | 파티션 | Replication | 용도 |
|------|--------|-------------|------|
| tripclick_raw_logs | 3 | 2 | 원시 로그 |

**Dual Listener 구성:**
- INTERNAL (kafka-N:29092): 브로커 간 통신
- EXTERNAL (0.0.0.0:909X): 외부 클라이언트 접속

### 3. Processing Layer

| 항목 | 내용 |
|------|------|
| 엔진 | Apache Spark 3.4.1 |
| 구성 | Master 1, Worker 2 |
| 출력 | S3 Bronze/Silver (Parquet) |

**Spark Jobs:**
| Job | 입력 | 출력 | 처리 |
|-----|------|------|------|
| streaming_to_silver.py | Kafka Stream | S3 Silver | Watermark + dedup_key 중복 제거 |
| batch_to_bronze.py | Kafka Batch | S3 Bronze | 전체 데이터 적재 (메타데이터 포함) |
| etl_to_gold.py | S3 Silver | PostgreSQL | 집계/변환 후 마트 테이블 적재 |
| load_to_postgres.py | S3 Silver | PostgreSQL | JDBC를 통한 마트 테이블 적재 |

### 4. Orchestration Layer

| 항목 | 내용 |
|------|------|
| 플랫폼 | Apache Airflow 2.10.5 |
| Executor | CeleryExecutor |
| 위치 | 별도 EC2 인스턴스 |

**DAG 구성:**
| DAG ID | Operator | 목적 |
|--------|----------|------|
| tripclick_producer_realtime | DockerOperator | 실시간 스트리밍 시뮬레이션 |
| tripclick_producer_batch | DockerOperator | 백필/일괄 전송 |
| tripclick_streaming_silver | SSHOperator | Kafka → Silver |
| tripclick_batch_bronze | SSHOperator | Kafka → Bronze |
| tripclick_gold_etl | SSHOperator | Silver → Gold 마트 생성 |
| tripclick_load_postgres | SSHOperator | Gold → PostgreSQL 적재 |
| tripclick_daily_pipeline | TriggerDagRunOperator | 메인 오케스트레이션 |

**SSHOperator 선택 이유:**

SparkSubmitOperator의 Client Mode 네트워크 문제로 인해 SSHOperator로 전환:

| 문제 상황 | 설명 |
|----------|------|
| 환경 | Airflow와 Spark가 각각 다른 서버의 Docker 컨테이너에서 실행 |
| Client Mode 이슈 | Driver가 Airflow 컨테이너에서 실행되어 Worker들이 내부 IP(172.x.x.x)로 통신 시도 |
| 근본 원인 | 서로 다른 Docker Bridge Network 간 직접 통신 불가 |

| 검토 대안 | 결론 |
|----------|------|
| spark.driver.host 설정 | 호스트 IP 지정 + 포트 매핑 필요, 설정 복잡 |
| Host Network Mode | 양쪽 모두 변경 필요, 기존 환경 영향 큼 |
| **SSHOperator** | Spark 서버에서 직접 실행하므로 네트워크 문제 회피 ✅ |

```
┌─────────────────────┐         SSH          ┌─────────────────────┐
│   Airflow Server    │ ───────────────────▶ │   Spark Server      │
│                     │                       │                     │
│  SSHOperator        │                       │  docker exec        │
│  (spark_ssh conn)   │                       │  spark-master       │
│                     │                       │  spark-submit ...   │
└─────────────────────┘                       └─────────────────────┘
```

> **프로덕션 개선 방향**: Spark on Kubernetes + KubernetesPodOperator 권장

### 5. Gold Layer (Data Mart & BI)

| 항목 | 내용 |
|------|------|
| 데이터베이스 | PostgreSQL 15 |
| 시각화 | Apache Superset 3.1.0 |
| 네트워크 | gold-network (Docker) |

**데이터 마트:**
| 마트 | 설명 |
|------|------|
| mart_session_analysis | 세션별 행동 분석 |
| mart_daily_traffic | 일별 트래픽 현황 |
| mart_clinical_areas | 임상 분야별 관심도 |
| mart_popular_documents | 인기 문서 순위 |

---

## EC2 Instance Types

대부분의 OS는 **Ubuntu 22.04 LTS** 사용

### Ingestion Layer

| 서버 | 인스턴스 | vCPU | Memory | 용도 | 비고 |
|------|----------|------|--------|------|------|
| WebServer (server0) | m5.large | 2 | 8GB | Kafka Producer 실행 | CPU 크레딧 이슈로 t타입 대신 m타입 사용 |
| WebServer (server1) | m5.large | 2 | 8GB | Kafka Producer 실행 | 동일 |

### Messaging Layer

| 서버 | 인스턴스 | vCPU | Memory | 용도 | 비고 |
|------|----------|------|--------|------|------|
| Kafka Cluster | t3.large | 2 | 8GB | Kafka 3 Brokers + Zookeeper | 단일 노드에 Docker Compose |
| (Zookeeper 1GB + Broker 2GB x 3 = 7GB) |

### Processing Layer

| 서버 | 인스턴스 | vCPU | Memory | 용도 | 비고 |
|------|----------|------|--------|------|------|
| Spark Cluster | m5.large | 2 | 8GB | Master + Workers | 단일 노드 Docker Compose |

### Orchestration Layer

| 서버 | 인스턴스 | vCPU | Memory | EBS | 비고 |
|------|----------|------|--------|-----|------|
| Airflow Server | m5.large | 2 | 8GB | 16GB | EBS 8GB→16GB 증설 (space 부족) |

### Gold Layer

| 서버 | 인스턴스 | vCPU | Memory | 용도 | 비고 |
|------|----------|------|--------|------|------|
| Gold Server | t3.small | 2 | 2GB | PostgreSQL + Superset | 단일 노드 |

### Storage

| 서비스 | 용도 | 경로 |
|--------|------|------|
| S3 (tripclick-lake) | 데이터 레이크 | /bronze, /silver |

---

## Network Configuration

### VPC

| 항목 | 값 | 비고 |
|------|-----|------|
| VPC CIDR | 10.0.0.0/16 | |
| Public Subnet | 10.0.1.0/24 | WebServer, Airflow |
| Private Subnet | 10.0.2.0/24 | Kafka, Spark, PostgreSQL |

### Security Groups

#### sg-webserver (WebServer)

| 방향 | 포트 | 소스 | 용도 |
|------|------|------|------|
| Inbound | 22 | My IP | SSH 접속 |
| Inbound | 2375 | sg-airflow | Remote Docker API |
| Outbound | 9092-9094 | sg-kafka | Kafka 연결 |
| Outbound | 443 | 0.0.0.0/0 | HTTPS (AWS API 등) |

#### sg-kafka (Kafka Cluster)

| 방향 | 포트 | 소스 | 용도 |
|------|------|------|------|
| Inbound | 22 | My IP | SSH 접속 |
| Inbound | 2181 | sg-kafka | Zookeeper |
| Inbound | 9092-9094 | sg-webserver, sg-spark | Kafka Broker |
| Inbound | 29092-29094 | sg-kafka | Internal Broker 통신 |
| Inbound | 8080 | My IP | Kafka UI |

#### sg-spark (Spark Cluster)

| 방향 | 포트 | 소스 | 용도 |
|------|------|------|------|
| Inbound | 22 | My IP, sg-airflow | SSH 접속 (관리자 + SSHOperator) |
| Inbound | 7077 | sg-airflow | Spark Master |
| Inbound | 8080, 8181 | My IP | Spark UI |
| Inbound | 4040 | My IP | Spark Job UI |
| Outbound | 9092-9094 | sg-kafka | Kafka 연결 |
| Outbound | 443 | 0.0.0.0/0 | S3 접근 |
| Outbound | 5432 | sg-gold | PostgreSQL 연결 |

#### sg-airflow (Airflow Server)

| 방향 | 포트 | 소스 | 용도 |
|------|------|------|------|
| Inbound | 22 | My IP | SSH 접속 |
| Inbound | 8080 | My IP | Airflow Web UI |
| Outbound | 22 | sg-spark | SSH to Spark (SSHOperator) |
| Outbound | 2375 | sg-webserver | Docker API |

#### sg-gold (PostgreSQL, Superset)

| 방향 | 포트 | 소스 | 용도 |
|------|------|------|------|
| Inbound | 22 | My IP | SSH 접속 |
| Inbound | 5432 | sg-spark | PostgreSQL (Spark 적재) |
| Inbound | 8088 | My IP | Superset Web UI |

---

## Port Summary

| 서비스 | 포트 | 프로토콜 | 용도 |
|--------|------|----------|------|
| SSH | 22 | TCP | 원격 접속 |
| Docker API | 2375 | TCP | Remote Docker (비보안) |
| Zookeeper | 2181 | TCP | Kafka 코디네이션 |
| Kafka Broker | 9092-9094 | TCP | 외부 클라이언트 |
| Kafka Internal | 29092-29094 | TCP | 브로커 간 통신 |
| Spark Master | 7077 | TCP | 작업 제출 |
| Spark UI | 8080, 8181 | HTTP | 웹 UI |
| Kafka UI | 8080 | HTTP | 웹 UI |
| Airflow | 8080 | HTTP | 웹 UI |
| Superset | 8088 | HTTP | 웹 UI |
| PostgreSQL | 5432 | TCP | 데이터베이스 |

---

## 데이터 레이어 정의

| 레이어 | 저장소 | 경로/테이블 | 설명 |
|--------|--------|-------------|------|
| **Bronze** | S3 | `s3://tripclick-lake/bronze/event_date=YYYY-MM-DD/` | 원시 데이터, Kafka 메타데이터 포함, 중복 허용 |
| **Silver** | S3 | `s3://tripclick-lake/silver/event_date=YYYY-MM-DD/` | 정제 데이터, dedup_key 기반 중복 제거 |
| **Gold** | PostgreSQL | `mart_*` 테이블 | 분석 마트, Superset 대시보드 연동 |

> **Note**: Gold 레이어는 S3가 아닌 PostgreSQL 테이블로 직접 적재합니다. BI 도구(Superset) 연동 및 실시간 쿼리 성능을 위해 RDB를 선택했습니다.

---

## 일일 실행 흐름

```
15:00 ─┬─▶ Producer 시작 (server0, server1) - 실시간 전송
       └─▶ Spark Streaming 시작 - Kafka → Silver (1시간)
              ↓
16:00 ─────▶ Producer/Streaming 종료
              ↓
17:00 ─────▶ Spark Batch - Kafka 전체 → Bronze
              ↓
18:00 ─────▶ Spark ETL - Silver → PostgreSQL (마트 테이블 적재)
              ↓
       ─────▶ 완료 (Superset 대시보드 갱신)
```

---

## Airflow 설정 요약

### Variables

| Key | 설명 | 예시 |
|-----|------|------|
| `KAFKA_BROKERS` | Kafka 브로커 주소 | `10.0.1.10:9092,10.0.1.10:9093,10.0.1.10:9094` |
| `S3_BRONZE_PATH` | Bronze 레이어 경로 | `s3a://tripclick-lake/bronze/` |
| `S3_SILVER_PATH` | Silver 레이어 경로 | `s3a://tripclick-lake/silver/` |
| `S3_GOLD_PATH` | Gold 레이어 경로 | `s3a://tripclick-lake/gold/` |
| `WEBSERVER_INGESTION_PATH` | 웹서버 홈 경로 | `/home/ubuntu` |

### Connections

| Conn ID | Type | 설명 |
|---------|------|------|
| `docker_server0` | Docker | WebServer 0 Docker Remote API |
| `docker_server1` | Docker | WebServer 1 Docker Remote API |
| `spark_ssh` | SSH | Spark 서버 SSH 연결 (SSHOperator용) |
| `aws_s3` | AWS | S3 접근용 IAM 자격증명 |
| `postgres_gold` | Postgres | Gold 레이어 PostgreSQL |

---

## 서비스 접속 정보

| 서비스 | URL | 계정 |
|--------|-----|------|
| Kafka UI | http://{KAFKA_IP}:8080 | - |
| Spark UI | http://{SPARK_IP}:8181 | - |
| Airflow | http://{AIRFLOW_IP}:8080 | admin/admin |
| Superset | http://{GOLD_IP}:8088 | admin/admin |

---

## 배포 명령어

```bash
# 1. Kafka 클러스터
cd messaging
KAFKA_HOST=<KAFKA_IP> docker-compose -f kafka-compose.yaml up -d

# 2. Spark 클러스터
cd processing
docker-compose -f spark-compose.yaml up -d

# 3. PostgreSQL + Superset (Gold Layer)
cd gold
docker-compose up -d

# 4. Airflow (Orchestration)
cd orchestration
sudo chown -R 50000:0 logs dags plugins config
docker compose up -d
```

---

## 보안 고려사항

| 항목 | 현재 상태 | 개선 필요 |
|------|----------|----------|
| Docker API | tcp://0.0.0.0:2375 (비보안) | TLS 인증 설정 |
| Kafka | PLAINTEXT | SASL/SSL 설정 |
| Airflow Fernet Key | 기본값 | 커스텀 키 설정 |
| Connection 정보 | Airflow DB | AWS Secrets Manager |

---

## TODO

- [ ] Terraform/CloudFormation 템플릿 작성
- [ ] 비용 추정 (월간)
- [ ] 백업/복구 전략
- [ ] Docker TLS 인증 설정
- [ ] CI/CD 파이프라인 (GitHub Actions)
- [ ] 모니터링 (Prometheus + Grafana)
- [ ] 알림 설정 (Slack/Email)
- [ ] 데이터 품질 검증 (Great Expectations)