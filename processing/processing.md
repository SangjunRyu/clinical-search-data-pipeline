## 테스트 가이드

### 1단계: Spark 클러스터 실행

```bash
# processing 디렉토리로 이동
cd processing

# config 파일 생성
cp spark/config/config.yaml.example spark/config/config.yaml
# config.yaml에서 KAFKA_BROKERS IP 수정

# 환경변수 설정
export KAFKA_BROKERS="<KAFKA_IP>:9092,<KAFKA_IP>:9093,<KAFKA_IP>:9094"
export S3_BRONZE_PATH="s3a://tripclick-lake/bronze/"
export S3_SILVER_PATH="s3a://tripclick-lake/silver/"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"

# Spark 클러스터 시작
docker-compose -f spark-compose.yaml up -d

# Spark UI 확인: http://localhost:8181
```

### 2단계: Batch Job 테스트 (Kafka → Bronze)

```bash
# Spark Master 컨테이너에서 직접 실행
docker exec -it spark-master spark-submit \
  --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
  --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /opt/spark/jobs/batch_to_bronze.py
```

### 3단계: Streaming Job 테스트 (Kafka → Silver)

```bash
# Producer 실행 (다른 터미널에서)
# ingestion 서버에서 producer 실행 필요

# Streaming Job 실행 (1시간 동안 실행)
docker exec -it spark-master spark-submit \
  --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
  --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /opt/spark/jobs/streaming_to_silver.py
```

### 4단계: Airflow 연동 테스트

1. **Airflow Connection 설정**:
```bash
# Spark Connection 추가
airflow connections add spark_cluster \
  --conn-type spark \
  --conn-host spark://<SPARK_MASTER_IP> \
  --conn-port 7077

# AWS S3 Connection 추가
airflow connections add aws_s3 \
  --conn-type aws \
  --conn-login $AWS_ACCESS_KEY_ID \
  --conn-password $AWS_SECRET_ACCESS_KEY
```

2. **Airflow Variables 설정**:
```bash
airflow variables set KAFKA_BROKERS "<KAFKA_IP>:9092"
airflow variables set S3_BRONZE_PATH "s3a://tripclick-lake/bronze/"
airflow variables set S3_SILVER_PATH "s3a://tripclick-lake/silver/"
```

3. **DAG 실행**:
- `tripclick_batch_bronze`: Batch → Bronze 테스트
- `tripclick_streaming_silver`: Streaming → Silver 테스트

---




# Processing Layer

Kafka 데이터를 처리하여 S3 Data Lake에 저장하는 레이어

## 개요

| 항목 | 내용 |
|------|------|
| 입력 | Kafka 토픽 (`tripclick_raw_logs`) |
| 출력 | S3 Bronze / Silver Lake |
| 처리 엔진 | Apache Spark 3.4.1 |
| 오케스트레이션 | Apache Airflow (별도 서버) |

---

## 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                    PROCESSING LAYER                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐     ┌──────────────────────────────────────┐  │
│  │   Kafka     │────▶│  Spark Streaming (1시간)             │  │
│  │   Topic     │     │  - dedup (watermark + dedup_key)     │  │
│  │             │     │  - S3 Silver Layer                   │  │
│  └─────────────┘     └──────────────────────────────────────┘  │
│         │                                                       │
│         │            ┌──────────────────────────────────────┐  │
│         └───────────▶│  Spark Batch (Daily)                 │  │
│                      │  - 전체 데이터 적재                   │  │
│                      │  - S3 Bronze Layer                   │  │
│                      └──────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      S3 DATA LAKE                               │
├─────────────────────────────────────────────────────────────────┤
│  Bronze: s3://tripclick-lake/bronze/event_date=YYYY-MM-DD/     │
│  Silver: s3://tripclick-lake/silver/event_date=YYYY-MM-DD/     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 디렉터리 구조

```
processing/
├── spark/
│   ├── jobs/
│   │   ├── streaming_to_silver.py   # 실시간 → Silver
│   │   ├── batch_to_bronze.py       # 배치 → Bronze
│   │   └── consumer_batch.py        # 분석용 (테스트)
│   ├── config/
│   │   └── config.yaml              # Spark 설정
│   ├── jars/                        # Kafka/S3 커넥터
│   └── Dockerfile
└── spark-compose.yaml               # Spark 클러스터
```

---

## Spark Jobs

### 1. streaming_to_silver.py (실시간)

Kafka 스트림을 읽어 dedup 처리 후 S3 Silver에 저장

| 항목 | 내용 |
|------|------|
| 실행 시간 | 1시간 (producer와 동시 실행) |
| 중복 제거 | Watermark(10분) + dedup_key |
| 출력 형식 | Parquet |
| 파티션 | event_date |

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  /opt/spark/jobs/streaming_to_silver.py
```

#### 핵심 로직
```python
# Watermark + Dedup
deduped_stream = (
    parsed_stream
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["dedup_key"])
)

# 1시간 실행 후 종료
query.awaitTermination(timeout=3600)
```

---

### 2. batch_to_bronze.py (일배치)

전체 Kafka 데이터를 배치로 읽어 S3 Bronze에 저장

| 항목 | 내용 |
|------|------|
| 실행 주기 | Daily (17:00 이후) |
| 읽기 범위 | earliest → latest |
| 출력 형식 | Parquet |
| 메타데이터 | Kafka offset, partition, timestamp |

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  /opt/spark/jobs/batch_to_bronze.py
```

---

## 데이터 레이어 정의

### Bronze Layer (원시 데이터)
- **목적**: 원본 데이터 보존 (Data Lineage)
- **특징**: Kafka 메타데이터 포함, 중복 허용
- **경로**: `s3://tripclick-lake/bronze/`

### Silver Layer (정제 데이터)
- **목적**: 분석 가능한 정제 데이터
- **특징**: 중복 제거, 스키마 정규화
- **경로**: `s3://tripclick-lake/silver/`

---

## 설정

### config/config.yaml.example
```yaml
kafka:
  brokers:
    - <KAFKA_BROKER_IP>:9092
    - <KAFKA_BROKER_IP>:9093
    - <KAFKA_BROKER_IP>:9094

s3:
  bronze_path: "s3a://tripclick-lake/bronze/"
  silver_path: "s3a://tripclick-lake/silver/"
```

### 환경변수
```bash
export KAFKA_BROKERS="broker1:9092,broker2:9092"
export S3_BRONZE_PATH="s3a://my-bucket/bronze/"
export S3_SILVER_PATH="s3a://my-bucket/silver/"
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
```

---

## 외부 Airflow 연동

별도 Airflow 서버에서 SparkSubmitOperator로 실행 예정:

```python
# 실시간 Streaming Job
streaming_task = SparkSubmitOperator(
    task_id="streaming_to_silver",
    application="/opt/spark/jobs/streaming_to_silver.py",
    conn_id="spark_remote",
    ...
)

# 배치 Bronze Job
batch_task = SparkSubmitOperator(
    task_id="batch_to_bronze",
    application="/opt/spark/jobs/batch_to_bronze.py",
    conn_id="spark_remote",
    ...
)
```

---

## 실행 순서 (Daily)

```
15:00  Producer 시작 (실시간 전송)
       ↓
15:00  Streaming Job 시작 (1시간)
       ↓
16:00  Producer 종료
16:00  Streaming Job 종료
       ↓
17:00  Batch Job 실행 (Bronze 적재)
```

---

## TODO

- [ ] Spark 클러스터 설정 최적화
- [ ] Checkpoint 외부 스토리지 (S3)로 이동
- [ ] 모니터링 (Spark UI, Prometheus)
- [ ] 실패 시 알림 (Slack/Email)
