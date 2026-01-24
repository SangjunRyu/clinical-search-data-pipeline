# TripClick DAGs 구성

## 개요

기존 `tripclick_daily_pipeline.py`에서 모든 처리 로직을 하나의 DAG으로 관리했으나, 디버깅 및 부분 실행의 어려움으로 인해 기능별로 DAG을 분리하여 구성한다. 각 DAG은 독립적으로 테스트 가능하며, 메인 오케스트레이션 DAG에서 `TriggerDagRunOperator`로 순차 호출하는 구조로 변경한다.


## 디렉터리 구조

```
dags/
├── tripclick_daily_pipeline.py      # (기존) 올인원 DAG - deprecated 예정
│
├── ingestion/
│   └── tripclick_producer_dag.py    # ✅ Kafka Producer
│
├── processing/
│   ├── tripclick_streaming_dag.py   # ✅ Kafka → Silver
│   └── tripclick_batch_dag.py       # ✅ Kafka → Bronze
│
├── mart/
│   ├── tripclick_gold_dag.py        # ✅ Silver → Gold
│   └── tripclick_load_postgres.py   # ✅ Gold → PostgreSQL
│
└── pipeline/
    └── tripclick_main_dag.py        # ✅ 메인 오케스트레이션
```


## DAG 상세 정의

### 1. Ingestion: `tripclick_producer_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_producer` |
| 스케줄 | `None` (수동 실행) |
| Operator | `DockerOperator` |
| 목적 | Kafka Producer 단독 테스트 |

**Task Flow:**
```
start → [producer_server0, producer_server1] → end
```

**필요 설정:**
- Airflow Variables: `KAFKA_BROKERS`, `WEBSERVER_INGESTION_PATH`
- Airflow Connections: `docker_server0`, `docker_server1` (Docker Remote API)


### 2. Processing: `tripclick_streaming_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_streaming_silver` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SparkSubmitOperator` |
| 목적 | Kafka → Silver 스트리밍 처리 |

**Task Flow:**
```
start → streaming_to_silver → end
```

**필요 설정:**
- Airflow Variables: `KAFKA_BROKERS`, `S3_SILVER_PATH`
- Airflow Connections: `spark_cluster`, `aws_s3`


### 3. Processing: `tripclick_batch_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_batch_bronze` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SparkSubmitOperator` |
| 목적 | Kafka → Bronze 배치 처리 |

**Task Flow:**
```
start → batch_to_bronze → end
```

**필요 설정:**
- Airflow Variables: `KAFKA_BROKERS`, `S3_BRONZE_PATH`
- Airflow Connections: `spark_cluster`, `aws_s3`


### 4. Mart: `tripclick_gold_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_gold_etl` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SparkSubmitOperator` |
| 목적 | Silver → Gold 집계 처리 |

**Task Flow:**
```
start → etl_to_gold → end
```

**필요 설정:**
- Airflow Variables: `S3_SILVER_PATH`, `S3_GOLD_PATH`
- Airflow Connections: `spark_cluster`, `aws_s3`


### 5. Mart: `tripclick_load_postgres.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_load_postgres` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SparkSubmitOperator` |
| 목적 | Gold → PostgreSQL 적재 |

**Task Flow:**
```
start → load_to_postgres → end
```

**필요 설정:**
- Airflow Variables: `S3_GOLD_PATH`
- Airflow Connections: `spark_cluster`, `postgres_gold`


### 6. Pipeline: `tripclick_main_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_daily_pipeline` |
| 스케줄 | `0 15 * * *` (KST 00:00) |
| Operator | `TriggerDagRunOperator` |
| 목적 | 전체 파이프라인 오케스트레이션 |

**Task Flow:**
```
start
  → trigger_producer
  → trigger_streaming
  → trigger_batch
  → trigger_gold
  → trigger_load_postgres
→ end
```

**샘플 코드:**
```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger_producer = TriggerDagRunOperator(
    task_id="trigger_producer",
    trigger_dag_id="tripclick_producer",
    wait_for_completion=True,
    poke_interval=30,
)

trigger_streaming = TriggerDagRunOperator(
    task_id="trigger_streaming",
    trigger_dag_id="tripclick_streaming_silver",
    wait_for_completion=True,
    poke_interval=30,
)

trigger_producer >> trigger_streaming >> ...
```


## Airflow 설정 요약

### Variables

| Key | 설명 | 예시 |
|-----|------|------|
| `KAFKA_BROKERS` | Kafka 브로커 주소 | `kafka1:9092,kafka2:9092` |
| `S3_BRONZE_PATH` | Bronze 레이어 경로 | `s3a://tripclick/bronze` |
| `S3_SILVER_PATH` | Silver 레이어 경로 | `s3a://tripclick/silver` |
| `S3_GOLD_PATH` | Gold 레이어 경로 | `s3a://tripclick/gold` |
| `WEBSERVER_INGESTION_PATH` | 웹서버 ingestion 경로 | `/home/ubuntu/ingestion` |

### Connections

| Conn ID | Type | 설명 |
|---------|------|------|
| `docker_server0` | Docker | 웹서버 0 Docker Remote API (`tcp://10.0.1.x:2375`) |
| `docker_server1` | Docker | 웹서버 1 Docker Remote API (`tcp://10.0.1.x:2375`) |
| `spark_cluster` | Spark | Spark Master (`spark://10.0.2.x:7077`) |
| `aws_s3` | Amazon Web Services | S3 접근용 IAM 자격증명 |
| `postgres_gold` | Postgres | Gold 레이어 PostgreSQL |


## 테스트 순서

1. **Airflow 기동 확인**: Webserver, Scheduler, Worker 정상 동작 확인
2. **Variables/Connections 설정**: UI 또는 CLI로 등록
3. **DAG 단위 테스트**: 아래 순서로 수동 실행
   ```
   tripclick_producer → tripclick_streaming_silver → tripclick_batch_bronze
   → tripclick_gold_etl → tripclick_load_postgres
   ```
4. **메인 DAG 테스트**: `tripclick_daily_pipeline` 수동 실행
5. **스케줄 실행 검증**: 스케줄 활성화 후 모니터링


## 진행 상황

| DAG | 상태 | 비고 |
|-----|------|------|
| `tripclick_producer` | ✅ 완료 | `ingestion/tripclick_producer_dag.py` |
| `tripclick_streaming_silver` | ✅ 완료 | `processing/tripclick_streaming_dag.py` |
| `tripclick_batch_bronze` | ✅ 완료 | `processing/tripclick_batch_dag.py` |
| `tripclick_gold_etl` | ✅ 완료 | `mart/tripclick_gold_dag.py` |
| `tripclick_load_postgres` | ✅ 완료 | `mart/tripclick_load_postgres.py` |
| `tripclick_daily_pipeline` | ✅ 완료 | `pipeline/tripclick_main_dag.py` (TriggerDagRunOperator 사용) |
