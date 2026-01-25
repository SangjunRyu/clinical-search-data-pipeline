# TripClick DAGs 구성

## 개요

기존 `tripclick_daily_pipeline.py`에서 모든 처리 로직을 하나의 DAG으로 관리했으나, 디버깅 및 부분 실행의 어려움으로 인해 기능별로 DAG을 분리하여 구성한다. 각 DAG은 독립적으로 테스트 가능하며, 메인 오케스트레이션 DAG에서 `TriggerDagRunOperator`로 순차 호출하는 구조로 변경한다.


## 디렉터리 구조

```
dags/
├── tripclick_daily_pipeline.py           # (기존) 올인원 DAG - deprecated 예정
│
├── ingestion/
│   ├── tripclick_producer_realtime_dag.py   # ✅ Kafka Producer (실시간)
│   └── tripclick_producer_batch_dag.py      # ✅ Kafka Producer (배치/백필)
│
├── processing/
│   ├── tripclick_streaming_dag.py        # ✅ Kafka → Silver
│   └── tripclick_batch_dag.py            # ✅ Kafka → Bronze
│
├── mart/
│   ├── tripclick_gold_dag.py             # ✅ Silver → Gold
│   └── tripclick_load_postgres.py        # ✅ Gold → PostgreSQL
│
└── pipeline/
    └── tripclick_main_dag.py             # ✅ 메인 오케스트레이션
```


## DAG 상세 정의

### 1-A. Ingestion: `tripclick_producer_realtime_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_producer_realtime` |
| 스케줄 | `None` (수동 실행) |
| Operator | `DockerOperator` |
| 목적 | **실시간 스트리밍 시뮬레이션** |

**특징:**
- event_ts 기준으로 시간 경과에 따라 Kafka 전송
- 스트리밍 처리 테스트용
- 실제 시간 흐름 재현

**Task Flow:**
```
start → producer_server0_realtime → end
```

**필요 설정:**
- Airflow Variables: `KAFKA_BROKERS`, `WEBSERVER_INGESTION_PATH`
- Airflow Connections: `docker_server0` (Docker Remote API)


### 1-B. Ingestion: `tripclick_producer_batch_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_producer_batch` |
| 스케줄 | `None` (수동 실행) |
| Operator | `DockerOperator` |
| 목적 | **과거 데이터 백필 / 일괄 전송** |

**특징:**
- 대기 없이 즉시 전송 (고속 처리)
- 과거 데이터 재처리, 초기 데이터 적재용
- catchup=True로 여러 날짜 백필 가능

**Task Flow:**
```
start → producer_server0_batch → end
```

**필요 설정:**
- Airflow Variables: `KAFKA_BROKERS`, `WEBSERVER_INGESTION_PATH`
- Airflow Connections: `docker_server0` (Docker Remote API)


### 2. Processing: `tripclick_streaming_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_streaming_silver` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SSHOperator` |
| 목적 | Kafka → Silver 스트리밍 처리 |

**특징:**
- SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- 네트워크 문제 없이 안정적인 실행 보장
- 1시간 동안 실시간 데이터 처리 후 종료

**Task Flow:**
```
start → streaming_to_silver → end
```

**필요 설정:**
- Airflow Variables: `KAFKA_BROKERS`, `S3_SILVER_PATH`
- Airflow Connections: `spark_ssh`, `aws_s3`


### 3. Processing: `tripclick_batch_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_batch_bronze` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SSHOperator` |
| 목적 | Kafka → Bronze 배치 처리 |

**특징:**
- SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- 전체 Kafka 데이터를 배치로 적재

**Task Flow:**
```
start → batch_to_bronze → end
```

**필요 설정:**
- Airflow Variables: `KAFKA_BROKERS`, `S3_BRONZE_PATH`
- Airflow Connections: `spark_ssh`, `aws_s3`


### 4. Mart: `tripclick_gold_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_gold_etl` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SSHOperator` |
| 목적 | Silver → Gold 집계 처리 → PostgreSQL 적재 |

**특징:**
- SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- Silver 데이터를 집계하여 PostgreSQL 마트 테이블로 적재

**Task Flow:**
```
start → etl_to_gold → end
```

**필요 설정:**
- Airflow Variables: `S3_SILVER_PATH`
- Airflow Connections: `spark_ssh`, `aws_s3`, `postgres_gold`


### 5. Mart: `tripclick_load_postgres.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_load_postgres` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SSHOperator` |
| 목적 | Silver → PostgreSQL 마트 테이블 적재 |

**특징:**
- SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- 4개 마트 테이블 적재 (세션, 트래픽, 임상분야, 인기문서)

**Task Flow:**
```
start → load_to_postgres → end
```

**필요 설정:**
- Airflow Variables: `S3_SILVER_PATH`
- Airflow Connections: `spark_ssh`, `postgres_gold`


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
  → trigger_producer_batch (또는 realtime)
  → trigger_streaming
  → trigger_batch
  → trigger_gold
  → trigger_load_postgres
→ end
```

**샘플 코드:**
```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# 배치 모드로 데이터 적재 (백필/일괄 처리)
trigger_producer = TriggerDagRunOperator(
    task_id="trigger_producer",
    trigger_dag_id="tripclick_producer_batch",  # 또는 tripclick_producer_realtime
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
| `KAFKA_BROKERS` | Kafka 브로커 주소 | `10.0.1.10:9092` |
| `S3_BRONZE_PATH` | Bronze 레이어 경로 | `s3a://tripclick/bronze` |
| `S3_SILVER_PATH` | Silver 레이어 경로 | `s3a://tripclick/silver` |
| `S3_GOLD_PATH` | Gold 레이어 경로 | `s3a://tripclick/gold` |
| `WEBSERVER_INGESTION_PATH` | 웹서버 홈 경로 | `/home/ubuntu` |

### Connections

| Conn ID | Type | 설명 |
|---------|------|------|
| `docker_server0` | Docker | 웹서버 0 Docker Remote API (`tcp://10.0.0.43:2375`) |
| `spark_ssh` | SSH | Spark 서버 SSH 연결 (SSHOperator용) |
| `aws_s3` | Amazon Web Services | S3 접근용 IAM 자격증명 |
| `postgres_gold` | Postgres | Gold 레이어 PostgreSQL |


## 테스트 순서

1. **Airflow 기동 확인**: Webserver, Scheduler, Worker 정상 동작 확인
2. **Variables/Connections 설정**: UI 또는 CLI로 등록
3. **DAG 단위 테스트**: 아래 순서로 수동 실행
   ```
   # 백필/초기 적재 시
   tripclick_producer_batch → tripclick_streaming_silver → tripclick_batch_bronze
   → tripclick_gold_etl → tripclick_load_postgres

   # 실시간 테스트 시
   tripclick_producer_realtime → tripclick_streaming_silver → ...
   ```
4. **메인 DAG 테스트**: `tripclick_daily_pipeline` 수동 실행
5. **스케줄 실행 검증**: 스케줄 활성화 후 모니터링


## 진행 상황

| DAG | 상태 | 비고 |
|-----|------|------|
| `tripclick_producer_realtime` | ✅ 완료 | `ingestion/tripclick_producer_realtime_dag.py` (DockerOperator) |
| `tripclick_producer_batch` | ✅ 완료 | `ingestion/tripclick_producer_batch_dag.py` (DockerOperator) |
| `tripclick_streaming_silver` | ✅ 완료 | `processing/tripclick_streaming_dag.py` (SSHOperator) |
| `tripclick_batch_bronze` | ✅ 완료 | `processing/tripclick_batch_dag.py` (SSHOperator) |
| `tripclick_gold_etl` | ✅ 완료 | `mart/tripclick_gold_dag.py` (SSHOperator) |
| `tripclick_load_postgres` | ✅ 완료 | `mart/tripclick_load_postgres.py` (SSHOperator) |
| `tripclick_daily_pipeline` | ✅ 완료 | `pipeline/tripclick_main_dag.py` (TriggerDagRunOperator) |

> **Note**: SparkSubmitOperator의 Client Mode 네트워크 문제로 인해 Spark 관련 DAG들은 모두 SSHOperator로 구현했습니다.
