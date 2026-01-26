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
│   ├── tripclick_streaming_curated_dag.py        # ✅ Kafka → Curated Stream
│   └── tripclick_batch_dag.py            # ✅ Kafka → Archive Raw
│
├── mart/
│   ├── tripclick_analytics_mart_dag.py             # ✅ Curated Stream → Analytics Mart (Cold)
│   ├── tripclick_load_postgres.py        # ✅ Analytics Mart → PostgreSQL (Cold)
│   └── tripclick_analytics_mart_realtime_dag.py    # ✅ Curated Stream → PostgreSQL (Hot, Near Real-Time)
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


### 2. Processing: `tripclick_streaming_curated_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_streaming_curated` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SSHOperator` |
| 목적 | Kafka → Curated Stream 스트리밍 처리 |

**특징:**
- SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- 네트워크 문제 없이 안정적인 실행 보장
- 1시간 동안 실시간 데이터 처리 후 종료

**Task Flow:**
```
start → streaming_to_curated_stream → end
```

**필요 설정:**
- Airflow Variables: `KAFKA_BROKERS`, `S3_CURATED_STREAM_PATH`
- Airflow Connections: `spark_ssh`, `aws_s3`


### 3. Processing: `tripclick_batch_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_spark_archive_raw_batch` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SSHOperator` |
| 목적 | Kafka → Archive Raw 배치 처리 |

**특징:**
- SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- 전체 Kafka 데이터를 배치로 적재

**Task Flow:**
```
start → batch_to_archive_raw → end
```

**필요 설정:**
- Airflow Variables: `KAFKA_BROKERS`, `S3_ARCHIVE_RAW_PATH`
- Airflow Connections: `spark_ssh`, `aws_s3`


### 4. Mart (Cold): `tripclick_analytics_mart_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_analytics_mart_etl` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SSHOperator` |
| 목적 | Curated Stream → Analytics Mart 집계 처리 (Cold Analytics Mart, Daily Batch) |
| Analytics Mart 유형 | **Cold** (T+1 정합성) |

**특징:**
- SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- Curated Stream 데이터를 집계하여 S3 Analytics Mart 레이어로 적재
- 일배치 Full Recompute로 최종 정합성 보장

**Task Flow:**
```
start → etl_to_analytics_mart → end
```

**필요 설정:**
- Airflow Variables: `S3_CURATED_STREAM_PATH`, `S3_ANALYTICS_MART_PATH`
- Airflow Connections: `spark_ssh`, `aws_s3`


### 5. Mart (Cold): `tripclick_load_postgres.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_load_postgres` |
| 스케줄 | `None` (수동 실행) |
| Operator | `SSHOperator` |
| 목적 | S3 Analytics Mart → PostgreSQL 마트 테이블 적재 (Cold Analytics Mart) |
| Analytics Mart 유형 | **Cold** (T+1 정합성) |

**특징:**
- SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- 4개 Cold 마트 테이블 적재 (세션, 트래픽, 임상분야, 인기문서)
- mode("overwrite")로 전체 갱신

**Task Flow:**
```
start → load_to_postgres → end
```

**필요 설정:**
- Airflow Variables: `S3_ANALYTICS_MART_PATH`
- Airflow Connections: `spark_ssh`, `postgres_gold`


### 6. Mart (Hot): `tripclick_analytics_mart_realtime_dag.py` ✅ **NEW**

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_analytics_mart_realtime` |
| 스케줄 | `None` (수동 실행, 장기 실행) |
| Operator | `SSHOperator` |
| 목적 | Curated Stream → PostgreSQL 실시간 마트 적재 (Hot Analytics Mart) |
| Analytics Mart 유형 | **Hot** (1~5분 Near Real-Time) |

**특징:**
- Spark Structured Streaming으로 Curated Stream을 마이크로배치 처리
- 1~5분 주기로 4개 Hot 마트 동시 적재
- 1시간 실행 후 자동 종료 (지속 운영 시 재시작)

**Hot Analytics Mart 마트:**

| 테이블 | 설명 | 업데이트 방식 |
|--------|------|---------------|
| `mart_realtime_traffic_minute` | 분 단위 트래픽 | Upsert |
| `mart_realtime_top_docs_1h` | 인기 문서 TOP 20 | Insert (스냅샷) |
| `mart_realtime_clinical_trend_24h` | 임상영역 트렌드 | Insert (스냅샷) |
| `mart_realtime_anomaly_sessions` | 이상징후 감지 | Insert Only |

**Task Flow:**
```
start → streaming_to_analytics_mart_realtime → end
```

**필요 설정:**
- Airflow Variables: `S3_CURATED_STREAM_PATH`, `S3_CHECKPOINT_PATH`
- Airflow Connections: `spark_ssh`, `aws_s3`, `postgres_gold`

**설계 원칙:**
- **Idempotency**: PK 기반 Upsert 또는 스냅샷 방식으로 재실행 안전
- **Late Event**: Hot Analytics Mart는 대략적 최신값, Cold Analytics Mart가 매일 정합성 보정
- **부하 관리**: 5분 마이크로배치로 PostgreSQL upsert 부담 최소화


### 7. Pipeline: `tripclick_main_dag.py` ✅

| 항목 | 값 |
|------|-----|
| DAG ID | `tripclick_daily_pipeline` |
| 스케줄 | `0 15 * * *` (KST 00:00) |
| Operator | `TriggerDagRunOperator` |
| 목적 | 전체 파이프라인 오케스트레이션 (Cold Analytics Mart 배치) |

**Task Flow (Cold Analytics Mart 배치):**
```
start
  → trigger_producer_batch (또는 realtime)
  → trigger_streaming
  → trigger_batch
  → trigger_analytics_mart
  → trigger_load_postgres
→ end
```

> **Note**: Hot Analytics Mart DAG(`tripclick_analytics_mart_realtime`)은 장기 실행 특성상 메인 파이프라인과 별도로 운영합니다. 데모/모니터링 목적으로 수동 트리거하거나, 별도 스케줄로 주기적 재시작을 권장합니다.

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
    trigger_dag_id="tripclick_streaming_curated",
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
| `S3_ARCHIVE_RAW_PATH` | Archive Raw 레이어 경로 | `s3a://tripclick-lake-sangjun/archive_raw` |
| `S3_CURATED_STREAM_PATH` | Curated Stream 레이어 경로 | `s3a://tripclick-lake-sangjun/curated_stream` |
| `S3_ANALYTICS_MART_PATH` | Analytics Mart 레이어 경로 | `s3a://tripclick-lake-sangjun/analytics_mart` |
| `S3_CHECKPOINT_PATH` | Spark Checkpoint 경로 | `s3a://tripclick-lake-sangjun/checkpoint/` |
| `WEBSERVER_INGESTION_PATH` | 웹서버 홈 경로 | `/home/ubuntu` |

### Connections

| Conn ID | Type | 설명 |
|---------|------|------|
| `docker_server0` | Docker | 웹서버 0 Docker Remote API (`tcp://10.0.0.43:2375`) |
| `spark_ssh` | SSH | Spark 서버 SSH 연결 (SSHOperator용) |
| `aws_s3` | Amazon Web Services | S3 접근용 IAM 자격증명 |
| `postgres_gold` | Postgres | Analytics Mart 레이어 PostgreSQL |


## 테스트 순서

1. **Airflow 기동 확인**: Webserver, Scheduler, Worker 정상 동작 확인
2. **Variables/Connections 설정**: UI 또는 CLI로 등록
3. **DAG 단위 테스트**: 아래 순서로 수동 실행
   ```
   # Cold Analytics Mart (백필/초기 적재) 테스트
   tripclick_producer_batch → tripclick_streaming_curated → tripclick_spark_archive_raw_batch
   → tripclick_analytics_mart_etl → tripclick_load_postgres

   # Hot Analytics Mart (실시간) 테스트
   tripclick_producer_realtime → tripclick_streaming_curated
   → tripclick_analytics_mart_realtime (별도 실행)
   ```
4. **메인 DAG 테스트**: `tripclick_daily_pipeline` 수동 실행
5. **Hot Analytics Mart 테스트**: `tripclick_analytics_mart_realtime` 수동 실행 후 Superset에서 실시간 차트 확인
6. **스케줄 실행 검증**: 스케줄 활성화 후 모니터링


## 진행 상황

| DAG | 유형 | 상태 | 비고 |
|-----|------|------|------|
| `tripclick_producer_realtime` | Ingestion | ✅ 완료 | `ingestion/tripclick_producer_realtime_dag.py` (DockerOperator) |
| `tripclick_producer_batch` | Ingestion | ✅ 완료 | `ingestion/tripclick_producer_batch_dag.py` (DockerOperator) |
| `tripclick_streaming_curated` | Processing | ✅ 완료 | `processing/tripclick_streaming_curated_dag.py` (SSHOperator) |
| `tripclick_spark_archive_raw_batch` | Processing | ✅ 완료 | `processing/tripclick_batch_dag.py` (SSHOperator) |
| `tripclick_analytics_mart_etl` | Cold Analytics Mart | ✅ 완료 | `mart/tripclick_analytics_mart_dag.py` (SSHOperator) |
| `tripclick_load_postgres` | Cold Analytics Mart | ✅ 완료 | `mart/tripclick_load_postgres.py` (SSHOperator) |
| `tripclick_analytics_mart_realtime` | **Hot Analytics Mart** | ✅ 완료 | `mart/tripclick_analytics_mart_realtime_dag.py` (SSHOperator, Streaming) |
| `tripclick_daily_pipeline` | Pipeline | ✅ 완료 | `pipeline/tripclick_main_dag.py` (TriggerDagRunOperator) |

> **Note**: SparkSubmitOperator의 Client Mode 네트워크 문제로 인해 Spark 관련 DAG들은 모두 SSHOperator로 구현했습니다.

---

## Hot/Cold Analytics Mart 아키텍처 요약

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         ANALYTICS MART LAYER (2계층)                        │
├────────────────────────────────┬─────────────────────────────────────────┤
│        HOT ANALYTICS MART      │           COLD ANALYTICS MART           │
│   (Near Real-Time, 1~5분)      │       (Daily Batch, T+1)                │
├────────────────────────────────┼─────────────────────────────────────────┤
│ tripclick_analytics_mart_realtime │ tripclick_analytics_mart_etl          │
│ → Spark Streaming              │ → Spark Batch                           │
│ → 4개 실시간 마트              │                                          │
│   - traffic_minute             │ tripclick_load_postgres                 │
│   - top_docs_1h                │ → 4개 Cold 마트                          │
│   - clinical_trend_24h         │   - session_analysis                    │
│   - anomaly_sessions           │   - daily_traffic                       │
│                                │   - clinical_areas                      │
│                                │   - popular_documents                   │
├────────────────────────────────┴─────────────────────────────────────────┤
│                           PostgreSQL                                      │
│                              ↓                                            │
│                          Superset                                         │
│                     (실시간 + 리포트 대시보드)                            │
└──────────────────────────────────────────────────────────────────────────┘
```
