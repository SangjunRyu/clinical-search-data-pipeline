"""
TripClick Realtime Mart DAG

- 목적:
  Kafka → PostgreSQL 실시간 마트 적재 (Structured Streaming)
- 구성:
  - SSHOperator로 Spark 서버에서 streaming_to_realtime_mart.py 실행
- 특징:
  - Kafka에서 직접 스트리밍 읽기
  - 5분 마이크로배치로 4개 Realtime Mart 생성
  - 1시간 실행 후 종료 (재시작으로 지속 운영)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.empty import EmptyOperator


# =========================
# Default Arguments
# =========================
DEFAULT_ARGS = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# =========================
# Airflow Variables & Connections
# =========================
KAFKA_BROKERS = Variable.get("KAFKA_BROKERS")
S3_CHECKPOINT_PATH = Variable.get(
    "S3_CHECKPOINT_PATH",
    default_var="s3a://tripclick-lake-sangjun/checkpoint/"
)

# AWS 자격증명
aws_conn = BaseHook.get_connection("aws_s3")
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password

# PostgreSQL 자격증명
pg_conn = BaseHook.get_connection("postgres_mart")
POSTGRES_HOST = pg_conn.host
POSTGRES_PORT = str(pg_conn.port) if pg_conn.port else "5432"
POSTGRES_DB = pg_conn.schema
POSTGRES_USER = pg_conn.login
POSTGRES_PASSWORD = pg_conn.password


# =========================
# Static Config
# =========================
SPARK_SSH_CONN_ID = "spark_ssh"

# 실행 설정
TRIGGER_INTERVAL = "5 minutes"
RUN_DURATION_HOURS = 1


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_realtime_mart",
    description="TripClick Kafka → PostgreSQL Realtime Mart DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tripclick", "realtime", "streaming", "mart", "processing"],
    doc_md="""
    ## TripClick Realtime Mart DAG

    ### 목적
    Kafka에서 직접 스트리밍으로 읽어 4개 Realtime Mart를 PostgreSQL에 적재합니다.

    ### Realtime Mart 테이블
    | 테이블 | 설명 | 업데이트 방식 |
    |--------|------|---------------|
    | `mart_realtime_traffic_minute` | 분 단위 트래픽 | Upsert |
    | `mart_realtime_top_docs_1h` | 인기 문서 TOP 20 | Append (스냅샷) |
    | `mart_realtime_clinical_trend_24h` | 임상영역 트렌드 | Append (스냅샷) |
    | `mart_realtime_anomaly_sessions` | 이상징후 감지 | Append Only |

    ### 실행 방법
    - 수동 트리거 또는 스케줄 실행
    - 1시간 실행 후 자동 종료
    - 지속 운영 시 재시작 필요
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Streaming to Realtime Mart (via SSH)
    # =========================
    streaming_to_realtime_mart = SSHOperator(
        task_id="streaming_to_realtime_mart",
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=f"""
docker exec \\
  -e KAFKA_BROKERS="{KAFKA_BROKERS}" \\
  -e S3_CHECKPOINT_PATH="{S3_CHECKPOINT_PATH}realtime_mart/" \\
  -e POSTGRES_HOST="{POSTGRES_HOST}" \\
  -e POSTGRES_PORT="{POSTGRES_PORT}" \\
  -e POSTGRES_DB="{POSTGRES_DB}" \\
  -e POSTGRES_USER="{POSTGRES_USER}" \\
  -e POSTGRES_PASSWORD="{POSTGRES_PASSWORD}" \\
  -e TRIGGER_INTERVAL="{TRIGGER_INTERVAL}" \\
  -e RUN_DURATION_HOURS="{RUN_DURATION_HOURS}" \\
  spark-master spark-submit \\
  --master spark://spark-master:7077 \\
  --conf spark.hadoop.fs.s3a.access.key={AWS_ACCESS_KEY} \\
  --conf spark.hadoop.fs.s3a.secret.key={AWS_SECRET_KEY} \\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
  --conf spark.hadoop.fs.s3a.endpoint=s3.ap-northeast-2.amazonaws.com \\
  --conf spark.executor.memory=1g \\
  --conf spark.driver.memory=1g \\
  --conf spark.sql.streaming.schemaInference=true \\
  /opt/spark/jobs/streaming_to_realtime_mart.py
""",
        cmd_timeout=7200,
        conn_timeout=30,
    )

    # =========================
    # Dependencies
    # =========================
    start >> streaming_to_realtime_mart >> end
