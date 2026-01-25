"""
TripClick Gold Realtime DAG (Hot Gold)

- 목적:
  Silver → PostgreSQL 실시간 마트 적재 (Near Real-Time)
- 구성:
  - SSHOperator로 Spark 서버에서 Structured Streaming 실행
- 특징:
  - 1~5분 마이크로배치로 Hot Gold 마트 생성
  - 4개 실시간 마트 동시 적재:
    - mart_realtime_traffic_minute: 분 단위 트래픽
    - mart_realtime_top_docs_1h: 인기 문서 TOP 20
    - mart_realtime_clinical_trend_24h: 임상영역 트렌드
    - mart_realtime_anomaly_sessions: 이상징후 감지
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
S3_SILVER_PATH = Variable.get("S3_SILVER_PATH")
S3_CHECKPOINT_PATH = Variable.get("S3_CHECKPOINT_PATH", default_var="s3a://tripclick-lake/checkpoint/")

# AWS 자격증명
aws_conn = BaseHook.get_connection("aws_s3")
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password

# PostgreSQL 자격증명
pg_conn = BaseHook.get_connection("postgres_gold")
POSTGRES_HOST = pg_conn.host
POSTGRES_PORT = str(pg_conn.port) if pg_conn.port else "5433"
POSTGRES_DB = pg_conn.schema
POSTGRES_USER = pg_conn.login
POSTGRES_PASSWORD = pg_conn.password


# =========================
# Static Config
# =========================
SPARK_SSH_CONN_ID = "spark_ssh"

# Spark packages (S3 + PostgreSQL JDBC)
SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.6.0",
])

# 실행 설정
TRIGGER_INTERVAL = "5 minutes"  # 마이크로배치 주기
RUN_DURATION_HOURS = 1  # 1시간 실행 후 종료 (DAG 재실행으로 지속)


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_gold_realtime",
    description="TripClick Silver → PostgreSQL 실시간 마트 (Hot Gold) DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # 수동 실행 (장기 실행 Job)
    catchup=False,
    tags=["tripclick", "gold", "realtime", "streaming", "hot"],
    doc_md="""
    ## TripClick Gold Realtime DAG

    ### 목적
    Silver 데이터를 1~5분 마이크로배치로 읽어 Hot Gold 마트를 PostgreSQL에 적재합니다.

    ### Hot Gold 마트
    | 테이블 | 설명 | 업데이트 방식 |
    |--------|------|---------------|
    | `mart_realtime_traffic_minute` | 분 단위 트래픽 | Upsert |
    | `mart_realtime_top_docs_1h` | 인기 문서 TOP 20 | Insert (스냅샷) |
    | `mart_realtime_clinical_trend_24h` | 임상영역 트렌드 | Insert (스냅샷) |
    | `mart_realtime_anomaly_sessions` | 이상징후 감지 | Insert Only |

    ### 실행 방법
    1. 수동 트리거 또는 메인 파이프라인에서 호출
    2. 1시간 실행 후 자동 종료
    3. 지속 운영 시 스케줄 또는 외부 트리거로 재시작

    ### 필요 설정
    - Variables: `S3_SILVER_PATH`, `S3_CHECKPOINT_PATH`
    - Connections: `spark_ssh`, `aws_s3`, `postgres_gold`
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Streaming to Gold Realtime (via SSH)
    # =========================
    streaming_to_gold_realtime = SSHOperator(
        task_id="streaming_to_gold_realtime",
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=f"""
docker exec \\
  -e S3_SILVER_PATH="{S3_SILVER_PATH}" \\
  -e S3_CHECKPOINT_PATH="{S3_CHECKPOINT_PATH}" \\
  -e POSTGRES_HOST="{POSTGRES_HOST}" \\
  -e POSTGRES_PORT="{POSTGRES_PORT}" \\
  -e POSTGRES_DB="{POSTGRES_DB}" \\
  -e POSTGRES_USER="{POSTGRES_USER}" \\
  -e POSTGRES_PASSWORD="{POSTGRES_PASSWORD}" \\
  -e TRIGGER_INTERVAL="{TRIGGER_INTERVAL}" \\
  -e RUN_DURATION_HOURS="{RUN_DURATION_HOURS}" \\
  spark-master spark-submit \\
  --master spark://spark-master:7077 \\
  --packages {SPARK_PACKAGES} \\
  --conf spark.hadoop.fs.s3a.access.key={AWS_ACCESS_KEY} \\
  --conf spark.hadoop.fs.s3a.secret.key={AWS_SECRET_KEY} \\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
  --conf spark.hadoop.fs.s3a.endpoint=s3.ap-northeast-2.amazonaws.com \\
  --conf spark.executor.memory=1g \\
  --conf spark.driver.memory=1g \\
  --conf spark.sql.streaming.schemaInference=true \\
  /opt/spark/jobs/streaming_to_gold_realtime.py
""",
        cmd_timeout=7200,  # 2시간 타임아웃 (1시간 실행 + 버퍼)
        conn_timeout=30,
    )

    # =========================
    # Dependencies
    # =========================
    start >> streaming_to_gold_realtime >> end
