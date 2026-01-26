"""
TripClick Load to PostgreSQL DAG

- 목적:
  Analytics Mart → PostgreSQL 적재
- 구성:
  - SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- 특징:
  - BI 대시보드(Superset) 연동용 데이터 적재
  - 4개 마트 테이블 적재 (세션, 트래픽, 임상분야, 인기문서)
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# =========================
# Airflow Variables & Connections
# =========================
S3_ANALYTICS_MART_PATH = Variable.get("S3_ANALYTICS_MART_PATH")

# AWS 자격증명
aws_conn = BaseHook.get_connection("aws_s3")
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password

# PostgreSQL 자격증명
pg_conn = BaseHook.get_connection("postgres_gold")
POSTGRES_HOST = pg_conn.host
POSTGRES_PORT = str(pg_conn.port) if pg_conn.port else "5432"
POSTGRES_DB = pg_conn.schema
POSTGRES_USER = pg_conn.login
POSTGRES_PASSWORD = pg_conn.password


# =========================
# Static Config
# =========================
SPARK_SSH_CONN_ID = "spark_ssh"  # Spark 서버 SSH Connection

# Spark packages (S3 + PostgreSQL JDBC)
SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.6.0",
])


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_load_postgres",
    description="TripClick Analytics Mart → PostgreSQL 적재 DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # 수동 실행 전용
    catchup=False,
    tags=["tripclick", "postgres", "load", "mart"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Load to PostgreSQL (via SSH)
    # =========================
    # Spark 서버의 Docker 컨테이너에서 직접 spark-submit 실행
    load_to_postgres = SSHOperator(
        task_id="load_to_postgres",
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=f"""
docker exec -e S3_ANALYTICS_MART_PATH="{S3_ANALYTICS_MART_PATH}" \\
  -e POSTGRES_HOST="{POSTGRES_HOST}" \\
  -e POSTGRES_PORT="{POSTGRES_PORT}" \\
  -e POSTGRES_DB="{POSTGRES_DB}" \\
  -e POSTGRES_USER="{POSTGRES_USER}" \\
  -e POSTGRES_PASSWORD="{POSTGRES_PASSWORD}" \\
  spark-master spark-submit \\
  --master spark://spark-master:7077 \\
  --packages {SPARK_PACKAGES} \\
  --conf spark.hadoop.fs.s3a.access.key={AWS_ACCESS_KEY} \\
  --conf spark.hadoop.fs.s3a.secret.key={AWS_SECRET_KEY} \\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
  --conf spark.hadoop.fs.s3a.endpoint=s3.ap-northeast-2.amazonaws.com \\
  --conf spark.executor.memory=1g \\
  --conf spark.driver.memory=1g \\
  /opt/spark/jobs/load_to_postgres.py
""",
        cmd_timeout=1800,  # 30분 타임아웃
        conn_timeout=30,
    )

    # =========================
    # Dependencies
    # =========================
    start >> load_to_postgres >> end
