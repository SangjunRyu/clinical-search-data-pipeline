"""
TripClick Batch Mart DAG

- 목적:
  S3 Archive Raw → PostgreSQL Batch Mart 적재
- 구성:
  - SSHOperator로 Spark 서버에서 etl_to_batch_mart.py 실행
- 특징:
  - 중복 제거 (dedup_key 기준)
  - 4개 Batch Mart 생성 후 PostgreSQL 직접 적재
  - Daily Batch 파이프라인의 마지막 단계
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
S3_ARCHIVE_RAW_PATH = Variable.get("S3_ARCHIVE_RAW_PATH")

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


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_batch_mart",
    description="TripClick S3 Archive Raw → PostgreSQL Batch Mart DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["tripclick", "batch", "mart", "processing"],
    doc_md="""
    ## TripClick Batch Mart DAG

    ### 목적
    S3 Archive Raw 데이터를 읽어 중복 제거 후 4개 Batch Mart를 PostgreSQL에 직접 적재합니다.

    ### Batch Mart 테이블
    | 테이블 | 설명 |
    |--------|------|
    | `mart_session_analysis` | 세션별 클릭 분석 |
    | `mart_daily_traffic` | 일별 트래픽 집계 |
    | `mart_clinical_areas` | 임상 분야별 검색 통계 |
    | `mart_popular_documents` | 인기 문서 순위 |

    ### 실행 흐름
    1. S3 Archive Raw 데이터 읽기
    2. dedup_key 기준 중복 제거
    3. 4개 마트 집계 생성
    4. PostgreSQL 직접 적재 (overwrite)
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # ETL to Batch Mart (via SSH)
    # =========================
    etl_to_batch_mart = SSHOperator(
        task_id="etl_to_batch_mart",
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=f"""
docker exec \\
  -e S3_ARCHIVE_RAW_PATH="{S3_ARCHIVE_RAW_PATH}" \\
  -e POSTGRES_HOST="{POSTGRES_HOST}" \\
  -e POSTGRES_PORT="{POSTGRES_PORT}" \\
  -e POSTGRES_DB="{POSTGRES_DB}" \\
  -e POSTGRES_USER="{POSTGRES_USER}" \\
  -e POSTGRES_PASSWORD="{POSTGRES_PASSWORD}" \\
  spark-master spark-submit \\
  --master spark://spark-master:7077 \\
  --conf spark.hadoop.fs.s3a.access.key={AWS_ACCESS_KEY} \\
  --conf spark.hadoop.fs.s3a.secret.key={AWS_SECRET_KEY} \\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
  --conf spark.hadoop.fs.s3a.endpoint=s3.ap-northeast-2.amazonaws.com \\
  --conf spark.executor.memory=1g \\
  --conf spark.driver.memory=1g \\
  /opt/spark/jobs/etl_to_batch_mart.py
""",
        cmd_timeout=1800,
        conn_timeout=30,
    )

    # =========================
    # Dependencies
    # =========================
    start >> etl_to_batch_mart >> end
