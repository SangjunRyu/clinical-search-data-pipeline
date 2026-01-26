"""
TripClick Analytics Mart ETL DAG

- 목적:
  Curated Stream → Analytics Mart 집계 처리
- 구성:
  - SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- 특징:
  - BI 연계 전용 Analytics Mart 레이어 생성
  - 세션 분석, 일별 트래픽, 임상 분야, 인기 문서 마트 생성
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
S3_CURATED_STREAM_PATH = Variable.get("S3_CURATED_STREAM_PATH")
S3_ANALYTICS_MART_PATH = Variable.get("S3_ANALYTICS_MART_PATH")

# AWS 자격증명은 Connection에서 가져오기
aws_conn = BaseHook.get_connection("aws_s3")
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password


# =========================
# Static Config
# =========================
SPARK_SSH_CONN_ID = "spark_ssh"  # Spark 서버 SSH Connection

# Spark packages (S3 접근용)
SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_analytics_mart_etl",
    description="TripClick Curated Stream → Analytics Mart 집계 처리 DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # 수동 실행 전용
    catchup=False,
    tags=["tripclick", "analytics_mart", "etl", "mart"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # ETL to Analytics Mart (via SSH)
    # =========================
    # Spark 서버의 Docker 컨테이너에서 직접 spark-submit 실행
    etl_to_analytics_mart = SSHOperator(
        task_id="etl_to_analytics_mart",
        ssh_conn_id=SPARK_SSH_CONN_ID,
        command=f"""
docker exec spark-master spark-submit \\
  --master spark://spark-master:7077 \\
  --packages {SPARK_PACKAGES} \\
  --conf spark.hadoop.fs.s3a.access.key={AWS_ACCESS_KEY} \\
  --conf spark.hadoop.fs.s3a.secret.key={AWS_SECRET_KEY} \\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
  --conf spark.hadoop.fs.s3a.endpoint=s3.ap-northeast-2.amazonaws.com \\
  --conf spark.executor.memory=1g \\
  --conf spark.driver.memory=1g \\
  /opt/spark/jobs/etl_to_analytics_mart.py
""",
        environment={
            "S3_CURATED_STREAM_PATH": S3_CURATED_STREAM_PATH,
            "S3_ANALYTICS_MART_PATH": S3_ANALYTICS_MART_PATH,
        },
        cmd_timeout=1800,  # 30분 타임아웃
        conn_timeout=30,
    )

    # =========================
    # Dependencies
    # =========================
    start >> etl_to_analytics_mart >> end
