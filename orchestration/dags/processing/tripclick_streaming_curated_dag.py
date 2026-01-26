"""
TripClick Streaming Curated Stream DAG

- 목적:
  Kafka → Curated Stream 스트리밍 처리 (1시간 실행)
- 구성:
  - SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- 특징:
  - 네트워크 문제 없이 Spark 클러스터에서 직접 실행
  - 1시간 동안 실시간 데이터 처리 후 종료
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
    "retries": 0,  # Streaming job은 retry 안 함
    "retry_delay": timedelta(minutes=5),
}


# =========================
# Airflow Variables & Connections
# =========================
KAFKA_BROKERS = Variable.get("KAFKA_BROKERS")
S3_CURATED_STREAM_PATH = Variable.get("S3_CURATED_STREAM_PATH")

# AWS 자격증명은 Connection에서 가져오기
aws_conn = BaseHook.get_connection("aws_s3")
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password


# =========================
# Static Config
# =========================
SPARK_SSH_CONN_ID = "spark_ssh"  # Spark 서버 SSH Connection

# Spark packages
SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_streaming_curated",
    description="TripClick Kafka → Curated Stream 스트리밍 처리 DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # 수동 실행 전용
    catchup=False,
    tags=["tripclick", "streaming", "curated_stream", "processing"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Streaming to Curated Stream (via SSH)
    # =========================
    # Spark 서버의 Docker 컨테이너에서 직접 spark-submit 실행
    streaming_to_curated_stream = SSHOperator(
        task_id="streaming_to_curated_stream",
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
  /opt/spark/jobs/streaming_to_curated_stream.py
""",
        cmd_timeout=4200,  # 70분 타임아웃 (1시간 실행 + 여유)
        conn_timeout=30,
    )

    # =========================
    # Dependencies
    # =========================
    start >> streaming_to_curated_stream >> end
