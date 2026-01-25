"""
TripClick Batch Bronze DAG

- 목적:
  Kafka/Raw Source → Bronze 배치 처리
- 구성:
  - SSHOperator로 Spark 서버에서 직접 spark-submit 실행
- 특징:
  - 네트워크 문제 없이 Spark 클러스터에서 직접 실행
  - Client mode에서 Driver-Executor 통신 문제 해결
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
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
# Airflow Variables
# =========================
KAFKA_BROKERS = Variable.get("KAFKA_BROKERS")
S3_BRONZE_PATH = Variable.get("S3_BRONZE_PATH")
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")


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
    dag_id="tripclick_spark_bronze_batch",
    description="TripClick Kafka/Raw → Bronze 배치 처리 DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # 수동 실행 전용
    catchup=False,
    tags=["tripclick", "batch", "bronze", "processing"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Batch to Bronze (via SSH)
    # =========================
    # Spark 서버의 Docker 컨테이너에서 직접 spark-submit 실행
    batch_to_bronze = SSHOperator(
        task_id="batch_to_bronze",
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
  /opt/spark/jobs/batch_to_bronze.py
""",
        cmd_timeout=1800,  # 30분 타임아웃
        conn_timeout=30,
    )

    # =========================
    # Dependencies
    # =========================
    start >> batch_to_bronze >> end