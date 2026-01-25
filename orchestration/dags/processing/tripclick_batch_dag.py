"""
TripClick Batch Bronze DAG

- 목적:
  Kafka/Raw Source → Bronze 배치 처리 단독 테스트
- 구성:
  - SparkSubmitOperator로 batch_to_bronze.py 실행
- 특징:
  - 배치 로직만 독립적으로 검증 가능
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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


# =========================
# Static Config
# =========================
SPARK_CONN_ID = "spark_cluster"

# Spark packages (Kafka connector)
SPARK_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_batch_bronze",
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
    # Batch to Bronze
    # =========================
    batch_to_bronze = SparkSubmitOperator(
        task_id="batch_to_bronze",
        application="/opt/spark/jobs/batch_to_bronze.py",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        conf={
            # S3 Hadoop 설정
            "spark.hadoop.fs.s3a.access.key": "{{ conn.aws_s3.login }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ conn.aws_s3.password }}",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
            # 메모리 설정
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
        },
        env_vars={
            "KAFKA_BROKERS": KAFKA_BROKERS,
            "S3_BRONZE_PATH": S3_BRONZE_PATH,
        },
        verbose=True,
    )

    # =========================
    # Dependencies
    # =========================
    start >> batch_to_bronze >> end