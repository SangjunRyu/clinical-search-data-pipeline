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
        env_vars={
            "KAFKA_BROKERS": KAFKA_BROKERS,
            "S3_BRONZE_PATH": S3_BRONZE_PATH,
            "AWS_ACCESS_KEY_ID": "{{ conn.aws_s3.login }}",
            "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_s3.password }}",
        },
        verbose=True,
    )

    # =========================
    # Dependencies
    # =========================
    start >> batch_to_bronze >> end