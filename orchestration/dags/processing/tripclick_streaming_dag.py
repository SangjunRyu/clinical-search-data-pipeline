"""
TripClick Streaming Silver DAG

- 목적:
  Kafka → Silver 스트리밍 처리 단독 테스트
- 구성:
  - SparkSubmitOperator로 streaming_to_silver.py 실행
- 특징:
  - Producer 없이도 Kafka consume 테스트 가능
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
S3_SILVER_PATH = Variable.get("S3_SILVER_PATH")


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
    dag_id="tripclick_streaming_silver",
    description="TripClick Kafka → Silver 스트리밍 처리 DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # 수동 실행 전용
    catchup=False,
    tags=["tripclick", "streaming", "silver", "processing"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Streaming to Silver
    # =========================
    streaming_to_silver = SparkSubmitOperator(
        task_id="streaming_to_silver",
        application="/opt/spark/jobs/streaming_to_silver.py",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        conf={
            # S3 Hadoop 설정
            "spark.hadoop.fs.s3a.access.key": "{{ conn.aws_s3.login }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ conn.aws_s3.password }}",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
            # Streaming 설정
            "spark.sql.streaming.checkpointLocation": "s3a://tripclick-lake/checkpoint/silver",
            # 메모리 설정
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
        },
        env_vars={
            "KAFKA_BROKERS": KAFKA_BROKERS,
            "S3_SILVER_PATH": S3_SILVER_PATH,
        },
        verbose=True,
    )

    # =========================
    # Dependencies
    # =========================
    start >> streaming_to_silver >> end