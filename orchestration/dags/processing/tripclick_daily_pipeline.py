"""
TripClick Daily Pipeline DAG

- Ingestion: Remote Docker Producer
- Processing: Spark Streaming / Batch
- Storage: S3 Archive Raw / Curated Stream / Analytics Mart
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from docker.types import Mount


# =========================
# Default Arguments
# =========================
DEFAULT_ARGS = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# =========================
# Airflow Variables
# =========================
KAFKA_BROKERS = Variable.get("KAFKA_BROKERS")

S3_ARCHIVE_RAW_PATH = Variable.get("S3_ARCHIVE_RAW_PATH")
S3_CURATED_STREAM_PATH = Variable.get("S3_CURATED_STREAM_PATH")
S3_ANALYTICS_MART_PATH = Variable.get("S3_ANALYTICS_MART_PATH")

WEBSERVER_INGESTION_PATH = Variable.get("WEBSERVER_INGESTION_PATH")


# =========================
# Static Identifiers
# =========================
PRODUCER_IMAGE = "tripclick-producer:latest"
SPARK_CONN_ID = "spark_cluster"


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_daily_pipeline",
    description="TripClick 전체 데이터 파이프라인 (Ingestion → Processing → Analytics Mart)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 15 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["tripclick", "pipeline", "daily"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Ingestion
    # =========================
    with TaskGroup("ingestion", tooltip="Kafka Producer 실행") as ingestion_group:

        producer_server0 = DockerOperator(
            task_id="producer_server0",
            image=PRODUCER_IMAGE,
            command=[
                "--file", "/app/data/server0/date={{ ds }}/events.json",
                "--topic", "tripclick_raw_logs",
                "--mode", "realtime",
            ],
            docker_conn_id="docker_server0",
            network_mode="host",
            environment={
                "KAFKA_BROKERS": KAFKA_BROKERS,
            },
            mounts=[
                Mount(
                    source=f"{WEBSERVER_INGESTION_PATH}/data",
                    target="/app/data",
                    type="bind",
                    read_only=True,
                ),
                Mount(
                    source=f"{WEBSERVER_INGESTION_PATH}/config",
                    target="/app/config",
                    type="bind",
                    read_only=True,
                ),
            ],
            mount_tmp_dir=False,
            auto_remove="success",
        )

        producer_server1 = DockerOperator(
            task_id="producer_server1",
            image=PRODUCER_IMAGE,
            command=[
                "--file", "/app/data/server1/date={{ ds }}/events.json",
                "--topic", "tripclick_raw_logs",
                "--mode", "realtime",
            ],
            docker_conn_id="docker_server1",
            network_mode="host",
            environment={
                "KAFKA_BROKERS": KAFKA_BROKERS,
            },
            mounts=[
                Mount(
                    source=f"{WEBSERVER_INGESTION_PATH}/data",
                    target="/app/data",
                    type="bind",
                    read_only=True,
                ),
                Mount(
                    source=f"{WEBSERVER_INGESTION_PATH}/config",
                    target="/app/config",
                    type="bind",
                    read_only=True,
                ),
            ],
            mount_tmp_dir=False,
            auto_remove="success",
        )

    # =========================
    # Processing
    # =========================
    with TaskGroup("processing", tooltip="Spark Processing") as processing_group:

        streaming_to_curated_stream = SparkSubmitOperator(
            task_id="streaming_to_curated_stream",
            application="/opt/spark/jobs/streaming_to_curated_stream.py",
            conn_id=SPARK_CONN_ID,
            env_vars={
                "KAFKA_BROKERS": KAFKA_BROKERS,
                "S3_CURATED_STREAM_PATH": S3_CURATED_STREAM_PATH,
                "AWS_ACCESS_KEY_ID": "{{ conn.aws_s3.login }}",
                "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_s3.password }}",
            },
            verbose=True,
        )

        batch_to_archive_raw = SparkSubmitOperator(
            task_id="batch_to_archive_raw",
            application="/opt/spark/jobs/batch_to_archive_raw.py",
            conn_id=SPARK_CONN_ID,
            env_vars={
                "KAFKA_BROKERS": KAFKA_BROKERS,
                "S3_ARCHIVE_RAW_PATH": S3_ARCHIVE_RAW_PATH,
                "AWS_ACCESS_KEY_ID": "{{ conn.aws_s3.login }}",
                "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_s3.password }}",
            },
            verbose=True,
        )

        streaming_to_curated_stream >> batch_to_archive_raw

    # =========================
    # Analytics Mart ETL
    # =========================
    with TaskGroup("analytics_mart_etl", tooltip="Analytics Mart 생성") as mart_group:

        etl_to_analytics_mart = SparkSubmitOperator(
            task_id="etl_to_analytics_mart",
            application="/opt/spark/jobs/etl_to_analytics_mart.py",
            conn_id=SPARK_CONN_ID,
            env_vars={
                "S3_CURATED_STREAM_PATH": S3_CURATED_STREAM_PATH,
                "S3_ANALYTICS_MART_PATH": S3_ANALYTICS_MART_PATH,
                "AWS_ACCESS_KEY_ID": "{{ conn.aws_s3.login }}",
                "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_s3.password }}",
            },
            verbose=True,
        )

        load_to_postgres = SparkSubmitOperator(
            task_id="load_to_postgres",
            application="/opt/spark/jobs/load_to_postgres.py",
            conn_id=SPARK_CONN_ID,
            env_vars={
                "S3_ANALYTICS_MART_PATH": S3_ANALYTICS_MART_PATH,
                "POSTGRES_HOST": "{{ conn.postgres_gold.host }}",
                "POSTGRES_PORT": "{{ conn.postgres_gold.port }}",
                "POSTGRES_DB": "{{ conn.postgres_gold.schema }}",
                "POSTGRES_USER": "{{ conn.postgres_gold.login }}",
                "POSTGRES_PASSWORD": "{{ conn.postgres_gold.password }}",
            },
            verbose=True,
        )

        etl_to_analytics_mart >> load_to_postgres

    # =========================
    # Dependencies
    # =========================
    start >> ingestion_group
    start >> streaming_to_curated_stream

    ingestion_group >> batch_to_archive_raw
    streaming_to_curated_stream >> batch_to_archive_raw

    batch_to_archive_raw >> mart_group >> end
