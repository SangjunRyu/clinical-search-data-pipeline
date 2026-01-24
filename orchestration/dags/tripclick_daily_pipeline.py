"""
TripClick Daily Pipeline DAG

전체 데이터 파이프라인을 오케스트레이션하는 메인 DAG
- Producer: DockerOperator로 원격 웹서버에서 실행
- Spark: SparkSubmitOperator로 Spark 클러스터에서 실행
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
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
# Common Configuration
# =========================
# 환경변수에서 설정 로드 (실제 값은 Airflow Variables 또는 환경변수로 주입)
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
S3_BRONZE_PATH = os.getenv("S3_BRONZE_PATH", "s3a://tripclick-lake/bronze/")
S3_SILVER_PATH = os.getenv("S3_SILVER_PATH", "s3a://tripclick-lake/silver/")
S3_GOLD_PATH = os.getenv("S3_GOLD_PATH", "s3a://tripclick-lake/gold/")

PRODUCER_IMAGE = "tripclick-producer:latest"
SPARK_CONN_ID = "spark_cluster"

# Docker API 엔드포인트 (환경변수로 주입)
DOCKER_SERVER0_URL = os.getenv("DOCKER_SERVER0_URL", "tcp://webserver0:2375")
DOCKER_SERVER1_URL = os.getenv("DOCKER_SERVER1_URL", "tcp://webserver1:2375")

# WebServer 호스트의 ingestion 경로
WEBSERVER_INGESTION_PATH = "/home/ubuntu/tripclick/ingestion"


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_daily_pipeline",
    description="TripClick 전체 데이터 파이프라인 (Ingestion → Processing → Gold)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 15 * * *",  # 매일 15:00 KST 시작
    catchup=False,
    max_active_runs=1,
    tags=["tripclick", "pipeline", "daily"],
) as dag:

    # =========================
    # Start
    # =========================
    start = EmptyOperator(task_id="start")

    # =========================
    # Ingestion Task Group
    # =========================
    with TaskGroup("ingestion", tooltip="Producer 실행") as ingestion_group:

        # Producer - Server 0
        producer_server0 = DockerOperator(
            task_id="producer_server0",
            image=PRODUCER_IMAGE,
            command=[
                "--file", "/app/data/server0/date={{ ds }}/events.json",
                "--topic", "tripclick_raw_logs",
                "--mode", "realtime",
            ],
            docker_url=DOCKER_SERVER0_URL,  # Remote Docker API
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
            auto_remove=True,
            force_pull=False,
        )

        # Producer - Server 1
        producer_server1 = DockerOperator(
            task_id="producer_server1",
            image=PRODUCER_IMAGE,
            command=[
                "--file", "/app/data/server1/date={{ ds }}/events.json",
                "--topic", "tripclick_raw_logs",
                "--mode", "realtime",
            ],
            docker_url=DOCKER_SERVER1_URL,  # Remote Docker API
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
            auto_remove=True,
            force_pull=False,
        )

    # =========================
    # Processing Task Group
    # =========================
    with TaskGroup("processing", tooltip="Spark 처리") as processing_group:

        # Streaming → Silver (Producer와 동시 실행, 1시간)
        streaming_to_silver = SparkSubmitOperator(
            task_id="streaming_to_silver",
            application="/opt/spark/jobs/streaming_to_silver.py",
            name="streaming-to-silver",
            conn_id=SPARK_CONN_ID,
            conf={
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": (
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
                ),
            },
            env_vars={
                "KAFKA_BROKERS": KAFKA_BROKERS,
                "S3_SILVER_PATH": S3_SILVER_PATH,
                "AWS_ACCESS_KEY_ID": "{{ conn.aws_s3.login }}",
                "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_s3.password }}",
            },
            verbose=True,
        )

        # Batch → Bronze (Streaming 완료 후)
        batch_to_bronze = SparkSubmitOperator(
            task_id="batch_to_bronze",
            application="/opt/spark/jobs/batch_to_bronze.py",
            name="batch-to-bronze",
            conn_id=SPARK_CONN_ID,
            conf={
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": (
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
                ),
            },
            env_vars={
                "KAFKA_BROKERS": KAFKA_BROKERS,
                "S3_BRONZE_PATH": S3_BRONZE_PATH,
                "AWS_ACCESS_KEY_ID": "{{ conn.aws_s3.login }}",
                "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_s3.password }}",
            },
            verbose=True,
        )

        streaming_to_silver >> batch_to_bronze

    # =========================
    # Gold ETL Task Group
    # =========================
    with TaskGroup("gold_etl", tooltip="Gold Mart ETL") as gold_group:

        # Silver → Gold (분석 마트 생성)
        etl_to_gold = SparkSubmitOperator(
            task_id="etl_to_gold",
            application="/opt/spark/jobs/etl_to_gold.py",
            name="etl-to-gold",
            conn_id=SPARK_CONN_ID,
            conf={
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": (
                    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
                ),
            },
            env_vars={
                "S3_SILVER_PATH": S3_SILVER_PATH,
                "S3_GOLD_PATH": S3_GOLD_PATH,
                "AWS_ACCESS_KEY_ID": "{{ conn.aws_s3.login }}",
                "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_s3.password }}",
            },
            verbose=True,
        )

        # Gold → PostgreSQL (Superset용)
        load_to_postgres = SparkSubmitOperator(
            task_id="load_to_postgres",
            application="/opt/spark/jobs/load_to_postgres.py",
            name="load-to-postgres",
            conn_id=SPARK_CONN_ID,
            env_vars={
                "S3_GOLD_PATH": S3_GOLD_PATH,
                "POSTGRES_HOST": "{{ conn.postgres_gold.host }}",
                "POSTGRES_PORT": "{{ conn.postgres_gold.port }}",
                "POSTGRES_DB": "{{ conn.postgres_gold.schema }}",
                "POSTGRES_USER": "{{ conn.postgres_gold.login }}",
                "POSTGRES_PASSWORD": "{{ conn.postgres_gold.password }}",
            },
            verbose=True,
        )

        etl_to_gold >> load_to_postgres

    # =========================
    # End
    # =========================
    end = EmptyOperator(task_id="end")

    # =========================
    # Task Dependencies
    # =========================
    # Ingestion과 Streaming은 동시 시작
    start >> [ingestion_group, processing_group.streaming_to_silver]

    # Streaming 완료 후 Batch
    ingestion_group >> processing_group.batch_to_bronze
    processing_group.streaming_to_silver >> processing_group.batch_to_bronze

    # Batch 완료 후 Gold ETL
    processing_group >> gold_group >> end
