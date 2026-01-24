"""
TripClick Producer DAG

- 목적:
  Kafka ingestion 단계 단독 테스트
- 구성:
  - server0 / server1 에서 Docker 기반 producer 실행
- 특징:
  - Kafka / Docker endpoint / path 전부 Airflow Variable & Connection 사용
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount


# =========================
# Default Arguments
# =========================
DEFAULT_ARGS = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


# =========================
# Airflow Variables
# =========================
KAFKA_BROKERS = Variable.get("KAFKA_BROKERS")
WEBSERVER_INGESTION_PATH = Variable.get("WEBSERVER_INGESTION_PATH")


# =========================
# Static Config
# =========================
PRODUCER_IMAGE = "tripclick-producer:latest"


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_producer",
    description="TripClick Kafka Producer 단독 실행 DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,          # 수동 실행 전용
    catchup=False,
    tags=["tripclick", "producer", "ingestion"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Producer - Server 0
    # =========================
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

    # =========================
    # Producer - Server 1
    # =========================
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
    # Dependencies
    # =========================
    start >> [producer_server0, producer_server1] >> end
