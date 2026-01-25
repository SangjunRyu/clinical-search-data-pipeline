"""
TripClick Producer Batch DAG

- 목적:
  과거 데이터 백필 / 하루치 데이터 일괄 전송
- 구성:
  - DockerOperator로 producer_batch 실행
- 특징:
  - 대기 없이 즉시 전송 (고속 처리)
  - 과거 데이터 재처리, 초기 데이터 적재용
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
    dag_id="tripclick_producer_batch",
    description="TripClick Kafka Producer - Batch 일괄 전송 모드",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # 수동 실행 전용
    catchup=False,
    tags=["tripclick", "producer", "ingestion", "batch", "backfill"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Producer - Server 0
    # =========================
    producer_server0 = DockerOperator(
        task_id="producer_server0_batch",
        image=PRODUCER_IMAGE,
        command=[
            "--mode", "batch",
            "--file", "/app/data/date={{ ds }}/events.json",
            "--topic", "tripclick_raw_logs",
        ],
        docker_url="tcp://10.0.0.43:2375",
        network_mode="host",
        environment={
            "KAFKA_BROKERS": KAFKA_BROKERS,
        },
        mounts=[
            Mount(
                source=f"{WEBSERVER_INGESTION_PATH}/server0",
                target="/app/data",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=f"{WEBSERVER_INGESTION_PATH}/ingestion/config",
                target="/app/config",
                type="bind",
                read_only=True,
            ),
            Mount(
                source="/home/ubuntu/ingestion/logs",
                target="/app/logs",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    # =========================
    # Producer - Server 1
    # =========================
    producer_server1 = DockerOperator(
        task_id="producer_server1_batch",
        image=PRODUCER_IMAGE,
        command=[
            "--mode", "batch",
            "--file", "/app/data/date={{ ds }}/events.json",
            "--topic", "tripclick_raw_logs",
        ],
        docker_url="tcp://10.0.16.8:2375",
        network_mode="host",
        environment={
            "KAFKA_BROKERS": KAFKA_BROKERS,
        },
        mounts=[
            Mount(
                source=f"{WEBSERVER_INGESTION_PATH}/server1",
                target="/app/data",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=f"{WEBSERVER_INGESTION_PATH}/ingestion/config",
                target="/app/config",
                type="bind",
                read_only=True,
            ),
            Mount(
                source="/home/ubuntu/ingestion/logs",
                target="/app/logs",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    # =========================
    # Dependencies (병렬 실행)
    # =========================
    start >> [producer_server0, producer_server1] >> end