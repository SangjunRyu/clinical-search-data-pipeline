"""
TripClick Producer Realtime DAG

- 목적:
  실시간 스트리밍 시뮬레이션 (event_ts 기준 시간 경과에 따라 전송)
- 구성:
  - DockerOperator로 producer_realtime 실행
- 특징:
  - 실제 시간 흐름에 맞춰 Kafka로 데이터 전송
  - 스트리밍 처리 테스트용
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
    dag_id="tripclick_producer_realtime",
    description="TripClick Kafka Producer - Realtime 스트리밍 모드",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # 수동 실행 전용
    catchup=False,
    tags=["tripclick", "producer", "ingestion", "realtime"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Producer - Server 0 (Realtime)
    # =========================
    producer_server0 = DockerOperator(
        task_id="producer_server0_realtime",
        image=PRODUCER_IMAGE,
        force_pull=False,
        command=[
            "--mode", "realtime",
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
    # Dependencies
    # =========================
    start >> producer_server0 >> end
