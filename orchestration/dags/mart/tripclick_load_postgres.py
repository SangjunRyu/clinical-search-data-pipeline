"""
TripClick Load to PostgreSQL DAG

- 목적:
  Gold → PostgreSQL 적재 단독 테스트
- 구성:
  - SparkSubmitOperator로 load_to_postgres.py 실행
- 특징:
  - BI 대시보드 연동용 데이터 적재
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
S3_GOLD_PATH = Variable.get("S3_GOLD_PATH")


# =========================
# Static Config
# =========================
SPARK_CONN_ID = "spark_cluster"


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_load_postgres",
    description="TripClick Gold → PostgreSQL 적재 DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # 수동 실행 전용
    catchup=False,
    tags=["tripclick", "postgres", "load", "mart"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Load to PostgreSQL
    # =========================
    load_to_postgres = SparkSubmitOperator(
        task_id="load_to_postgres",
        application="/opt/spark/jobs/load_to_postgres.py",
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

    # =========================
    # Dependencies
    # =========================
    start >> load_to_postgres >> end