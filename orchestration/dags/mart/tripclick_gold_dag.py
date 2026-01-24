"""
TripClick Gold ETL DAG

- 목적:
  Silver → Gold 집계 처리 단독 테스트
- 구성:
  - SparkSubmitOperator로 etl_to_gold.py 실행
- 특징:
  - BI 연계 전용 Gold 레이어 생성
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
S3_SILVER_PATH = Variable.get("S3_SILVER_PATH")
S3_GOLD_PATH = Variable.get("S3_GOLD_PATH")


# =========================
# Static Config
# =========================
SPARK_CONN_ID = "spark_cluster"


# =========================
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_gold_etl",
    description="TripClick Silver → Gold 집계 처리 DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # 수동 실행 전용
    catchup=False,
    tags=["tripclick", "gold", "etl", "mart"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # ETL to Gold
    # =========================
    etl_to_gold = SparkSubmitOperator(
        task_id="etl_to_gold",
        application="/opt/spark/jobs/etl_to_gold.py",
        conn_id=SPARK_CONN_ID,
        env_vars={
            "S3_SILVER_PATH": S3_SILVER_PATH,
            "S3_GOLD_PATH": S3_GOLD_PATH,
            "AWS_ACCESS_KEY_ID": "{{ conn.aws_s3.login }}",
            "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_s3.password }}",
        },
        verbose=True,
    )

    # =========================
    # Dependencies
    # =========================
    start >> etl_to_gold >> end