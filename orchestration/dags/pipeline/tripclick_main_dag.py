"""
TripClick Main Pipeline DAG

- 목적:
  전체 파이프라인 오케스트레이션
- 구성:
  - TriggerDagRunOperator로 각 단계별 DAG 순차 호출
- 특징:
  - 운영 스케줄 전용 (KST 00:00 = UTC 15:00)
  - 각 DAG 완료 대기 후 다음 단계 진행
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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
# DAG Definition
# =========================
with DAG(
    dag_id="tripclick_daily_pipeline",
    description="TripClick 전체 데이터 파이프라인 오케스트레이션",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 15 * * *",  # KST 00:00 = UTC 15:00
    catchup=False,
    max_active_runs=1,
    tags=["tripclick", "pipeline", "daily", "orchestration"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Step 1: Ingestion (Producer)
    # =========================
    trigger_producer = TriggerDagRunOperator(
        task_id="trigger_producer",
        trigger_dag_id="tripclick_producer",
        wait_for_completion=True,
        poke_interval=30,
        execution_date="{{ ds }}",
        reset_dag_run=True,
        failed_states=["failed"],
    )

    # =========================
    # Step 2: Processing - Streaming
    # =========================
    trigger_streaming = TriggerDagRunOperator(
        task_id="trigger_streaming",
        trigger_dag_id="tripclick_streaming_curated",
        wait_for_completion=True,
        poke_interval=30,
        execution_date="{{ ds }}",
        reset_dag_run=True,
        failed_states=["failed"],
    )

    # =========================
    # Step 3: Processing - Batch
    # =========================
    trigger_batch = TriggerDagRunOperator(
        task_id="trigger_batch",
        trigger_dag_id="tripclick_spark_archive_raw_batch",
        wait_for_completion=True,
        poke_interval=30,
        execution_date="{{ ds }}",
        reset_dag_run=True,
        failed_states=["failed"],
    )

    # =========================
    # Step 4: Mart - Analytics Mart ETL
    # =========================
    trigger_analytics_mart = TriggerDagRunOperator(
        task_id="trigger_analytics_mart",
        trigger_dag_id="tripclick_analytics_mart_etl",
        wait_for_completion=True,
        poke_interval=30,
        execution_date="{{ ds }}",
        reset_dag_run=True,
        failed_states=["failed"],
    )

    # =========================
    # Step 5: Mart - Load to PostgreSQL
    # =========================
    trigger_load_postgres = TriggerDagRunOperator(
        task_id="trigger_load_postgres",
        trigger_dag_id="tripclick_load_postgres",
        wait_for_completion=True,
        poke_interval=30,
        execution_date="{{ ds }}",
        reset_dag_run=True,
        failed_states=["failed"],
    )

    # =========================
    # Dependencies
    # =========================
    (
        start
        >> trigger_producer
        >> trigger_streaming
        >> trigger_batch
        >> trigger_analytics_mart
        >> trigger_load_postgres
        >> end
    )
