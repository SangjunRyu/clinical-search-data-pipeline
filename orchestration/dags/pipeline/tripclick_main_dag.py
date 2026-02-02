"""
TripClick Main Pipeline DAG

- 목적:
  Daily Batch 파이프라인 오케스트레이션
- 구성:
  1. Producer (Kafka로 데이터 전송)
  2. batch_to_archive_raw (Kafka → S3)
  3. etl_to_batch_mart (S3 → PostgreSQL)
- 스케줄:
  매일 KST 00:00 (UTC 15:00)
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
    description="TripClick Daily Batch 파이프라인 오케스트레이션",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 15 * * *",  # KST 00:00 = UTC 15:00
    catchup=False,
    max_active_runs=1,
    tags=["tripclick", "pipeline", "daily", "orchestration"],
    doc_md="""
    ## TripClick Daily Pipeline

    ### 실행 흐름
    ```
    Producer (Kafka) → batch_to_archive_raw (S3) → etl_to_batch_mart (PostgreSQL)
    ```

    ### 단계별 설명
    | 단계 | DAG ID | 설명 |
    |------|--------|------|
    | 1 | `tripclick_producer_batch` | 웹서버 이벤트를 Kafka로 전송 |
    | 2 | `tripclick_spark_archive_raw_batch` | Kafka → S3 Archive Raw 저장 |
    | 3 | `tripclick_batch_mart` | S3 → PostgreSQL Batch Mart 적재 |

    ### 스케줄
    - 매일 KST 00:00 (UTC 15:00) 실행
    - 전날 데이터 처리

    ### 참고
    - Realtime Mart는 별도 DAG (`tripclick_realtime_mart`)로 독립 운영
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # =========================
    # Step 1: Ingestion (Producer)
    # =========================
    trigger_producer = TriggerDagRunOperator(
        task_id="trigger_producer",
        trigger_dag_id="tripclick_producer_batch",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=["failed"],
    )

    # =========================
    # Step 2: Processing - Batch to Archive Raw
    # =========================
    trigger_archive_raw = TriggerDagRunOperator(
        task_id="trigger_archive_raw",
        trigger_dag_id="tripclick_spark_archive_raw_batch",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=["failed"],
    )

    # =========================
    # Step 3: ETL to Batch Mart
    # =========================
    trigger_batch_mart = TriggerDagRunOperator(
        task_id="trigger_batch_mart",
        trigger_dag_id="tripclick_batch_mart",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=["failed"],
    )

    # =========================
    # Dependencies
    # =========================
    (
        start
        >> trigger_producer
        >> trigger_archive_raw
        >> trigger_batch_mart
        >> end
    )
