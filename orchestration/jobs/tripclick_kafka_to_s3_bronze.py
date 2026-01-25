from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="tripclick_kafka_to_s3_bronze",
    description="Kafka TripClick logs -> S3 Bronze via Spark batch",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 19),
    schedule_interval=None,   # 🔥 수동 실행 (안정화 후 @daily 가능)
    catchup=False,
    tags=["tripclick", "kafka", "spark", "bronze"],
) as dag:

    kafka_to_s3_bronze = SparkSubmitOperator(
        task_id="kafka_to_s3_bronze",
        application="/opt/spark/jobs/kafka_to_s3_bronze.py",
        name="kafka-to-s3-bronze",
        conn_id="spark_default",

        # 🔑 AWS Access Key → Spark에 환경변수로 전달
        env_vars={
            "AWS_ACCESS_KEY_ID": "{{ conn.aws_s3_conn.login }}",
            "AWS_SECRET_ACCESS_KEY": "{{ conn.aws_s3_conn.password }}",
            "AWS_DEFAULT_REGION": "ap-northeast-2",
        },

        # Spark 설정 (필요 시 추가)
        conf={
            # S3 접근용 (Access Key 방식)
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",

            # Kafka 안정성 (선택)
            "spark.sql.shuffle.partitions": "4",
        },

        verbose=True,
    )

    kafka_to_s3_bronze
