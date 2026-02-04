"""
Spark Batch Job: Kafka → S3 Archive Raw Layer

과거 데이터를 Kafka에서 읽어 S3 Archive Raw에 저장
- Producer가 과거 데이터를 현재 시점에 Kafka로 전송하는 시나리오
- Kafka timestamp (도착 시점) 기준으로 읽기 범위 지정
- event_date (원본 이벤트 시간) 기준으로 S3 파티션

핵심 전략:
1. Kafka timestamp 기반 범위 읽기 (startingOffsetsByTimestamp)
2. event_date 파티션 기준 Dynamic Overwrite (중복 방지)
3. Consumer Group 사용 안 함 (Stateless Batch)
4. Airflow execution_date 연동으로 Idempotent 처리
"""

import os
import yaml
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    lit,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, ArrayType
)


# =========================
# JARs (마운트 경로: /opt/spark/jars)
# =========================
JARS_DIR = "/opt/spark/jars"
EXTRA_JARS = ",".join([
    # Kafka connector
    f"{JARS_DIR}/spark-sql-kafka-0-10_2.12-3.4.1.jar",
    f"{JARS_DIR}/kafka-clients-3.3.2.jar",
    f"{JARS_DIR}/commons-pool2-2.11.1.jar",
    f"{JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.4.1.jar",
    # Hadoop AWS (S3A)
    f"{JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
])


# =========================
# Config
# =========================
def load_config(path="/opt/spark/config/config.yaml"):
    """설정 파일 로드 (환경변수 오버라이드 지원)"""
    config = {
        "kafka": {"brokers": ["localhost:9092"]},
        "s3": {"archive_raw_path": "s3a://tripclick-lake-sangjun/archive_raw/"}
    }

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

    # 환경변수 오버라이드
    if os.getenv("KAFKA_BROKERS"):
        config["kafka"]["brokers"] = os.getenv("KAFKA_BROKERS").split(",")
    if os.getenv("S3_ARCHIVE_RAW_PATH"):
        config["s3"]["archive_raw_path"] = os.getenv("S3_ARCHIVE_RAW_PATH")

    return config


# =========================
# Schema
# =========================
TRIPCLICK_SCHEMA = StructType([
    StructField("DateCreated", StringType()),
    StructField("SessionId", StringType()),
    StructField("DocumentId", IntegerType()),
    StructField("Url", StringType()),
    StructField("Title", StringType()),
    StructField("DOI", StringType()),
    StructField("ClinicalAreas", StringType()),
    StructField("Keywords", StringType()),
    StructField("Documents", ArrayType(StringType())),
    StructField("event_ts", StringType()),
    StructField("event_date", StringType()),
    StructField("dedup_key", StringType()),
])


# =========================
# Main
# =========================
def main():
    config = load_config()
    kafka_brokers = ",".join(config["kafka"]["brokers"])
    kafka_topic = config["kafka"].get("topic", "tripclick_raw_logs")
    archive_raw_path = config["s3"]["archive_raw_path"]

    # Spark Session
    spark = (
        SparkSession.builder
        .appName("TripClick-Batch-to-ArchiveRaw")
        .config("spark.jars", EXTRA_JARS)
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Kafka brokers: {kafka_brokers}")
    print(f"[INFO] Kafka topic: {kafka_topic}")
    print(f"[INFO] Archive Raw path: {archive_raw_path}")

    # =========================
    # Execution Date (Airflow 연동)
    # =========================
    # EXECUTION_DATE: 처리 대상 날짜 (Airflow ds)
    # 없으면 어제 날짜를 기본값으로 사용
    execution_date_str = os.getenv("EXECUTION_DATE")
    if execution_date_str:
        execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d")
    else:
        # 기본값: 오늘 00:00 기준
        execution_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Kafka timestamp 범위: execution_date 전날 00:00 ~ execution_date 00:00
    # 예: EXECUTION_DATE=2026-02-03 → 2026-02-02 00:00 ~ 2026-02-03 00:00
    start_dt = execution_date - timedelta(days=1)
    end_dt = execution_date

    start_ts = int(start_dt.timestamp() * 1000)  # milliseconds
    end_ts = int(end_dt.timestamp() * 1000)

    print(f"[INFO] Execution date: {execution_date_str or 'not set (using today)'}")
    print(f"[INFO] Kafka timestamp range: {start_dt} ~ {end_dt}")
    print(f"[INFO] Timestamp (ms): {start_ts} ~ {end_ts}")

    # =========================
    # Kafka Batch Read (Timestamp 기반)
    # =========================
    # Consumer Group 사용 안 함 (Stateless Batch)
    # startingTimestamp/endingTimestamp: 모든 파티션에 동일 timestamp 적용
    kafka_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", kafka_topic)
        .option("startingTimestamp", start_ts)
        .option("endingTimestamp", end_ts)
        .load()
    )

    total_count = kafka_df.count()
    print(f"[INFO] Total records from Kafka: {total_count}")

    if total_count == 0:
        print("[WARN] No records found in Kafka. Exiting.")
        spark.stop()
        return

    # =========================
    # Parse JSON
    # =========================
    parsed_df = (
        kafka_df
        .selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as json_str",
            "topic",
            "partition",
            "offset",
            "timestamp as kafka_timestamp",
        )
        .withColumn("data", from_json(col("json_str"), TRIPCLICK_SCHEMA))
        .select(
            # Kafka 메타데이터
            col("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            # 원본 데이터
            col("data.DateCreated").alias("date_created"),
            col("data.SessionId").alias("session_id"),
            col("data.DocumentId").alias("document_id"),
            col("data.Url").alias("url"),
            col("data.Title").alias("title"),
            col("data.DOI").alias("doi"),
            col("data.ClinicalAreas").alias("clinical_areas"),
            col("data.Keywords").alias("keywords"),
            col("data.Documents").alias("documents"),
            to_timestamp(col("data.event_ts")).alias("event_ts"),
            col("data.event_date").alias("event_date"),
            col("data.dedup_key").alias("dedup_key"),
            # 적재 메타데이터
            current_timestamp().alias("ingested_at"),
            lit("batch").alias("ingestion_type"),
        )
    )

    # 파싱된 데이터 검증
    valid_df = parsed_df.filter(col("event_date").isNotNull())
    valid_count = valid_df.count()
    print(f"[INFO] Valid records (with event_date): {valid_count}")

    if valid_count == 0:
        print("[WARN] No valid records after parsing. Exiting.")
        spark.stop()
        return

    # event_date 분포 확인
    print("[INFO] Event date distribution:")
    valid_df.groupBy("event_date").count().orderBy("event_date").show(truncate=False)

    # =========================
    # Write to S3 Archive Raw
    # =========================
    # Dynamic Partition Overwrite: 해당 event_date 파티션만 덮어쓰기
    # → 같은 날짜 재실행해도 중복 없음
    # → 다른 날짜 파티션은 건드리지 않음
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    print(f"[INFO] Writing to Archive Raw: {archive_raw_path}")
    print("[INFO] Using Dynamic Partition Overwrite mode")

    (
        valid_df
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(archive_raw_path)
    )

    print(f"[INFO] Successfully wrote {valid_count} records to Archive Raw.")

    # =========================
    # 검증
    # =========================
    verify_df = spark.read.parquet(archive_raw_path)
    print(f"[INFO] Archive Raw total records: {verify_df.count()}")
    print("[INFO] Archive Raw partitions:")
    verify_df.groupBy("event_date").count().orderBy("event_date").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
