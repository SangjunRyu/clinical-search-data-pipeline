"""
Spark Batch Job: Kafka → S3 Archive Raw Layer

전체 Kafka 데이터를 배치로 읽어 S3 Archive Raw에 저장
- 일배치로 실행 (Daily)
- 전체 offset 읽기 (earliest → latest)
- 파티션: event_date 기준
"""

import os
import yaml
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
    archive_raw_path = config["s3"]["archive_raw_path"]

    # Spark Session
    spark = (
        SparkSession.builder
        .appName("TripClick-Batch-to-ArchiveRaw")
        .config("spark.jars", EXTRA_JARS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Kafka brokers: {kafka_brokers}")
    print(f"[INFO] Archive Raw path: {archive_raw_path}")

    # -----------------------
    # Kafka Batch Read (전체)
    # -----------------------
    kafka_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", "tripclick_raw_logs")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    total_count = kafka_df.count()
    print(f"[INFO] Total records from Kafka: {total_count}")

    if total_count == 0:
        print("[WARN] No records found in Kafka. Exiting.")
        spark.stop()
        return

    # -----------------------
    # Parse JSON
    # -----------------------
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

    # -----------------------
    # Write to S3 Archive Raw (Parquet)
    # -----------------------
    print(f"[INFO] Writing to Archive Raw: {archive_raw_path}")

    (
        parsed_df
        .write
        .mode("append")  # 기존 데이터에 추가
        .partitionBy("event_date")
        .parquet(archive_raw_path)
    )

    print(f"[INFO] Successfully wrote {total_count} records to Archive Raw.")

    # -----------------------
    # 검증
    # -----------------------
    verify_df = spark.read.parquet(archive_raw_path)
    print(f"[INFO] Archive Raw total records: {verify_df.count()}")
    verify_df.printSchema()
    verify_df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
