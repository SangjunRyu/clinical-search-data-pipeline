"""
Spark Structured Streaming: Kafka → S3 Silver Layer

실시간 스트리밍으로 Kafka 데이터를 읽어 dedup 처리 후 S3 Silver에 저장
- Watermark 기반 중복 제거 (dedup_key 사용)
- 1시간 실행 후 종료 (processingTime trigger)
"""

import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    expr,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, ArrayType
)


# =========================
# Config
# =========================
def load_config(path="/opt/spark/config/config.yaml"):
    """설정 파일 로드 (환경변수 오버라이드 지원)"""
    config = {
        "kafka": {"brokers": ["localhost:9092"]},
        "s3": {"silver_path": "s3a://tripclick-lake/silver/"}
    }

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

    # 환경변수 오버라이드
    if os.getenv("KAFKA_BROKERS"):
        config["kafka"]["brokers"] = os.getenv("KAFKA_BROKERS").split(",")
    if os.getenv("S3_SILVER_PATH"):
        config["s3"]["silver_path"] = os.getenv("S3_SILVER_PATH")

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
    StructField("dedup_key", StringType()),  # Producer가 추가한 dedup 키
])


# =========================
# Main
# =========================
def main():
    config = load_config()
    kafka_brokers = ",".join(config["kafka"]["brokers"])
    silver_path = config["s3"]["silver_path"]
    checkpoint_path = silver_path.rstrip("/").rsplit("/", 1)[0] + "/checkpoint/silver"

    # Spark Session
    spark = (
        SparkSession.builder
        .appName("TripClick-Streaming-to-Silver")
        .config("spark.sql.streaming.checkpointLocation", checkpoint_path)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Kafka brokers: {kafka_brokers}")
    print(f"[INFO] Silver path: {silver_path}")
    print(f"[INFO] Checkpoint path: {checkpoint_path}")

    # -----------------------
    # Kafka Streaming Read
    # -----------------------
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", "tripclick_raw_logs")
        .option("startingOffsets", "latest")  # 실시간: 최신부터
        .option("failOnDataLoss", "false")
        .load()
    )

    # -----------------------
    # Parse JSON
    # -----------------------
    parsed_stream = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .withColumn("data", from_json(col("json_str"), TRIPCLICK_SCHEMA))
        .select(
            col("data.SessionId").alias("session_id"),
            col("data.DocumentId").alias("document_id"),
            col("data.Url").alias("url"),
            col("data.Title").alias("title"),
            col("data.DOI").alias("doi"),
            col("data.ClinicalAreas").alias("clinical_areas"),
            col("data.Keywords").alias("keywords"),
            to_timestamp(col("data.event_ts")).alias("event_ts"),
            col("data.event_date").alias("event_date"),
            col("data.dedup_key").alias("dedup_key"),
        )
        .filter(col("dedup_key").isNotNull())  # dedup_key 없는 레코드 제외
    )

    # -----------------------
    # Dedup with Watermark
    # -----------------------
    # Watermark: 10분 지연 허용
    # dropDuplicates: dedup_key 기준 중복 제거
    deduped_stream = (
        parsed_stream
        .withWatermark("event_ts", "10 minutes")
        .dropDuplicates(["dedup_key"])
    )

    # -----------------------
    # Write to S3 Silver (Parquet)
    # -----------------------
    query = (
        deduped_stream.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", silver_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("event_date")
        .trigger(processingTime="30 seconds")  # 30초마다 마이크로배치
        .start() # .maxRecordsPerFile()
    )

    print("[INFO] Streaming query started. Running for 1 hour...")

    # 1시간 실행 후 종료
    query.awaitTermination(timeout=3600)  # 3600초 = 1시간

    print("[INFO] Streaming query completed.")
    spark.stop()


if __name__ == "__main__":
    main()
