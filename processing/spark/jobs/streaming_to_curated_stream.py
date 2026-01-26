"""
Spark Structured Streaming: Kafka → S3 Curated Stream Layer

실시간 스트리밍으로 Kafka 데이터를 읽어 dedup 처리 후 S3 Curated Stream에 저장
- Watermark 기반 중복 제거 (dedup_key 사용)
- 1시간 실행 후 종료 (processingTime trigger)
- 종료 후 Compaction: 작은 parquet 파일들을 event_date별 1개로 병합
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
        "s3": {"curated_stream_path": "s3a://tripclick-lake-sangjun/curated_stream/"}
    }

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

    # 환경변수 오버라이드
    if os.getenv("KAFKA_BROKERS"):
        config["kafka"]["brokers"] = os.getenv("KAFKA_BROKERS").split(",")
    if os.getenv("S3_CURATED_STREAM_PATH"):
        config["s3"]["curated_stream_path"] = os.getenv("S3_CURATED_STREAM_PATH")

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
    curated_stream_path = config["s3"]["curated_stream_path"]
    checkpoint_path = curated_stream_path.rstrip("/").rsplit("/", 1)[0] + "/checkpoint/curated_stream"

    # Spark Session
    spark = (
        SparkSession.builder
        .appName("TripClick-Streaming-to-CuratedStream")
        .config("spark.sql.streaming.checkpointLocation", checkpoint_path)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Kafka brokers: {kafka_brokers}")
    print(f"[INFO] Curated Stream path: {curated_stream_path}")
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
    # Write to S3 Curated Stream (Parquet)
    # -----------------------
    # foreachBatch + coalesce(1)로 배치당 파티션별 1개 파일로 합쳐서 쓰기
    def write_batch(batch_df, _batch_id):
        if batch_df.isEmpty():
            return
        (
            batch_df
            .coalesce(1)
            .write
            .mode("append")
            .partitionBy("event_date")
            .parquet(curated_stream_path)
        )

    query = (
        deduped_stream.writeStream
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="1 minute")
        .foreachBatch(write_batch)
        .start()
    )

    print("[INFO] Streaming query started. Running for 1 hour...")

    # 1시간 실행 후 종료
    query.awaitTermination(timeout=3600)  # 3600초 = 1시간

    print("[INFO] Streaming completed. Starting compaction...")

    # -----------------------
    # Compaction: 작은 parquet 파일들을 event_date별 1개로 병합
    # -----------------------
    compact_df = spark.read.parquet(curated_stream_path)
    record_count = compact_df.count()

    if record_count > 0:
        compact_tmp = curated_stream_path.rstrip("/") + "_compacted"
        (
            compact_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(compact_tmp)
        )

        # 기존 curated_stream 덮어쓰기: tmp → curated_stream
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(curated_stream_path), hadoop_conf
        )
        curated_hdfs = spark._jvm.org.apache.hadoop.fs.Path(curated_stream_path)
        tmp_hdfs = spark._jvm.org.apache.hadoop.fs.Path(compact_tmp)

        # 기존 파일 삭제 후 tmp를 curated_stream으로 이동
        fs.delete(curated_hdfs, True)
        fs.rename(tmp_hdfs, curated_hdfs)

        print(f"[INFO] Compaction done. {record_count} records → event_date별 1 parquet")
    else:
        print("[INFO] No records to compact. Skipping.")

    spark.stop()


if __name__ == "__main__":
    main()
