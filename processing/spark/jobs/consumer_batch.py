# 테스트를 위한 batch job
# 세션 별 클릭 수, 일자 별 이벤트 수 집계

import yaml
import os
from pyspark.sql import SparkSession

def load_config(path="config/config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Kafka brokers override
    brokers_env = os.getenv("KAFKA_BROKERS")
    if brokers_env:
        config["kafka"]["brokers"] = brokers_env.split(",")

from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    count
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
])

# -----------------------
# Spark Session
# -----------------------
spark = (
    SparkSession.builder
    .appName("TripClick-Batch-Consumer")
    .config("spark.jars", EXTRA_JARS)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
config = load_config()
kafka_brokers = ",".join(config["kafka"]["brokers"])

# -----------------------
# Kafka batch read
# -----------------------
kafka_df = (
    spark.read
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        kafka_brokers
    )
    .option("subscribe", "tripclick_raw_logs")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

print(f"📥 Kafka raw count: {kafka_df.count()}")

# -----------------------
# value parsing
# -----------------------
raw_df = kafka_df.selectExpr(
    "CAST(key AS STRING) as session_id",
    "CAST(value AS STRING) as json_str"
)

schema = StructType([
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
])

parsed_df = (
    raw_df
    .withColumn("data", from_json(col("json_str"), schema))
    .select(
        col("session_id"),
        col("data.*"),
        to_timestamp(col("event_ts")).alias("event_time")
    )
)

# -----------------------
# 기본 검증
# -----------------------
parsed_df.show(5, truncate=False)
parsed_df.printSchema()

# -----------------------
# 배치 분석 예시 1
# 세션별 클릭 수
# -----------------------
session_agg_df = (
    parsed_df
    .groupBy("SessionId")
    .agg(count("*").alias("click_count"))
    .orderBy(col("click_count").desc())
)

print("📊 Session click counts")
session_agg_df.show(10, truncate=False)

# -----------------------
# 배치 분석 예시 2
# 일자별 이벤트 수
# -----------------------
daily_agg_df = (
    parsed_df
    .groupBy("event_date")
    .agg(count("*").alias("event_count"))
    .orderBy("event_date")
)

print("📊 Daily event counts")
daily_agg_df.show(truncate=False)

spark.stop()
