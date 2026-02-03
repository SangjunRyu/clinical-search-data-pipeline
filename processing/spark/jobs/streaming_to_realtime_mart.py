"""
Spark Structured Streaming: Kafka → PostgreSQL Realtime Mart

Lambda Architecture의 Speed Layer 구현
- Kafka에서 단일 Consumer Group으로 스트림 소비
- 5분 마이크로배치로 4개 Realtime Mart를 PostgreSQL에 적재
- Batch Layer(S3 Archive)와 독립적으로 운영

Realtime Mart 테이블:
- mart_realtime_traffic_minute: 분 단위 트래픽 (Upsert)
- mart_realtime_top_docs_1h: 최근 1시간 인기 문서 TOP 20 (Snapshot Append)
- mart_realtime_clinical_trend_24h: 임상영역 트렌드 (Snapshot Append)
- mart_realtime_anomaly_sessions: 세션 클릭 폭증 감지 (Event Append)

핵심 전략:
1. Consumer Group 1개로 Kafka 소비 (Spark 내부에서 branching)
2. Checkpoint 기반 오프셋 관리 (exactly-once semantics)
3. dedup_key + watermark로 중복 제거
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    count,
    countDistinct,
    date_trunc,
    current_timestamp,
    lit,
    row_number,
    when,
    explode,
    split,
    trim,
    window,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, ArrayType
)
from pyspark.sql.window import Window

# psycopg2 import (upsert용)
try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


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
    # Hadoop AWS (S3A - for checkpoint)
    f"{JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
    # PostgreSQL JDBC
    f"{JARS_DIR}/postgresql-42.6.0.jar",
])


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
# Config
# =========================
def load_config():
    """환경변수에서 설정 로드"""
    return {
        "kafka": {
            "brokers": os.getenv("KAFKA_BROKERS", "localhost:9092"),
            "topic": os.getenv("KAFKA_TOPIC", "tripclick_raw_logs"),
            "group_id": os.getenv("KAFKA_GROUP_ID", "tripclick-realtime-mart"),
        },
        "checkpoint": {
            "path": os.getenv(
                "S3_CHECKPOINT_PATH",
                "s3a://tripclick-lake-sangjun/checkpoint/realtime_mart/"
            ),
        },
        "postgres": {
            "host": os.getenv("POSTGRES_HOST", "postgres-mart"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
            "database": os.getenv("POSTGRES_DB", "tripclick_mart"),
            "user": os.getenv("POSTGRES_USER", "mart"),
            "password": os.getenv("POSTGRES_PASSWORD", "mart_password"),
        },
        "trigger_interval": os.getenv("TRIGGER_INTERVAL", "5 minutes"),
        "run_duration_hours": int(os.getenv("RUN_DURATION_HOURS", "1")),
    }


# =========================
# PostgreSQL Helpers
# =========================
def get_jdbc_url(config):
    """PostgreSQL JDBC URL 생성"""
    pg = config["postgres"]
    return f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"


def upsert_to_postgres(df: DataFrame, table_name: str, config: dict, key_columns: list):
    """PostgreSQL Upsert (temp table → INSERT ON CONFLICT)"""
    if df.isEmpty():
        print(f"[INFO] No data to upsert for {table_name}")
        return

    pg = config["postgres"]
    jdbc_url = get_jdbc_url(config)

    # psycopg2 없으면 단순 append로 fallback
    if not PSYCOPG2_AVAILABLE:
        print(f"[WARN] psycopg2 not available, using append for {table_name}")
        (
            df
            .write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", pg["user"])
            .option("password", pg["password"])
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        return

    # Upsert: temp table → INSERT ON CONFLICT
    temp_table = f"temp_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

    # 1. Temp 테이블에 적재
    (
        df
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", temp_table)
        .option("user", pg["user"])
        .option("password", pg["password"])
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    # 2. Upsert SQL 실행
    conn = None
    try:
        conn = psycopg2.connect(
            host=pg["host"],
            port=pg["port"],
            database=pg["database"],
            user=pg["user"],
            password=pg["password"],
        )
        cursor = conn.cursor()

        columns = df.columns
        key_cols_str = ", ".join(key_columns)
        update_cols = [c for c in columns if c not in key_columns]
        update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

        upsert_sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM {temp_table}
            ON CONFLICT ({key_cols_str}) DO UPDATE SET {update_set}
        """

        cursor.execute(upsert_sql)
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        conn.commit()

        print(f"[INFO] Upserted to {table_name}")
        cursor.close()

    except Exception as e:
        print(f"[ERROR] Upsert failed for {table_name}: {e}")
    finally:
        if conn:
            conn.close()


def append_to_postgres(df: DataFrame, table_name: str, config: dict):
    """단순 INSERT (append-only 테이블용)"""
    if df.isEmpty():
        print(f"[INFO] No data to append for {table_name}")
        return

    pg = config["postgres"]
    jdbc_url = get_jdbc_url(config)

    (
        df
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", pg["user"])
        .option("password", pg["password"])
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(f"[INFO] Appended to {table_name}")


# =========================
# Mart Processing Functions
# =========================
def process_realtime_traffic(batch_df: DataFrame, batch_id: int, config: dict):
    """분 단위 트래픽 집계 → mart_realtime_traffic_minute"""
    if batch_df.isEmpty():
        return

    traffic_df = (
        batch_df
        .withColumn("event_minute", date_trunc("minute", col("event_ts")))
        .groupBy("event_minute")
        .agg(
            count("*").alias("total_clicks"),
            countDistinct("session_id").alias("unique_sessions"),
            countDistinct("document_id").alias("unique_docs"),
        )
        .withColumn("updated_at", current_timestamp())
    )

    upsert_to_postgres(traffic_df, "mart_realtime_traffic_minute", config, ["event_minute"])


def process_top_docs(batch_df: DataFrame, batch_id: int, config: dict):
    """인기 문서 TOP 20 → mart_realtime_top_docs_1h"""
    if batch_df.isEmpty():
        return

    snapshot_ts = datetime.now()

    top_docs_df = (
        batch_df
        .groupBy("document_id", "title")
        .agg(
            count("*").alias("click_count"),
            countDistinct("session_id").alias("unique_sessions"),
        )
        .orderBy(col("click_count").desc())
        .limit(20)
    )

    window_spec = Window.orderBy(col("click_count").desc())
    top_docs_ranked = (
        top_docs_df
        .withColumn("rank", row_number().over(window_spec))
        .withColumn("snapshot_ts", lit(snapshot_ts))
        .select("snapshot_ts", "rank", "document_id", "title", "click_count", "unique_sessions")
    )

    append_to_postgres(top_docs_ranked, "mart_realtime_top_docs_1h", config)


def process_clinical_trend(batch_df: DataFrame, batch_id: int, config: dict):
    """임상영역 트렌드 → mart_realtime_clinical_trend_24h"""
    if batch_df.isEmpty():
        return

    snapshot_ts = datetime.now()

    clinical_df = (
        batch_df
        .filter(col("clinical_areas").isNotNull())
        .filter(col("clinical_areas") != "")
        .withColumn("clinical_area", explode(split(col("clinical_areas"), ",")))
        .withColumn("clinical_area", trim(col("clinical_area")))
        .filter(col("clinical_area") != "")
        .groupBy("clinical_area")
        .agg(
            count("*").alias("click_count"),
            countDistinct("session_id").alias("unique_sessions"),
        )
        .withColumn("snapshot_ts", lit(snapshot_ts))
        .withColumn("trend_pct", lit(0.0))
        .select("snapshot_ts", "clinical_area", "click_count", "unique_sessions", "trend_pct")
    )

    append_to_postgres(clinical_df, "mart_realtime_clinical_trend_24h", config)


def process_anomaly_detection(batch_df: DataFrame, batch_id: int, config: dict):
    """세션 클릭 폭증 감지 → mart_realtime_anomaly_sessions"""
    if batch_df.isEmpty():
        return

    detected_ts = datetime.now()

    # 5분 윈도우 내 세션별 클릭 수 집계
    session_clicks = (
        batch_df
        .groupBy(
            window(col("event_ts"), "5 minutes"),
            "session_id"
        )
        .agg(count("*").alias("click_count"))
        .filter(col("click_count") >= 50)
        .withColumn(
            "severity",
            when(col("click_count") >= 100, "CRITICAL").otherwise("WARNING")
        )
        .withColumn("detected_ts", lit(detected_ts))
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .select("detected_ts", "session_id", "window_start", "window_end", "click_count", "severity")
    )

    # count() 한 번만 호출
    anomaly_count = session_clicks.count()
    if anomaly_count > 0:
        append_to_postgres(session_clicks, "mart_realtime_anomaly_sessions", config)
        print(f"[ALERT] Detected {anomaly_count} anomaly sessions!")


# =========================
# Main
# =========================
def main():
    config = load_config()
    kafka_brokers = config["kafka"]["brokers"]
    kafka_topic = config["kafka"]["topic"]
    kafka_group_id = config["kafka"]["group_id"]
    checkpoint_path = config["checkpoint"]["path"]
    trigger_interval = config["trigger_interval"]
    run_duration = config["run_duration_hours"] * 3600

    # Spark Session
    spark = (
        SparkSession.builder
        .appName("TripClick-Streaming-to-RealtimeMart")
        .config("spark.jars", EXTRA_JARS)
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Kafka: {kafka_brokers} / {kafka_topic}")
    print(f"[INFO] Consumer Group: {kafka_group_id}")
    print(f"[INFO] Checkpoint: {checkpoint_path}")
    print(f"[INFO] PostgreSQL: {config['postgres']['host']}:{config['postgres']['port']}")
    print(f"[INFO] Trigger interval: {trigger_interval}")
    print(f"[INFO] Run duration: {run_duration} seconds")
    print(f"[INFO] psycopg2 available: {PSYCOPG2_AVAILABLE}")

    # -----------------------
    # Kafka Streaming Read
    # -----------------------
    # Consumer Group: 단일 그룹으로 Kafka 소비 (Spark 내부에서 4개 마트로 branching)
    # startingOffsets: latest
    #   - 체크포인트 있으면 → 체크포인트에서 오프셋 복원
    #   - 체크포인트 없으면 → 최신부터 시작 (과거 데이터는 버림)
    #   - 실시간 마트는 "현재 상황"이 목적이므로 latest가 적합
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", kafka_topic)
        .option("kafka.group.id", kafka_group_id)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")  # 파티션 재할당 시 에러 방지
        .load()
    )

    # Parse JSON
    parsed_stream = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .withColumn("data", from_json(col("json_str"), TRIPCLICK_SCHEMA))
        .select(
            col("data.SessionId").alias("session_id"),
            col("data.DocumentId").alias("document_id"),
            col("data.Title").alias("title"),
            col("data.ClinicalAreas").alias("clinical_areas"),
            to_timestamp(col("data.event_ts")).alias("event_ts"),
            col("data.event_date").alias("event_date"),
            col("data.dedup_key").alias("dedup_key"),
        )
        .withWatermark("event_ts", "10 minutes")
        .dropDuplicates(["dedup_key"])
    )

    # -----------------------
    # foreachBatch 처리
    # -----------------------
    def process_all_marts(batch_df: DataFrame, batch_id: int):
        """모든 Realtime Mart를 한 번에 처리"""
        if batch_df.isEmpty():
            print(f"[INFO] Batch {batch_id} is empty, skipping")
            return

        batch_df.cache()
        record_count = batch_df.count()
        print(f"[INFO] Batch {batch_id}: {record_count} records")

        try:
            process_realtime_traffic(batch_df, batch_id, config)
            process_top_docs(batch_df, batch_id, config)
            process_clinical_trend(batch_df, batch_id, config)
            process_anomaly_detection(batch_df, batch_id, config)
        finally:
            batch_df.unpersist()

    # -----------------------
    # Start Streaming Query
    # -----------------------
    query = (
        parsed_stream.writeStream
        .foreachBatch(process_all_marts)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    print(f"[INFO] Streaming started. Running for {run_duration} seconds...")

    query.awaitTermination(timeout=run_duration)

    print("[INFO] Streaming to Realtime Mart completed.")
    spark.stop()


if __name__ == "__main__":
    main()
