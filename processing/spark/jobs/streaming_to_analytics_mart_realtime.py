"""
Spark Structured Streaming: Curated Stream → PostgreSQL Analytics Mart (Near Real-Time)

Curated Stream 데이터를 1~5분 마이크로배치로 읽어 Hot Analytics Mart를 PostgreSQL에 적재
- mart_realtime_traffic_minute: 분 단위 트래픽
- mart_realtime_top_docs_1h: 최근 1시간 인기 문서 TOP 20
- mart_realtime_clinical_trend_24h: 최근 24시간 임상영역 트렌드
- mart_realtime_anomaly_sessions: 세션 클릭 폭증 이상징후 감지
"""

import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
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
from pyspark.sql.window import Window


# =========================
# Config
# =========================
def load_config():
    """환경변수에서 설정 로드"""
    return {
        "s3": {
            "curated_stream_path": os.getenv("S3_CURATED_STREAM_PATH", "s3a://tripclick-lake-sangjun/curated_stream/"),
            "checkpoint_path": os.getenv(
                "S3_CHECKPOINT_PATH",
                "s3a://tripclick-lake-sangjun/checkpoint/analytics_mart_realtime/"
            ),
        },
        "postgres": {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": os.getenv("POSTGRES_PORT", "5433"),
            "database": os.getenv("POSTGRES_DB", "tripclick_gold"),
            "user": os.getenv("POSTGRES_USER", "gold"),
            "password": os.getenv("POSTGRES_PASSWORD", "gold_password"),
        },
        "trigger_interval": os.getenv("TRIGGER_INTERVAL", "5 minutes"),
        "run_duration_hours": int(os.getenv("RUN_DURATION_HOURS", "1")),
    }


# =========================
# PostgreSQL Upsert Helper
# =========================
def get_jdbc_url(config):
    """PostgreSQL JDBC URL 생성"""
    pg = config["postgres"]
    return f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"


def upsert_to_postgres(df: DataFrame, table_name: str, config: dict, key_columns: list):
    """
    PostgreSQL Upsert (INSERT ... ON CONFLICT DO UPDATE)

    Spark JDBC는 upsert를 직접 지원하지 않으므로,
    temp 테이블에 먼저 적재 후 SQL로 upsert 수행
    """
    if df.isEmpty():
        print(f"[INFO] No data to upsert for {table_name}")
        return

    pg = config["postgres"]
    jdbc_url = get_jdbc_url(config)
    temp_table = f"temp_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

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

    # 2. Upsert SQL 실행 (psycopg2 사용)
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=pg["host"],
            port=pg["port"],
            database=pg["database"],
            user=pg["user"],
            password=pg["password"],
        )
        cursor = conn.cursor()

        # 컬럼 목록 가져오기
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

        print(f"[INFO] Upserted {df.count()} records to {table_name}")

        cursor.close()
        conn.close()

    except ImportError:
        # psycopg2 없으면 단순 overwrite
        print(f"[WARN] psycopg2 not available, using simple write for {table_name}")
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


def simple_write_to_postgres(df: DataFrame, table_name: str, config: dict):
    """단순 INSERT (이상징후 같은 append-only 테이블용)"""
    if df.isEmpty():
        print(f"[INFO] No data to write for {table_name}")
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

    print(f"[INFO] Inserted {df.count()} records to {table_name}")


# =========================
# foreachBatch 처리 함수들
# =========================
def process_realtime_traffic(batch_df: DataFrame, batch_id: int, config: dict):
    """분 단위 트래픽 집계 → mart_realtime_traffic_minute"""
    print(f"[INFO] Processing batch {batch_id} for realtime_traffic...")

    if batch_df.isEmpty():
        print(f"[INFO] Batch {batch_id} is empty, skipping realtime_traffic")
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
    """최근 1시간 인기 문서 TOP 20 → mart_realtime_top_docs_1h (스냅샷)"""
    print(f"[INFO] Processing batch {batch_id} for top_docs...")

    if batch_df.isEmpty():
        print(f"[INFO] Batch {batch_id} is empty, skipping top_docs")
        return

    # 최근 1시간 필터 (batch_df가 이미 최근 데이터라 가정)
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

    # 순위 부여
    window_spec = Window.orderBy(col("click_count").desc())
    top_docs_ranked = (
        top_docs_df
        .withColumn("rank", row_number().over(window_spec))
        .withColumn("snapshot_ts", lit(snapshot_ts))
        .select("snapshot_ts", "rank", "document_id", "title", "click_count", "unique_sessions")
    )

    simple_write_to_postgres(top_docs_ranked, "mart_realtime_top_docs_1h", config)


def process_clinical_trend(batch_df: DataFrame, batch_id: int, config: dict):
    """최근 24시간 임상영역 트렌드 → mart_realtime_clinical_trend_24h (스냅샷)"""
    print(f"[INFO] Processing batch {batch_id} for clinical_trend...")

    if batch_df.isEmpty():
        print(f"[INFO] Batch {batch_id} is empty, skipping clinical_trend")
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
        .withColumn("trend_pct", lit(0.0))  # 전일 대비는 별도 계산 필요
        .select("snapshot_ts", "clinical_area", "click_count", "unique_sessions", "trend_pct")
    )

    simple_write_to_postgres(clinical_df, "mart_realtime_clinical_trend_24h", config)


def process_anomaly_detection(batch_df: DataFrame, batch_id: int, config: dict):
    """세션 클릭 폭증 이상징후 감지 → mart_realtime_anomaly_sessions"""
    print(f"[INFO] Processing batch {batch_id} for anomaly_detection...")

    if batch_df.isEmpty():
        print(f"[INFO] Batch {batch_id} is empty, skipping anomaly_detection")
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
        .filter(col("click_count") >= 50)  # 50회 이상만 이상징후
        .withColumn(
            "severity",
            when(col("click_count") >= 100, "CRITICAL").otherwise("WARNING")
        )
        .withColumn("detected_ts", lit(detected_ts))
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .select("detected_ts", "session_id", "window_start", "window_end", "click_count", "severity")
    )

    if session_clicks.count() > 0:
        simple_write_to_postgres(session_clicks, "mart_realtime_anomaly_sessions", config)
        print(f"[ALERT] Detected {session_clicks.count()} anomaly sessions!")


# =========================
# Main
# =========================
def main():
    config = load_config()
    curated_stream_path = config["s3"]["curated_stream_path"]
    checkpoint_base = config["s3"]["checkpoint_path"]
    trigger_interval = config["trigger_interval"]
    run_duration = config["run_duration_hours"] * 3600  # seconds

    # Spark Session
    spark = (
        SparkSession.builder
        .appName("TripClick-Streaming-to-AnalyticsMart-Realtime")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Curated Stream path: {curated_stream_path}")
    print(f"[INFO] Checkpoint base: {checkpoint_base}")
    print(f"[INFO] Trigger interval: {trigger_interval}")
    print(f"[INFO] Run duration: {run_duration} seconds")

    # -----------------------
    # Streaming Read from Curated Stream (Parquet)
    # -----------------------
    curated_stream = (
        spark.readStream
        .format("parquet")
        .option("path", curated_stream_path)
        .option("maxFilesPerTrigger", 10)  # 배치당 최대 10파일
        .load()
    )

    # -----------------------
    # foreachBatch로 4개 마트 동시 처리
    # -----------------------
    def process_all_marts(batch_df: DataFrame, batch_id: int):
        """모든 Hot Analytics Mart를 한 번에 처리"""
        if batch_df.isEmpty():
            print(f"[INFO] Batch {batch_id} is empty, skipping all marts")
            return

        # 배치 캐시 (여러 번 사용되므로)
        batch_df.cache()
        record_count = batch_df.count()
        print(f"[INFO] Batch {batch_id}: {record_count} records")

        try:
            # 1. 분 단위 트래픽
            process_realtime_traffic(batch_df, batch_id, config)

            # 2. 인기 문서 TOP 20
            process_top_docs(batch_df, batch_id, config)

            # 3. 임상영역 트렌드
            process_clinical_trend(batch_df, batch_id, config)

            # 4. 이상징후 감지
            process_anomaly_detection(batch_df, batch_id, config)

        finally:
            batch_df.unpersist()

    # -----------------------
    # Write Stream
    # -----------------------
    query = (
        curated_stream.writeStream
        .foreachBatch(process_all_marts)
        .option("checkpointLocation", f"{checkpoint_base}analytics_mart_realtime_all/")
        .trigger(processingTime=trigger_interval)
        .start()
    )

    print(f"[INFO] Streaming query started. Running for {run_duration} seconds...")

    # 지정된 시간 동안 실행
    query.awaitTermination(timeout=run_duration)

    print("[INFO] Streaming to Analytics Mart Realtime completed.")
    spark.stop()


if __name__ == "__main__":
    main()
