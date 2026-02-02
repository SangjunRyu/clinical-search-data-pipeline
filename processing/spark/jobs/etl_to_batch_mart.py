"""
Spark Batch ETL: S3 Archive Raw → PostgreSQL Batch Mart

Archive Raw 데이터를 읽어 중복 제거 후 4개 Batch Mart를 PostgreSQL에 직접 적재
- mart_session_analysis: 세션별 클릭 분석
- mart_daily_traffic: 일별 트래픽 집계
- mart_clinical_areas: 임상 분야별 검색 통계
- mart_popular_documents: 인기 문서 순위

실행: Airflow DAG에서 daily batch로 트리거
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    min as spark_min,
    max as spark_max,
    first,
    hour,
    explode,
    split,
    trim,
    unix_timestamp,
    row_number,
    desc,
)
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window


# =========================
# JARs (마운트 경로: /opt/spark/jars)
# =========================
JARS_DIR = "/opt/spark/jars"
EXTRA_JARS = ",".join([
    # Hadoop AWS (S3A)
    f"{JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
    # PostgreSQL JDBC
    f"{JARS_DIR}/postgresql-42.6.0.jar",
])


# =========================
# Config
# =========================
def load_config():
    """환경변수에서 설정 로드"""
    return {
        "s3": {
            "archive_raw_path": os.getenv(
                "S3_ARCHIVE_RAW_PATH",
                "s3a://tripclick-lake-sangjun/archive_raw/"
            ),
        },
        "postgres": {
            "host": os.getenv("POSTGRES_HOST", "postgres-mart"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
            "database": os.getenv("POSTGRES_DB", "tripclick_mart"),
            "user": os.getenv("POSTGRES_USER", "mart"),
            "password": os.getenv("POSTGRES_PASSWORD", "mart_password"),
        }
    }


# =========================
# PostgreSQL Writer
# =========================
def write_to_postgres(df, table_name, config):
    """DataFrame을 PostgreSQL 테이블에 적재"""
    pg = config["postgres"]
    jdbc_url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"

    (
        df
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", pg["user"])
        .option("password", pg["password"])
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

    print(f"[INFO] Loaded {df.count()} records to {table_name}")


# =========================
# Main
# =========================
def main():
    config = load_config()
    archive_raw_path = config["s3"]["archive_raw_path"]

    # Spark Session
    spark = (
        SparkSession.builder
        .appName("TripClick-ETL-to-BatchMart")
        .config("spark.jars", EXTRA_JARS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Archive Raw path: {archive_raw_path}")
    print(f"[INFO] PostgreSQL: {config['postgres']['host']}:{config['postgres']['port']}")

    # -----------------------
    # Read Archive Raw Data
    # -----------------------
    raw_df = spark.read.parquet(archive_raw_path)
    total_count = raw_df.count()
    print(f"[INFO] Archive Raw records: {total_count}")

    if total_count == 0:
        print("[WARN] No records in Archive Raw. Exiting.")
        spark.stop()
        return

    # -----------------------
    # Deduplication (dedup_key 기준)
    # -----------------------
    print("[INFO] Deduplicating data using dedup_key...")

    # dedup_key 기준으로 첫 번째 레코드만 유지
    dedup_window = Window.partitionBy("dedup_key").orderBy("kafka_timestamp")
    deduped_df = (
        raw_df
        .withColumn("row_num", row_number().over(dedup_window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    deduped_count = deduped_df.count()
    print(f"[INFO] After deduplication: {deduped_count} records (removed {total_count - deduped_count} duplicates)")

    # 캐시 (여러 마트에서 사용)
    deduped_df.cache()

    # -----------------------
    # 1. 세션 분석 마트
    # -----------------------
    print("[INFO] Creating mart_session_analysis...")

    session_mart = (
        deduped_df
        .groupBy("session_id", "event_date")
        .agg(
            count("*").alias("click_count"),
            countDistinct("document_id").alias("unique_docs"),
            spark_min("event_ts").alias("first_click_ts"),
            spark_max("event_ts").alias("last_click_ts"),
        )
        .withColumn(
            "session_duration_sec",
            (
                unix_timestamp(col("last_click_ts")) -
                unix_timestamp(col("first_click_ts"))
            ).cast(IntegerType())
        )
    )

    try:
        write_to_postgres(session_mart, "mart_session_analysis", config)
    except Exception as e:
        print(f"[ERROR] mart_session_analysis failed: {e}")

    # -----------------------
    # 2. 일별 트래픽 마트
    # -----------------------
    print("[INFO] Creating mart_daily_traffic...")

    # 피크 시간 계산
    hourly_df = (
        deduped_df
        .withColumn("hour", hour(col("event_ts")))
        .groupBy("event_date", "hour")
        .agg(count("*").alias("hourly_count"))
    )

    peak_window = Window.partitionBy("event_date").orderBy(desc("hourly_count"))
    peak_hour_df = (
        hourly_df
        .withColumn("rank", row_number().over(peak_window))
        .filter(col("rank") == 1)
        .select("event_date", col("hour").alias("peak_hour"))
    )

    daily_mart = (
        deduped_df
        .groupBy("event_date")
        .agg(
            count("*").alias("total_events"),
            countDistinct("session_id").alias("unique_sessions"),
            countDistinct("document_id").alias("unique_documents"),
        )
        .join(peak_hour_df, "event_date", "left")
    )

    try:
        write_to_postgres(daily_mart, "mart_daily_traffic", config)
    except Exception as e:
        print(f"[ERROR] mart_daily_traffic failed: {e}")

    # -----------------------
    # 3. 임상 분야 마트
    # -----------------------
    print("[INFO] Creating mart_clinical_areas...")

    clinical_mart = (
        deduped_df
        .filter(col("clinical_areas").isNotNull())
        .filter(col("clinical_areas") != "")
        .withColumn("clinical_area", explode(split(col("clinical_areas"), ",")))
        .withColumn("clinical_area", trim(col("clinical_area")))
        .filter(col("clinical_area") != "")
        .groupBy("event_date", "clinical_area")
        .agg(
            count("*").alias("search_count"),
            countDistinct("session_id").alias("unique_sessions"),
        )
    )

    try:
        write_to_postgres(clinical_mart, "mart_clinical_areas", config)
    except Exception as e:
        print(f"[ERROR] mart_clinical_areas failed: {e}")

    # -----------------------
    # 4. 인기 문서 마트
    # -----------------------
    print("[INFO] Creating mart_popular_documents...")

    popular_mart = (
        deduped_df
        .groupBy("event_date", "document_id", "title")
        .agg(
            count("*").alias("view_count"),
            countDistinct("session_id").alias("unique_sessions"),
        )
        .orderBy(col("event_date"), col("view_count").desc())
    )

    try:
        write_to_postgres(popular_mart, "mart_popular_documents", config)
    except Exception as e:
        print(f"[ERROR] mart_popular_documents failed: {e}")

    # -----------------------
    # Summary
    # -----------------------
    deduped_df.unpersist()

    print("[INFO] ETL to Batch Mart completed successfully.")
    print(f"  - Source: {archive_raw_path}")
    print(f"  - Target: PostgreSQL {config['postgres']['host']}")

    spark.stop()


if __name__ == "__main__":
    main()
