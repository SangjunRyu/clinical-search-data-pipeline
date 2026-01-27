"""
Spark ETL: Curated Stream → Analytics Mart Layer

Curated Stream 데이터를 분석용 Analytics Mart로 변환
- 세션 분석 마트
- 일별 트래픽 마트
- 임상 분야 마트
- 인기 문서 마트
"""

import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    min as spark_min,
    max as spark_max,
    hour,
    explode,
    split,
    trim,
    when,
    unix_timestamp,
)
from pyspark.sql.types import IntegerType


# =========================
# JARs (마운트 경로: /opt/spark/jars)
# =========================
JARS_DIR = "/opt/spark/jars"
EXTRA_JARS = ",".join([
    # Hadoop AWS (S3A)
    f"{JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
])


# =========================
# Config
# =========================
def load_config():
    """환경변수에서 설정 로드"""
    return {
        "s3": {
            "curated_stream_path": os.getenv("S3_CURATED_STREAM_PATH", "s3a://tripclick-lake-sangjun/curated_stream/"),
            "analytics_mart_path": os.getenv("S3_ANALYTICS_MART_PATH", "s3a://tripclick-lake-sangjun/analytics_mart/"),
        }
    }


# =========================
# Main
# =========================
def main():
    config = load_config()
    curated_stream_path = config["s3"]["curated_stream_path"]
    analytics_mart_path = config["s3"]["analytics_mart_path"]

    # Spark Session
    spark = (
        SparkSession.builder
        .appName("TripClick-ETL-to-AnalyticsMart")
        .config("spark.jars", EXTRA_JARS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Curated Stream path: {curated_stream_path}")
    print(f"[INFO] Analytics Mart path: {analytics_mart_path}")

    # -----------------------
    # Read Curated Stream Data
    # -----------------------
    curated_df = spark.read.parquet(curated_stream_path)
    total_count = curated_df.count()
    print(f"[INFO] Curated Stream records: {total_count}")

    if total_count == 0:
        print("[WARN] No records in Curated Stream. Exiting.")
        spark.stop()
        return

    # -----------------------
    # 1. 세션 분석 마트
    # -----------------------
    print("[INFO] Creating mart_session_analysis...")

    session_mart = (
        curated_df
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

    session_mart.write.mode("overwrite").partitionBy("event_date").parquet(
        f"{analytics_mart_path}mart_session_analysis/"
    )
    print(f"[INFO] mart_session_analysis: {session_mart.count()} records")

    # -----------------------
    # 2. 일별 트래픽 마트
    # -----------------------
    print("[INFO] Creating mart_daily_traffic...")

    # 피크 시간 계산을 위한 시간별 집계
    hourly_df = (
        curated_df
        .withColumn("hour", hour(col("event_ts")))
        .groupBy("event_date", "hour")
        .agg(count("*").alias("hourly_count"))
    )

    # 각 날짜별 피크 시간 찾기
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc

    window_spec = Window.partitionBy("event_date").orderBy(desc("hourly_count"))
    peak_hour_df = (
        hourly_df
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") == 1)
        .select("event_date", col("hour").alias("peak_hour"))
    )

    daily_mart = (
        curated_df
        .groupBy("event_date")
        .agg(
            count("*").alias("total_events"),
            countDistinct("session_id").alias("unique_sessions"),
            countDistinct("document_id").alias("unique_documents"),
        )
        .join(peak_hour_df, "event_date", "left")
    )

    daily_mart.write.mode("overwrite").parquet(
        f"{analytics_mart_path}mart_daily_traffic/"
    )
    print(f"[INFO] mart_daily_traffic: {daily_mart.count()} records")

    # -----------------------
    # 3. 임상 분야 마트
    # -----------------------
    print("[INFO] Creating mart_clinical_areas...")

    # ClinicalAreas 필드 파싱 (쉼표로 구분된 문자열)
    clinical_mart = (
        curated_df
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

    clinical_mart.write.mode("overwrite").partitionBy("event_date").parquet(
        f"{analytics_mart_path}mart_clinical_areas/"
    )
    print(f"[INFO] mart_clinical_areas: {clinical_mart.count()} records")

    # -----------------------
    # 4. 인기 문서 마트
    # -----------------------
    print("[INFO] Creating mart_popular_documents...")

    popular_mart = (
        curated_df
        .groupBy("event_date", "document_id", "title")
        .agg(
            count("*").alias("view_count"),
            countDistinct("session_id").alias("unique_sessions"),
        )
        .orderBy(col("event_date"), col("view_count").desc())
    )

    popular_mart.write.mode("overwrite").partitionBy("event_date").parquet(
        f"{analytics_mart_path}mart_popular_documents/"
    )
    print(f"[INFO] mart_popular_documents: {popular_mart.count()} records")

    # -----------------------
    # Summary
    # -----------------------
    print("[INFO] ETL to Analytics Mart completed successfully.")
    print(f"  - mart_session_analysis: {session_mart.count()} records")
    print(f"  - mart_daily_traffic: {daily_mart.count()} records")
    print(f"  - mart_clinical_areas: {clinical_mart.count()} records")
    print(f"  - mart_popular_documents: {popular_mart.count()} records")

    spark.stop()


if __name__ == "__main__":
    main()
