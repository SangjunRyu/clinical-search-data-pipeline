"""
Spark ETL: Silver → Gold Layer

Silver 데이터를 분석용 Gold 마트로 변환
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
# Config
# =========================
def load_config():
    """환경변수에서 설정 로드"""
    return {
        "s3": {
            "silver_path": os.getenv("S3_SILVER_PATH", "s3a://tripclick-lake-sangjun/silver/"),
            "gold_path": os.getenv("S3_GOLD_PATH", "s3a://tripclick-lake-sangjun/gold/"),
        }
    }


# =========================
# Main
# =========================
def main():
    config = load_config()
    silver_path = config["s3"]["silver_path"]
    gold_path = config["s3"]["gold_path"]

    # Spark Session
    spark = (
        SparkSession.builder
        .appName("TripClick-ETL-to-Gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Silver path: {silver_path}")
    print(f"[INFO] Gold path: {gold_path}")

    # -----------------------
    # Read Silver Data
    # -----------------------
    silver_df = spark.read.parquet(silver_path)
    total_count = silver_df.count()
    print(f"[INFO] Silver records: {total_count}")

    if total_count == 0:
        print("[WARN] No records in Silver. Exiting.")
        spark.stop()
        return

    # -----------------------
    # 1. 세션 분석 마트
    # -----------------------
    print("[INFO] Creating mart_session_analysis...")

    session_mart = (
        silver_df
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
        f"{gold_path}mart_session_analysis/"
    )
    print(f"[INFO] mart_session_analysis: {session_mart.count()} records")

    # -----------------------
    # 2. 일별 트래픽 마트
    # -----------------------
    print("[INFO] Creating mart_daily_traffic...")

    # 피크 시간 계산을 위한 시간별 집계
    hourly_df = (
        silver_df
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
        silver_df
        .groupBy("event_date")
        .agg(
            count("*").alias("total_events"),
            countDistinct("session_id").alias("unique_sessions"),
            countDistinct("document_id").alias("unique_documents"),
        )
        .join(peak_hour_df, "event_date", "left")
    )

    daily_mart.write.mode("overwrite").parquet(
        f"{gold_path}mart_daily_traffic/"
    )
    print(f"[INFO] mart_daily_traffic: {daily_mart.count()} records")

    # -----------------------
    # 3. 임상 분야 마트
    # -----------------------
    print("[INFO] Creating mart_clinical_areas...")

    # ClinicalAreas 필드 파싱 (쉼표로 구분된 문자열)
    clinical_mart = (
        silver_df
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
        f"{gold_path}mart_clinical_areas/"
    )
    print(f"[INFO] mart_clinical_areas: {clinical_mart.count()} records")

    # -----------------------
    # 4. 인기 문서 마트
    # -----------------------
    print("[INFO] Creating mart_popular_documents...")

    popular_mart = (
        silver_df
        .groupBy("event_date", "document_id", "title")
        .agg(
            count("*").alias("view_count"),
            countDistinct("session_id").alias("unique_sessions"),
        )
        .orderBy(col("event_date"), col("view_count").desc())
    )

    popular_mart.write.mode("overwrite").partitionBy("event_date").parquet(
        f"{gold_path}mart_popular_documents/"
    )
    print(f"[INFO] mart_popular_documents: {popular_mart.count()} records")

    # -----------------------
    # Summary
    # -----------------------
    print("[INFO] ETL to Gold completed successfully.")
    print(f"  - mart_session_analysis: {session_mart.count()} records")
    print(f"  - mart_daily_traffic: {daily_mart.count()} records")
    print(f"  - mart_clinical_areas: {clinical_mart.count()} records")
    print(f"  - mart_popular_documents: {popular_mart.count()} records")

    spark.stop()


if __name__ == "__main__":
    main()
