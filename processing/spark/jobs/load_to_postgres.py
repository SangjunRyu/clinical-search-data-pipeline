"""
Spark Job: Analytics Mart → PostgreSQL

Analytics Mart 데이터를 PostgreSQL에 적재 (Superset 시각화용)
"""

import os
from pyspark.sql import SparkSession


# =========================
# Config
# =========================
def load_config():
    """환경변수에서 설정 로드"""
    return {
        "s3": {
            "analytics_mart_path": os.getenv("S3_ANALYTICS_MART_PATH", "s3a://tripclick-lake-sangjun/analytics_mart/"),
        },
        "postgres": {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": os.getenv("POSTGRES_PORT", "5433"),
            "database": os.getenv("POSTGRES_DB", "tripclick_gold"),
            "user": os.getenv("POSTGRES_USER", "gold"),
            "password": os.getenv("POSTGRES_PASSWORD", "gold_password"),
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
        .mode("overwrite")  # 전체 갱신 (일배치)
        .save()
    )

    print(f"[INFO] Loaded {df.count()} records to {table_name}")


# =========================
# Main
# =========================
def main():
    config = load_config()
    analytics_mart_path = config["s3"]["analytics_mart_path"]

    # Spark Session
    spark = (
        SparkSession.builder
        .appName("TripClick-Load-to-Postgres")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Analytics Mart path: {analytics_mart_path}")
    print(f"[INFO] PostgreSQL: {config['postgres']['host']}:{config['postgres']['port']}")

    # -----------------------
    # 1. 세션 분석 마트
    # -----------------------
    print("[INFO] Loading mart_session_analysis...")
    try:
        session_df = spark.read.parquet(f"{analytics_mart_path}mart_session_analysis/")
        write_to_postgres(session_df, "mart_session_analysis", config)
    except Exception as e:
        print(f"[WARN] mart_session_analysis failed: {e}")

    # -----------------------
    # 2. 일별 트래픽 마트
    # -----------------------
    print("[INFO] Loading mart_daily_traffic...")
    try:
        daily_df = spark.read.parquet(f"{analytics_mart_path}mart_daily_traffic/")
        write_to_postgres(daily_df, "mart_daily_traffic", config)
    except Exception as e:
        print(f"[WARN] mart_daily_traffic failed: {e}")

    # -----------------------
    # 3. 임상 분야 마트
    # -----------------------
    print("[INFO] Loading mart_clinical_areas...")
    try:
        clinical_df = spark.read.parquet(f"{analytics_mart_path}mart_clinical_areas/")
        write_to_postgres(clinical_df, "mart_clinical_areas", config)
    except Exception as e:
        print(f"[WARN] mart_clinical_areas failed: {e}")

    # -----------------------
    # 4. 인기 문서 마트
    # -----------------------
    print("[INFO] Loading mart_popular_documents...")
    try:
        popular_df = spark.read.parquet(f"{analytics_mart_path}mart_popular_documents/")
        write_to_postgres(popular_df, "mart_popular_documents", config)
    except Exception as e:
        print(f"[WARN] mart_popular_documents failed: {e}")

    print("[INFO] Load to PostgreSQL completed.")
    spark.stop()


if __name__ == "__main__":
    main()
