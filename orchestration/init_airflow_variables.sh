#!/bin/bash
set -e

source ./env/airflow_variables.env

airflow variables set KAFKA_BROKERS "$KAFKA_BROKERS"
airflow variables set S3_ARCHIVE_RAW_PATH "$S3_ARCHIVE_RAW_PATH"
airflow variables set S3_CURATED_STREAM_PATH "$S3_CURATED_STREAM_PATH"
airflow variables set S3_ANALYTICS_MART_PATH "$S3_ANALYTICS_MART_PATH"
airflow variables set WEBSERVER_INGESTION_PATH "$WEBSERVER_INGESTION_PATH"

echo "✅ Airflow Variables initialized"
