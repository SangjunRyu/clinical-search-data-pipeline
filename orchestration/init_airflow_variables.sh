#!/bin/bash
set -e

source ./env/airflow_variables.env

airflow variables set KAFKA_BROKERS "$KAFKA_BROKERS"
airflow variables set S3_BRONZE_PATH "$S3_BRONZE_PATH"
airflow variables set S3_SILVER_PATH "$S3_SILVER_PATH"
airflow variables set S3_GOLD_PATH "$S3_GOLD_PATH"
airflow variables set WEBSERVER_INGESTION_PATH "$WEBSERVER_INGESTION_PATH"

echo "✅ Airflow Variables initialized"
