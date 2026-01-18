FROM apache/airflow:2.10.5

USER root

# Java 설치 (Debian 12에서 동작하는 JDK17)
RUN apt-get update && apt-get install -y openjdk-17-jre-headless curl procps

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt" \
    -r /requirements.txt