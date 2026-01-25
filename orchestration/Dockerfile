FROM apache/airflow:2.10.5-python3.11

USER root

# =========================
# Java + 필수 유틸 설치
# =========================
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# =========================
# Spark S3A (Hadoop AWS) JAR 추가
# =========================
# Spark 3.4.x ↔ Hadoop 3.3.x 호환
RUN mkdir -p /opt/spark/jars && \
    curl -fLo /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fLo /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Spark Driver(JVM)가 이 JAR들을 항상 보도록 classpath 지정
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars/*

USER airflow

# =========================
# Python 의존성 설치 (Airflow constraint 사용)
# =========================
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
    -r /tmp/requirements.txt

# 🔥 redis만 강제로 재설치 (constraint override)
RUN pip install --no-cache-dir --force-reinstall "redis==4.6.0"