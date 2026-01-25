# ==============================================
# TripClick Spark Processing Image
# ==============================================
# Kafka → S3 Bronze/Silver 처리용 Spark 이미지
#
# Build:
#   docker build -t tripclick-spark:latest .
#
# ==============================================

FROM bitnamilegacy/spark:3.4.1

USER root

# Python 의존성 설치
RUN pip install --no-cache-dir pyyaml

# Spark jobs 복사
COPY jobs /opt/spark/jobs

# Config 복사
COPY config /opt/spark/config

# JAR 복사 (있는 경우만)
# Note: Kafka connector는 --packages로 다운로드 가능
# COPY jars /opt/spark/jars

# 작업 디렉토리 권한 보장
RUN chown -R 1001:1001 /opt/spark

USER 1001

# 환경변수
ENV PYTHONUNBUFFERED=1

