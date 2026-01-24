# ==============================================
# TripClick Producer Docker Image
# ==============================================
# Airflow DockerOperator에서 Remote Docker API로 실행
#
# Build:
#   docker build -t tripclick-producer:latest .
#
# Run (local test):
#   docker run --rm \
#     -v $(pwd)/data:/app/data \
#     -v $(pwd)/config:/app/config \
#     -e KAFKA_BROKERS=kafka:9092 \
#     tripclick-producer:latest \
#     --file /app/data/server0/date=2026-01-01/events.json \
#     --topic tripclick_raw_logs \
#     --mode realtime
# ==============================================

FROM python:3.11-slim

LABEL maintainer="TripClick Pipeline Team"
LABEL description="Kafka Producer for TripClick log ingestion"

# 작업 디렉터리 설정
WORKDIR /app

# 시스템 의존성 설치 (최소화)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY producer/ ./producer/
COPY utils/ ./utils/
COPY config/config.yaml.example ./config/config.yaml.example

# 로그 디렉터리 생성
RUN mkdir -p /app/logs

# 환경변수 기본값
ENV KAFKA_BROKERS=""
ENV PYTHONUNBUFFERED=1

# 엔트리포인트 설정
ENTRYPOINT ["python", "producer/producer.py"]

# 기본 인자 (override 가능)
CMD ["--help"]
