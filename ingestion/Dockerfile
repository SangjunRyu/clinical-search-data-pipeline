# ==============================================
# TripClick Producer Docker Image
# ==============================================
# 이미지에 config.yaml과 data를 포함하여 간단하게 실행
#
# Build:
#   docker build -t tripclick-producer:latest .
#
# Run (Batch mode):
#   docker run --rm tripclick-producer:latest \
#     batch --file /app/data/server0/date=2026-01-01/events.json
#

# docker run --rm \
#   -v $(pwd)/data:/app/data:ro \
#   -v $(pwd)/config:/app/config:ro \
#   -v $(pwd)/logs:/app/logs \
#   -v /home/ubuntu/server0:/app/data/server0:ro \
#   tripclick-producer:latest \
#   --file /app/data/server0/date=2026-01-02/events.json \
#   --mode batch

# Run (Realtime mode):
# batch mode와 비슷한 구성으로 실행할 수 있도록 할 예정
==============================================

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

# config.yaml 복사 (Kafka 브로커 설정 포함)
COPY config/config.yaml ./config/config.yaml

# 로그 디렉터리 생성
RUN mkdir -p /app/logs

# 환경변수 기본값
ENV PYTHONUNBUFFERED=1

# 엔트리포인트 설정
ENTRYPOINT ["python", "-m", "producer.producer"]
CMD ["--help"]
