"""
TripClick Producer - 공통 유틸리티

Kafka Producer 생성, Config 로드, Dedup Key 생성 등 공통 기능
"""

from kafka import KafkaProducer
import json
import yaml
import os
from datetime import datetime, timezone

try:
    import xxhash
    USE_XXHASH = True
except ImportError:
    import hashlib
    USE_XXHASH = False


# =========================
# Config Loader
# =========================
def load_config(path="config/config.yaml"):
    """
    Kafka 설정 파일 로드
    - 환경변수 KAFKA_BROKERS가 있으면 오버라이드
    """
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    brokers_env = os.getenv("KAFKA_BROKERS")
    if brokers_env:
        config["kafka"]["brokers"] = brokers_env.split(",")

    return config


# =========================
# Dedup Key Generator
# =========================
def generate_dedup_key(session_id: str, document_id: int, event_ts: str) -> str:
    """
    xxhash 기반 dedup 키 생성
    - session_id + documentId + event_ts 조합
    - Spark에서 streaming SQL로 중복 제거 시 사용
    """
    raw_key = f"{session_id}|{document_id}|{event_ts}"

    if USE_XXHASH:
        return xxhash.xxh64(raw_key.encode("utf-8")).hexdigest()
    else:
        return hashlib.md5(raw_key.encode("utf-8")).hexdigest()


# =========================
# Timestamp Parser
# =========================
def parse_event_ts(event_ts: str) -> datetime:
    """
    ISO-8601 형식의 event_ts를 datetime으로 변환
    예: "2026-01-01T15:00:01.902456+00:00"
    """
    try:
        return datetime.fromisoformat(event_ts)
    except ValueError:
        # 폴백: timezone 부분 제거 후 파싱
        if "+" in event_ts:
            event_ts = event_ts.rsplit("+", 1)[0]
        return datetime.fromisoformat(event_ts).replace(tzinfo=timezone.utc)


# =========================
# Kafka Producer Factory
# =========================
def create_producer(bootstrap_servers):
    """
    Kafka Producer 인스턴스 생성
    - acks=1: 안정성/성능 균형
    - linger_ms=50: 배치 처리
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        retries=1,
        linger_ms=50,
    )
