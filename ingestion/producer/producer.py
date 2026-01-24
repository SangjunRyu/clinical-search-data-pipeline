from kafka import KafkaProducer
import json
import time
import argparse
import yaml
import os
from datetime import datetime, timezone
from utils.logging import setup_logger

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
    # Python 3.11+에서는 fromisoformat이 timezone 포함 문자열도 처리
    try:
        return datetime.fromisoformat(event_ts)
    except ValueError:
        # 폴백: timezone 부분 제거 후 파싱
        if "+" in event_ts:
            event_ts = event_ts.rsplit("+", 1)[0]
        return datetime.fromisoformat(event_ts).replace(tzinfo=timezone.utc)


# =========================
# Kafka Producer
# =========================
def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,            # 안정성/성능 균형
        retries=1,
        linger_ms=50,      # batching
    )


# =========================
# Realtime Streaming Send Logic
# =========================
def send_file_realtime(
    producer,
    topic,
    input_file,
    logger,
    base_time: datetime,
    log_every=1000,
):
    """
    실시간 스트리밍 방식으로 데이터 전송
    - event_ts와 현재 시간(base_time 기준 경과 시간)을 비교
    - 시간이 지난 레코드만 Kafka로 전송
    - at-least-once 보장을 위해 dedup_key 추가
    """
    sent_count = 0
    pending_records = []

    # 1단계: 파일의 모든 레코드 로드 및 정렬
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            event_ts_str = record.get("event_ts")
            if not event_ts_str:
                continue

            event_dt = parse_event_ts(event_ts_str)
            pending_records.append((event_dt, record))

    # event_ts 기준 정렬
    pending_records.sort(key=lambda x: x[0])

    if not pending_records:
        logger.warning("No records found in file: %s", input_file)
        return

    # 첫 번째 이벤트 시간을 기준으로 상대 시간 계산
    first_event_time = pending_records[0][0]
    logger.info(
        "Loaded %d records, first_event=%s, base_time=%s",
        len(pending_records),
        first_event_time.isoformat(),
        base_time.isoformat(),
    )

    # 2단계: 실시간 전송 (시간 경과에 따라)
    start_real_time = time.time()

    for event_dt, record in pending_records:
        # 상대 시간 계산: 첫 이벤트로부터 얼마나 지났는지
        offset_seconds = (event_dt - first_event_time).total_seconds()

        # 실제 경과 시간이 offset_seconds에 도달할 때까지 대기
        elapsed = time.time() - start_real_time
        wait_time = offset_seconds - elapsed

        if wait_time > 0:
            time.sleep(wait_time)

        # dedup_key 추가
        session_id = record.get("SessionId", "unknown")
        document_id = record.get("DocumentId", 0)
        event_ts_str = record.get("event_ts", "")

        dedup_key = generate_dedup_key(session_id, document_id, event_ts_str)
        record["dedup_key"] = dedup_key

        # Kafka 전송
        producer.send(
            topic=topic,
            key=session_id,
            value=record,
        )

        sent_count += 1

        if sent_count % log_every == 0:
            logger.info(
                "progress sent=%d topic=%s session=%s event_ts=%s dedup_key=%s",
                sent_count,
                topic,
                session_id,
                event_ts_str,
                dedup_key[:8],  # 앞 8자리만 로깅
            )

    producer.flush()
    logger.info("DONE total_sent=%d file=%s", sent_count, input_file)


# =========================
# Batch Send Logic (기존 방식)
# =========================
def send_file_batch(
    producer,
    topic,
    input_file,
    logger,
    sleep_sec=0.0,
    log_every=1000,
):
    """
    배치 방식으로 전체 파일을 빠르게 전송
    - dedup_key 추가
    - 실시간 대기 없이 즉시 전송
    """
    sent_count = 0

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)

            session_id = record.get("SessionId", "unknown")
            document_id = record.get("DocumentId", 0)
            event_ts_str = record.get("event_ts", "")

            # dedup_key 추가
            dedup_key = generate_dedup_key(session_id, document_id, event_ts_str)
            record["dedup_key"] = dedup_key

            producer.send(
                topic=topic,
                key=session_id,
                value=record,
            )

            sent_count += 1

            if sent_count % log_every == 0:
                logger.info(
                    "progress sent=%d topic=%s session=%s event_ts=%s",
                    sent_count,
                    topic,
                    session_id,
                    event_ts_str,
                )

            if sleep_sec > 0:
                time.sleep(sleep_sec)

    producer.flush()
    logger.info("DONE total_sent=%d file=%s", sent_count, input_file)


# =========================
# Main
# =========================
def main():
    config = load_config()

    parser = argparse.ArgumentParser(description="TripClick Kafka Producer")
    parser.add_argument("--file", required=True, help="Input JSON file path")
    parser.add_argument("--topic", default="tripclick_raw_logs")
    parser.add_argument(
        "--mode",
        choices=["realtime", "batch"],
        default="realtime",
        help="전송 모드: realtime(실시간 시뮬레이션) / batch(즉시 전송)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="배치 모드에서 레코드 간 sleep 시간(초)",
    )

    args = parser.parse_args()

    logger = setup_logger(
        name="tripclick-producer",
        log_file="tripclick_producer.log",
    )

    brokers = config["kafka"]["brokers"]
    producer = create_producer(brokers)

    logger.info(
        "START file=%s topic=%s mode=%s brokers=%s xxhash=%s",
        args.file,
        args.topic,
        args.mode,
        brokers,
        USE_XXHASH,
    )

    if args.mode == "realtime":
        base_time = datetime.now(timezone.utc)
        send_file_realtime(
            producer=producer,
            topic=args.topic,
            input_file=args.file,
            logger=logger,
            base_time=base_time,
        )
    else:
        send_file_batch(
            producer=producer,
            topic=args.topic,
            input_file=args.file,
            logger=logger,
            sleep_sec=args.sleep,
        )


if __name__ == "__main__":
    main()
