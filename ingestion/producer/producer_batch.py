"""
TripClick Producer - Batch Mode

배치/백필용 Producer
- 대기 없이 즉시 전송 (고속 처리)
- 과거 데이터 재처리, 초기 데이터 적재용
"""

import json
import time
import argparse

from utils.logging import setup_logger
from producer.producer_base import (
    load_config,
    create_producer,
    generate_dedup_key,
    USE_XXHASH,
)


# =========================
# Batch Send Logic
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

    parser = argparse.ArgumentParser(
        description="TripClick Kafka Producer - Batch Mode"
    )
    parser.add_argument("--file", required=True, help="Input JSON file path")
    parser.add_argument("--topic", default="tripclick_raw_logs")
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="레코드 간 sleep 시간(초)",
    )

    args = parser.parse_args()

    logger = setup_logger(
        name="tripclick-producer-batch",
        log_file="tripclick_producer.log",
    )

    brokers = config["kafka"]["brokers"]
    producer = create_producer(brokers)

    logger.info(
        "START file=%s topic=%s mode=batch brokers=%s xxhash=%s",
        args.file,
        args.topic,
        brokers,
        USE_XXHASH,
    )

    send_file_batch(
        producer=producer,
        topic=args.topic,
        input_file=args.file,
        logger=logger,
        sleep_sec=args.sleep,
    )


if __name__ == "__main__":
    main()
