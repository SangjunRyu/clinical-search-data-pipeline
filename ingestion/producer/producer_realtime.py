"""
TripClick Producer - Realtime Mode

실시간 스트리밍 시뮬레이션용 Producer
- event_ts 기준으로 시간 경과에 따라 Kafka 전송
- 스트리밍 처리 테스트용
"""

import json
import time
import argparse
from datetime import datetime, timezone

from utils.logging import setup_logger
from producer.producer_base import (
    load_config,
    create_producer,
    generate_dedup_key,
    parse_event_ts,
    USE_XXHASH,
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
                dedup_key[:8],
            )

    producer.flush()
    logger.info("DONE total_sent=%d file=%s", sent_count, input_file)


# =========================
# Main
# =========================
def main():
    config = load_config()

    parser = argparse.ArgumentParser(
        description="TripClick Kafka Producer - Realtime Mode"
    )
    parser.add_argument("--file", required=True, help="Input JSON file path")
    parser.add_argument("--topic", default="tripclick_raw_logs")

    args = parser.parse_args()

    logger = setup_logger(
        name="tripclick-producer-realtime",
        log_file="tripclick_producer.log",
    )

    brokers = config["kafka"]["brokers"]
    producer = create_producer(brokers)

    logger.info(
        "START file=%s topic=%s mode=realtime brokers=%s xxhash=%s",
        args.file,
        args.topic,
        brokers,
        USE_XXHASH,
    )

    base_time = datetime.now(timezone.utc)
    send_file_realtime(
        producer=producer,
        topic=args.topic,
        input_file=args.file,
        logger=logger,
        base_time=base_time,
    )


if __name__ == "__main__":
    main()
