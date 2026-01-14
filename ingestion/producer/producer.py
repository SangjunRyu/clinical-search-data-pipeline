from kafka import KafkaProducer
import json
import time
import argparse
import yaml
import os
from utils.logging import setup_logger


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
# Kafka Producer
# =========================
def create_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,            # 🔥 all → 1 (안정성/성능 균형)
        retries=1,
        linger_ms=50,     # batching
    )


# =========================
# Send Logic
# =========================
def send_file_to_kafka(
    producer,
    topic,
    input_file,
    logger,
    sleep_sec=0.0,
    log_every=1000,
):
    sent_count = 0

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            session_id = record.get("SessionId", "unknown")

            producer.send(
                topic=topic,
                key=session_id,
                value=record,
            )

# TODO(decision):
# KafkaProducer.send() 결과를 future.get()으로 동기 확인할지,
# 비동기 fire-and-forget 방식으로 유지할지 결정 필요.
#
# - 동기 방식(future.get):
#   * 장점: 전송 성공/실패를 즉시 확인 가능
#   * 단점: 대량 전송 시 성능 저하, t3.micro에서 CPU 크레딧 소모 큼
#
# - 비동기 방식(send only):
#   * 장점: 높은 처리량, Kafka Producer 권장 패턴
#   * 단점: 개별 메시지 전송 성공 여부 즉시 확인 불가
#
# 현재는 성능 안정성을 위해 비동기 방식 사용 중이며,
# 멘토 의견에 따라 배치/주기 단위 검증 로직 추가 여부 결정 예정.

            # future = producer.send(
            #     topic=topic,
            #     key=session_id,
            #     value=record,
            # )

            # metadata = future.get(timeout=10)

            sent_count += 1

            # 🔥 주기 로그만
            if sent_count % log_every == 0:
                logger.info(
                    "progress sent=%d topic=%s last_session=%s event_ts=%s",
                    sent_count,
                    topic,
                    session_id,
                    record.get("event_ts"),
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

    parser = argparse.ArgumentParser(description="TripClick file Kafka producer")
    parser.add_argument("--file", required=True, help="input json file")
    parser.add_argument("--topic", default="tripclick_raw_logs")
    parser.add_argument("--sleep", type=float, default=0.0)

    args = parser.parse_args()

    logger = setup_logger(
        name="tripclick-producer",
        log_file="tripclick_producer.log",
    )

    brokers = config["kafka"]["brokers"]
    producer = create_producer(brokers)

    logger.info(
        "START file=%s topic=%s brokers=%s",
        args.file,
        args.topic,
        brokers,
    )

    send_file_to_kafka(
        producer=producer,
        topic=args.topic,
        input_file=args.file,
        logger=logger,
        sleep_sec=args.sleep,
    )


if __name__ == "__main__":
    main()
