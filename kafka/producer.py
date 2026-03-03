import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


EVENT_TYPES = ["order_created", "payment_received", "shipment_ready", "refund_requested"]
STATUSES = ["new", "processing", "completed", "failed"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Send a user-defined number of random Kafka messages."
    )
    parser.add_argument("count", type=int, help="Number of random messages to send")
    parser.add_argument("--topic", default="testtopic", help="Kafka topic name")
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9094",
        help="Kafka bootstrap server",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Optional delay in seconds between messages",
    )
    return parser.parse_args()


def build_message(sequence):
    return {
        "message_no": sequence,
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "user_id": random.randint(1000, 9999),
        "amount": round(random.uniform(10, 500), 2),
        "status": random.choice(STATUSES),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    args = parse_args()
    if args.count < 1:
        raise SystemExit("count must be greater than 0")

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    try:
        for index in range(1, args.count + 1):
            payload = build_message(index)
            metadata = producer.send(args.topic, value=payload).get(timeout=10)
            print(
                f"[{index}/{args.count}] sent partition={metadata.partition}, "
                f"offset={metadata.offset}, value={payload}"
            )
            if args.sleep > 0:
                time.sleep(args.sleep)
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    main()
