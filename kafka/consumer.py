import argparse
import json
import threading
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition


BASE_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run multiple Kafka consumers in the same group."
    )
    parser.add_argument("--topic", default="testtopic", help="Kafka topic name")
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9094",
        help="Kafka bootstrap server",
    )
    parser.add_argument("--group-id", default="testconsumer", help="Consumer group id")
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Number of consumer workers to start",
    )
    parser.add_argument(
        "--output-dir",
        default=str(BASE_DIR),
        help="Directory for consumer_N.txt output files",
    )
    parser.add_argument(
        "--partition",
        type=int,
        default=None,
        help="Optional partition number to assign this consumer directly",
    )
    return parser.parse_args()


def consume(worker_id: int, args: argparse.Namespace, stop_event: threading.Event) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"consumer_test_{worker_id}.txt"
    output_file.write_text("", encoding="utf-8")

    common_kwargs = {
        "bootstrap_servers": [args.bootstrap_server],
        "auto_offset_reset": "earliest",
        "value_deserializer": lambda value: value.decode("utf-8", errors="replace"),
        "key_deserializer": (
            lambda key: key.decode("utf-8", errors="replace") if key is not None else None
        ),
    }

    if args.partition is not None:
        if args.partition < 0:
            raise SystemExit(f"partition must be >= 0, got {args.partition}")

        consumer = KafkaConsumer(
            bootstrap_servers=[args.bootstrap_server],
            consumer_timeout_ms=1000,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            client_id=f"{args.group_id}-worker-{worker_id}-p{args.partition}",
            value_deserializer=common_kwargs["value_deserializer"],
            key_deserializer=common_kwargs["key_deserializer"],
        )
        consumer.assign([TopicPartition(args.topic, args.partition)])
        mode = "manual assignment"
    else:
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=common_kwargs["bootstrap_servers"],
            auto_offset_reset=common_kwargs["auto_offset_reset"],
            enable_auto_commit=True,
            group_id=args.group_id,
            client_id=f"{args.group_id}-worker-{worker_id}",
            consumer_timeout_ms=1000,
            value_deserializer=common_kwargs["value_deserializer"],
            key_deserializer=common_kwargs["key_deserializer"],
        )
        mode = "group subscription"

    print(
        f"consumer_{worker_id} subscribed to '{args.topic}' "
        f"({mode}) and writing to {output_file.name}"
    )

    try:
        with output_file.open("a", encoding="utf-8") as handle:
            while not stop_event.is_set():
                for message in consumer:
                    if stop_event.is_set():
                        break
                    record = {
                        "consumer": worker_id,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key,
                        "value": message.value,
                    }
                    line = json.dumps(record, ensure_ascii=False)
                    print(line)
                    handle.write(line + "\n")
                    handle.flush()
    finally:
        consumer.close()


def main():
    args = parse_args()
    if args.workers < 1:
        raise SystemExit("workers must be greater than 0")

    stop_event = threading.Event()
    threads = []

    try:
        for worker_id in range(1, args.workers + 1):
            thread = threading.Thread(
                target=consume,
                args=(worker_id, args, stop_event),
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nStopping consumers...")
        stop_event.set()
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    main()
