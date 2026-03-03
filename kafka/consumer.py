import argparse
import json
import threading
from pathlib import Path

from kafka import KafkaConsumer


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
    return parser.parse_args()


def consume(worker_id, args, stop_event):
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"consumer_test_{worker_id}.txt"
    output_file.write_text("", encoding="utf-8")

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[args.bootstrap_server],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=args.group_id,
        client_id=f"{args.group_id}-worker-{worker_id}",
        consumer_timeout_ms=1000,
        value_deserializer=lambda value: value.decode("utf-8", errors="replace"),
        key_deserializer=(
            lambda key: key.decode("utf-8", errors="replace") if key is not None else None
        ),
    )

    print(
        f"consumer_{worker_id} subscribed to '{args.topic}' "
        f"and writing to {output_file.name}"
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
