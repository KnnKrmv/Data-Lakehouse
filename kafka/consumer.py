import json 
from pathlib import Path
from kafka import KafkaConsumer

base_dir = Path(__file__).resolve().parent
config = json.loads((base_dir/"message.json").read_text(encoding="utf-8"))
output_file = base_dir / "messages.txt"

consumer = KafkaConsumer(
    config["topic"],
    bootstrap_servers=["localhost:9094"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id = "testconsumer",
    value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
    key_deserializer=lambda k: k.decode("utf-8", errors="replace") if k is not None else None,
)

print(f"Subscribed to topic '{config['topic']}' and waiting for messages...")
print(f"Messages will be written to: {output_file}")

with output_file.open("a", encoding="utf-8") as f:
    for message in consumer:
        line = f"partition = {message.partition}, offset = {message.offset}, key = {message.key}, value = {message.value}\n"
        print(line)
        f.write(line + "\n")
        f.flush()