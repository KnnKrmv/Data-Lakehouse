import argparse
import json 
from pathlib import Path
from kafka import KafkaProducer

base_dir = Path(__file__).resolve().parent
data = json.loads((base_dir/"message.json").read_text(encoding="utf-8"))

producer = KafkaProducer(
    bootstrap_servers=["localhost:9094"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k : k.encode("utf-8") if k is not None else None,
)
producer.send(
    data["topic"],
    key=data.get("key"),
    value=data.get("message")
).get(timeout=10)

producer.flush()  
producer.close()

print(f"Message sent to topic '{data['topic']}': {data['message']}")
