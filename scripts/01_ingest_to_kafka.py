# scripts/01_ingest_to_kafka.py
from kafka import KafkaProducer
import json, time

bootstrap_servers = ["localhost:29092", "localhost:9092"]
producer = None

for server in bootstrap_servers:
    try:
        print(f"Attempting to connect to Kafka broker: {server}...")
        producer = KafkaProducer(
            bootstrap_servers=server,
            value_serializer=lambda v: json.dumps(v).encode(),
            request_timeout_ms=5000,
            api_version=(0, 10)
        )
        print(f"Successfully connected to Kafka broker at: {server}")
        break
    except Exception as e:
        print(f"Could not connect to {server}: {e}")

if not producer:
    raise Exception("Could not connect to any Kafka broker candidates!")

def ingest_data(records: list[dict]):
    for record in records:
        producer.send("data.raw", value=record)
        print(f"Sent: {record['id']}")
    producer.flush()

# Test
sample_data = [
    {"id": "doc_001", "text": "AI platform integration test", "timestamp": time.time()},
    {"id": "doc_002", "text": "Kafka to Airflow pipeline", "timestamp": time.time()},
]
ingest_data(sample_data)
print("Integration 1 OK: Data -> Kafka")
