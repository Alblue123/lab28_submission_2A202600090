# prefect/flows/kafka_to_delta.py
from prefect import flow, task
from kafka import KafkaConsumer
import json, os
import pandas as pd
from datetime import datetime

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:29092,localhost:9092")
DELTA_LAKE_PATH = os.environ.get("DELTA_LAKE_PATH", "delta-lake/raw")

@task
def consume_and_process():
    """Consume data from Kafka topic"""
    print(f"Connecting to Kafka broker: {KAFKA_BROKER}")
    try:
        consumer = KafkaConsumer(
            "data.raw",
            bootstrap_servers=KAFKA_BROKER.split(","),
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode()),
            api_version=(0, 10)
        )
        records = []
        for msg in consumer:
            records.append(msg.value)
        print(f"Consumed {len(records)} records from Kafka")
        return records
    except Exception as e:
        print(f"Kafka consumption failed: {e}")
        return []

@task
def save_to_delta(records):
    """Save records to Delta Lake (parquet format)"""
    if not records:
        print("No records to save")
        return

    df = pd.DataFrame(records)
    # Giả lập Delta Lake bằng parquet (local volume)
    os.makedirs(DELTA_LAKE_PATH, exist_ok=True)
    batch_file = f"{DELTA_LAKE_PATH}/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(batch_file)
    print(f"Saved {len(df)} records to Delta Lake at {batch_file}")

@flow(name="Kafka to Delta Pipeline")
def kafka_to_delta_flow():
    """Main flow: consume from Kafka and save to Delta Lake"""
    records = consume_and_process()
    save_to_delta(records)

if __name__ == "__main__":
    if "PREFECT_API_URL" not in os.environ:
        os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"

    print(f"Deploying flow to Prefect server at: {os.environ['PREFECT_API_URL']}")
    
    # Dynamically deploy depending on Prefect version
    try:
        kafka_to_delta_flow.deploy(
            name="kafka-to-delta",
            work_queue_name="lab28-worker"
        )
        print("Flow successfully deployed (Prefect 2.x style)")
    except Exception as e:
        print(f"Prefect 2.x deploy style skipped/failed: {e}. Trying Prefect 3.x style...")
        try:
            kafka_to_delta_flow.deploy(
                name="kafka-to-delta",
                work_pool_name="default-agent-pool"
            )
            print("Flow successfully deployed (Prefect 3.x style)")
        except Exception as ex:
            print(f"Flow deployment skipped/failed: {ex}. Starting local flow execution instead.")
            kafka_to_delta_flow()
