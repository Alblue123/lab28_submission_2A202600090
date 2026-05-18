import json
import os
import glob
import time
import requests
import pandas as pd
from datetime import datetime
import redis
from kafka import KafkaConsumer
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

# Load local .env manually if exists
if os.path.exists(".env"):
    with open(".env") as f:
        for line in f:
            if "=" in line and not line.strip().startswith("#"):
                key, val = line.strip().split("=", 1)
                os.environ[key] = val

# Environment variables
VLLM_URL = os.environ.get("VLLM_NGROK_URL", "http://localhost:8001")
EMBED_URL = os.environ.get("EMBED_NGROK_URL", "http://localhost:8001")
QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")

print(f"Starting consumer. VLLM_URL={VLLM_URL}, EMBED_URL={EMBED_URL}, QDRANT_HOST={QDRANT_HOST}, REDIS_HOST={REDIS_HOST}")

# Initialize clients
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
qdrant = QdrantClient(host=QDRANT_HOST, port=6333)

# Ensure Qdrant collection exists
try:
    if not qdrant.collection_exists(collection_name="documents"):
        qdrant.create_collection(
            collection_name="documents",
            vectors_config=VectorParams(size=384, distance=Distance.COSINE)
        )
        print("Qdrant collection 'documents' created.")
    else:
        print("Qdrant collection 'documents' already exists.")
except Exception as e:
    print(f"Error initializing Qdrant: {e}")

# Kafka consumer
while True:
    try:
        servers = ["localhost:29092", "localhost:9092"] if QDRANT_HOST == "localhost" else ["kafka:9092"]
        print(f"Connecting consumer to Kafka bootstrap servers: {servers}")
        consumer = KafkaConsumer(
            "data.raw",
            bootstrap_servers=servers,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode()),
            group_id="platform-consumer-group",
            api_version=(0, 10)
        )
        print("Connected to Kafka!")
        break
    except Exception as e:
        print(f"Waiting for Kafka: {e}")
        time.sleep(2)

# Counter for Qdrant IDs
point_id_counter = 1000

while True:
    try:
        # Non-blocking poll
        msg_pack = consumer.poll(timeout_ms=1000)
        messages = []
        for tp, messages_list in msg_pack.items():
            for msg in messages_list:
                messages.append(msg.value)
        
        if messages:
            print(f"Processing {len(messages)} messages...")
            
            # 1. Delta Lake (Parquet)
            df = pd.DataFrame(messages)
            delta_path = "delta-lake/raw" if QDRANT_HOST == "localhost" else "/opt/delta-lake/raw"
            os.makedirs(delta_path, exist_ok=True)
            batch_file = os.path.join(delta_path, f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
            df.to_parquet(batch_file)
            print(f"Saved {len(df)} records to Parquet at {batch_file}")

            # 2. Redis (Feast)
            for _, row in df.iterrows():
                feature_key = f"feature:{row['id']}"
                r.set(feature_key, json.dumps({
                    "text": row["text"],
                    "timestamp": row.get("timestamp", time.time()),
                    "processed": True
                }))
                print(f"Pushed to Feast: {feature_key}")

            # 3. Qdrant (Embed & Store)
            texts = [row["text"] for _, row in df.iterrows()]
            try:
                response = requests.post(f"{EMBED_URL}/embed", json={"texts": texts})
                embeddings = response.json()["embeddings"]
                
                points = []
                for emb, (_, row) in zip(embeddings, df.iterrows()):
                    pid = point_id_counter
                    point_id_counter += 1
                    points.append(PointStruct(id=pid, vector=emb, payload=dict(row)))
                
                qdrant.upsert(collection_name="documents", points=points)
                print(f"Stored {len(points)} vectors in Qdrant documents collection")
            except Exception as e:
                print(f"Embedding/Qdrant upload failed: {e}")
                
    except Exception as e:
        print(f"Error in consumer loop: {e}")
        time.sleep(1)
