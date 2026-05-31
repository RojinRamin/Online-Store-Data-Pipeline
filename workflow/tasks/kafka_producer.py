from airflow.providers.mongo.hooks.mongo import MongoHook
from confluent_kafka import Producer
import json
import logging

logger = logging.getLogger(__name__)

# ── Kafka Configuration ───────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": "kafka:9092",
    "queue.buffering.max.messages": 100000,
}

# ── Collections to publish ────────────────────────────────
EVENT_COLLECTIONS = [
    "page_view",
    "product_search",
    "add_to_cart",
    "remove_from_cart",
    "wishlist_add",
    "cart_view",
    "checkout_start",
    "payment_attempt",
    "order_complete",
    "review_submit",
]

# ── Global producer instance ──────────────────────────────
_PRODUCER = None

def get_producer():
    global _PRODUCER
    if _PRODUCER is None:
        _PRODUCER = Producer(KAFKA_CONFIG)
    return _PRODUCER

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for topic {msg.topic()}: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")

def publish_mongo_events_to_kafka():
    producer = get_producer()
    hook = MongoHook(mongo_conn_id="mongo_test")
    client = hook.get_conn()
    db = client["mongodb"]
    total_sent = 0

    for collection_name in EVENT_COLLECTIONS:
        collection = db[collection_name]

        # Only fetch documents not yet sent to Kafka
        unsent_docs = collection.find({"kafka_sent": {"$ne": True}})
        batch_count = 0

        for doc in unsent_docs:
            mongo_id = doc["_id"]

            # Convert ObjectId to string for JSON serialization
            doc["_id"] = str(doc["_id"])

            # Convert datetime objects to string
            if "timestamp" in doc and hasattr(doc["timestamp"], "isoformat"):
                doc["timestamp"] = doc["timestamp"].isoformat()

            if "ingestion" in doc and "loaded_at" in doc["ingestion"]:
                if hasattr(doc["ingestion"]["loaded_at"], "isoformat"):
                    doc["ingestion"]["loaded_at"] = doc["ingestion"]["loaded_at"].isoformat()

            # Publish to Kafka
            producer.produce(
                topic=collection_name,
                value=json.dumps(doc, default=str),
                callback=delivery_report,
            )

            # Mark as sent in MongoDB
            collection.update_one(
                {"_id": mongo_id},
                {"$set": {"kafka_sent": True}}
            )

            batch_count += 1
            total_sent += 1

            # Poll every 1000 messages to handle delivery callbacks
            if total_sent % 1000 == 0:
                producer.poll(0)
                logger.info(f"Progress: {total_sent} messages published so far...")

        if batch_count > 0:
            logger.info(f"Collection '{collection_name}': {batch_count} messages published")

    # Flush all remaining messages
    producer.flush()
    logger.info(f"Done! Total messages published to Kafka: {total_sent}")
    client.close()
    return total_sent
