from airflow.providers.mongo.hooks.mongo import MongoHook
from utils.kafka_config import publish_message, flush_kafka_messages
import logging

logger = logging.getLogger(__name__)

EVENT_COLLECTIONS = [
    "page_view",
    "product_search",
    "cart_view",
    "add_to_cart",
    "remove_from_cart",
    "wishlist_add",
    "checkout_start",
    "payment_attempt",
    "order_complete",
    "review_submit",
]

def publish_mongo_events_to_kafka():
    hook = MongoHook(mongo_conn_id="mongo_test")
    client = hook.get_conn()
    db = client["mongodb"]
    total_sent = 0

    for collection_name in EVENT_COLLECTIONS:
        collection = db[collection_name]
        unsent_docs = collection.find({"kafka_sent": {"$ne": True}})
        batch_count = 0

        for doc in unsent_docs:
            mongo_id = doc["_id"]
            doc["_id"] = str(doc["_id"])

            publish_message(topic=collection_name, message=doc)

            collection.update_one(
                {"_id": mongo_id},
                {"$set": {"kafka_sent": True}}
            )

            batch_count += 1
            total_sent += 1

        if batch_count > 0:
            logger.info(f"Collection '{collection_name}': {batch_count} messages published")

    flush_kafka_messages()
    logger.info(f"Done! Total messages published: {total_sent}")
    client.close()
    return total_sent
