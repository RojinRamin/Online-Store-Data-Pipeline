from datetime import datetime
from bson import ObjectId

from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

import logging

logger = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────
MONGO_CONN_ID           = "mongo_test"
MONGO_DB_NAME           = "mongodb"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SCHEMA_REGISTRY_URL     = "http://schema-registry:8081"
BATCH_SIZE              = 1000

# ── Schemas ───────────────────────────────────────────────────────────────────
PAGE_VIEW_SCHEMA = """
{
  "type": "record",
  "name": "PageView",
  "namespace": "behavior.events",
  "fields": [
    {"name": "timestamp",   "type": "string"},
    {"name": "user_id",     "type": "string"},
    {"name": "session_id",  "type": "string"},
    {"name": "event_type",  "type": "string"},
    {"name": "device",      "type": "string"},
    {
      "name": "event_data",
      "type": {
        "type": "record",
        "name": "PageViewData",
        "fields": [
          {"name": "url_path",     "type": "string"},
          {"name": "duration_sec", "type": "int"},
          {"name": "http_status",  "type": "int"}
        ]
      }
    }
  ]
}
"""

PRODUCT_SEARCH_SCHEMA = """
{
  "type": "record",
  "name": "ProductSearch",
  "namespace": "behavior.events",
  "fields": [
    {"name": "timestamp",   "type": "string"},
    {"name": "user_id",     "type": "string"},
    {"name": "session_id",  "type": "string"},
    {"name": "event_type",  "type": "string"},
    {"name": "device",      "type": "string"},
    {
      "name": "event_data",
      "type": {
        "type": "record",
        "name": "ProductSearchData",
        "fields": [
          {"name": "query",            "type": "string"},
          {"name": "results_count",    "type": "int"},
          {"name": "clicked_position", "type": ["null", "int"], "default": null}
        ]
      }
    }
  ]
}
"""

ADD_TO_CART_SCHEMA = """
{
  "type": "record",
  "name": "AddToCart",
  "namespace": "behavior.events",
  "fields": [
    {"name": "timestamp",   "type": "string"},
    {"name": "user_id",     "type": "string"},
    {"name": "session_id",  "type": "string"},
    {"name": "event_type",  "type": "string"},
    {"name": "device",      "type": "string"},
    {
      "name": "event_data",
      "type": {
        "type": "record",
        "name": "AddToCartData",
        "fields": [
          {"name": "product_id",       "type": "string"},
          {"name": "quantity",         "type": "int"},
          {"name": "cart_total_items", "type": ["null", "int"], "default": null}
        ]
      }
    }
  ]
}
"""

REMOVE_FROM_CART_SCHEMA = """
{
  "type": "record",
  "name": "RemoveFromCart",
  "namespace": "behavior.events",
  "fields": [
    {"name": "timestamp",   "type": "string"},
    {"name": "user_id",     "type": "string"},
    {"name": "session_id",  "type": "string"},
    {"name": "event_type",  "type": "string"},
    {"name": "device",      "type": "string"},
    {
      "name": "event_data",
      "type": {
        "type": "record",
        "name": "RemoveFromCartData",
        "fields": [
          {"name": "product_id",       "type": "string"},
          {"name": "quantity",         "type": "int"},
          {"name": "cart_total_items", "type": ["null", "int"], "default": null}
        ]
      }
    }
  ]
}
"""

CART_VIEW_SCHEMA = """
{
  "type": "record",
  "name": "CartView",
  "namespace": "behavior.events",
  "fields": [
    {"name": "timestamp",   "type": "string"},
    {"name": "user_id",     "type": "string"},
    {"name": "session_id",  "type": "string"},
    {"name": "event_type",  "type": "string"},
    {"name": "device",      "type": "string"},
    {
      "name": "event_data",
      "type": {
        "type": "record",
        "name": "CartViewData",
        "fields": [
          {
            "name": "cart_items",
            "type": {
              "type": "array",
              "items": {
                "type": "record",
                "name": "CartItem",
                "fields": [
                  {"name": "product_id", "type": "string"},
                  {"name": "price",      "type": "double"},
                  {"name": "quantity",   "type": "int"}
                ]
              }
            }
          },
          {"name": "cart_value", "type": "double"}
        ]
      }
    }
  ]
}
"""

CHECKOUT_START_SCHEMA = """
{
  "type": "record",
  "name": "CheckoutStart",
  "namespace": "behavior.events",
  "fields": [
    {"name": "timestamp",   "type": "string"},
    {"name": "user_id",     "type": "string"},
    {"name": "session_id",  "type": "string"},
    {"name": "event_type",  "type": "string"},
    {"name": "device",      "type": "string"},
    {
      "name": "event_data",
      "type": {
        "type": "record",
        "name": "CheckoutStartData",
        "fields": [
          {"name": "shipping_method", "type": ["null", "string"], "default": null},
          {"name": "cart_value",      "type": "double"}
        ]
      }
    }
  ]
}
"""

PAYMENT_ATTEMPT_SCHEMA = """
{
  "type": "record",
  "name": "PaymentAttempt",
  "namespace": "behavior.events",
  "fields": [
    {"name": "timestamp",   "type": "string"},
    {"name": "user_id",     "type": "string"},
    {"name": "session_id",  "type": "string"},
    {"name": "event_type",  "type": "string"},
    {"name": "device",      "type": "string"},
    {
      "name": "event_data",
      "type": {
        "type": "record",
        "name": "PaymentAttemptData",
        "fields": [
          {"name": "payment_type", "type": "string"},
          {"name": "success",      "type": "boolean"},
          {"name": "error_code",   "type": ["null", "string"], "default": null}
        ]
      }
    }
  ]
}
"""

ORDER_COMPLETE_SCHEMA = """
{
  "type": "record",
  "name": "OrderComplete",
  "namespace": "behavior.events",
  "fields": [
    {"name": "timestamp",   "type": "string"},
    {"name": "user_id",     "type": "string"},
    {"name": "session_id",  "type": "string"},
    {"name": "event_type",  "type": "string"},
    {"name": "device",      "type": "string"},
    {
      "name": "event_data",
      "type": {
        "type": "record",
        "name": "OrderCompleteData",
        "fields": [
          {"name": "order_id",          "type": "string"},
          {"name": "fulfillment_speed", "type": ["null", "string"], "default": null}
        ]
      }
    }
  ]
}
"""

REVIEW_SUBMIT_SCHEMA = """
{
  "type": "record",
  "name": "ReviewSubmit",
  "namespace": "behavior.events",
  "fields": [
    {"name": "timestamp",   "type": "string"},
    {"name": "user_id",     "type": "string"},
    {"name": "session_id",  "type": "string"},
    {"name": "event_type",  "type": "string"},
    {"name": "device",      "type": "string"},
    {
      "name": "event_data",
      "type": {
        "type": "record",
        "name": "ReviewSubmitData",
        "fields": [
          {"name": "product_id",  "type": "string"},
          {"name": "rating",      "type": "int"},
          {"name": "text_length", "type": "int"}
        ]
      }
    }
  ]
}
"""

WISHLIST_ADD_SCHEMA = """
{
  "type": "record",
  "name": "WishlistAdd",
  "namespace": "behavior.events",
  "fields": [
    {"name": "timestamp",   "type": "string"},
    {"name": "user_id",     "type": "string"},
    {"name": "session_id",  "type": "string"},
    {"name": "event_type",  "type": "string"},
    {"name": "device",      "type": "string"},
    {
      "name": "event_data",
      "type": {
        "type": "record",
        "name": "WishlistAddData",
        "fields": [
          {"name": "product_id",    "type": "string"},
          {"name": "wishlist_name", "type": "string"}
        ]
      }
    }
  ]
}
"""

# ── Event configs ─────────────────────────────────────────────────────────────
#
# Mirrors TABLE_CONFIGS in postgres_to_kafka.py.
# Each entry maps an event_type to:
#   - topic:  the Kafka topic to publish to
#   - schema: the Avro schema string for that event type

EVENT_CONFIGS = {
    "page_view": {
        "collection": "page_view",
        "topic":      "events.page_view",
        "schema":     PAGE_VIEW_SCHEMA,
    },
    "product_search": {
        "collection": "product_search",
        "topic":      "events.search",
        "schema":     PRODUCT_SEARCH_SCHEMA,
    },
    "add_to_cart": {
        "collection": "add_to_cart",
        "topic":      "events.add_to_cart",
        "schema":     ADD_TO_CART_SCHEMA,
    },
    "remove_from_cart": {
        "collection": "remove_from_cart",
        "topic":      "events.remove_from_cart",
        "schema":     REMOVE_FROM_CART_SCHEMA,
    },
    "cart_view": {
        "collection": "cart_view",
        "topic":      "events.cart_view",
        "schema":     CART_VIEW_SCHEMA,
    },
    "checkout_start": {
        "collection": "checkout_start",
        "topic":      "events.checkout",
        "schema":     CHECKOUT_START_SCHEMA,
    },
    "payment_attempt": {
        "collection": "payment_attempt",
        "topic":      "events.payment",
        "schema":     PAYMENT_ATTEMPT_SCHEMA,
    },
    "order_complete": {
        "collection": "order_complete",
        "topic":      "events.order",
        "schema":     ORDER_COMPLETE_SCHEMA,
    },
    "review_submit": {
        "collection": "review_submit",
        "topic":      "events.review",
        "schema":     REVIEW_SUBMIT_SCHEMA,
    },
    "wishlist_add": {
        "collection": "wishlist_add",
        "topic":      "events.wishlist",
        "schema":     WISHLIST_ADD_SCHEMA,
    },
}

# ── Normalization ─────────────────────────────────────────────────────────────
def normalize_value(value):
    if isinstance(value, ObjectId):
        return str(value)

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, dict):
        return normalize_record(value)

    if isinstance(value, list):
        return [normalize_value(v) for v in value]

    return value


def normalize_record(record: dict) -> dict:
    return {
        key: normalize_value(value)
        for key, value in record.items()
    }

# ── Airflow Variable state ────────────────────────────────────────────────────
#
# Mirrors get_last_processed_id / update_last_processed_id from
# postgres_to_kafka.py, using Airflow Variables keyed by event_type.
def get_variable_name(event_type: str) -> str:
    return f"mongo_to_kafka_{event_type}_last_id"


def get_last_processed_id(event_type: str):
    value = Variable.get(
        get_variable_name(event_type),
        default_var=None
    )

    if value is None:
        return None

    return ObjectId(value)


def update_last_processed_id(event_type: str, last_processed_id: ObjectId):
    Variable.set(
        get_variable_name(event_type),
        str(last_processed_id)
    )

# ── Fetch ─────────────────────────────────────────────────────────────────────
def fetch_batch(collection, last_processed_id) -> list:
    query = {}

    if last_processed_id is not None:
        query["_id"] = {"$gt": last_processed_id}

    return list(
        collection
        .find(query)
        .sort("_id", 1)
        .limit(BATCH_SIZE)
    )


# ── Producer ──────────────────────────────────────────────────────────────────
def delivery_report(err, msg):
    if err:
        logger.error(
            f"Delivery failed: topic={msg.topic()} | error={err}"
        )
    else:
        logger.debug(
            f"Delivered: topic={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )


def build_producer(schema_str: str) -> SerializingProducer:
    schema_registry_client = SchemaRegistryClient({
        "url": SCHEMA_REGISTRY_URL
    })

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
        to_dict=lambda obj, ctx: obj,
    )

    return SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "key.serializer":    StringSerializer("utf_8"),
        "value.serializer":  avro_serializer,
        "acks":              "all",
        "retries":           5,
    })


# ── Core publish logic ────────────────────────────────────────────────────────
#
# Mirrors publish_table() from postgres_to_kafka.py exactly:
#   - one producer per event_type (own schema)
#   - cursor-based pagination via Airflow Variable
#   - fetch → produce → flush → update state → repeat until empty
def publish_event_type(db, event_type: str, config: dict):
    collection = db[config["collection"]]
    topic      = config["topic"]
    producer   = build_producer(config["schema"])

    logger.info(f"Start publishing event_type: {event_type}")

    while True:
        last_id = get_last_processed_id(event_type)

        batch = fetch_batch(
            collection=collection,
            last_processed_id=last_id,
        )

        if not batch:
            logger.info(f"No new records for {event_type}")
            break

        for doc in batch:
            normalized_doc = normalize_record(doc)

            producer.produce(
                topic=topic,
                key=normalized_doc["_id"],
                value=normalized_doc,
                on_delivery=delivery_report,
            )

            producer.poll(0)

        producer.flush()

        last_record_id = batch[-1]["_id"]

        update_last_processed_id(
            event_type=event_type,
            last_processed_id=last_record_id,
        )

        logger.info(
            f"{event_type}: sent {len(batch)} records. "
            f"Last ID: {last_record_id}"
        )

    producer.flush()
    logger.info(f"Finished publishing event_type: {event_type}")


# ── Airflow entrypoint ────────────────────────────────────────────────────────
def main():
    hook   = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    client = hook.get_conn()
    db     = client[MONGO_DB_NAME]

    try:
        for event_type, config in EVENT_CONFIGS.items():
            publish_event_type(
                db=db,
                event_type=event_type,
                config=config,
            )

    finally:
        client.close()
