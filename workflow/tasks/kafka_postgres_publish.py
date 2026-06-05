from datetime import datetime, date
from decimal import Decimal

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer


POSTGRES_CONN_ID = "postgres_business"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
BATCH_SIZE = 1000


USER_SCHEMA = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User",
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "name": {"type": "string"},
    "email": {"type": "string"},
    "signup_date": {"type": ["string", "null"]},
    "device": {"type": ["string", "null"]},
    "loyalty_tier": {"type": ["string", "null"]},
    "location": {"type": ["string", "null"]}
  },
  "required": ["user_id", "name", "email"]
}
"""


PRODUCT_SCHEMA = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Product",
  "type": "object",
  "properties": {
    "product_id": {"type": "string"},
    "name": {"type": "string"},
    "price": {"type": ["number", "null"]},
    "category": {"type": ["string", "null"]},
    "inventory": {"type": ["integer", "null"]},
    "popularity_score": {"type": ["number", "null"]}
  },
  "required": ["product_id", "name"]
}
"""


ORDER_SCHEMA = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Order",
  "type": "object",
  "properties": {
    "order_id": {"type": "string"},
    "user_id": {"type": ["string", "null"]},
    "created_at": {"type": ["string", "null"]},
    "total": {"type": ["number", "null"]},
    "status": {"type": ["string", "null"]},
    "payment_method": {"type": ["string", "null"]}
  },
  "required": ["order_id"]
}
"""


TABLE_CONFIGS = {
    "users": {
        "pk": "user_id",
        "topic": "postgres.users",
        "schema": USER_SCHEMA,
    },
    "products": {
        "pk": "product_id",
        "topic": "postgres.products",
        "schema": PRODUCT_SCHEMA,
    },
    "orders": {
        "pk": "order_id",
        "topic": "postgres.orders",
        "schema": ORDER_SCHEMA,
    },
}


def normalize_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    return value


def normalize_record(record):
    return {
        key: normalize_value(value)
        for key, value in record.items()
    }


def get_number_from_id(value):
    return int(str(value)[1:])


def get_variable_name(table_name):
    return f"postgres_to_kafka_{table_name}_last_id"


def get_last_processed_id(table_name):
    return Variable.get(
        get_variable_name(table_name),
        default_var=None
    )


def update_last_processed_id(table_name, last_processed_id):
    Variable.set(
        get_variable_name(table_name),
        str(last_processed_id)
    )


def fetch_batch(cursor, table_name, pk_column, last_processed_id):
    id_number_expr = f"CAST(SUBSTRING({pk_column} FROM 2) AS BIGINT)"

    if last_processed_id is None:
        query = f"""
            SELECT *
            FROM {table_name}
            ORDER BY {id_number_expr}
            LIMIT %s;
        """
        cursor.execute(query, (BATCH_SIZE,))

    else:
        last_number = get_number_from_id(last_processed_id)

        query = f"""
            SELECT *
            FROM {table_name}
            WHERE {id_number_expr} > %s
            ORDER BY {id_number_expr}
            LIMIT %s;
        """
        cursor.execute(query, (last_number, BATCH_SIZE))

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    return [
        dict(zip(columns, row))
        for row in rows
    ]


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered: topic={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )


def build_producer(schema_str):
    schema_registry_client = SchemaRegistryClient({
        "url": SCHEMA_REGISTRY_URL
    })

    json_serializer = JSONSerializer(
        schema_str=schema_str,
        schema_registry_client=schema_registry_client,
        to_dict=lambda obj, ctx: obj
    )

    producer = SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": json_serializer,
        "acks": "all",
        "retries": 5
    })

    return producer


def publish_table(cursor, table_name, config):
    pk_column = config["pk"]
    topic = config["topic"]

    producer = build_producer(config["schema"])

    print(f"Start publishing table: {table_name}")

    while True:
        last_id = get_last_processed_id(table_name)

        batch = fetch_batch(
            cursor=cursor,
            table_name=table_name,
            pk_column=pk_column,
            last_processed_id=last_id
        )

        if not batch:
            print(f"No new records for {table_name}")
            break

        for record in batch:
            normalized_record = normalize_record(record)

            producer.produce(
                topic=topic,
                key=str(normalized_record[pk_column]),
                value=normalized_record,
                on_delivery=delivery_report
            )

            producer.poll(0)

        producer.flush()

        last_record_id = batch[-1][pk_column]

        update_last_processed_id(
            table_name=table_name,
            last_processed_id=last_record_id
        )

        print(
            f"{table_name}: sent {len(batch)} records. "
            f"Last ID: {last_record_id}"
        )

    producer.flush()
    print(f"Finished publishing table: {table_name}")


def main():
    postgres_hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        for table_name, config in TABLE_CONFIGS.items():
            publish_table(
                cursor=cursor,
                table_name=table_name,
                config=config
            )

    finally:
        cursor.close()
        conn.close()
