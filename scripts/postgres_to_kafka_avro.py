# scripts/postgres_to_kafka_avro.py

from datetime import date, datetime
from decimal import Decimal

from airflow.providers.postgres.hooks.postgres import PostgresHook

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


POSTGRES_CONN_ID = "postgres_business"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
BATCH_SIZE = 1000


TABLE_CONFIGS = {
    "users": {
        "pk": "user_id",
        "topic": "postgres.users",
        "schema": """
        {
          "type": "record",
          "name": "User",
          "namespace": "digikala.postgres",
          "fields": [
            {"name": "user_id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "signup_date", "type": ["null", "string"], "default": null},
            {"name": "device", "type": ["null", "string"], "default": null},
            {"name": "loyalty_tier", "type": ["null", "string"], "default": null},
            {"name": "location", "type": ["null", "string"], "default": null}
          ]
        }
        """
    },
    "products": {
        "pk": "product_id",
        "topic": "postgres.products",
        "schema": """
        {
          "type": "record",
          "name": "Product",
          "namespace": "digikala.postgres",
          "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "price", "type": ["null", "double"], "default": null},
            {"name": "category", "type": ["null", "string"], "default": null},
            {"name": "inventory", "type": ["null", "int"], "default": null},
            {"name": "popularity_score", "type": ["null", "double"], "default": null}
          ]
        }
        """
    },
    "orders": {
        "pk": "order_id",
        "topic": "postgres.orders",
        "schema": """
        {
          "type": "record",
          "name": "Order",
          "namespace": "digikala.postgres",
          "fields": [
            {"name": "order_id", "type": "string"},
            {"name": "user_id", "type": ["null", "string"], "default": null},
            {"name": "created_at", "type": ["null", "string"], "default": null},
            {"name": "total", "type": ["null", "double"], "default": null},
            {"name": "status", "type": ["null", "string"], "default": null},
            {"name": "payment_method", "type": ["null", "string"], "default": null}
          ]
        }
        """
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


def extract_number_from_id(value):
    digits = "".join(ch for ch in str(value) if ch.isdigit())

    if not digits:
        raise ValueError(f"ID does not contain any numeric part: {value}")

    return int(digits)


def create_topics_if_not_exist():
    admin = AdminClient({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    })

    existing_topics = admin.list_topics(timeout=10).topics
    topics_to_create = []

    for config in TABLE_CONFIGS.values():
        topic_name = config["topic"]

        if topic_name not in existing_topics:
            topics_to_create.append(
                NewTopic(
                    topic=topic_name,
                    num_partitions=1,
                    replication_factor=1,
                )
            )

    if topics_to_create:
        futures = admin.create_topics(topics_to_create)

        for topic, future in futures.items():
            try:
                future.result()
                print(f"Created topic: {topic}")
            except Exception as exc:
                print(f"Topic creation skipped/failed for {topic}: {exc}")


def create_state_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kafka_publish_state (
            table_name TEXT PRIMARY KEY,
            last_processed_id TEXT,
            last_processed_number BIGINT,
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)


def get_last_processed_state(cursor, table_name):
    cursor.execute(
        """
        SELECT last_processed_id, last_processed_number
        FROM kafka_publish_state
        WHERE table_name = %s;
        """,
        (table_name,),
    )

    result = cursor.fetchone()

    if not result:
        return None, None

    return result[0], result[1]


def update_last_processed_state(cursor, table_name, last_processed_id):
    last_processed_number = extract_number_from_id(last_processed_id)

    cursor.execute(
        """
        INSERT INTO kafka_publish_state
            (table_name, last_processed_id, last_processed_number, updated_at)
        VALUES
            (%s, %s, %s, NOW())
        ON CONFLICT (table_name)
        DO UPDATE SET
            last_processed_id = EXCLUDED.last_processed_id,
            last_processed_number = EXCLUDED.last_processed_number,
            updated_at = NOW();
        """,
        (table_name, str(last_processed_id), last_processed_number),
    )


def fetch_batch(cursor, table_name, pk_column, last_processed_number):
    numeric_id_expr = (
        f"CAST(REGEXP_REPLACE({pk_column}, '[^0-9]', '', 'g') AS BIGINT)"
    )

    if last_processed_number is None:
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE REGEXP_REPLACE({pk_column}, '[^0-9]', '', 'g') <> ''
            ORDER BY {numeric_id_expr}
            LIMIT %s;
        """
        cursor.execute(query, (BATCH_SIZE,))
    else:
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE REGEXP_REPLACE({pk_column}, '[^0-9]', '', 'g') <> ''
              AND {numeric_id_expr} > %s
            ORDER BY {numeric_id_expr}
            LIMIT %s;
        """
        cursor.execute(query, (last_processed_number, BATCH_SIZE))

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    return [dict(zip(columns, row)) for row in rows]

# EDIT: added info for airflow
def delivery_report(err, msg):
    if err is not None:
        print(f"[INFO]Delivery failed: {err}")
    else:
        print(
            f"[INFO]Delivered to topic={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )


def build_producer(schema_str):
    schema_registry_client = SchemaRegistryClient({
        "url": SCHEMA_REGISTRY_URL
    })

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
    )

    return SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "acks": "all",
        "retries": 5,
    })


def publish_table_to_kafka(cursor, conn, table_name, config):
    pk_column = config["pk"]
    topic = config["topic"]

    producer = build_producer(config["schema"])
    total_sent = 0

    print(f"Start publishing {table_name} to {topic}")

    while True:
        last_processed_id, last_processed_number = get_last_processed_state(
            cursor,
            table_name,
        )

        print(
            f"{table_name}: last_processed_id={last_processed_id}, "
            f"last_processed_number={last_processed_number}"
        )

        batch = fetch_batch(
            cursor=cursor,
            table_name=table_name,
            pk_column=pk_column,
            last_processed_number=last_processed_number,
        )

        if not batch:
            break

        for record in batch:
            normalized_record = normalize_record(record)

            producer.produce(
                topic=topic,
                key=str(normalized_record[pk_column]),
                value=normalized_record,
                on_delivery=delivery_report,
            )

            producer.poll(0)

        producer.flush()

        last_processed_id = batch[-1][pk_column]

        update_last_processed_state(
            cursor=cursor,
            table_name=table_name,
            last_processed_id=last_processed_id,
        )

        conn.commit()

        total_sent += len(batch)

        print(
            f"{table_name}: sent {len(batch)} records. "
            f"Last ID: {last_processed_id}. Total sent: {total_sent}"
        )

    producer.flush()
    print(f"Finished {table_name}. Total sent: {total_sent}")


def main():
    create_topics_if_not_exist()

    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        create_state_table(cursor)
        conn.commit()

        for table_name, config in TABLE_CONFIGS.items():
            publish_table_to_kafka(
                cursor=cursor,
                conn=conn,
                table_name=table_name,
                config=config,
            )

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()