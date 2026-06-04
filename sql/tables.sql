CREATE DATABASE IF NOT EXISTS digikala;

USE digikala;


CREATE TABLE digikala.users
(
    user_id String,
    name String,
    email String,
    signup_date Nullable(DateTime),
    device Nullable(String),
    loyalty_tier Nullable(String),
    location Nullable(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY user_id;

CREATE TABLE digikala.products
(
    product_id String,
    name String,
    price Nullable(Float64),
    category Nullable(String),
    inventory Nullable(Int32),
    popularity_score Nullable(Float64)
)
ENGINE = ReplacingMergeTree()
ORDER BY product_id;

CREATE TABLE digikala.orders
(
    order_id String,
    user_id Nullable(String),
    created_at Nullable(DateTime),
    total Nullable(Float64),
    status Nullable(String),
    payment_method Nullable(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY order_id;

CREATE TABLE digikala.kafka_users
(
    raw_message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'postgres.users',
    kafka_group_name = 'clickhouse_users_json_schema_v2',
    kafka_format = 'RawBLOB';

CREATE TABLE digikala.kafka_products
(
    raw_message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'postgres.products',
    kafka_group_name = 'clickhouse_products_json_schema_v2',
    kafka_format = 'RawBLOB';

CREATE TABLE digikala.kafka_orders
(
    raw_message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'postgres.orders',
    kafka_group_name = 'clickhouse_orders_json_schema_v2',
    kafka_format = 'RawBLOB';
