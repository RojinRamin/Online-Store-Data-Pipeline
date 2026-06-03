CREATE DATABASE IF NOT EXISTS digikala;

USE digikala;


CREATE TABLE IF NOT EXISTS users
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


CREATE TABLE IF NOT EXISTS products
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


CREATE TABLE IF NOT EXISTS orders
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

CREATE TABLE kafka_users
(
    user_id String,
    name String,
    email String,
    signup_date Nullable(String),
    device Nullable(String),
    loyalty_tier Nullable(String),
    location Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'postgres.users',
    kafka_group_name = 'clickhouse_users',
    kafka_format = 'JSONEachRow';


CREATE TABLE kafka_orders
(
    order_id String,
    user_id Nullable(String),
    created_at Nullable(String),
    total Nullable(Float64),
    status Nullable(String),
    payment_method Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'postgres.orders',
    kafka_group_name = 'clickhouse_orders',
    kafka_format = 'JSONEachRow';


CREATE TABLE kafka_products
(
    product_id String,
    name String,
    price Nullable(Float64),
    category Nullable(String),
    inventory Nullable(Int32),
    popularity_score Nullable(Float64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'postgres.products',
    kafka_group_name = 'clickhouse_products',
    kafka_format = 'JSONEachRow';
