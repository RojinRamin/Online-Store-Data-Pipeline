CREATE MATERIALIZED VIEW digikala.users_mv
TO digikala.users
AS
SELECT
    JSONExtractString(substring(raw_message, 6), 'user_id') AS user_id,
    JSONExtractString(substring(raw_message, 6), 'name') AS name,
    JSONExtractString(substring(raw_message, 6), 'email') AS email,
    parseDateTimeBestEffortOrNull(JSONExtractString(substring(raw_message, 6), 'signup_date')) AS signup_date,
    JSONExtractString(substring(raw_message, 6), 'device') AS device,
    JSONExtractString(substring(raw_message, 6), 'loyalty_tier') AS loyalty_tier,
    JSONExtractString(substring(raw_message, 6), 'location') AS location
FROM digikala.kafka_users;

CREATE MATERIALIZED VIEW digikala.products_mv
TO digikala.products
AS
SELECT
    JSONExtractString(substring(raw_message, 6), 'product_id') AS product_id,
    JSONExtractString(substring(raw_message, 6), 'name') AS name,
    JSONExtract(substring(raw_message, 6), 'price', 'Nullable(Float64)') AS price,
    JSONExtractString(substring(raw_message, 6), 'category') AS category,
    JSONExtract(substring(raw_message, 6), 'inventory', 'Nullable(Int32)') AS inventory,
    JSONExtract(substring(raw_message, 6), 'popularity_score', 'Nullable(Float64)') AS popularity_score
FROM digikala.kafka_products;

CREATE MATERIALIZED VIEW digikala.orders_mv
TO digikala.orders
AS
SELECT
    JSONExtractString(substring(raw_message, 6), 'order_id') AS order_id,
    JSONExtractString(substring(raw_message, 6), 'user_id') AS user_id,
    parseDateTimeBestEffortOrNull(JSONExtractString(substring(raw_message, 6), 'created_at')) AS created_at,
    JSONExtract(substring(raw_message, 6), 'total', 'Nullable(Float64)') AS total,
    JSONExtractString(substring(raw_message, 6), 'status') AS status,
    JSONExtractString(substring(raw_message, 6), 'payment_method') AS payment_method
FROM digikala.kafka_orders;
