CREATE MATERIALIZED VIEW IF NOT EXISTS users_mv
TO users
AS
SELECT
    user_id,
    name,
    email,
    parseDateTimeBestEffortOrNull(signup_date) AS signup_date,
    device,
    loyalty_tier,
    location
FROM kafka_users;

CREATE MATERIALIZED VIEW IF NOT EXISTS products_mv
TO products
AS
SELECT
    product_id,
    name,
    price,
    category,
    inventory,
    popularity_score
FROM kafka_products;

CREATE MATERIALIZED VIEW IF NOT EXISTS orders_mv
TO orders
AS
SELECT
    order_id,
    user_id,
    parseDateTimeBestEffortOrNull(created_at) AS created_at,
    total,
    status,
    payment_method
FROM kafka_orders;
