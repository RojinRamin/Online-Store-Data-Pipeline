-- Query

-- SELECT
--     p.product_id,
--     p.name AS product_name,
--     p.category,
--     SUM(a.quantity) AS total_quantity,
--     COUNT(*) AS add_to_cart_count,
--     ROUND(SUM(a.quantity * p.price), 2) AS estimated_revenue
-- FROM digikala.add_to_cart a
-- INNER JOIN digikala.products p
--     ON a.product_id = p.product_id
-- GROUP BY
--     p.product_id,
--     p.name,
--     p.category
-- ORDER BY estimated_revenue DESC
-- LIMIT 10;

CREATE TABLE IF NOT EXISTS digikala.top_products_report
(
    product_id String,
    product_name String,
    category String,
    total_quantity UInt64,
    add_to_cart_count UInt64,
    estimated_revenue Float64
)
ENGINE = SummingMergeTree
ORDER BY (category, product_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS digikala.top_products_report_mv
TO digikala.top_products_report
AS
SELECT
    p.product_id,
    p.name AS product_name,
    ifNull(p.category, 'Unknown') AS category,
    SUM(a.quantity) AS total_quantity,
    COUNT(*) AS add_to_cart_count,
    SUM(a.quantity * ifNull(p.price, 0)) AS estimated_revenue
FROM digikala.add_to_cart a
INNER JOIN digikala.products p
    ON a.product_id = p.product_id
GROUP BY
    p.product_id,
    p.name,
    category;


-- Metabase

-- SELECT
--     p.product_id,
--     p.name AS product_name,
--     p.category,
--     SUM(a.quantity) AS total_quantity,
--     COUNT(*) AS add_to_cart_count,
--     ROUND(SUM(a.quantity * p.price), 2) AS estimated_revenue
-- FROM digikala.add_to_cart a
-- JOIN digikala.products p
--     ON a.product_id = p.product_id
-- GROUP BY
--     p.product_id,
--     p.name,
--     p.category
-- ORDER BY estimated_revenue DESC
-- LIMIT 10;