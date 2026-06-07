

CREATE MATERIALIZED VIEW digikala.device_page_view_mv
TO digikala.device_usage_report
AS
SELECT
    device,

    count() AS page_views,

    0 AS searches,
    0 AS add_to_carts,
    0 AS checkout_starts,
    0 AS payment_attempts,
    0 AS completed_orders
FROM digikala.page_view
GROUP BY device;


CREATE MATERIALIZED VIEW digikala.device_search_mv
TO digikala.device_usage_report
AS
SELECT
    device,

    0 AS page_views,
    count() AS searches,

    0 AS add_to_carts,
    0 AS checkout_starts,
    0 AS payment_attempts,
    0 AS completed_orders
FROM digikala.product_search
GROUP BY device;

CREATE MATERIALIZED VIEW digikala.device_add_to_cart_mv
TO digikala.device_usage_report
AS
SELECT
    device,

    0 AS page_views,
    0 AS searches,

    count() AS add_to_carts,

    0 AS checkout_starts,
    0 AS payment_attempts,
    0 AS completed_orders
FROM digikala.add_to_cart
GROUP BY device;


CREATE MATERIALIZED VIEW digikala.device_order_mv
TO digikala.device_usage_report
AS
SELECT
    device,

    0 AS page_views,
    0 AS searches,
    0 AS add_to_carts,
    0 AS checkout_starts,
    0 AS payment_attempts,

    count() AS completed_orders
FROM digikala.order_complete
GROUP BY device;

INSERT INTO digikala.device_usage_report
SELECT
    device,
    count(),
    0,
    0,
    0,
    0,
    0
FROM digikala.page_view
GROUP BY device;


INSERT INTO digikala.device_usage_report
SELECT
    device,
    0,
    count(),
    0,
    0,
    0,
    0
FROM digikala.product_search
GROUP BY device;


INSERT INTO digikala.device_usage_report
SELECT
    device,
    0,
    0,
    count(),
    0,
    0,
    0
FROM digikala.add_to_cart
GROUP BY device;


INSERT INTO digikala.device_usage_report
SELECT
    device,
    0,
    0,
    0,
    0,
    0,
    count()
FROM digikala.order_complete
GROUP BY device;


SELECT
    device,

    SUM(page_views) AS page_views,
    SUM(searches) AS searches,
    SUM(add_to_carts) AS add_to_carts,
    SUM(completed_orders) AS completed_orders
FROM digikala.device_usage_report
GROUP BY device
ORDER BY completed_orders DESC;
   



-- Metbase
-- SELECT
--     device,

--     SUM(page_views) AS page_views,
--     SUM(searches) AS searches,
--     SUM(add_to_carts) AS add_to_carts,
--     SUM(completed_orders) AS completed_orders
-- FROM digikala.device_usage_report
-- GROUP BY device
-- ORDER BY completed_orders DESC;