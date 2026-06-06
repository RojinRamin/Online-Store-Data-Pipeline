-- User Behavior Funnel Report (FINAL)

--Query

CREATE TABLE IF NOT EXISTS digikala.user_behavior_funnel_daily_report
(
    report_date Date,
    searches UInt64,
    product_views UInt64,
    add_to_cart_events UInt64,
    checkout_starts UInt64,
    purchases UInt64
)
ENGINE = SummingMergeTree
ORDER BY report_date;


CREATE MATERIALIZED VIEW IF NOT EXISTS digikala.funnel_product_search_mv
TO digikala.user_behavior_funnel_daily_report
AS
SELECT
    toDate(timestamp) AS report_date,
    count() AS searches,
    0 AS product_views,
    0 AS add_to_cart_events,
    0 AS checkout_starts,
    0 AS purchases
FROM digikala.product_search
GROUP BY report_date;


CREATE MATERIALIZED VIEW IF NOT EXISTS digikala.funnel_page_view_mv
TO digikala.user_behavior_funnel_daily_report
AS
SELECT
    toDate(timestamp) AS report_date,
    0 AS searches,
    count() AS product_views,
    0 AS add_to_cart_events,
    0 AS checkout_starts,
    0 AS purchases
FROM digikala.page_view
WHERE lower(url_path) LIKE '%product%'
GROUP BY report_date;


CREATE MATERIALIZED VIEW IF NOT EXISTS digikala.funnel_add_to_cart_mv
TO digikala.user_behavior_funnel_daily_report
AS
SELECT
    toDate(timestamp) AS report_date,
    0 AS searches,
    0 AS product_views,
    count() AS add_to_cart_events,
    0 AS checkout_starts,
    0 AS purchases
FROM digikala.add_to_cart
GROUP BY report_date;


CREATE MATERIALIZED VIEW IF NOT EXISTS digikala.funnel_checkout_start_mv
TO digikala.user_behavior_funnel_daily_report
AS
SELECT
    toDate(timestamp) AS report_date,
    0 AS searches,
    0 AS product_views,
    0 AS add_to_cart_events,
    count() AS checkout_starts,
    0 AS purchases
FROM digikala.checkout_start
GROUP BY report_date;


CREATE MATERIALIZED VIEW IF NOT EXISTS digikala.funnel_order_complete_mv
TO digikala.user_behavior_funnel_daily_report
AS
SELECT
    toDate(timestamp) AS report_date,
    0 AS searches,
    0 AS product_views,
    0 AS add_to_cart_events,
    0 AS checkout_starts,
    count() AS purchases
FROM digikala.order_complete
GROUP BY report_date;


-- METABASE

-- SELECT
--     report_date,
--     sum(searches) AS searches,
--     sum(product_views) AS product_views,
--     sum(add_to_cart_events) AS add_to_cart_events,
--     sum(checkout_starts) AS checkout_starts,
--     sum(purchases) AS purchases,
--     round(100.0 * sum(add_to_cart_events) / nullIf(sum(product_views), 0), 2) AS view_to_cart_rate,
--     round(100.0 * sum(checkout_starts) / nullIf(sum(add_to_cart_events), 0), 2) AS cart_to_checkout_rate,
--     round(100.0 * sum(purchases) / nullIf(sum(checkout_starts), 0), 2) AS checkout_to_purchase_rate,
--     round(100.0 * sum(purchases) / nullIf(sum(product_views), 0), 2) AS view_to_purchase_rate
-- FROM digikala.user_behavior_funnel_daily_report
-- GROUP BY report_date
-- ORDER BY report_date;