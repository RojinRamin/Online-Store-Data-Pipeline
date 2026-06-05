-- sales performance, daily report
CREATE TABLE IF NOT EXISTS digikala.sales_performance_daily_report
(
    report_date Date,
    order_count UInt64,
    total_revenue Float64,
    purchasing_users UInt64
)
ENGINE = SummingMergeTree
ORDER BY report_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS digikala.sales_performance_daily_report_mv
TO sales_performance_daily_report
AS
SELECT
    toDate(created_at) AS report_date,
    count() AS order_count,
    sum(total) AS total_revenue,
    countDistinct(user_id) AS purchasing_users
FROM orders
WHERE status = 'completed'
GROUP BY report_date;
