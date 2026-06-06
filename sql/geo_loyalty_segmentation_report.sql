-- Query 

-- SELECT 
--     location,
--     loyalty_tier,
--     COUNT(*) AS user_count
-- FROM digikala.users
-- GROUP BY location, loyalty_tier
-- ORDER BY location, 
--     CASE loyalty_tier
--         WHEN 'Gold' THEN 1
--         WHEN 'Silver' THEN 2
--         WHEN 'Bronze' THEN 3
--         ELSE 4
--     END;


CREATE TABLE IF NOT EXISTS digikala.user_loyalty_segment_report
(
    location String,
    loyalty_tier String,
    user_count UInt64
)
ENGINE = SummingMergeTree
ORDER BY (location, loyalty_tier);


CREATE MATERIALIZED VIEW IF NOT EXISTS digikala.user_loyalty_segment_report_mv
TO digikala.user_loyalty_segment_report
AS
SELECT
    location,
    loyalty_tier,
    count() AS user_count
FROM digikala.users
GROUP BY
    location,
    loyalty_tier;


-- Metabase 

-- SELECT 
--     location,
--     loyalty_tier,
--     SUM(user_count) AS user_count
-- FROM digikala.user_loyalty_segment_report
-- GROUP BY location, loyalty_tier
-- ORDER BY location, 
--     CASE loyalty_tier
--         WHEN 'Gold' THEN 1
--         WHEN 'Silver' THEN 2
--         WHEN 'Bronze' THEN 3
--         ELSE 4
--     END;