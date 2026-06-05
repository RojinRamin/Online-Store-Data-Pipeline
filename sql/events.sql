CREATE DATABASE IF NOT EXISTS digikala;


-- =============================================================================
-- PAGE VIEW
-- =============================================================================

CREATE TABLE digikala.kafka_page_view
(
    timestamp              String,
    user_id                String,
    session_id             String,
    event_type             String,
    device                 String,
    `event_data.url_path`     String,
    `event_data.duration_sec` Int32,
    `event_data.http_status`  Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list               = 'kafka:9092',
    kafka_topic_list                = 'events.page_view',
    kafka_group_name                = 'clickhouse_page_view_v1',
    kafka_format                    = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE digikala.page_view
(
    timestamp    DateTime,
    user_id      String,
    session_id   String,
    event_type   String,
    device       String,
    url_path     String,
    duration_sec Int32,
    http_status  Int32
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_id, session_id, timestamp);

CREATE MATERIALIZED VIEW digikala.page_view_mv
TO digikala.page_view
AS
SELECT
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    user_id,
    session_id,
    event_type,
    device,
    `event_data.url_path`     AS url_path,
    `event_data.duration_sec` AS duration_sec,
    `event_data.http_status`  AS http_status
FROM digikala.kafka_page_view;


-- =============================================================================
-- PRODUCT SEARCH
-- =============================================================================

CREATE TABLE digikala.kafka_product_search
(
    timestamp                       String,
    user_id                         String,
    session_id                      String,
    event_type                      String,
    device                          String,
    `event_data.query`              String,
    `event_data.results_count`      Int32,
    `event_data.clicked_position`   Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list               = 'kafka:9092',
    kafka_topic_list                = 'events.search',
    kafka_group_name                = 'clickhouse_product_search_v1',
    kafka_format                    = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE digikala.product_search
(
    timestamp        DateTime,
    user_id          String,
    session_id       String,
    event_type       String,
    device           String,
    query            String,
    results_count    Int32,
    clicked_position Nullable(Int32)
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_id, session_id, timestamp);

CREATE MATERIALIZED VIEW digikala.product_search_mv
TO digikala.product_search
AS
SELECT
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    user_id,
    session_id,
    event_type,
    device,
    `event_data.query`            AS query,
    `event_data.results_count`    AS results_count,
    `event_data.clicked_position` AS clicked_position
FROM digikala.kafka_product_search;


-- =============================================================================
-- ADD TO CART
-- =============================================================================

CREATE TABLE digikala.kafka_add_to_cart
(
    timestamp                       String,
    user_id                         String,
    session_id                      String,
    event_type                      String,
    device                          String,
    `event_data.product_id`         String,
    `event_data.quantity`           Int32,
    `event_data.cart_total_items`   Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list               = 'kafka:9092',
    kafka_topic_list                = 'events.add_to_cart',
    kafka_group_name                = 'clickhouse_add_to_cart_v1',
    kafka_format                    = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE digikala.add_to_cart
(
    timestamp        DateTime,
    user_id          String,
    session_id       String,
    event_type       String,
    device           String,
    product_id       String,
    quantity         Int32,
    cart_total_items Nullable(Int32)
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_id, session_id, timestamp);

CREATE MATERIALIZED VIEW digikala.add_to_cart_mv
TO digikala.add_to_cart
AS
SELECT
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    user_id,
    session_id,
    event_type,
    device,
    `event_data.product_id`       AS product_id,
    `event_data.quantity`         AS quantity,
    `event_data.cart_total_items` AS cart_total_items
FROM digikala.kafka_add_to_cart;


-- =============================================================================
-- REMOVE FROM CART
-- =============================================================================

CREATE TABLE digikala.kafka_remove_from_cart
(
    timestamp                       String,
    user_id                         String,
    session_id                      String,
    event_type                      String,
    device                          String,
    `event_data.product_id`         String,
    `event_data.quantity`           Int32,
    `event_data.cart_total_items`   Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list               = 'kafka:9092',
    kafka_topic_list                = 'events.remove_from_cart',
    kafka_group_name                = 'clickhouse_remove_from_cart_v1',
    kafka_format                    = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE digikala.remove_from_cart
(
    timestamp        DateTime,
    user_id          String,
    session_id       String,
    event_type       String,
    device           String,
    product_id       String,
    quantity         Int32,
    cart_total_items Nullable(Int32)
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_id, session_id, timestamp);

CREATE MATERIALIZED VIEW digikala.remove_from_cart_mv
TO digikala.remove_from_cart
AS
SELECT
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    user_id,
    session_id,
    event_type,
    device,
    `event_data.product_id`       AS product_id,
    `event_data.quantity`         AS quantity,
    `event_data.cart_total_items` AS cart_total_items
FROM digikala.kafka_remove_from_cart;


-- =============================================================================
-- CART VIEW
-- =============================================================================

CREATE TABLE digikala.kafka_cart_view
(
    timestamp              String,
    user_id                String,
    session_id             String,
    event_type             String,
    device                 String,
    `event_data.cart_value` Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list               = 'kafka:9092',
    kafka_topic_list                = 'events.cart_view',
    kafka_group_name                = 'clickhouse_cart_view_v1',
    kafka_format                    = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE digikala.cart_view
(
    timestamp  DateTime,
    user_id    String,
    session_id String,
    event_type String,
    device     String,
    cart_value Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_id, session_id, timestamp);

CREATE MATERIALIZED VIEW digikala.cart_view_mv
TO digikala.cart_view
AS
SELECT
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    user_id,
    session_id,
    event_type,
    device,
    `event_data.cart_value` AS cart_value
FROM digikala.kafka_cart_view;


-- =============================================================================
-- CHECKOUT START
-- =============================================================================

CREATE TABLE digikala.kafka_checkout_start
(
    timestamp                       String,
    user_id                         String,
    session_id                      String,
    event_type                      String,
    device                          String,
    `event_data.shipping_method`    Nullable(String),
    `event_data.cart_value`         Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list               = 'kafka:9092',
    kafka_topic_list                = 'events.checkout',
    kafka_group_name                = 'clickhouse_checkout_start_v1',
    kafka_format                    = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE digikala.checkout_start
(
    timestamp       DateTime,
    user_id         String,
    session_id      String,
    event_type      String,
    device          String,
    shipping_method Nullable(String),
    cart_value      Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_id, session_id, timestamp);

CREATE MATERIALIZED VIEW digikala.checkout_start_mv
TO digikala.checkout_start
AS
SELECT
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    user_id,
    session_id,
    event_type,
    device,
    `event_data.shipping_method` AS shipping_method,
    `event_data.cart_value`      AS cart_value
FROM digikala.kafka_checkout_start;


-- =============================================================================
-- PAYMENT ATTEMPT
-- =============================================================================

CREATE TABLE digikala.kafka_payment_attempt
(
    timestamp                   String,
    user_id                     String,
    session_id                  String,
    event_type                  String,
    device                      String,
    `event_data.payment_type`   String,
    `event_data.success`        Bool,
    `event_data.error_code`     Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list               = 'kafka:9092',
    kafka_topic_list                = 'events.payment',
    kafka_group_name                = 'clickhouse_payment_attempt_v1',
    kafka_format                    = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE digikala.payment_attempt
(
    timestamp    DateTime,
    user_id      String,
    session_id   String,
    event_type   String,
    device       String,
    payment_type String,
    success      Bool,
    error_code   Nullable(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_id, session_id, timestamp);

CREATE MATERIALIZED VIEW digikala.payment_attempt_mv
TO digikala.payment_attempt
AS
SELECT
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    user_id,
    session_id,
    event_type,
    device,
    `event_data.payment_type` AS payment_type,
    `event_data.success`      AS success,
    `event_data.error_code`   AS error_code
FROM digikala.kafka_payment_attempt;


-- =============================================================================
-- ORDER COMPLETE
-- =============================================================================

CREATE TABLE digikala.kafka_order_complete
(
    timestamp                       String,
    user_id                         String,
    session_id                      String,
    event_type                      String,
    device                          String,
    `event_data.order_id`           String,
    `event_data.fulfillment_speed`  Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list               = 'kafka:9092',
    kafka_topic_list                = 'events.order',
    kafka_group_name                = 'clickhouse_order_complete_v1',
    kafka_format                    = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE digikala.order_complete
(
    timestamp         DateTime,
    user_id           String,
    session_id        String,
    event_type        String,
    device            String,
    order_id          String,
    fulfillment_speed Nullable(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_id, session_id, timestamp);

CREATE MATERIALIZED VIEW digikala.order_complete_mv
TO digikala.order_complete
AS
SELECT
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    user_id,
    session_id,
    event_type,
    device,
    `event_data.order_id`          AS order_id,
    `event_data.fulfillment_speed` AS fulfillment_speed
FROM digikala.kafka_order_complete;


-- =============================================================================
-- REVIEW SUBMIT
-- =============================================================================

CREATE TABLE digikala.kafka_review_submit
(
    timestamp                   String,
    user_id                     String,
    session_id                  String,
    event_type                  String,
    device                      String,
    `event_data.product_id`     String,
    `event_data.rating`         Int32,
    `event_data.text_length`    Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list               = 'kafka:9092',
    kafka_topic_list                = 'events.review',
    kafka_group_name                = 'clickhouse_review_submit_v1',
    kafka_format                    = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE digikala.review_submit
(
    timestamp   DateTime,
    user_id     String,
    session_id  String,
    event_type  String,
    device      String,
    product_id  String,
    rating      Int32,
    text_length Int32
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_id, session_id, timestamp);

CREATE MATERIALIZED VIEW digikala.review_submit_mv
TO digikala.review_submit
AS
SELECT
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    user_id,
    session_id,
    event_type,
    device,
    `event_data.product_id`  AS product_id,
    `event_data.rating`      AS rating,
    `event_data.text_length` AS text_length
FROM digikala.kafka_review_submit;


-- =============================================================================
-- WISHLIST ADD
-- =============================================================================

CREATE TABLE digikala.kafka_wishlist_add
(
    timestamp                       String,
    user_id                         String,
    session_id                      String,
    event_type                      String,
    device                          String,
    `event_data.product_id`         String,
    `event_data.wishlist_name`      String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list               = 'kafka:9092',
    kafka_topic_list                = 'events.wishlist',
    kafka_group_name                = 'clickhouse_wishlist_add_v1',
    kafka_format                    = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE digikala.wishlist_add
(
    timestamp     DateTime,
    user_id       String,
    session_id    String,
    event_type    String,
    device        String,
    product_id    String,
    wishlist_name String
)
ENGINE = ReplacingMergeTree()
ORDER BY (user_id, session_id, timestamp);

CREATE MATERIALIZED VIEW digikala.wishlist_add_mv
TO digikala.wishlist_add
AS
SELECT
    parseDateTimeBestEffortOrNull(timestamp) AS timestamp,
    user_id,
    session_id,
    event_type,
    device,
    `event_data.product_id`    AS product_id,
    `event_data.wishlist_name` AS wishlist_name
FROM digikala.kafka_wishlist_add;