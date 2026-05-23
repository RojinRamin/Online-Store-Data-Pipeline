BASE_PATH = "/opt/airflow/data"


EXPECTED_COLUMNS = {
    "users": [
        "user_id",
        "name",
        "email",
        "date_signup",
        "device",
        "loyalty_tier",
        "location"
    ],

    "products": [
        "product_id",
        "name",
        "price",
        "category",
        "inventory",
        "score_popularity"
    ],

    "orders": [
        "order_id",
        "user_id",
        "timestamp",
        "total",
        "status",
        "payment_method"
    ]
}

PRIMARY_KEYS = {
    "users": "user_id",
    "products": "product_id",
    "orders": "order_id"
}