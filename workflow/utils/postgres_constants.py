BASE_PATH = "/opt/airflow/data"


EXPECTED_COLUMNS = {
    "users": [
        "user_id",
        "name",
        "email",
        "signup_date",
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
        "popularity_score"
    ],
    "orders": [
        "order_id",
        "user_id",
        "created_at",
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