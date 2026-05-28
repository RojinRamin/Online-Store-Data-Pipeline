# scripts/behavioral_transform.py

from datetime import datetime
import os

COMMON_REQUIRED_FIELDS = [
    "timestamp",
    "user_id",
    "event_type",
    "device",
    "session_id",
]


VALID_EVENT_TYPES = {
    "page_view",
    "product_search",
    "cart_view",
    "add_to_cart",
    "remove_from_cart",
    "wishlist_add",
    "checkout_start",
    "payment_attempt",
    "order_complete",
}


class TransformError(Exception):
    """Raised when a raw event cannot be transformed safely."""
    pass


def is_null(value):
    """
    Detects missing/null-like values from JSON/CSV/string sources.
   """
    return (
        value is None
        or value == ""
        or str(value).strip().lower() in ["null", "none", "nan"]
    )


def cast_str(value, field_name, required=False, default=None):
    if is_null(value):
        if required:
            raise TransformError(f"Required string field is null: {field_name}")
        return default

    return str(value).strip()


def cast_int(value, field_name, required=False, default=None):
    if is_null(value):
        if required:
            raise TransformError(f"Required int field is null: {field_name}")
        return default

    try:
        return int(value)
    except (ValueError, TypeError):
        raise TransformError(f"Cannot cast field '{field_name}' to int: {value}")


def cast_float(value, field_name, required=False, default=None):
    if is_null(value):
        if required:
            raise TransformError(f"Required float field is null: {field_name}")
        return default

    try:
        return float(value)
    except (ValueError, TypeError):
        raise TransformError(f"Cannot cast field '{field_name}' to float: {value}")


def cast_bool(value, field_name, required=False, default=None):
    if is_null(value):
        if required:
            raise TransformError(f"Required bool field is null: {field_name}")
        return default

    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()

    if normalized in ["true", "1", "yes", "y"]:
        return True

    if normalized in ["false", "0", "no", "n"]:
        return False

    raise TransformError(f"Cannot cast field '{field_name}' to bool: {value}")


def cast_datetime(value, field_name="timestamp", required=True, default=None):
    if is_null(value):
        if required:
            raise TransformError(f"Required datetime field is null: {field_name}")
        return default

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise TransformError(f"Cannot cast field '{field_name}' to datetime: {value}")


def validate_common_fields(raw_event):
    for field in COMMON_REQUIRED_FIELDS:
        if is_null(raw_event.get(field)):
            raise TransformError(f"Missing required common field: {field}")

    event_type = raw_event.get("event_type")

    if event_type not in VALID_EVENT_TYPES:
        raise TransformError(f"Unknown event_type: {event_type}")


def transform_page_view(raw_event):
    return {
        "url_path": cast_str(
            raw_event.get("url_path"),
            "url_path",
            required=True
        ),
        "duration_sec": cast_int(
            raw_event.get("duration_sec"),
            "duration_sec",
            required=False,
            default=0
        ),
        "http_status": cast_int(
            raw_event.get("http_status"),
            "http_status",
            required=True
        ),
    }


def transform_product_search(raw_event):
    return {
        "query": cast_str(
            raw_event.get("query"),
            "query",
            required=True
        ),
        "results_count": cast_int(
            raw_event.get("results_count"),
            "results_count",
            required=False,
            default=0
        ),
        "clicked_position": cast_int(
            raw_event.get("clicked_position"),
            "clicked_position",
            required=False,
            default=None
        ),
    }


def transform_cart_view(raw_event):
    cart_items = raw_event.get("cart_items")

    if cart_items is None:
        cart_items = []

    if not isinstance(cart_items, list):
        raise TransformError(f"cart_items must be list, got: {type(cart_items)}")

    cleaned_items = []

    for item in cart_items:
        cleaned_items.append({
            "product_id": cast_str(
                item.get("product_id"),
                "cart_items.product_id",
                required=True
            ),
            "price": cast_float(
                item.get("price"),
                "cart_items.price",
                required=False,
                default=0.0
            ),
            "quantity": cast_int(
                item.get("quantity"),
                "cart_items.quantity",
                required=False,
                default=1
            ),
        })

    return {
        "cart_items": cleaned_items,
        "cart_value": cast_float(
            raw_event.get("cart_value"),
            "cart_value",
            required=False,
            default=0.0
        ),
    }


def transform_add_to_cart(raw_event):
    return {
        "product_id": cast_str(
            raw_event.get("product_id"),
            "product_id",
            required=True
        ),
        "quantity": cast_int(
            raw_event.get("quantity"),
            "quantity",
            required=True
        ),
        "cart_total_items": cast_int(
            raw_event.get("cart_total_items"),
            "cart_total_items",
            required=False,
            default=None
        ),
    }


def transform_remove_from_cart(raw_event):
    return {
        "product_id": cast_str(
            raw_event.get("product_id"),
            "product_id",
            required=True
        ),
        "quantity": cast_int(
            raw_event.get("quantity"),
            "quantity",
            required=True
        ),
        "cart_total_items": cast_int(
            raw_event.get("cart_total_items"),
            "cart_total_items",
            required=False,
            default=None
        ),
    }


def transform_wishlist_add(raw_event):
    return {
        "product_id": cast_str(
            raw_event.get("product_id"),
            "product_id",
            required=True
        ),
        "wishlist_name": cast_str(
            raw_event.get("wishlist_name"),
            "wishlist_name",
            required=False,
            default="default"
        ),
    }


def transform_checkout_start(raw_event):
    return {
        "shipping_method": cast_str(
            raw_event.get("shipping_method"),
            "shipping_method",
            required=False,
            default=None
        ),
        "cart_value": cast_float(
            raw_event.get("cart_value"),
            "cart_value",
            required=True
        ),
    }


def transform_payment_attempt(raw_event):
    return {
        "payment_type": cast_str(
            raw_event.get("payment_type"),
            "payment_type",
            required=True
        ),
        "success": cast_bool(
            raw_event.get("success"),
            "success",
            required=True
        ),
        "error_code": cast_str(
            raw_event.get("error_code"),
            "error_code",
            required=False,
            default=None
        ),
    }


def transform_order_complete(raw_event):
    return {
        "order_id": cast_str(
            raw_event.get("order_id"),
            "order_id",
            required=True
        ),
        "fulfillment_speed": cast_str(
            raw_event.get("fulfillment_speed"),
            "fulfillment_speed",
            required=False,
            default=None
        ),
    }


EVENT_TRANSFORMERS = {
    "page_view": transform_page_view,
    "product_search": transform_product_search,
    "cart_view": transform_cart_view,
    "add_to_cart": transform_add_to_cart,
    "remove_from_cart": transform_remove_from_cart,
    "wishlist_add": transform_wishlist_add,
    "checkout_start": transform_checkout_start,
    "payment_attempt": transform_payment_attempt,
    "order_complete": transform_order_complete,
}


def transform_event(raw_event, source_file):
    """
    Main transform function.

    Input:
        raw_event: one JSON object from one line of JSONL file
        source_file: file path or file name

    Output:
        standardized MongoDB document
    """

    validate_common_fields(raw_event)

    event_type = raw_event.get("event_type")
    event_transformer = EVENT_TRANSFORMERS[event_type]

    event_data = event_transformer(raw_event)

    transformed_event = {
        "timestamp": cast_datetime(
            raw_event.get("timestamp"),
            "timestamp",
            required=True
        ),
        "user_id": cast_str(
            raw_event.get("user_id"),
            "user_id",
            required=True
        ),
        "session_id": cast_str(
            raw_event.get("session_id"),
            "session_id",
            required=True
        ),
        "event_type": cast_str(
            raw_event.get("event_type"),
            "event_type",
            required=True
        ),
        "device": cast_str(
            raw_event.get("device"),
            "device",
            required=True
        ),
        "event_data": event_data,
        "ingestion": {
            "source_file": os.path.basename(source_file),
            "loaded_at": datetime.utcnow(),
        }
    }

    return transformed_event


def transform_events(raw_events, source_file):
    """
    Transforms a list of raw events.

    Returns:
        successful_events
        rejected_events
    """

    successful_events = []
    rejected_events = []

    for index, raw_event in enumerate(raw_events):
        try:
            transformed = transform_event(raw_event, source_file)
            successful_events.append(transformed)

        except Exception as error:
            rejected_events.append({
                "source_file": os.path.basename(source_file),
                "line_number": index + 1,
                "raw_event": raw_event,
                "error": str(error),
                "rejected_at": datetime.utcnow(),
            })

    return successful_events, rejected_events
