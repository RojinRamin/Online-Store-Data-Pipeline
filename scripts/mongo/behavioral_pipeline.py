from datetime import datetime, timedelta
from airflow.providers.mongo.hooks.mongo import MongoHook
import os
import json
import glob
import pandas as pd
import sys

sys.path.insert(0, '/opt/airflow/mongo')

from cast_utils import (
    TransformError,
    is_null,
    cast_str,
    cast_int,
    cast_float,
    cast_bool,
    cast_datetime 
)

# ============================================
FOLDER_PATH = "/opt/airflow/behavioral-data/"  
FILE_PATTERN = "*.json"  
# ============================================

def get_file_list_from_server(**kwargs):
    all_files = glob.glob(os.path.join(FOLDER_PATH, FILE_PATTERN))
    file_names = [os.path.basename(f) for f in all_files]
    kwargs['ti'].xcom_push(key='all_files', value=file_names)
    return file_names

def get_state_from_mongo(**kwargs):
    hook = MongoHook(mongo_conn_id='mongo_test')
    client = hook.get_conn()
    db = client['mongodb']  
    collection = db['state']
    
    processed_docs = collection.find({}, {"file_name": 1, "_id": 0})
    state = [doc['file_name'] for doc in processed_docs]
    
    kwargs['ti'].xcom_push(key='state', value=state)
    return state

def find_new_files(**kwargs):
    all_files = kwargs['ti'].xcom_pull(key='all_files', task_ids='get_file_list')
    state = kwargs['ti'].xcom_pull(key='state', task_ids='get_state')
    
    new_files = [f for f in all_files if f not in state]
    
    kwargs['ti'].xcom_push(key='new_files', value=new_files)

    return new_files


def process_single_file(file_name, **kwargs):

    hook = MongoHook(mongo_conn_id='mongo_test')

    client = hook.get_conn()

    db = client['mongodb']

    state_collection = db['state']

    file_path = os.path.join(FOLDER_PATH, file_name)

    # Transform records
    if FILE_PATTERN == "*.json":

        records = transform_jsonl_file(file_path)

    else:

        records = []

    if not records:

        print(f"[INFO] No valid records found in {file_name}")

        return 0

    # Load records into MongoDB
    inserted_count = load_records_to_mongodb(records, db)

    # Update processing state
    state_collection.insert_one({

        'file_name': file_name,

        'processed_at': datetime.utcnow(),

        'status': 'success',

        'records_inserted': inserted_count
    })

   # print(
   #     f"[INFO] File processed successfully | "
   #     f"file={file_name} | "
   #     f"inserted_records={inserted_count}"
   # )

    return inserted_count




def process_all_new_files(**kwargs):

    new_files = kwargs['ti'].xcom_pull(key='new_files', task_ids='find_new_files')
    
    if not new_files:
        print("There is no new file")
        return 0
    
    total_records = 0
    failed_files = []
    
    for file_name in new_files:
        try:
            records_count = process_single_file(file_name, **kwargs)
            total_records += records_count
        except Exception as e:
            failed_files.append(file_name)


    print("\n[INFO] Processing summary:")
    print(f"   - Successful files: {len(new_files) - len(failed_files)}")
    print(f"   - Total records inserted: {total_records}")
    
    if failed_files:
        print(f"   - Failed files: {failed_files}")
    
    return total_records


# scripts/behavioral_transform.py


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
    "review_submit"
}


def validate_common_fields(raw_event):
    for field in COMMON_REQUIRED_FIELDS:
        if is_null(raw_event.get(field)):
            raise TransformError(f"Missing required common field: {field}")

    event_type = raw_event.get("event_type")

    if event_type not in VALID_EVENT_TYPES:
        raise TransformError(f"Unknown event_type: {event_type}")


def transform_page_view(raw_event):
    return {
        "url_path": cast_str(raw_event.get("url_path"), "url_path", required=True),
        "duration_sec": cast_int(raw_event.get("duration_sec"), "duration_sec", required=False, default=0),
        "http_status": cast_int(raw_event.get("http_status"), "http_status", required=True),
    }


def transform_product_search(raw_event):
    return {
        "query": cast_str(raw_event.get("query"), "query", required=True),
        "results_count": cast_int(raw_event.get("results_count"), "results_count", required=False, default=0),
        "clicked_position": cast_int(raw_event.get("clicked_position"), "clicked_position", required=False, default=None),
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
            "product_id": cast_str(item.get("product_id"), "cart_items.product_id", required=True),
            "price": cast_float(item.get("price"), "cart_items.price", required=False, default=0.0),
            "quantity": cast_int(item.get("quantity"), "cart_items.quantity", required=False, default=1),
        })

    return {
        "cart_items": cleaned_items,
        "cart_value": cast_float(raw_event.get("cart_value"), "cart_value", required=False, default=0.0),
    }


def transform_add_to_cart(raw_event):
    return {
        "product_id": cast_str(raw_event.get("product_id"), "product_id", required=True),
        "quantity": cast_int(raw_event.get("quantity"), "quantity", required=True, default=1),
        "cart_total_items": cast_int(raw_event.get("cart_total_items"), "cart_total_items", required=False, default=None),
    }


def transform_remove_from_cart(raw_event):
    return {
        "product_id": cast_str(raw_event.get("product_id"), "product_id", required=True),
        "quantity": cast_int(raw_event.get("quantity"), "quantity", required=True, default=1),
        "cart_total_items": cast_int(raw_event.get("cart_total_items"), "cart_total_items", required=False, default=None),
    }


def transform_wishlist_add(raw_event):
    return {
        "product_id": cast_str(raw_event.get("product_id"), "product_id", required=True),
        "wishlist_name": cast_str(raw_event.get("wishlist_name"), "wishlist_name", required=False, default="default"),
    }


def transform_checkout_start(raw_event):
    return {
        "shipping_method": cast_str(raw_event.get("shipping_method"), "shipping_method", required=False, default=None),
        "cart_value": cast_float(raw_event.get("cart_value"), "cart_value", required=True),
    }


def transform_payment_attempt(raw_event):
    return {
        "payment_type": cast_str(raw_event.get("payment_type"), "payment_type", required=True),
        "success": cast_bool(raw_event.get("success"), "success", required=True),
        "error_code": cast_str(raw_event.get("error_code"), "error_code", required=False, default=None),
    }


def transform_order_complete(raw_event):
    
    return {
        "order_id": cast_str(raw_event.get("order_id"), "order_id", required=True),
        "fulfillment_speed": cast_str(raw_event.get("fulfillment_speed"), "fulfillment_speed", required=False, default=None),
    }

def transform_review_submit(raw_event):
    return {
        "product_id": cast_str(raw_event.get("product_id"), "product_id", required=True),
        "rating": cast_int(raw_event.get("rating"), "rating", required=True), 
        "text_length": cast_int(raw_event.get("text_length"), "text_length", required=False, default=0)  
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
    "review_submit": transform_review_submit
}


def transform_event(raw_event, source_file):
    validate_common_fields(raw_event)

    event_type = raw_event.get("event_type")
    event_transformer = EVENT_TRANSFORMERS[event_type]
    event_data = event_transformer(raw_event)

    return {
        "timestamp": cast_datetime(raw_event.get("timestamp"), "timestamp", required=True),
        "user_id": cast_str(raw_event.get("user_id"), "user_id", required=True),
        "session_id": cast_str(raw_event.get("session_id"), "session_id", required=True),
        "event_type": cast_str(raw_event.get("event_type"), "event_type", required=True),
        "device": cast_str(raw_event.get("device"), "device", required=True),
        "event_data": event_data,
        "ingestion": {
            "source_file": os.path.basename(source_file),
            "loaded_at": datetime.utcnow(),
        },
    }

def transform_jsonl_file(file_path):
    """
    Reads a JSONL file line by line and transforms valid events.

    Input:
        file_path: path of JSONL file

    Output:
        list of transformed events ready to insert into MongoDB behavioral_events collection
    """

    transformed_events = []

    with open(file_path, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                    continue

            try:
                raw_event = json.loads(line)
                transformed_event = transform_event(raw_event, file_path)
                transformed_events.append(transformed_event)

               # if len(transformed_events) == 1:
               #     print(f"\n[DEBUG] Sample transformed event (line {line_number}):")
               #     print(json.dumps(transformed_event, indent=2, default=str))

            except Exception as error:

                print(
                    f"Skipped invalid event | "
                    f"file={os.path.basename(file_path)} | "
                    f"line={line_number} | "
                    f"error={str(error)}"
                )

    return transformed_events


def load_records_to_mongodb(records, db):

    inserted_count = 0

    for record in records:

        try:

            event_type = record["event_type"]

            # Dynamic collection selection
            collection = db[event_type]

            # Create unique index
            collection.create_index(
                [
                    ("timestamp", 1),
                    ("session_id", 1)
                ],
                unique=True
            )

            collection.insert_one(record)

            inserted_count += 1

        except Exception as e:

            print(
                f"[WARNING] Failed to insert record | "
                f"event_type={record.get('event_type')} | "
                f"error={str(e)}"
            )

    return inserted_count

# ============================================

