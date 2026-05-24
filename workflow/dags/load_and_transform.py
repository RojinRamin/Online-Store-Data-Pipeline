from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor  
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import os
import json
import glob
import pandas as pd
from hashlib import md5

# ============================================
FOLDER_PATH = "/opt/airflow/data/"  
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

    file_path = os.path.join(FOLDER_PATH, file_name)
    
    if FILE_PATTERN == "*.json":
        transformed_events = transform_jsonl_file(file_path)
    

    records = transformed_events = transform_jsonl_file(file_path)

    
    if not records:
        return 0
    
    status_collection = db['state']

    status_collection.insert_one({
        'file_name': file_name,
        'processed_at': datetime.utcnow(),
        'status': 'success'
    })

    return len(records)




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
}


class TransformError(Exception):
    """Raised when a raw event cannot be transformed safely."""
    pass


def is_null(value):
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
        "quantity": cast_int(raw_event.get("quantity"), "quantity", required=True),
        "cart_total_items": cast_int(raw_event.get("cart_total_items"), "cart_total_items", required=False, default=None),
    }


def transform_remove_from_cart(raw_event):
    return {
        "product_id": cast_str(raw_event.get("product_id"), "product_id", required=True),
        "quantity": cast_int(raw_event.get("quantity"), "quantity", required=True),
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

                if len(transformed_events) == 1:
                    print(f"\n[DEBUG] Sample transformed event (line {line_number}):")
                    print(json.dumps(transformed_event, indent=2, default=str))

            except Exception as error:

                print(
                    f"Skipped invalid event | "
                    f"file={os.path.basename(file_path)} | "
                    f"line={line_number} | "
                    f"error={str(error)}"
                )

    return transformed_events

# ============================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='process_new_files_to_mongodb',
    default_args=default_args,
    schedule='@hourly',  
    catchup=False,

) as dag:


    wait_file = FileSensor(

        task_id = "wait_file" ,

        filepath = "/opt/airflow/data/*.json",

        poke_interval=10,

        timeout=300 ,

        fs_conn_id = "fs_default" 
    )
    
    task1 = PythonOperator(
        task_id='get_file_list',
        python_callable=get_file_list_from_server,
    )
    
    task2 = PythonOperator(
        task_id='get_state',
        python_callable=get_state_from_mongo,
    )
    
    task3 = PythonOperator(
        task_id='find_new_files',
        python_callable=find_new_files,
    )
    
    task4 = PythonOperator(
        task_id='process_all_new_files',
        python_callable=process_all_new_files,
    )
    
    wait_file >> task1 >> task2 >> task3 >> task4
