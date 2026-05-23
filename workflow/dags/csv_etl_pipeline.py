from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime

import os

from utils.postgres_constants import BASE_PATH

from utils.last_modified_sensor import FileModifiedSensor

from tasks.postgres_load import load_to_postgres


with DAG(
    dag_id="csv_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    tags=["etl", "csv", "postgres"]
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    wait_users = FileModifiedSensor(
        task_id="wait_users",

        file_path=os.path.join(
            BASE_PATH,
            "users.csv"
        ),

        variable_key="users_mtime",

        poke_interval=30
    )

    wait_products = FileModifiedSensor(
        task_id="wait_products",

        file_path=os.path.join(
            BASE_PATH,
            "products.csv"
        ),

        variable_key="products_mtime",

        poke_interval=30
    )

    wait_orders = FileModifiedSensor(
        task_id="wait_orders",

        file_path=os.path.join(
            BASE_PATH,
            "orders.csv"
        ),

        variable_key="orders_mtime",

        poke_interval=30
    )

    load_users = PythonOperator(
        task_id="load_users",

        python_callable=load_to_postgres,

        op_kwargs={
            "table_name": "users",
            "file_name": "users.csv"
        }
    )

    load_products = PythonOperator(
        task_id="load_products",

        python_callable=load_to_postgres,

        op_kwargs={
            "table_name": "products",
            "file_name": "products.csv"
        }
    )

    load_orders = PythonOperator(
        task_id="load_orders",

        python_callable=load_to_postgres,

        op_kwargs={
            "table_name": "orders",
            "file_name": "orders.csv"
        }
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> [wait_users, wait_products]

    wait_users >> load_users

    wait_products >> load_products

    load_users >> wait_orders

    wait_orders >> load_orders

    [load_products, load_orders] >> end