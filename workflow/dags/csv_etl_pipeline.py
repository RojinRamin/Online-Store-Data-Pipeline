from airflow import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator
)

from airflow.providers.standard.operators.empty import (
    EmptyOperator
)

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
    tags=["etl", "csv", "postgres"],
    max_active_runs=1
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # File Monitoring Sensors
    wait_users = FileModifiedSensor(
        task_id="wait_users",

        file_path=os.path.join(
            BASE_PATH,
            "users.csv"
        ),

        variable_key="users_mtime",
        mode="reschedule",
        timeout=60 * 30,
        poke_interval=30
    )


    wait_orders = FileModifiedSensor(
        task_id="wait_orders",

        file_path=os.path.join(
            BASE_PATH,
            "orders.csv"
        ),

        variable_key="orders_mtime",
        mode="reschedule",
        timeout=60 * 30,
        poke_interval=30
    )

    load_users = PythonOperator(
        task_id="load_users",
        python_callable=load_to_postgres,
        op_kwargs={"table_name": "users", "file_name": "users.csv"}
    )

    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_to_postgres,
        op_kwargs={"table_name": "orders", "file_name": "orders.csv"}
    )

    load_products = PythonOperator(
        task_id="load_products",
        python_callable=load_to_postgres,
        op_kwargs={"table_name": "products", "file_name": "products.csv"}
    )

    # Task Pipeline mapping
    start >> wait_users
    
    wait_users >> load_users 
    
    # Enforce foreign key requirement: users must load before orders can clear
    [load_users, wait_orders] >> load_orders
    
    
    [load_orders, load_products] >> end