from airflow import DAG
from datetime import datetime
import os

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.models import Variable

from utils.postgres_constants import BASE_PATH
from tasks.postgres_load import load_to_postgres


def file_changed(file_name, variable_key):
    file_path = os.path.join(BASE_PATH, file_name)

    if not os.path.exists(file_path):
        return False

    current_mtim = os.path.getmtim(file_path)
    last_mtime = float(Variable.get(variable_key, default_var=0.0))

    return current_mtim > last_mtime


def update_mtime(file_name, variable_key):
    file_path = os.path.join(BASE_PATH, file_name)

    current_mtim = os.path.getmtim(file_path)
    Variable.set(variable_key, str(current_mtim))



def load_users_if_needed():
    file_name = "users.csv"
    key = "users_mtime"

    if file_changed(file_name, key):
        load_to_postgres("users", file_name)
        update_mtime(file_name, key)
    else:
        print('ُThere is no change for users')


def load_orders_if_needed():
    file_name = "orders.csv"
    key = "orders_mtime"

    if file_changed(file_name, key):
        load_to_postgres("orders", file_name)
        update_mtime(file_name, key)
    else:
        print('ُThere is no change for orders')


def load_products_if_needed():
    file_name = "products.csv"
    key = "products_mtime"

    if file_changed(file_name, key):
        load_to_postgres("products", file_name)
        update_mtime(file_name, key)
    else:
        print('ُThere is no change for products')


with DAG(
    dag_id="csv_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "csv", "postgres"]
) as dag:

    start = EmptyOperator(task_id="start")

    load_users = PythonOperator(
        task_id="load_users",
        python_callable=load_users_if_needed
    )

    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders_if_needed
    )

    load_products = PythonOperator(
        task_id="load_products",
        python_callable=load_products_if_needed
    )

    end = EmptyOperator(task_id="end")


    start >> load_users >> load_orders >> load_products >> end