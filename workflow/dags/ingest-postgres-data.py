from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
from psycopg2.extras import execute_values
from airflow.operators.empty import EmptyOperator
import os
from airflow.sensors.base import BaseSensorOperator
from airflow.models import Variable


BASE_PATH = "/opt/airflow/data"

COLUMN_MAPPING = {
    "signup_date": "date_signup",
    "popularity_score": "score_popularity"
}

EXPECTED_COLUMNS = {
    "users": ["user_id", "name", "email", "signup_date","device","loyalty_tier","location"],
    "products": ["product_id", "name", "price", "category", "inventory","popularity_score"],
    "orders": ["order_id", "user_id", "timestamp", "total", "status", "payment_method"]
}

class FileModifiedSensor(BaseSensorOperator):
    def __init__(self, file_path, variable_key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.variable_key = variable_key

    def poke(self, context):
        if not os.path.exists(self.file_path):
            return False
            
        current_mtime = os.path.getmtime(self.file_path)
        last_mtime = float(Variable.get(self.variable_key, default_var=0.0))
        
        if current_mtime > last_mtime:
            Variable.set(self.variable_key, str(current_mtime))
            return True
        return False


def load_to_postgres(table_name, file_name):
    file_path = os.path.join(BASE_PATH, file_name)
    
    df = pd.read_csv(
        file_path, 
        engine='python', 
        on_bad_lines='warn'
    )
    
    df = df.dropna(how='all')
    df.columns = [c.lower().strip() for c in df.columns]
    
    df = df.rename(columns=COLUMN_MAPPING)

    if table_name in EXPECTED_COLUMNS:
        cols_to_keep = [c for c in df.columns if c in EXPECTED_COLUMNS[table_name]]
        df = df[cols_to_keep]

    critical_columns = {
        "users": "user_id",
        "products": "product_id",
        "orders": "order_id"
    }
    
    if table_name in critical_columns and critical_columns[table_name] in df.columns:
        df = df.dropna(subset=[critical_columns[table_name]])

    if "email" in df.columns:
        df["email"] = df["email"].str.lower()

    hook = PostgresHook(postgres_conn_id="postgres_business")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cols = ",".join(df.columns)
    
    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES %s ON CONFLICT DO NOTHING;"

    df = df.dropna()
    values = [tuple(x) for x in df.to_numpy()]

    if values:
        execute_values(cursor, insert_query, values)
        conn.commit()
    
    cursor.close()
    conn.close()

with DAG(
    dag_id="csv_etl_pipeline", 
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    wait_users = FileModifiedSensor(
        task_id='wait_users',
        file_path=os.path.join(BASE_PATH, 'users.csv'),
        variable_key='last_mtime_users',
        poke_interval=30 
    )

    wait_products = FileModifiedSensor(
        task_id='wait_products',
        file_path=os.path.join(BASE_PATH, 'products.csv'),
        variable_key='last_mtime_products',
        poke_interval=30
    )

    wait_orders = FileModifiedSensor(
        task_id='wait_orders',
        file_path=os.path.join(BASE_PATH, 'orders.csv'),
        variable_key='last_mtime_orders',
        poke_interval=30
    )
    load_users = PythonOperator(
        task_id="load_users",
        python_callable=load_to_postgres,
        op_kwargs={"table_name": "users", "file_name": "users.csv"}
    )

    load_products = PythonOperator(
        task_id="load_products",
        python_callable=load_to_postgres,
        op_kwargs={"table_name": "products", "file_name": "products.csv"}
    )

    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_to_postgres,
        op_kwargs={"table_name": "orders", "file_name": "orders.csv"}
    )

    wait_users >> load_users
    wait_products >> load_products
    [load_users, load_products] >> wait_orders >> load_orders
