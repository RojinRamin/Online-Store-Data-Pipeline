from airflow import DAG

from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator
)

from datetime import datetime


with DAG(
    dag_id="schema_setup",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["schema", "ddl"]
) as dag:

    create_tables = SQLExecuteQueryOperator(
        task_id="create_postgres_tables",

        conn_id="postgres_business",

        sql="""

        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR(50) PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            date_signup TIMESTAMP,
            device VARCHAR(50),
            loyalty_tier VARCHAR(50),
            location TEXT
        );

        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(50) PRIMARY KEY,
            name TEXT NOT NULL,
            price NUMERIC(10,2),
            category VARCHAR(100),
            inventory INTEGER,
            score_popularity FLOAT
        );

        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(50) PRIMARY KEY,
            user_id VARCHAR(50),
            timestamp TIMESTAMP,
            total NUMERIC(10,2),
            status VARCHAR(50),
            payment_method VARCHAR(50),

            FOREIGN KEY (user_id)
            REFERENCES users(user_id)
        );

        """
    )