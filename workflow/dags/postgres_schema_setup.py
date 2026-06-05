from airflow import DAG

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime

with DAG(
        dag_id="postgres_schema_setup",
        start_date=datetime(2026, 1, 1),
        schedule="@once",
        catchup=False,
        tags=["schema", "ddl"]
) as dag:
    create_tables = SQLExecuteQueryOperator(
        task_id="create_postgres_tables",
        conn_id="postgres_business",
        sql="""
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS users;
        DROP TABLE IF EXISTS products;
        DROP TYPE IF EXISTS loyalty_tier_enum CASCADE;
        DROP TYPE IF EXISTS orders_status CASCADE;
        DROP TYPE IF EXISTS orders_payment_method CASCADE;
        DROP TYPE IF EXISTS products_category CASCADE;

        CREATE TYPE loyalty_tier_enum AS ENUM (
            'Bronze',
            'Silver',
            'Gold'
        );

         CREATE TYPE orders_status AS ENUM (
            'completed'
        );

        CREATE TYPE orders_payment_method AS ENUM (
            'credit_card',
            'apple_pay',
            'google_pay',
            'paypal'
        );

        CREATE TYPE products_category AS ENUM (
            'Beauty',
            'Clothing',
            'Electronics',
            'Home',
            'Other'
        );

        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR(50) PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            signup_date TIMESTAMP,
            device VARCHAR(50),
            loyalty_tier loyalty_tier_enum,
            location TEXT
        );

        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(50) PRIMARY KEY,
            name TEXT NOT NULL,
            price NUMERIC(10,2),
            category products_category,
            inventory INTEGER,
            popularity_score FLOAT
        );

        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(50) PRIMARY KEY,
            user_id VARCHAR(50),
            created_at TIMESTAMP,
            total NUMERIC(10,2),
            status orders_status,
            payment_method orders_payment_method,

            FOREIGN KEY (user_id)
            REFERENCES users(user_id)
        );
        """
    )
