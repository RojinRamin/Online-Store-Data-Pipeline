from datetime import datetime
import sys

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/scripts")

from clickhouse_setup import main


with DAG(
    dag_id="postgres_kafka_to_clickhouse_pyop",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["clickhouse", "kafka"],
) as dag:

    setup_clickhouse = PythonOperator(
        task_id="setup_clickhouse_tables_and_views",
        python_callable=main,
    )
