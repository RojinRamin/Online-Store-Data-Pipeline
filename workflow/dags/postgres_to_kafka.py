from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import sys

sys.path.insert(0, '/opt/airflow/scripts/')

from tasks.kafka_postgres_publish import main

with DAG(
    dag_id="postgres_to_kafka_batch_publish",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "kafka", "schema-registry"],
) as dag:

    publish_postgres_to_kafka_avro = PythonOperator(
        task_id="publish_postgres_to_kafka",
        python_callable=main,
    )
