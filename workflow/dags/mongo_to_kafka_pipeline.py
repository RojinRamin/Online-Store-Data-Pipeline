from airflow import DAG

from airflow.providers.standard.operators.python import (
    PythonOperator
)

from datetime import datetime

from tasks.kafka_producer import (
    publish_mongo_events_to_kafka
)


with DAG(
    dag_id="mongo_to_kafka_pipeline",

    start_date=datetime(2026, 1, 1),

    schedule="*/1 * * * *",

    catchup=False,

    tags=["mongo", "kafka"]
) as dag:

    publish_events = PythonOperator(
        task_id="publish_mongo_events",

        python_callable=publish_mongo_events_to_kafka
    )