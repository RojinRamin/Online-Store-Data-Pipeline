from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow')

from tasks.kafka_producer import publish_mongo_events_to_kafka

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="mongo_to_kafka_pipeline",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    tags=["mongo", "kafka"],
) as dag:

    publish_events = PythonOperator(
        task_id="publish_mongo_events",
        python_callable=publish_mongo_events_to_kafka,
        execution_timeout=timedelta(minutes=60),
    )
