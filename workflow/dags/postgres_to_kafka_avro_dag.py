# dags/postgres_to_kafka_avro_dag.py

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="postgres_to_kafka_avro_batch_publish",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres", "kafka", "avro", "schema-registry"],
) as dag:

    publish_postgres_to_kafka_avro = BashOperator(
        task_id="publish_postgres_to_kafka_avro",
        bash_command="python /opt/airflow/scripts/postgres_to_kafka_avro.py",
    )