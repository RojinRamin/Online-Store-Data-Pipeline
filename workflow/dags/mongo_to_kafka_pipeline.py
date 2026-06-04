
# dags/mongo_to_kafka_dag.py
 
from datetime import datetime
 
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import sys
 
sys.path.insert(0, '/opt/airflow/scripts/')
 
from mongo_to_kafka import main
 
with DAG(
    dag_id="mongo_to_kafka_batch_publish",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mongo", "kafka", "schema-registry"],
) as dag:
 
    publish_mongo_to_kafka = PythonOperator(
        task_id="publish_mongo_to_kafka",
        python_callable=main,
    )