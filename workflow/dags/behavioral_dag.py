from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator
import sys

sys.path.insert(0, '/opt/airflow/mongo')

from behavioral_pipeline import (
    get_file_list_from_server,
    get_state_from_mongo,
    find_new_files,
    process_all_new_files
)


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="process_new_files_to_mongodb",
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    catchup=False,
) as dag:

    wait_file = FileSensor(
        task_id="wait_file",
        filepath="/opt/airflow/behavioral-data/*.json",
        poke_interval=10,
        timeout=300,
        fs_conn_id="fs_default",
    )

    get_file_list = PythonOperator(
        task_id="get_file_list",
        python_callable=get_file_list_from_server,
    )

    get_state = PythonOperator(
        task_id="get_state",
        python_callable=get_state_from_mongo,
    )

    find_new = PythonOperator(
        task_id="find_new_files",
        python_callable=find_new_files,
    )

    process_files = PythonOperator(
        task_id="process_all_new_files",
        python_callable=process_all_new_files,
    )

    wait_file >> get_file_list >> get_state >> find_new >> process_files
