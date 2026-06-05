from datetime import datetime, timedelta
import glob
import sys

from workflow.tasks.mongo_pipeline import process_all_new_files

from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/mongo")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def find_daily_json_files(**kwargs):
    ds_nodash = kwargs["ds_nodash"]

    pattern = f"/opt/airflow/bootcamp_data/behavioral/sessions_{ds_nodash}_*.json"

    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(f"No json files found with pattern: {pattern}")

    return files

with DAG(
    dag_id="process_daily_behavioral_json_to_mongodb",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    catchup=False,
) as dag:

    wait_json_files = FileSensor(
        task_id="wait_behavioral_json",
        filepath="bootcamp_data/behavioral/sessions_{{ ds_nodash }}_*.json",
        poke_interval=30,
        timeout=600,
        fs_conn_id="fs_default",
    )

    find_new_files = PythonOperator(
        task_id="find_new_files",
        python_callable=find_daily_json_files,
    )

    process_files = PythonOperator(
        task_id="process_all_new_files",
        python_callable=process_all_new_files,
    )

    wait_json_files >> find_new_files >> process_files
