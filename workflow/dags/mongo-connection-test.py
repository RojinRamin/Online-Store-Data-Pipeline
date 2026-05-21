from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.providers.mongo.hooks.mongo import MongoHook

with DAG(
    dag_id="mongo_test_dag",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False
):

    @task
    def test_mongo():
        hook = MongoHook(mongo_conn_id="mongo_test")
        client = hook.get_conn()
        print(client.admin.command("ping"))

    test_mongo()
