from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor  
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import os
import json
import glob
import pandas as pd
from hashlib import md5

# ============================================
FOLDER_PATH = "/opt/airflow/data/"  
FILE_PATTERN = "*.json"  
# ============================================

def get_file_list_from_server(**kwargs):
    all_files = glob.glob(os.path.join(FOLDER_PATH, FILE_PATTERN))
    file_names = [os.path.basename(f) for f in all_files]
    kwargs['ti'].xcom_push(key='all_files', value=file_names)
    return file_names

def get_state_from_mongo(**kwargs):
    hook = MongoHook(mongo_conn_id='mongo_test')
    client = hook.get_conn()
    db = client['mongodb']  
    collection = db['state']
    
    processed_docs = collection.find({}, {"file_name": 1, "_id": 0})
    state = [doc['file_name'] for doc in processed_docs]
    
    kwargs['ti'].xcom_push(key='state', value=state)
    return state

def find_new_files(**kwargs):
    all_files = kwargs['ti'].xcom_pull(key='all_files', task_ids='get_file_list')
    state = kwargs['ti'].xcom_pull(key='state', task_ids='get_state')
    
    new_files = [f for f in all_files if f not in state]
    
    kwargs['ti'].xcom_push(key='new_files', value=new_files)

    return new_files


def process_single_file(file_name, **kwargs):

    hook = MongoHook(mongo_conn_id='mongo_test')
    client = hook.get_conn()
    db = client['mongodb']  

    file_path = os.path.join(FOLDER_PATH, file_name)
    
    if FILE_PATTERN == "*.json":
        df = pd.read_json(file_path, lines=True)  
        print(df)
    

    records = df.to_dict('records')
    
    if not records:
        return 0
    
    status_collection = db['state']

    status_collection.insert_one({
        'file_name': file_name,
        'processed_at': datetime.utcnow(),
        'status': 'success'
    })

    return len(records)




def process_all_new_files(**kwargs):

    new_files = kwargs['ti'].xcom_pull(key='new_files', task_ids='find_new_files')
    
    if not new_files:
        print("There is no new file")
        return 0
    
    total_records = 0
    failed_files = []
    
    for file_name in new_files:
        try:
            records_count = process_single_file(file_name, **kwargs)
            total_records += records_count
        except Exception as e:
            failed_files.append(file_name)


    print("\n[INFO] Processing summary:")
    print(f"   - Successful files: {len(new_files) - len(failed_files)}")
    print(f"   - Total records inserted: {total_records}")
    
    if failed_files:
        print(f"   - Failed files: {failed_files}")
    
    return total_records

# ============================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='process_new_files_to_mongodb',
    default_args=default_args,
    schedule='@hourly',  
    catchup=False,

) as dag:


    wait_file = FileSensor(

        task_id = "wait_file" ,

        filepath = "/opt/airflow/data/*.json",

        poke_interval=10,

        timeout=300 ,

        fs_conn_id = "mongo_test" 
    )
    
    task1 = PythonOperator(
        task_id='get_file_list',
        python_callable=get_file_list_from_server,
    )
    
    task2 = PythonOperator(
        task_id='get_state',
        python_callable=get_state_from_mongo,
    )
    
    task3 = PythonOperator(
        task_id='find_new_files',
        python_callable=find_new_files,
    )
    
    task4 = PythonOperator(
        task_id='process_all_new_files',
        python_callable=process_all_new_files,
    )
    
    wait_file >> task1 >> task2 >> task3 >> task4