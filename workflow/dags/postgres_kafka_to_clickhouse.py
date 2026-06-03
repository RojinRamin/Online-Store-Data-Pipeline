from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id="postgres_kafka_to_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="clickhouse_conn",
        sql="sql/tables.sql"
    )

    create_mv = SQLExecuteQueryOperator(
        task_id="create_mv",
        conn_id="clickhouse_conn",
        sql="sql/mv.sql"
    )

    create_tables >> create_mv
