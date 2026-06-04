from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id="postgres_kafka_to_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=["/opt/airflow/sql"],
) as dag:

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="clickhouse_conn",
        sql="tables.sql",
        split_statements=True,
    )

    create_mv = SQLExecuteQueryOperator(
        task_id="create_mv",
        conn_id="clickhouse_conn",
        sql="mv.sql",
        split_statements=True,
    )

    create_tables >> create_mv
