import clickhouse_connect

from airflow.hooks.base import BaseHook


SQL_FILES = [
    "/opt/airflow/sql/tables.sql",
    "/opt/airflow/sql/mv.sql",
]


def run_sql_file(client, file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        sql_text = file.read()

    statements = [
        statement.strip()
        for statement in sql_text.split(";")
        if statement.strip()
    ]

    for statement in statements:
        client.command(statement)


def main():
    conn = BaseHook.get_connection("clickhouse_conn")

    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port or 8123,
        username=conn.login,
        password=conn.password,
        database=conn.schema or "digikala",
    )

    for sql_file in SQL_FILES:
        run_sql_file(client, sql_file)

    print("ClickHouse setup completed.")
