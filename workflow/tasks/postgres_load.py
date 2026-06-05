import os
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from workflow.utils.postgres_constanst import BASE_PATH, EXPECTED_COLUMNS, PRIMARY_KEYS
from workflow.tasks.porstgres_transform import clean_dataframe


def load_to_postgres(table_name, file_name):

    file_path = os.path.join(BASE_PATH, file_name)

    df = pd.read_csv(
        file_path,
        engine="python",
        on_bad_lines="warn"
    )

    df = clean_dataframe(df, table_name)

    expected_cols = EXPECTED_COLUMNS.get(table_name, [])

    df = df[df.columns.intersection(expected_cols)]

    df = df.drop_duplicates()

    if df.empty:
        print(f"[{table_name}] No valid data to load.")
        return

    pk = PRIMARY_KEYS.get(table_name)

    if not pk or pk not in df.columns:
        raise ValueError(f"Primary key missing for table {table_name}")

    cols = list(df.columns)
    col_names = ",".join(cols)

    update_cols = [c for c in cols if c != pk]

    update_sql = ", ".join(
        [f"{col} = EXCLUDED.{col}" for col in update_cols]
    )

    query = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES %s
        ON CONFLICT ({pk})
        DO UPDATE SET {update_sql}
    """

    values = [tuple(row) for row in df.to_numpy()]

    hook = PostgresHook(postgres_conn_id="postgres_business")
    conn = hook.get_conn()
    cursor = conn.cursor()

    execute_values(cursor, query, values)

    conn.commit()
    cursor.close()
    conn.close()
