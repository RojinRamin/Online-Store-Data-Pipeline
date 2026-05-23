import os
import shutil

import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook

from psycopg2.extras import execute_values

from utils.postgres_constants import (
    BASE_PATH,
    EXPECTED_COLUMNS
)

from tasks.postgres_transformers import clean_dataframe


def load_to_postgres(
    table_name,
    file_name
):

    file_path = os.path.join(
        BASE_PATH,
        file_name
    )

    df = pd.read_csv(
        file_path,
        engine="python",
        on_bad_lines="warn"
    )

    df = clean_dataframe(
        df,
        table_name
    )

    expected_cols = EXPECTED_COLUMNS.get(
        table_name,
        []
    )

    cols_to_keep = [
        c for c in df.columns
        if c in expected_cols
    ]

    df = df[cols_to_keep]

    df = df.drop_duplicates()

    if df.empty:
        return

    hook = PostgresHook(
        postgres_conn_id="postgres_business"
    )

    conn = hook.get_conn()

    cursor = conn.cursor()

    cols = ",".join(df.columns)

    query = f"""
        INSERT INTO {table_name}
        ({cols})
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    values = [
        tuple(x)
        for x in df.to_numpy()
    ]

    execute_values(
        cursor,
        query,
        values
    )

    conn.commit()

    cursor.close()
    conn.close()

    processed_path = os.path.join(
        BASE_PATH,
        "processed",
        file_name
    )

    shutil.move(
        file_path,
        processed_path
    )