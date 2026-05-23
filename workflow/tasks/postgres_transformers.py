import pandas as pd

from utils.postgres_constants import (
    PRIMARY_KEYS
)


def clean_dataframe(df, table_name):

    df = df.dropna(how="all")

    df.columns = [
        c.lower().strip()
        for c in df.columns
    ]


    primary_key = PRIMARY_KEYS.get(table_name)

    if primary_key and primary_key in df.columns:

        df = df.dropna(
            subset=[primary_key]
        )

    if table_name == "users":

        if "email" in df.columns:

            df["email"] = (
                df["email"]
                .astype(str)
                .str.lower()
                .str.strip()
            )

        if "date_signup" in df.columns:

            df["date_signup"] = pd.to_datetime(
                df["date_signup"],
                errors="coerce"
            )

    elif table_name == "products":

        numeric_cols = [
            "price",
            "inventory",
            "score_popularity"
        ]

        for col in numeric_cols:

            if col in df.columns:

                df[col] = pd.to_numeric(
                    df[col],
                    errors="coerce"
                )

    elif table_name == "orders":

        if "timestamp" in df.columns:

            df["timestamp"] = pd.to_datetime(
                df["timestamp"],
                errors="coerce"
            )

        if "total" in df.columns:

            df["total"] = pd.to_numeric(
                df["total"],
                errors="coerce"
            )

    return df