import pandas as pd

from workflow.utils.postgres_constanst import PRIMARY_KEYS

LOYALTY_MAPPING = {
    "bronze": "Bronze",
    "silver": "Silver",
    "gold": "Gold"
}

CATEGORY_MAPPING = {
    "beauty": "Beauty",
    "clothing": "Clothing",
    "electronics": "Electronics",
    "home": "Home",
    "other": "Other"
}

STATUS_MAPPING = {
    "completed": "completed"
}

PAYMENT_METHOD_MAPPING = {
    "credit_card": "credit_card",
    "apple_pay": "apple_pay",
    "google_pay": "google_pay",
    "paypal": "paypal"
}

DEVICE_MAPPING = {
    "desktop":'desktop',
    "mobile":"mobile",
    "tablet":"tablet"
}


def clean_dataframe(df, table_name):

    df = df.dropna(how="all")

    df.columns = [c.lower().strip() for c in df.columns]

    primary_key = PRIMARY_KEYS.get(table_name)

    if primary_key and primary_key in df.columns:
        df = df.dropna(subset=[primary_key])

    # ---------------- USERS ----------------
    if table_name == "users":

        if "email" in df.columns:
            df["email"] = df["email"].astype(str).str.strip().str.lower()

        if "signup_date" in df.columns:
            df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")

        if "device" in df.columns:
            df["device"] = (
                df["device"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map(DEVICE_MAPPING)
            )

        if "loyalty_tier" in df.columns:
            df["loyalty_tier"] = (
                df["loyalty_tier"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map(LOYALTY_MAPPING)
            )

        df = df.dropna(subset=["email", "loyalty_tier"])

    # ---------------- PRODUCTS ----------------
    elif table_name == "products":

        for col in ["price", "inventory", "popularity_score"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "category" in df.columns:
            df["category"] = (
                df["category"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map(CATEGORY_MAPPING)
            )

        df = df.dropna(subset=["category"])

    # ---------------- ORDERS ----------------
    elif table_name == "orders":

        # FIX: CSV timestamp → DB created_at
        if "timestamp" in df.columns:
            df = df.rename(columns={"timestamp": "created_at"})

        if "created_at" in df.columns:
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

        if "total" in df.columns:
            df["total"] = pd.to_numeric(df["total"], errors="coerce")

        if "status" in df.columns:
            df["status"] = (
                df["status"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map(STATUS_MAPPING)
            )

        if "payment_method" in df.columns:
            df["payment_method"] = (
                df["payment_method"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map(PAYMENT_METHOD_MAPPING)
            )

        df = df.dropna(subset=["created_at", "status", "payment_method"])

    return df.drop_duplicates()
