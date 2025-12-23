import pandas as pd
import logging

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Starting transformation on {len(df)} rows")

    initial_rows = len(df)

    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    before = len(df)
    df = df.drop_duplicates()
    logging.info(f"Dropped {before - len(df)} duplicate rows")

    before = len(df)
    df = df[~df["invoiceno"].astype(str).str.startswith("c")]
    logging.info(f"Dropped {before - len(df)} cancelled invoices")

    critical_cols = ["quantity", "unitprice", "invoicedate"]
    before = len(df)
    df = df.dropna(subset=critical_cols)
    logging.info(f"Dropped {before - len(df)} rows with null critical fields")

    before = len(df)
    df = df[(df["quantity"] > 0) & (df["unitprice"] > 0)]
    logging.info(f"Dropped {before - len(df)} rows with invalid quantity or price")

    df["invoicedate"] = pd.to_datetime(df["invoicedate"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["invoicedate"])
    logging.info(f"Dropped {before - len(df)} rows with invalid invoice dates")

    df["total_amount"] = df["quantity"] * df["unitprice"]

    logging.info(
        f"Transformation completed: {initial_rows} → {len(df)} rows"
    )

    return df
