import os
import logging
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

def load_to_s3(df: pd.DataFrame, s3_path: str):
    try:
        logging.info(f"Writing processed data to {s3_path}")
        df.to_csv(s3_path, index=False)
        logging.info("Success: Processed data written to S3")
    except Exception as e:
        logging.error(f"S3 Upload Error: {e}")
        raise

def load_to_mysql(df):
    if df.empty:
        logging.info("No data to load into MySQL.")
        return

    cols = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "Country",
        "TotalPrice"
    ]

    df = df[cols]  # 🚨 THIS IS MANDATORY

    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

    cursor = conn.cursor()

    insert_sql = """
    INSERT INTO ecommerce_sales (
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    UnitPrice,
    Country,
    TotalPrice
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
    Quantity = VALUES(Quantity),
    UnitPrice = VALUES(UnitPrice),
    InvoiceDate = VALUES(InvoiceDate),
    TotalPrice = VALUES(TotalPrice)
    """


    data = list(df.itertuples(index=False, name=None))

    logging.info(f"Inserting {len(data)} records into MySQL...")

    cursor.executemany(insert_sql, data)
    conn.commit()

    cursor.close()
    conn.close()

