from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_to_s3, load_to_mysql
from etl.metadata import get_last_processed_ts, update_last_processed_ts
from etl.db import get_mysql_connection
import argparse


from dotenv import load_dotenv
import os
import logging
import pandas as pd

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["incremental", "reprocess"], default="incremental")
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    return parser.parse_args()

def run_pipeline(mode: str, year: int | None, month: int | None):
    args = parse_args()
    mode = args.mode
    year = args.year
    month = args.month

    mysql_conn = get_mysql_connection()
    cursor = mysql_conn.cursor()

    RAW_DATA_PATH = os.getenv("RAW_S3_PATH")
    df_raw = extract_data(RAW_DATA_PATH)
    df_clean = transform_data(df_raw)
    df_clean["InvoiceDate"] = pd.to_datetime(df_clean["InvoiceDate"])

    if mode == "incremental":
        last_ts = get_last_processed_ts(cursor, "ecommerce_pipeline")
        logging.info(f"Last processed timestamp: {last_ts}")
        df_to_process = df_clean[df_clean["InvoiceDate"] > last_ts]

    elif mode == "reprocess":
        if year is None or month is None:
            raise ValueError("Reprocess mode requires --year and --month")
        logging.info(f"Reprocessing data for {year}-{month:02d}")
        df_to_process = df_clean[
            (df_clean["InvoiceDate"].dt.year == year) &
            (df_clean["InvoiceDate"].dt.month == month)
        ]

    logging.info(f"Records to process: {len(df_to_process)}")

    if df_to_process.empty:
        logging.info("No data to process. Exiting.")
        cursor.close()
        mysql_conn.close()
        return

    PROCESSED_S3_PATH = os.getenv("PROCESSED_S3_PATH")
    load_to_s3(df_to_process, PROCESSED_S3_PATH)
    load_to_mysql(df_to_process)

    if mode == "incremental":
        max_ts = df_to_process["InvoiceDate"].max()
        update_last_processed_ts(cursor, "ecommerce_pipeline", max_ts)
        mysql_conn.commit()

    cursor.close()
    mysql_conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ecommerce ETL Pipeline")

    parser.add_argument(
        "--mode",
        choices=["incremental", "reprocess"],
        default="incremental",
        help="Pipeline execution mode"
    )

    parser.add_argument("--year", type=int, help="Year to reprocess")
    parser.add_argument("--month", type=int, help="Month to reprocess")

    args = parser.parse_args()

    run_pipeline(
        mode=args.mode,
        year=args.year,
        month=args.month
    )



