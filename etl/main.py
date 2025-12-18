from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data

def run_pipeline():
    df = extract_data("data.csv")
    df = transform_data(df)
    load_data(df)

if __name__ == "__main__":
    run_pipeline()
    print("ETL pipeline completed successfully")

