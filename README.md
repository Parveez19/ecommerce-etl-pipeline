let's get started with ecommerce etl pipeline

# E-commerce ETL Pipeline

This project demonstrates a simple ETL pipeline using Python and MySQL.

## Dataset
E-commerce transaction dataset from Kaggle.

## Steps
1. Load raw CSV data
2. Clean invalid records (negative quantity, cancelled orders, missing values)
3. Create derived column (TotalPrice)
4. Load cleaned data into MySQL using batch inserts

## Tech Stack
- Python (Pandas)
- MySQL
- SQL

## How to Run
1. Create MySQL database and table using `sql/ecommerce.sql`
2. Update DB credentials in `scripts/load_to_mysql.py`
3. Run:
```bash
python scripts/load_to_mysql.py
