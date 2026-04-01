from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

# Connection config
DB_CONFIG = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": 5432
}

DATA_DIR = "/opt/airflow/data"

def load_csv_to_postgres(filename, table_name):
    filepath = os.path.join(DATA_DIR, filename)
    df = pd.read_csv(filepath)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Drop and recreate table
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    cursor.execute(f"DROP TYPE IF EXISTS {table_name} CASCADE;")

    # Build CREATE TABLE from dataframe columns
    columns = []
    for col in df.columns:
        columns.append(f'"{col}" TEXT')
    create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
    cursor.execute(create_sql)

    # Insert rows
    for _, row in df.iterrows():
        values = tuple(str(v) if pd.notna(v) else None for v in row)
        placeholders = ', '.join(['%s'] * len(values))
        cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders});", values)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Loaded {len(df)} rows into {table_name}")


def load_orders():
    load_csv_to_postgres("olist_orders_dataset.csv", "raw_orders")

def load_customers():
    load_csv_to_postgres("olist_customers_dataset.csv", "raw_customers")

def load_order_items():
    load_csv_to_postgres("olist_order_items_dataset.csv", "raw_order_items")

def load_products():
    load_csv_to_postgres("olist_products_dataset.csv", "raw_products")

def load_payments():
    load_csv_to_postgres("olist_order_payments_dataset.csv", "raw_payments")


with DAG(
    dag_id="load_olist_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["retailflow", "ingestion"]
) as dag:

    t1 = PythonOperator(task_id="load_orders", python_callable=load_orders)
    t2 = PythonOperator(task_id="load_customers", python_callable=load_customers)
    t3 = PythonOperator(task_id="load_order_items", python_callable=load_order_items)
    t4 = PythonOperator(task_id="load_products", python_callable=load_products)
    t5 = PythonOperator(task_id="load_payments", python_callable=load_payments)

    t1 >> t2 >> t3 >> t4 >> t5
