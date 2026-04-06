from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def extract_data():
    os.makedirs("/opt/airflow/data", exist_ok=True)
    data = {
        "video_id": ["abc123", "def456", "ghi789"],
        "views": [1000, 25400, 4200],
        "likes": [200, 5040, 8040],
        "comments": [20, 445, 645]
    }
    df = pd.DataFrame(data)
    df.to_csv('/opt/airflow/data/youtubeee_data.csv', index=False)
    print("✅ Data extracted and saved.")

def transform_data():
    df = pd.read_csv('/opt/airflow/data/youtubeee_data.csv')
    df["like_rate"] = (df["likes"] / df["views"]) * 100
    df.to_csv('/opt/airflow/data/youtubeee_data_transformed.csv', index=False)
    print("✅ Data transformed and saved.")

def load_data():
    df = pd.read_csv('/opt/airflow/data/youtubeee_data_transformed.csv')
    print("✅ Final data ready for loading:")
    print(df.head())

with DAG(
    dag_id="youtube_etl_fulley",
    default_args=default_args,
    description="Simple YouTube ETL pipeline using pandas",
    schedule="@daily",
    start_date=datetime(2025, 10, 13),
    catchup=False,
    tags=["youtube", "etl", "eltttt"]
) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract >> transform >> load
