from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

DATA_PATH = '/opt/airflow/data/weather_data.csv'
CITY = 'Bangalore'

def fetch_weather_data():
    url = f'https://wttr.in/{CITY}?format=j1'
    response = requests.get(url)
    data = response.json()
    today = data['current_condition'][0]
    df = pd.DataFrame([{
        'city': CITY,
        'temperature_C': today['temp_C'],
        'humidity': today['humidity'],
        'weather_desc': today['weatherDesc'][0]['value'],
        'time': datetime.now().isoformat()
    }])
    os.makedirs('/opt/airflow/data', exist_ok=True)
    df.to_csv(DATA_PATH, mode='a', header=not os.path.exists(DATA_PATH), index=False)
    print("✅ Weather data saved:", df)

with DAG(
    dag_id='weather_api_etl',
    default_args=default_args,
    description='Fetch daily weather data from wttr.in API',
    schedule='@daily',
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['api', 'etl', 'weather']
) as dag:

    fetch_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

fetch_weather
