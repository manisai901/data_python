from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}
#apiweather@138
# API details
CITY = "Hyderabad"
API_KEY = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API key}"  # Replace with your OpenWeatherMap key
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

DATA_PATH = "/opt/airflow/data/weather_data_off_all.csv"

def extract_weather_data():
    params = {'q': CITY, 'appid': API_KEY, 'units': 'metric'}
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    weather = {
        "city": CITY,
        "temperature": data['main']['temp'],
        "humidity": data['main']['humidity'],
        "pressure": data['main']['pressure'],
        "weather": data['weather'][0]['description'],
        "wind_speed": data['wind']['speed'],
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    df = pd.DataFrame([weather])

    # Append to file if exists, else create
    if os.path.exists(DATA_PATH):
        df.to_csv(DATA_PATH, mode='a', header=False, index=False)
    else:
        df.to_csv(DATA_PATH, index=False)

    print(f"Weather data for {CITY} saved at {DATA_PATH}")
    print(df)

with DAG(
    dag_id="weather_etl_All_Areas",
    default_args=default_args,
    description="ETL DAG to fetch daily weather data from OpenWeatherMap API",
    schedule="*/2 * * * *",
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["weather", "api", "etl", "All Areas"]
) as dag:

    extract_weather = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data
    )

    extract_weather
