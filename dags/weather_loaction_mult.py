from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd

default_args = {
    'owner': 'weather_multi_city',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def fetch_weather_data():
    API_KEY = "cf8efad55c5de6a9db09329986710f5b"  # 🔑 Replace with your actual API key
    cities = {
        "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
        "Bangalore": {"lat": 12.9716, "lon": 77.5946},
        "Delhi": {"lat": 28.6139, "lon": 77.2090},
        "Mumbai": {"lat": 19.0760, "lon": 72.8777},
        "Chennai": {"lat": 13.0827, "lon": 80.2707},
        "Kolkata": {"lat": 22.5726, "lon": 88.3639}
    }

    all_data = []

    for city, coords in cities.items():
        url = (
            f"https://api.openweathermap.org/data/2.5/weather?"
            f"lat={coords['lat']}&lon={coords['lon']}&units=metric&appid={API_KEY}"
        )
        response = requests.get(url)
        data = response.json()

        if response.status_code == 200:
            weather_info = {
                "city": city,
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather": data["weather"][0]["description"],
                "timestamp": datetime.now()
            }
            all_data.append(weather_info)
        else:
            print(f"⚠️ Failed to fetch data for {city}: {data.get('message')}")

    df = pd.DataFrame(all_data)
    df.to_csv('/opt/airflow/data/multi_city_weather.csv', index=False)
    print("✅ Weather data for multiple cities saved successfully.")

with DAG(
    dag_id='multi_city_weather_etl',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['weather', 'api', 'multi-city']
) as dag:

    fetch_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )
