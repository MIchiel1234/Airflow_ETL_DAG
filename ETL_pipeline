import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine

# PostgreSQL connection details
DATABASE_URI = 'postgresql+psycopg2://user:password@localhost:5432/weather_db'

# OpenWeatherMap API configuration
API_KEY = 'your_openweathermap_api_key'
CITY = 'Amsterdam'
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"

def extract_data():
    """Extract weather data from the OpenWeatherMap API."""
    response = requests.get(URL)
    data = response.json()

    # Save the extracted data as a JSON file (or you can directly process it)
    with open('/tmp/weather_data.json', 'w') as f:
        json.dump(data, f)

def transform_data():
    """Clean and transform the raw weather data."""
    with open('/tmp/weather_data.json', 'r') as f:
        data = json.load(f)

    # Extract relevant fields from the API response
    weather_info = {
        'city': data['name'],
        'temperature': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'pressure': data['main']['pressure'],
        'description': data['weather'][0]['description'],
        'datetime': datetime.utcfromtimestamp(data['dt'])
    }

    # Convert to pandas DataFrame
    df = pd.DataFrame([weather_info])

    # Save the transformed data as a CSV for easy loading
    df.to_csv('/tmp/transformed_weather_data.csv', index=False)

def load_data():
    """Load the transformed data into a PostgreSQL database."""
    # Load the transformed CSV
    df = pd.read_csv('/tmp/transformed_weather_data.csv')

    # Create PostgreSQL engine
    engine = create_engine(DATABASE_URI)

    # Load data into the 'weather_data' table in PostgreSQL
    df.to_sql('weather_data', engine, if_exists='append', index=False)

# Define the Airflow DAG
with DAG('etl_weather_pipeline',
         start_date=datetime(2023, 9, 1),
         schedule_interval='@daily') as dag:

    extract_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_data
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task
