from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests, os
import pandas as pd

load_dotenv()

conn=BaseHook.get_connection('mysql_weather')
engine = create_engine(f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")

@dag(start_date=datetime(2025, 9, 20), schedule='0 9 * * *', catchup=False)
def weather_pipeline():

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def extract_data():
        cities = ['São Paulo', 'Rio de Janeiro', 'Curitiba']
        extracted_data = {
            'id_city': [],
            'nm_city': [],
            'ds_date': [],
            'nm_country': [],
            'vl_temp': [],
            'vl_humidity': [],
            'ds_description': []
        }

        for city in cities:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={os.environ.get('OPENWEATHER_API_KEY')}"
            response = requests.get(url)
            data = response.json()
            extracted_data['id_city'].append(data['id'])
            extracted_data['nm_city'].append(data['name'])
            extracted_data['ds_date'].append(data['dt'])
            extracted_data['nm_country'].append(data['sys']['country'])
            extracted_data['vl_temp'].append(data['main']['temp'])
            extracted_data['vl_humidity'].append(data['main']['humidity'])
            extracted_data['ds_description'].append(data['weather'][0]['description'])
        return extracted_data
    
    @task(retries=3, retry_delay=timedelta(minutes=1))
    def transform_data(data):
        transformed_data = pd.DataFrame(data)
        transformed_data['vl_temp'] = transformed_data['vl_temp'] - 273.15
        transformed_data['ds_date'] = pd.to_datetime(transformed_data['ds_date'], unit = 's').dt.date
        transformed_data = transformed_data.dropna()
        if transformed_data.empty:
            raise ValueError("Nenhum dado válido retornado da API.")
        return transformed_data.to_dict(orient='records')
    
    @task(retries=3, retry_delay=timedelta(minutes=1))
    def load_data(data):
        loadable_data = []
        return loadable_data
    
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    loadable_data = load_data(transformed_data)

dag = weather_pipeline()