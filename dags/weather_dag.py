import json

from airflow import DAG
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.python import PythonOperator

CITIES = [
    {"name": "Lviv", "lat": "49.8397", "lon": "24.0297"},
    {"name": "Kyiv", "lat": "50.4501", "lon": "30.5234"},
    {"name": "Kharkiv", "lat": "49.9935", "lon": "36.2304"},
    {"name": "Odesa", "lat": "46.4825", "lon": "30.7233"},
    {"name": "Zhmerynka", "lat": "49.0384", "lon": "28.1056"}
]


def _process_weather(ti, city_data):
    city_name = city_data["name"]
    info = ti.xcom_pull(task_ids=f"extract_data", map_indexes=ti.map_index)

    weather_data = info["data"][0] if "data" in info else info
    timestamp = weather_data["dt"]
    temp = weather_data["temp"]
    humidity = weather_data["humidity"]
    cloudiness = weather_data["clouds"]
    wind_speed = weather_data["wind_speed"]

    return {
        "city": city_name,
        "timestamp": timestamp,
        "temp": temp,
        "humidity": humidity,
        "cloudiness": cloudiness,
        "wind_speed": wind_speed
    }


with DAG(
    dag_id="weather_dag_postgres",
    start_date=datetime(2026, 3, 10),
    catchup=True,
    schedule="@daily"
) as dag:

    db_create = SQLExecuteQueryOperator(
        task_id="create_table_postgres",
        conn_id="weather_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS measures  (
            city VARCHAR(50),
            timestamp TIMESTAMP,
            temp FLOAT,
            humidity FLOAT,
            cloudiness FLOAT,
            wind_speed FLOAT
        );
        """
    )

    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="weather_api_conn",
        endpoint="data/3.0/onecall/timemachine",
        request_params={
            "appid": Variable.get("WEATHER_API_KEY"),
            "lat": "49.8397",
            "lon": "24.0297",
            "dt": "{{ logical_date.int_timestamp }}"
        })

    extract_data = HttpOperator.partial(
        task_id="extract_data",
        http_conn_id="weather_api_conn",
        endpoint="data/3.0/onecall/timemachine",
        method="GET",
        response_filter=lambda x: json.loads(x.text),
        log_response=True
    ).expand(
        data=[{
            "appid": Variable.get("WEATHER_API_KEY"),
            "lat": city["lat"],
            "lon": city["lon"],
            "dt": "{{ logical_date.int_timestamp }}"
        } for city in CITIES]
    )

    process_data = PythonOperator.partial(
        task_id="process_data",
        python_callable=_process_weather
    ).expand(
        op_kwargs=[{"city_data": city} for city in CITIES]
    )

    inject_data = SQLExecuteQueryOperator.partial(
        task_id="inject_data",
        conn_id="weather_conn",
        sql="""
            INSERT INTO measures (city, timestamp, temp, humidity, cloudiness, wind_speed)
            VALUES (%(city)s,
                    to_timestamp(%(timestamp)s),
                    %(temp)s,
                    %(humidity)s,
                    %(cloudiness)s,
                    %(wind_speed)s);
            """
    ).expand(
        parameters=process_data.output
    )

    db_create >> check_api >> extract_data >> process_data >> inject_data