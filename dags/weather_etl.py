import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup

CITIES = [
    {"name": "Lviv", "lat": "49.8397", "lon": "24.0297"},
    {"name": "Kyiv", "lat": "50.4501", "lon": "30.5234"},
]
WIND_THRESHOLD = 10.0


def extract_weather(response):
    try:
        data = json.loads(response.text)
        result = data["data"][0] if "data" in data else data
        print(f"Extracted weather: temp={result.get('temp')}, wind={result.get('wind_speed')}")
        return result
    except Exception as e:
        raise ValueError(f"Failed to extract weather data: {e}")


def transform_weather(city_name, **context):
    try:
        weather = context['ti'].xcom_pull(task_ids=f'city_{city_name}.extract')
        if not weather:
            raise ValueError(f"No weather data received for {city_name}")

        return {
            "city": city_name,
            "timestamp": weather["dt"],
            "temp": weather["temp"],
            "humidity": weather["humidity"],
            "cloudiness": weather["clouds"],
            "wind_speed": weather["wind_speed"]
        }
    except KeyError as e:
        raise ValueError(f"Missing field in weather data for {city_name}: {e}")


def check_wind(city_name, **context):
    try:
        data = context['ti'].xcom_pull(task_ids=f'city_{city_name}.transform')
        if not data:
            raise ValueError(f"No data to check for {city_name}")

        wind_speed = data["wind_speed"]
        print(f"Checking wind speed for {city_name}: {wind_speed} m/s (threshold: {WIND_THRESHOLD})")

        if wind_speed > WIND_THRESHOLD:
            return f"city_{city_name}.alert"
        return f"city_{city_name}.normal_load"
    except Exception as e:
        raise ValueError(f"Branch check failed for {city_name}: {e}")


def alert(city_name, **context):
    data = context['ti'].xcom_pull(task_ids=f'city_{city_name}.transform')
    print(f"ALERT: High wind {data['wind_speed']} m/s in {data['city']}")


def load(city_name, **context):
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        data = context['ti'].xcom_pull(task_ids=f'city_{city_name}.transform')
        if not data:
            raise ValueError(f"No data to load for {city_name}")

        hook = PostgresHook(postgres_conn_id='weather_conn')
        hook.run(
            sql="INSERT INTO measures VALUES (%(city)s, to_timestamp(%(timestamp)s), %(temp)s, %(humidity)s, %(cloudiness)s, %(wind_speed)s);",
            parameters=data
        )
        print(f"Successfully loaded data for {data['city']}")
    except Exception as e:
        raise ValueError(f"Failed to load data for {city_name}: {e}")


with DAG(
    dag_id="weather_dag_postgres",
    start_date=datetime(2026, 3, 10),
    schedule="@daily",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10)
    },
    catchup=False
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="weather_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS measures (
            city VARCHAR(50), timestamp TIMESTAMP, temp FLOAT,
            humidity FLOAT, cloudiness FLOAT, wind_speed FLOAT
        );
        """
    )

    for city in CITIES:
        with TaskGroup(group_id=f"city_{city['name']}") as city_group:

            extract = HttpOperator(
                task_id="extract",
                http_conn_id="weather_api_conn",
                endpoint="data/3.0/onecall/timemachine",
                method="GET",
                data={
                    "appid": "{{ var.value.WEATHER_API_KEY }}",
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "dt": "{{ logical_date.int_timestamp }}"
                },
                response_filter=extract_weather,
                retries=5,
                retry_delay=timedelta(minutes=1)
            )

            transform = PythonOperator(
                task_id="transform",
                python_callable=transform_weather,
                op_kwargs={"city_name": city["name"]}
            )

            branch = BranchPythonOperator(
                task_id="branch",
                python_callable=check_wind,
                op_kwargs={"city_name": city["name"]}
            )

            alert_task = PythonOperator(
                task_id="alert",
                python_callable=alert,
                op_kwargs={"city_name": city["name"]}
            )

            normal = EmptyOperator(
                task_id="normal_load"
            )

            join = EmptyOperator(
                task_id="join",
                trigger_rule="none_failed_min_one_success"
            )

            load_data = PythonOperator(
                task_id="load",
                python_callable=load,
                op_kwargs={"city_name": city["name"]},
                trigger_rule="none_failed_min_one_success"
            )

            extract >> transform >> branch
            branch >> normal >> join
            branch >> alert_task >> join
            join >> load_data

        create_table >> city_group