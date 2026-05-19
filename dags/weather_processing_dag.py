import json
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator


POSTGRES_CONN_ID = "weather_conn"

DEFAULT_TASK_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}


def validate_weather_fields(weather_data: Dict[str, Any]) -> None:
    required_fields = ['dt', 'temp', 'humidity', 'clouds', 'wind_speed']
    missing_fields = [field for field in required_fields if field not in weather_data]

    if missing_fields:
        raise AirflowException(f"Missing required fields: {', '.join(missing_fields)}")

    if not isinstance(weather_data['temp'], (int, float)):
        raise AirflowException("Temperature is not a number")

    if weather_data['humidity'] < 0 or weather_data['humidity'] > 100:
        raise AirflowException(f"Humidity out of range: {weather_data['humidity']}")


def transform_and_load_weather(**context) -> None:
    params = context['params']
    city = params['city']
    logical_date = params['logical_date']

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    try:
        result = pg_hook.get_first(
            """
            SELECT raw_data
            FROM weather_raw
            WHERE city = %s AND logical_date = %s::timestamp
            """,
            parameters=(city, logical_date)
        )

        if not result:
            raise AirflowException(f"No raw data found for {city} on {logical_date}")

        raw_data = result[0] if isinstance(result[0], dict) else json.loads(result[0])

        if "data" not in raw_data or not raw_data["data"]:
            raise AirflowException("Invalid raw data structure: missing 'data' field")

        weather_data = raw_data["data"][0]

        validate_weather_fields(weather_data)

        pg_hook.run(
            """
            INSERT INTO measures (city, timestamp, temp, humidity, cloudiness, wind_speed)
            VALUES (%(city)s,
                    to_timestamp(%(timestamp)s),
                    %(temp)s,
                    %(humidity)s,
                    %(cloudiness)s,
                    %(wind_speed)s)
            ON CONFLICT (city, timestamp)
            DO UPDATE SET
                temp = EXCLUDED.temp,
                humidity = EXCLUDED.humidity,
                cloudiness = EXCLUDED.cloudiness,
                wind_speed = EXCLUDED.wind_speed;
            """,
            parameters={
                "city": city,
                "timestamp": weather_data["dt"],
                "temp": weather_data["temp"],
                "humidity": weather_data["humidity"],
                "cloudiness": weather_data["clouds"],
                "wind_speed": weather_data["wind_speed"]
            }
        )

        print(f"Successfully processed weather data for {city} on {logical_date}")

    except Exception as e:
        print(f"Failed to process weather data for {city}: {str(e)}")
        raise


with DAG(
    dag_id="weather_processing_dag",
    default_args=DEFAULT_TASK_ARGS,
    start_date=datetime(2026, 3, 10),
    catchup=False,
    schedule=None,
    max_active_runs=3,
    params={
        "city": Param("Lviv", type="string", description="City name to process"),
        "logical_date": Param(
            None,
            type=["null", "string"],
            description="Logical date to process (YYYY-MM-DD)"
        )
    },
    tags=["weather", "processing", "transformation"],
) as dag:

    create_final_table = SQLExecuteQueryOperator(
        task_id="create_final_table",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS measures (
            city VARCHAR(50),
            timestamp TIMESTAMP,
            temp FLOAT,
            humidity FLOAT,
            cloudiness FLOAT,
            wind_speed FLOAT,
            PRIMARY KEY (city, timestamp)
        );
        """,
    )

    process_data = PythonOperator(
        task_id="process_weather_data",
        python_callable=transform_and_load_weather,
    )

    create_final_table >> process_data