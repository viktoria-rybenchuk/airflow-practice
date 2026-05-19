import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.sdk import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


POSTGRES_CONN_ID = "weather_conn"
HTTP_CONN_ID = "weather_api_conn"

DEFAULT_TASK_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}


def extract_and_store_weather(**context) -> None:
    params = context['params']
    city = params['city_name']
    latitude = params['latitude']
    longitude = params['longitude']
    logical_date = context['logical_date']
    logical_date_iso = logical_date.isoformat()

    try:
        http_hook = HttpHook(method='GET', http_conn_id=HTTP_CONN_ID)
        response = http_hook.run(
            'data/3.0/onecall/timemachine',
            data={
                'appid': Variable.get('WEATHER_API_KEY'),
                'lat': latitude,
                'lon': longitude,
                'dt': int(logical_date.timestamp())
            }
        )

        if response.status_code != 200:
            raise AirflowException(
                f"Weather API returned status {response.status_code}: {response.text}"
            )

        raw_data = response.json()

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(
            """
            INSERT INTO weather_raw (city, logical_date, raw_data)
            VALUES (%(city)s, %(logical_date)s, %(raw_data)s)
            ON CONFLICT (city, logical_date)
            DO UPDATE SET
                raw_data = EXCLUDED.raw_data,
                ingestion_timestamp = CURRENT_TIMESTAMP;
            """,
            parameters={
                "city": city,
                "logical_date": logical_date_iso,
                "raw_data": json.dumps(raw_data)
            }
        )

        print(f"Successfully ingested weather data for {city} on {logical_date}")

    except Exception as e:
        print(f"Failed to ingest weather data for {city}: {str(e)}")
        raise


with DAG(
    dag_id="weather_ingestion_dag",
    default_args=DEFAULT_TASK_ARGS,
    start_date=datetime(2026, 3, 10),
    catchup=False,
    schedule="@daily",
    max_active_runs=1,
    params={
        "city_name": Param("Lviv", type="string", description="City name"),
        "latitude": Param("49.8397", type="string", description="City latitude"),
        "longitude": Param("24.0297", type="string", description="City longitude"),
    },
    tags=["weather", "ingestion"],
) as dag:

    create_raw_table = SQLExecuteQueryOperator(
        task_id="create_raw_table",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS weather_raw (
            city VARCHAR(50),
            logical_date TIMESTAMP,
            raw_data JSONB,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (city, logical_date)
        );
        """,
    )

    ingest_data = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=extract_and_store_weather,
    )

    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_processing",
        trigger_dag_id="weather_processing_dag",
        conf={
            "city": "{{ params.city_name }}",
            "logical_date": "{{ ds }}"
        },
        wait_for_completion=False,
    )

    create_raw_table >> ingest_data >> trigger_processing