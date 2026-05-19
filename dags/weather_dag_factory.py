import json
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.models import Variable
from airflow.sdk import Param, TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.empty import EmptyOperator


POSTGRES_CONN_ID = "weather_conn"
HTTP_CONN_ID = "weather_api_conn"


class DataQualityStatus(str, Enum):
    VALID = "VALID"
    INVALID = "INVALID"
    PENDING = "PENDING"


DEFAULT_TASK_ARGS = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}


def get_pg_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def update_data_quality_status(table: str, city: str, logical_date: str, status: str) -> None:
    pg_hook = get_pg_hook()
    pg_hook.run(
        f"UPDATE {table} SET data_quality_status = %s WHERE city = %s AND logical_date = %s",
        parameters=(status, city, logical_date)
    )


def validate_weather_data_fields(weather_data: Dict[str, Any]) -> list[str]:
    errors = []

    required_fields = ['dt', 'temp', 'humidity', 'clouds', 'wind_speed']
    for field in required_fields:
        if field not in weather_data:
            errors.append(f"Missing required field: {field}")

    if 'temp' in weather_data:
        if not isinstance(weather_data['temp'], (int, float)):
            errors.append("Temperature is not a number")
        else:
            temp_celsius = weather_data['temp'] - 273.15
            if temp_celsius < -100 or temp_celsius > 60:
                errors.append(f"Temperature out of valid range: {temp_celsius}°C")

    if 'humidity' in weather_data and (
        weather_data['humidity'] < 0 or weather_data['humidity'] > 100
    ):
        errors.append(f"Humidity out of valid range: {weather_data['humidity']}%")

    return errors


def create_weather_etl_dag(
    city_name: str,
    latitude: str,
    longitude: str,
    dag_id: str = None,
    schedule: str = "@daily",
    start_date: datetime = datetime(2026, 3, 10),
) -> DAG:
    if dag_id is None:
        dag_id = f"weather_etl_{city_name.lower()}"

    dag = DAG(
        dag_id=dag_id,
        default_args=DEFAULT_TASK_ARGS,
        start_date=start_date,
        catchup=False,
        schedule=schedule,
        max_active_runs=1,
        params={
            "city_name": Param(city_name, type="string", description="City name"),
            "latitude": Param(latitude, type="string", description="City latitude"),
            "longitude": Param(longitude, type="string", description="City longitude"),
        },
        tags=["weather", "etl", city_name.lower()],
    )

    with dag:
        create_tables = SQLExecuteQueryOperator(
            task_id="create_tables",
            conn_id=POSTGRES_CONN_ID,
            sql="""
            CREATE TABLE IF NOT EXISTS weather_raw (
                city VARCHAR(50),
                logical_date TIMESTAMP,
                raw_data JSONB,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_quality_status VARCHAR(20),
                PRIMARY KEY (city, logical_date)
            );

            CREATE TABLE IF NOT EXISTS weather_staging (
                city VARCHAR(50),
                logical_date TIMESTAMP,
                timestamp BIGINT,
                temp FLOAT,
                humidity FLOAT,
                cloudiness FLOAT,
                wind_speed FLOAT,
                transformation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_quality_status VARCHAR(20),
                PRIMARY KEY (city, logical_date)
            );

            CREATE TABLE IF NOT EXISTS measures (
                city VARCHAR(50),
                timestamp TIMESTAMP,
                temp FLOAT,
                humidity FLOAT,
                cloudiness FLOAT,
                wind_speed FLOAT,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (city, timestamp)
            );
            """,
        )

        with TaskGroup("extract", tooltip="Extract weather data from API") as extract_group:

            def check_extraction_needed(**context) -> bool:
                city = context['params']['city_name']
                logical_date = context['logical_date'].isoformat()
                pg_hook = get_pg_hook()

                result = pg_hook.get_first(
                    "SELECT data_quality_status FROM weather_raw WHERE city = %s AND logical_date = %s",
                    parameters=(city, logical_date)
                )

                if result and result[0] == DataQualityStatus.VALID:
                    print(f"Extraction already completed for {city} on {logical_date}")
                    raise AirflowSkipException(f"Data already extracted and validated for {city}")

                print(f"Extraction needed for {city} on {logical_date}")
                return True

            check_extraction = ShortCircuitOperator(
                task_id="check_needed",
                python_callable=check_extraction_needed,
            )

            def extract_weather_data(**context) -> None:
                city = context['params']['city_name']
                lat = context['params']['latitude']
                lon = context['params']['longitude']
                logical_date = context['logical_date']
                logical_date_iso = logical_date.isoformat()
                pg_hook = get_pg_hook()

                try:
                    http_hook = HttpHook(method='GET', http_conn_id=HTTP_CONN_ID)
                    response = http_hook.run(
                        'data/3.0/onecall/timemachine',
                        data={
                            'appid': Variable.get('WEATHER_API_KEY'),
                            'lat': lat,
                            'lon': lon,
                            'dt': int(logical_date.timestamp())
                        }
                    )

                    if response.status_code != 200:
                        raise AirflowException(
                            f"Weather API returned status {response.status_code}: {response.text}"
                        )

                    raw_data = response.json()

                    pg_hook.run(
                        """
                        INSERT INTO weather_raw (city, logical_date, raw_data, data_quality_status)
                        VALUES (%(city)s, %(logical_date)s, %(raw_data)s, %(status)s)
                        ON CONFLICT (city, logical_date)
                        DO UPDATE SET
                            raw_data = EXCLUDED.raw_data,
                            data_quality_status = EXCLUDED.data_quality_status,
                            ingestion_timestamp = CURRENT_TIMESTAMP;
                        """,
                        parameters={
                            "city": city,
                            "logical_date": logical_date_iso,
                            "raw_data": json.dumps(raw_data),
                            "status": DataQualityStatus.PENDING
                        }
                    )

                    print(f"Successfully extracted data for {city}")

                except Exception as e:
                    print(f"Failed to extract data for {city}: {str(e)}")
                    raise

            extract_data = PythonOperator(
                task_id="fetch_api_data",
                python_callable=extract_weather_data,
            )

            def validate_raw_data(**context) -> None:
                city = context['params']['city_name']
                logical_date_iso = context['logical_date'].isoformat()
                pg_hook = get_pg_hook()

                result = pg_hook.get_first(
                    "SELECT raw_data FROM weather_raw WHERE city = %s AND logical_date = %s",
                    parameters=(city, logical_date_iso)
                )

                if not result:
                    raise AirflowException("No raw data found")

                raw_data = result[0] if isinstance(result[0], dict) else json.loads(result[0])

                if "data" not in raw_data or not raw_data["data"]:
                    update_data_quality_status("weather_raw", city, logical_date_iso, DataQualityStatus.INVALID)
                    raise AirflowException("Missing 'data' field or empty data array")

                weather_data = raw_data["data"][0]
                errors = validate_weather_data_fields(weather_data)

                if errors:
                    update_data_quality_status("weather_raw", city, logical_date_iso, DataQualityStatus.INVALID)
                    raise AirflowException(f"Data quality check failed: {'; '.join(errors)}")

                update_data_quality_status("weather_raw", city, logical_date_iso, DataQualityStatus.VALID)
                print(f"Data quality check passed for {city}")

            validate_raw = PythonOperator(
                task_id="validate_data",
                python_callable=validate_raw_data,
            )

            check_extraction >> extract_data >> validate_raw


        with TaskGroup("transform", tooltip="Transform raw data to staging") as transform_group:

            def check_transformation_needed(**context) -> bool:
                city = context['params']['city_name']
                logical_date = context['logical_date'].isoformat()
                pg_hook = get_pg_hook()

                result = pg_hook.get_first(
                    "SELECT data_quality_status FROM weather_staging WHERE city = %s AND logical_date = %s",
                    parameters=(city, logical_date)
                )

                if result and result[0] == DataQualityStatus.VALID:
                    print(f"Transformation already completed for {city}")
                    raise AirflowSkipException(f"Data already transformed for {city}")

                print(f"Transformation needed for {city}")
                return True

            check_transform = ShortCircuitOperator(
                task_id="check_needed",
                python_callable=check_transformation_needed,
            )

            def transform_weather_data(**context) -> None:
                city = context['params']['city_name']
                logical_date_iso = context['logical_date'].isoformat()
                pg_hook = get_pg_hook()

                try:
                    result = pg_hook.get_first(
                        """
                        SELECT raw_data FROM weather_raw
                        WHERE city = %s AND logical_date = %s AND data_quality_status = %s
                        """,
                        parameters=(city, logical_date_iso, DataQualityStatus.VALID)
                    )

                    if not result:
                        raise AirflowException("No valid raw data found for transformation")

                    raw_data = result[0] if isinstance(result[0], dict) else json.loads(result[0])
                    weather_data = raw_data["data"][0]

                    pg_hook.run(
                        """
                        INSERT INTO weather_staging
                        (city, logical_date, timestamp, temp, humidity, cloudiness, wind_speed, data_quality_status)
                        VALUES (%(city)s, %(logical_date)s, %(timestamp)s, %(temp)s,
                                %(humidity)s, %(cloudiness)s, %(wind_speed)s, %(status)s)
                        ON CONFLICT (city, logical_date)
                        DO UPDATE SET
                            timestamp = EXCLUDED.timestamp,
                            temp = EXCLUDED.temp,
                            humidity = EXCLUDED.humidity,
                            cloudiness = EXCLUDED.cloudiness,
                            wind_speed = EXCLUDED.wind_speed,
                            data_quality_status = EXCLUDED.data_quality_status,
                            transformation_timestamp = CURRENT_TIMESTAMP;
                        """,
                        parameters={
                            "city": city,
                            "logical_date": logical_date_iso,
                            "timestamp": weather_data["dt"],
                            "temp": weather_data["temp"],
                            "humidity": weather_data["humidity"],
                            "cloudiness": weather_data["clouds"],
                            "wind_speed": weather_data["wind_speed"],
                            "status": DataQualityStatus.PENDING
                        }
                    )

                    print(f"Successfully transformed data for {city}")

                except Exception as e:
                    print(f"Failed to transform data for {city}: {str(e)}")
                    raise

            transform_data = PythonOperator(
                task_id="parse_fields",
                python_callable=transform_weather_data,
            )

            def validate_transformed_data(**context) -> None:
                city = context['params']['city_name']
                logical_date_iso = context['logical_date'].isoformat()
                pg_hook = get_pg_hook()

                result = pg_hook.get_first(
                    """
                    SELECT temp, humidity, cloudiness, wind_speed
                    FROM weather_staging
                    WHERE city = %s AND logical_date = %s
                    """,
                    parameters=(city, logical_date_iso)
                )

                if not result:
                    raise AirflowException("No transformed data found in staging")

                temp, humidity, cloudiness, wind_speed = result
                errors = []

                if temp is None:
                    errors.append("Temperature is NULL")
                if humidity is None or humidity < 0 or humidity > 100:
                    errors.append(f"Invalid humidity: {humidity}")
                if cloudiness is None or cloudiness < 0 or cloudiness > 100:
                    errors.append(f"Invalid cloudiness: {cloudiness}")
                if wind_speed is None or wind_speed < 0:
                    errors.append(f"Invalid wind_speed: {wind_speed}")

                if errors:
                    update_data_quality_status("weather_staging", city, logical_date_iso, DataQualityStatus.INVALID)
                    raise AirflowException(f"Transformed data validation failed: {'; '.join(errors)}")

                update_data_quality_status("weather_staging", city, logical_date_iso, DataQualityStatus.VALID)
                print(f"Transformed data validation passed for {city}")

            validate_transformed = PythonOperator(
                task_id="validate_data",
                python_callable=validate_transformed_data,
            )

            check_transform >> transform_data >> validate_transformed


        with TaskGroup("load", tooltip="Load validated data to final table") as load_group:

            def check_load_needed(**context) -> bool:
                city = context['params']['city_name']
                logical_date_iso = context['logical_date'].isoformat()
                pg_hook = get_pg_hook()

                result = pg_hook.get_first(
                    """
                    SELECT EXISTS(
                        SELECT 1
                        FROM measures m
                        JOIN weather_staging s ON s.city = m.city AND m.timestamp = to_timestamp(s.timestamp)
                        WHERE s.city = %s AND s.logical_date = %s AND s.data_quality_status = %s
                    )
                    """,
                    parameters=(city, logical_date_iso, DataQualityStatus.VALID)
                )

                if result and result[0]:
                    print(f"Load already completed for {city}")
                    raise AirflowSkipException(f"Data already loaded for {city}")

                print(f"Load needed for {city}")
                return True

            check_load = ShortCircuitOperator(
                task_id="check_needed",
                python_callable=check_load_needed,
            )

            def load_to_measures(**context) -> None:
                city = context['params']['city_name']
                logical_date_iso = context['logical_date'].isoformat()
                pg_hook = get_pg_hook()

                try:
                    pg_hook.run(
                        """
                        INSERT INTO measures (city, timestamp, temp, humidity, cloudiness, wind_speed)
                        SELECT city, to_timestamp(timestamp), temp, humidity, cloudiness, wind_speed
                        FROM weather_staging
                        WHERE city = %(city)s AND logical_date = %(logical_date)s
                          AND data_quality_status = %(status)s
                        ON CONFLICT (city, timestamp)
                        DO UPDATE SET
                            temp = EXCLUDED.temp,
                            humidity = EXCLUDED.humidity,
                            cloudiness = EXCLUDED.cloudiness,
                            wind_speed = EXCLUDED.wind_speed,
                            load_timestamp = CURRENT_TIMESTAMP;
                        """,
                        parameters={
                            "city": city,
                            "logical_date": logical_date_iso,
                            "status": DataQualityStatus.VALID
                        }
                    )

                    print(f"Successfully loaded data to measures for {city}")

                except Exception as e:
                    print(f"Failed to load data for {city}: {str(e)}")
                    raise

            load_data = PythonOperator(
                task_id="insert_to_measures",
                python_callable=load_to_measures,
            )

            check_load >> load_data

        etl_complete = EmptyOperator(
            task_id="etl_complete",
            trigger_rule="none_failed",
        )

        create_tables >> extract_group >> transform_group >> load_group >> etl_complete

    return dag


lviv_dag = create_weather_etl_dag(
    city_name="Lviv",
    latitude="49.8397",
    longitude="24.0297"
)

kyiv_dag = create_weather_etl_dag(
    city_name="Kyiv",
    latitude="50.4501",
    longitude="30.5234"
)

london_dag = create_weather_etl_dag(
    city_name="London",
    latitude="51.5074",
    longitude="-0.1278"
)