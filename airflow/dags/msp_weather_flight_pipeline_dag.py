from datetime import datetime
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from ingestion.load_raw_msp_flights import load_raw_msp_flights
from ingestion.load_raw_kmsp_weather import load_raw_kmsp_weather
from transformation.build_flight_daily import build_flight_daily
from transformation.build_weather_daily import build_weather_daily
from transformation.build_mart_daily import build_mart_daily
from transformation.build_airline_monthly import build_airline_monthly
from utils.db import run_sql_file


def create_database_and_tables():
    print("Creating database if needed")
    run_sql_file("sql/ddl/01_create_database.sql", include_database=False)
    print("Creating tables if needed")
    run_sql_file("sql/ddl/02_create_schemas_and_tables.sql", include_database=True)
    print("Database setup complete")


def run_load_raw_msp_flights():
    print("Starting load_raw_msp_flights")
    row_count = load_raw_msp_flights()
    print(f"Finished load_raw_msp_flights with {row_count:,} rows")


def run_load_raw_kmsp_weather():
    print("Starting load_raw_kmsp_weather")
    row_count = load_raw_kmsp_weather()
    print(f"Finished load_raw_kmsp_weather with {row_count:,} rows")


def run_build_flight_daily():
    print("Starting build_flight_daily")
    row_count = build_flight_daily()
    print(f"Finished build_flight_daily with {row_count:,} rows")


def run_build_weather_daily():
    print("Starting build_weather_daily")
    row_count = build_weather_daily()
    print(f"Finished build_weather_daily with {row_count:,} rows")


def run_build_mart_daily():
    print("Starting build_mart_daily")
    row_count = build_mart_daily()
    print(f"Finished build_mart_daily with {row_count:,} rows")


def run_build_airline_monthly():
    print("Starting build_airline_monthly")
    row_count = build_airline_monthly()
    print(f"Finished build_airline_monthly with {row_count:,} rows")


with DAG(
    dag_id="msp_weather_flight_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    create_database_and_tables_task = PythonOperator(
        task_id="create_database_and_tables",
        python_callable=create_database_and_tables,
    )

    load_raw_msp_flights_task = PythonOperator(
        task_id="load_raw_msp_flights",
        python_callable=run_load_raw_msp_flights,
    )

    load_raw_kmsp_weather_task = PythonOperator(
        task_id="load_raw_kmsp_weather",
        python_callable=run_load_raw_kmsp_weather,
    )

    build_flight_daily_task = PythonOperator(
        task_id="build_flight_daily",
        python_callable=run_build_flight_daily,
    )

    build_weather_daily_task = PythonOperator(
        task_id="build_weather_daily",
        python_callable=run_build_weather_daily,
    )

    build_mart_daily_task = PythonOperator(
        task_id="build_mart_daily",
        python_callable=run_build_mart_daily,
    )

    build_airline_monthly_task = PythonOperator(
        task_id="build_airline_monthly",
        python_callable=run_build_airline_monthly,
    )

    (
        create_database_and_tables_task
        >> load_raw_msp_flights_task
        >> load_raw_kmsp_weather_task
        >> build_flight_daily_task
        >> build_weather_daily_task
        >> build_mart_daily_task
        >> build_airline_monthly_task
    )
