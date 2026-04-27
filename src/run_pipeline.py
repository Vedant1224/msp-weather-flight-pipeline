from pathlib import Path
import sys


SRC_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from ingestion.load_raw_msp_flights import load_raw_msp_flights
from ingestion.load_raw_kmsp_weather import load_raw_kmsp_weather
from transformation.build_flight_daily import build_flight_daily
from transformation.build_weather_daily import build_weather_daily
from transformation.build_mart_daily import build_mart_daily
from utils.db import run_sql_file


def main():
    print("Starting MSP weather and flight pipeline")

    print("Creating database if needed")
    run_sql_file("sql/ddl/01_create_database.sql", include_database=False)

    print("Creating tables if needed")
    run_sql_file("sql/ddl/02_create_schemas_and_tables.sql", include_database=True)

    flight_rows = load_raw_msp_flights()
    print(f"Success: raw_flights_msp_2024 rows = {flight_rows:,}")

    weather_rows = load_raw_kmsp_weather()
    print(f"Success: raw_weather_kmsp_2024 rows = {weather_rows:,}")

    flight_daily_rows = build_flight_daily()
    print(f"Success: stg_flights_msp_daily rows = {flight_daily_rows:,}")

    weather_daily_rows = build_weather_daily()
    print(f"Success: stg_weather_kmsp_daily rows = {weather_daily_rows:,}")

    mart_rows = build_mart_daily()
    print(f"Success: mart_msp_daily_weather_flights rows = {mart_rows:,}")

    print("Pipeline completed successfully")


if __name__ == "__main__":
    main()
