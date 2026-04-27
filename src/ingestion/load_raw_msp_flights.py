from pathlib import Path
import re
import sys

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from utils.db import get_engine


FLIGHT_COLUMNS = [
    "Year",
    "Quarter",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "FlightDate",
    "Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "OriginCityName",
    "Dest",
    "DestCityName",
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "DepDelayMinutes",
    "DepDel15",
    "TaxiOut",
    "CRSArrTime",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",
    "ArrDel15",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "CRSElapsedTime",
    "ActualElapsedTime",
    "AirTime",
    "Flights",
    "Distance",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
]

NUMERIC_COLUMNS = [
    "year",
    "quarter",
    "month",
    "dayof_month",
    "day_of_week",
    "crs_dep_time",
    "dep_time",
    "dep_delay",
    "dep_delay_minutes",
    "dep_del15",
    "taxi_out",
    "crs_arr_time",
    "arr_time",
    "arr_delay",
    "arr_delay_minutes",
    "arr_del15",
    "cancelled",
    "diverted",
    "crs_elapsed_time",
    "actual_elapsed_time",
    "air_time",
    "flights",
    "distance",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]


def to_snake_case(name):
    cleaned = name.strip().replace(" ", "_").replace("/", "_").replace("-", "_")
    cleaned = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", cleaned)
    cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.lower()


def ensure_output_dirs():
    (PROJECT_ROOT / "data" / "staged").mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "data" / "processed").mkdir(parents=True, exist_ok=True)


def load_raw_msp_flights():
    ensure_output_dirs()

    flights_dir = PROJECT_ROOT / "data" / "raw" / "flights" / "extracted"
    if not flights_dir.exists():
        raise FileNotFoundError(f"Missing flights directory: {flights_dir}")

    csv_files = sorted(path for path in flights_dir.iterdir() if path.suffix.lower() == ".csv")
    if not csv_files:
        raise FileNotFoundError(f"No flight CSV files found in: {flights_dir}")

    frames = []

    for csv_file in csv_files:
        print(f"Reading flight file: {csv_file.name}")
        header = pd.read_csv(csv_file, nrows=0)
        missing_columns = [column for column in FLIGHT_COLUMNS if column not in header.columns]
        if missing_columns:
            raise ValueError(
                f"Missing required flight columns in {csv_file.name}: {missing_columns}"
            )

        df = pd.read_csv(
            csv_file,
            usecols=lambda column: column in FLIGHT_COLUMNS,
            low_memory=False,
        )
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
        df["Origin"] = df["Origin"].astype(str).str.strip().str.upper()
        df["source_file"] = csv_file.name
        df = df[(df["Origin"] == "MSP") & (df["Year"] == 2024)].copy()
        frames.append(df)

    if not frames:
        raise ValueError("No MSP flight records were loaded.")

    flights = pd.concat(frames, ignore_index=True)
    flights.columns = [to_snake_case(column) for column in flights.columns]
    flights["flight_date"] = pd.to_datetime(flights["flight_date"], errors="coerce").dt.date

    for column in NUMERIC_COLUMNS:
        flights[column] = pd.to_numeric(flights[column], errors="coerce")

    engine = get_engine()
    flights.to_sql("raw_flights_msp_2024", engine, if_exists="replace", index=False)

    output_path = PROJECT_ROOT / "data" / "staged" / "raw_flights_msp_2024.csv"
    flights.to_csv(output_path, index=False)

    print(f"Loaded raw_flights_msp_2024 with {len(flights):,} rows")
    print(f"Wrote debug CSV: {output_path}")
    return len(flights)


def main():
    load_raw_msp_flights()


if __name__ == "__main__":
    main()
