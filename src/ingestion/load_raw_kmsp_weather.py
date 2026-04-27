from pathlib import Path
import re
import sys

import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from utils.db import get_engine


WEATHER_COLUMNS = [
    "STATION",
    "DATE",
    "NAME",
    "REPORT_TYPE",
    "SOURCE",
    "HourlyDryBulbTemperature",
    "HourlyDewPointTemperature",
    "HourlyRelativeHumidity",
    "HourlyPrecipitation",
    "HourlyPresentWeatherType",
    "HourlySkyConditions",
    "HourlyVisibility",
    "HourlyWindSpeed",
    "HourlyWindGustSpeed",
    "DailyAverageDryBulbTemperature",
    "DailyMaximumDryBulbTemperature",
    "DailyMinimumDryBulbTemperature",
    "DailyAverageWindSpeed",
    "DailyPeakWindSpeed",
    "DailyPrecipitation",
    "DailySnowfall",
    "DailySnowDepth",
    "DailyWeather",
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


def load_raw_kmsp_weather():
    ensure_output_dirs()

    weather_path = PROJECT_ROOT / "data" / "raw" / "weather" / "kmsp_lcd_2024.csv"
    if not weather_path.exists():
        raise FileNotFoundError(f"Missing weather file: {weather_path}")

    header = pd.read_csv(weather_path, nrows=0)
    available_columns = [column for column in WEATHER_COLUMNS if column in header.columns]
    required_columns = ["STATION", "DATE", "NAME", "REPORT_TYPE", "SOURCE"]
    missing_required = [column for column in required_columns if column not in header.columns]

    if missing_required:
        raise ValueError(f"Missing required weather columns: {missing_required}")

    weather = pd.read_csv(weather_path, usecols=available_columns, low_memory=False)
    weather["source_file"] = weather_path.name
    weather.columns = [to_snake_case(column) for column in weather.columns]
    weather["date"] = pd.to_datetime(weather["date"], errors="coerce")

    engine = get_engine()
    weather.to_sql("raw_weather_kmsp_2024", engine, if_exists="replace", index=False)

    output_path = PROJECT_ROOT / "data" / "staged" / "raw_weather_kmsp_2024.csv"
    weather.to_csv(output_path, index=False)

    print(f"Loaded raw_weather_kmsp_2024 with {len(weather):,} rows")
    print(f"Wrote debug CSV: {output_path}")
    return len(weather)


def main():
    load_raw_kmsp_weather()


if __name__ == "__main__":
    main()
