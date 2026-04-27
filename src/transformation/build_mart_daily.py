from pathlib import Path
import sys

import numpy as np
import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from utils.db import get_engine


def ensure_output_dirs():
    (PROJECT_ROOT / "data" / "staged").mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "data" / "processed").mkdir(parents=True, exist_ok=True)


def map_season(month):
    if month in [12, 1, 2]:
        return "Winter"
    if month in [3, 4, 5]:
        return "Spring"
    if month in [6, 7, 8]:
        return "Summer"
    return "Fall"


def build_mart_daily():
    ensure_output_dirs()
    engine = get_engine()

    flights = pd.read_sql("SELECT * FROM stg_flights_msp_daily", engine)
    weather = pd.read_sql("SELECT * FROM stg_weather_kmsp_daily", engine)

    if flights.empty:
        raise ValueError("stg_flights_msp_daily is empty.")
    if weather.empty:
        raise ValueError("stg_weather_kmsp_daily is empty.")

    flights["date"] = pd.to_datetime(flights["date"], errors="coerce")
    weather["date"] = pd.to_datetime(weather["date"], errors="coerce")

    mart = flights.merge(weather, on="date", how="inner").sort_values("date").copy()
    mart["month"] = mart["date"].dt.month
    mart["month_name"] = mart["date"].dt.strftime("%B")
    mart["season"] = mart["month"].apply(map_season)

    mart["temp_bucket"] = np.select(
        [
            mart["avg_temp_f"] < 32,
            (mart["avg_temp_f"] >= 32) & (mart["avg_temp_f"] < 50),
            (mart["avg_temp_f"] >= 50) & (mart["avg_temp_f"] < 70),
            (mart["avg_temp_f"] >= 70) & (mart["avg_temp_f"] < 85),
            mart["avg_temp_f"] >= 85,
        ],
        ["Freezing", "Cold", "Mild", "Warm", "Hot"],
        default="Unknown",
    )

    mart["delay_bucket"] = np.select(
        [
            mart["avg_dep_delay_minutes"] < 5,
            (mart["avg_dep_delay_minutes"] >= 5) & (mart["avg_dep_delay_minutes"] < 15),
            (mart["avg_dep_delay_minutes"] >= 15) & (mart["avg_dep_delay_minutes"] < 30),
            mart["avg_dep_delay_minutes"] >= 30,
        ],
        ["Low", "Moderate", "High", "Severe"],
        default="Unknown",
    )

    mart["cancellation_rate_pct"] = (mart["cancellation_rate"] * 100).round(2)
    mart["delayed_15_rate_pct"] = (mart["delayed_15_rate"] * 100).round(2)
    mart["date"] = mart["date"].dt.date

    ordered_columns = [
        "date",
        "month",
        "month_name",
        "season",
        "temp_bucket",
        "delay_bucket",
        "scheduled_departures",
        "completed_departures",
        "cancelled_departures",
        "cancellation_rate",
        "cancellation_rate_pct",
        "diverted_departures",
        "avg_dep_delay_minutes",
        "avg_arr_delay_minutes",
        "delayed_15_count",
        "delayed_15_rate",
        "delayed_15_rate_pct",
        "total_carrier_delay_minutes",
        "total_weather_delay_minutes",
        "total_nas_delay_minutes",
        "total_security_delay_minutes",
        "total_late_aircraft_delay_minutes",
        "avg_taxi_out_minutes",
        "weather_obs_count",
        "avg_temp_f",
        "min_temp_f",
        "max_temp_f",
        "precip_total_inches",
        "avg_wind_speed_mph",
        "max_wind_gust_mph",
        "min_visibility_miles",
        "hours_with_precip",
        "hours_with_snow",
        "hours_with_fog",
        "hours_with_thunder",
        "snow_day_flag",
        "fog_day_flag",
        "thunder_day_flag",
        "low_visibility_day_flag",
        "high_wind_day_flag",
        "precip_day_flag",
        "severe_weather_day",
        "weather_category",
    ]

    mart = mart[ordered_columns]
    mart.to_sql("mart_msp_daily_weather_flights", engine, if_exists="replace", index=False)

    output_path = PROJECT_ROOT / "data" / "processed" / "mart_msp_daily_weather_flights.csv"
    mart.to_csv(output_path, index=False)

    print(f"Built mart_msp_daily_weather_flights with {len(mart):,} rows")
    print(f"Wrote processed CSV: {output_path}")
    return len(mart)


def main():
    build_mart_daily()


if __name__ == "__main__":
    main()
