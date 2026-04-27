from pathlib import Path
import sys

import numpy as np
import pandas as pd


SRC_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from utils.db import get_engine


HOURLY_REPORT_TYPES = {"FM-12", "FM-15", "FM-16"}
EXPECTED_WEATHER_COLUMNS = [
    "hourly_dry_bulb_temperature",
    "hourly_dew_point_temperature",
    "hourly_relative_humidity",
    "hourly_precipitation",
    "hourly_present_weather_type",
    "hourly_sky_conditions",
    "hourly_visibility",
    "hourly_wind_speed",
    "hourly_wind_gust_speed",
    "daily_average_dry_bulb_temperature",
    "daily_maximum_dry_bulb_temperature",
    "daily_minimum_dry_bulb_temperature",
    "daily_average_wind_speed",
    "daily_peak_wind_speed",
    "daily_precipitation",
    "daily_snowfall",
    "daily_snow_depth",
    "daily_weather",
]


def ensure_output_dirs():
    (PROJECT_ROOT / "data" / "staged").mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "data" / "processed").mkdir(parents=True, exist_ok=True)


def clean_numeric_weather_value(value):
    if pd.isna(value):
        return np.nan

    text = str(value).strip()
    if text == "":
        return np.nan
    if text in {"T", "Ts"}:
        return 0.001
    if text.endswith("s"):
        text = text[:-1]
    if text in {"M", "MM"}:
        return np.nan

    try:
        return float(text)
    except ValueError:
        return np.nan


def contains_weather_code(value, codes):
    if pd.isna(value):
        return False
    text = str(value).upper()
    return any(code in text for code in codes)


def build_weather_daily():
    ensure_output_dirs()
    engine = get_engine()

    weather = pd.read_sql("SELECT * FROM raw_weather_kmsp_2024", engine)
    if weather.empty:
        raise ValueError("raw_weather_kmsp_2024 is empty.")

    for column in EXPECTED_WEATHER_COLUMNS:
        if column not in weather.columns:
            weather[column] = np.nan if column != "hourly_present_weather_type" and column != "daily_weather" else ""

    weather["date"] = pd.to_datetime(weather["date"], errors="coerce")
    weather["obs_date"] = weather["date"].dt.date

    hourly_mask = weather["report_type"].astype(str).str.strip().isin(HOURLY_REPORT_TYPES)
    hourly = weather.loc[hourly_mask].copy()
    if hourly.empty:
        raise ValueError("No hourly weather rows found in raw_weather_kmsp_2024.")

    for column in [
        "hourly_dry_bulb_temperature",
        "hourly_dew_point_temperature",
        "hourly_relative_humidity",
        "hourly_precipitation",
        "hourly_visibility",
        "hourly_wind_speed",
        "hourly_wind_gust_speed",
        "daily_average_dry_bulb_temperature",
        "daily_maximum_dry_bulb_temperature",
        "daily_minimum_dry_bulb_temperature",
        "daily_average_wind_speed",
        "daily_peak_wind_speed",
        "daily_precipitation",
        "daily_snowfall",
        "daily_snow_depth",
    ]:
        if column in weather.columns:
            weather[column] = weather[column].apply(clean_numeric_weather_value)
        if column in hourly.columns:
            hourly[column] = hourly[column].apply(clean_numeric_weather_value)

    if "hourly_wind_gust_speed" not in hourly.columns:
        hourly["hourly_wind_gust_speed"] = np.nan

    hourly["hourly_wind_gust_speed"] = hourly["hourly_wind_gust_speed"].fillna(
        hourly["hourly_wind_speed"]
    )

    if "hourly_present_weather_type" not in hourly.columns:
        hourly["hourly_present_weather_type"] = ""

    hourly["snow_flag"] = hourly["hourly_present_weather_type"].apply(
        lambda value: contains_weather_code(value, ["SN", "SG", "PL", "IC"])
    )
    hourly["fog_flag"] = hourly["hourly_present_weather_type"].apply(
        lambda value: contains_weather_code(value, ["FG", "BR"])
    )
    hourly["thunder_flag"] = hourly["hourly_present_weather_type"].apply(
        lambda value: contains_weather_code(value, ["TS"])
    )
    hourly["precip_flag"] = hourly["hourly_precipitation"].fillna(0) > 0

    hourly_daily = (
        hourly.groupby("obs_date", dropna=False)
        .agg(
            weather_obs_count=("obs_date", "size"),
            avg_temp_f=("hourly_dry_bulb_temperature", "mean"),
            min_temp_f=("hourly_dry_bulb_temperature", "min"),
            max_temp_f=("hourly_dry_bulb_temperature", "max"),
            precip_total_inches=("hourly_precipitation", "sum"),
            avg_wind_speed_mph=("hourly_wind_speed", "mean"),
            max_wind_gust_mph=("hourly_wind_gust_speed", "max"),
            min_visibility_miles=("hourly_visibility", "min"),
            hours_with_precip=("precip_flag", "sum"),
            hours_with_snow=("snow_flag", "sum"),
            hours_with_fog=("fog_flag", "sum"),
            hours_with_thunder=("thunder_flag", "sum"),
        )
        .reset_index()
        .rename(columns={"obs_date": "date"})
    )

    daily_support = weather.groupby("obs_date", dropna=False).agg(
        daily_precipitation=("daily_precipitation", "max"),
        daily_snowfall=("daily_snowfall", "max"),
        daily_snow_depth=("daily_snow_depth", "max"),
        daily_weather=("daily_weather", lambda values: " | ".join(sorted({str(value) for value in values if pd.notna(value) and str(value).strip()}))),
    ).reset_index().rename(columns={"obs_date": "date"})

    daily = hourly_daily.merge(daily_support, on="date", how="left")

    for column in [
        "avg_temp_f",
        "min_temp_f",
        "max_temp_f",
        "precip_total_inches",
        "avg_wind_speed_mph",
        "max_wind_gust_mph",
        "min_visibility_miles",
    ]:
        daily[column] = daily[column].round(2)

    daily["snow_day_flag"] = np.where(
        (daily["hours_with_snow"] > 0)
        | (daily["daily_snowfall"].fillna(0) > 0)
        | (daily["daily_weather"].fillna("").str.upper().str.contains("SN")),
        1,
        0,
    )
    daily["fog_day_flag"] = np.where(
        (daily["hours_with_fog"] > 0)
        | (daily["daily_weather"].fillna("").str.upper().str.contains("FG|BR")),
        1,
        0,
    )
    daily["thunder_day_flag"] = np.where(daily["hours_with_thunder"] > 0, 1, 0)
    daily["low_visibility_day_flag"] = np.where(daily["min_visibility_miles"] < 3, 1, 0)
    daily["high_wind_day_flag"] = np.where(daily["max_wind_gust_mph"] >= 30, 1, 0)
    daily["precip_day_flag"] = np.where(
        (daily["precip_total_inches"].fillna(0) > 0)
        | (daily["hours_with_precip"] > 0)
        | (daily["daily_precipitation"].fillna(0) > 0),
        1,
        0,
    )
    daily["severe_weather_day"] = np.where(
        (daily["snow_day_flag"] == 1)
        | (daily["fog_day_flag"] == 1)
        | (daily["thunder_day_flag"] == 1)
        | (daily["low_visibility_day_flag"] == 1)
        | (daily["high_wind_day_flag"] == 1)
        | (daily["precip_total_inches"].fillna(0) >= 0.25),
        1,
        0,
    )

    daily["weather_category"] = np.select(
        [
            (daily["hours_with_snow"] > 0) | (daily["snow_day_flag"] == 1),
            daily["hours_with_thunder"] > 0,
            daily["min_visibility_miles"] < 3,
            daily["max_wind_gust_mph"] >= 30,
            (daily["precip_total_inches"].fillna(0) > 0.05) | (daily["hours_with_precip"] > 0),
        ],
        [
            "Snow",
            "Thunderstorm",
            "Low Visibility",
            "High Wind",
            "Rain/Precipitation",
        ],
        default="Clear/Mild",
    )

    daily = daily[
        [
            "date",
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
    ].sort_values("date")

    daily.to_sql("stg_weather_kmsp_daily", engine, if_exists="replace", index=False)

    output_path = PROJECT_ROOT / "data" / "processed" / "stg_weather_kmsp_daily.csv"
    daily.to_csv(output_path, index=False)

    print(f"Built stg_weather_kmsp_daily with {len(daily):,} rows")
    print(f"Wrote processed CSV: {output_path}")
    return len(daily)


def main():
    build_weather_daily()


if __name__ == "__main__":
    main()
