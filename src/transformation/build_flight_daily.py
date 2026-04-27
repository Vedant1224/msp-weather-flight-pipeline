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


def build_flight_daily():
    ensure_output_dirs()
    engine = get_engine()

    flights = pd.read_sql("SELECT * FROM raw_flights_msp_2024", engine)
    if flights.empty:
        raise ValueError("raw_flights_msp_2024 is empty.")

    flights["flight_date"] = pd.to_datetime(flights["flight_date"], errors="coerce")

    numeric_columns = [
        "cancelled",
        "diverted",
        "dep_delay_minutes",
        "arr_delay_minutes",
        "dep_del15",
        "carrier_delay",
        "weather_delay",
        "nas_delay",
        "security_delay",
        "late_aircraft_delay",
        "taxi_out",
    ]

    for column in numeric_columns:
        flights[column] = pd.to_numeric(flights[column], errors="coerce")

    flights["cancelled_flag"] = np.where(flights["cancelled"].fillna(0) >= 1, 1, 0)
    flights["diverted_flag"] = np.where(flights["diverted"].fillna(0) >= 1, 1, 0)
    flights["delayed_15_flag"] = np.where(flights["dep_del15"].fillna(0) >= 1, 1, 0)
    flights["completed_flag"] = 1 - flights["cancelled_flag"]
    flights["date"] = flights["flight_date"].dt.date

    daily = (
        flights.groupby("date", dropna=False)
        .apply(
            lambda group: pd.Series(
                {
                    "scheduled_departures": int(len(group)),
                    "completed_departures": int(group["completed_flag"].sum()),
                    "cancelled_departures": int(group["cancelled_flag"].sum()),
                    "cancellation_rate": round(group["cancelled_flag"].mean(), 4),
                    "diverted_departures": int(group["diverted_flag"].sum()),
                    "avg_dep_delay_minutes": round(
                        group.loc[group["cancelled_flag"] == 0, "dep_delay_minutes"].mean(), 2
                    ),
                    "avg_arr_delay_minutes": round(
                        group.loc[group["cancelled_flag"] == 0, "arr_delay_minutes"].mean(), 2
                    ),
                    "delayed_15_count": int(group["delayed_15_flag"].sum()),
                    "delayed_15_rate": round(
                        group.loc[group["cancelled_flag"] == 0, "delayed_15_flag"].mean(), 4
                    ),
                    "total_carrier_delay_minutes": round(group["carrier_delay"].fillna(0).sum(), 2),
                    "total_weather_delay_minutes": round(group["weather_delay"].fillna(0).sum(), 2),
                    "total_nas_delay_minutes": round(group["nas_delay"].fillna(0).sum(), 2),
                    "total_security_delay_minutes": round(group["security_delay"].fillna(0).sum(), 2),
                    "total_late_aircraft_delay_minutes": round(
                        group["late_aircraft_delay"].fillna(0).sum(), 2
                    ),
                    "avg_taxi_out_minutes": round(
                        group.loc[group["cancelled_flag"] == 0, "taxi_out"].mean(), 2
                    ),
                }
            )
        )
        .reset_index()
        .sort_values("date")
    )

    daily.to_sql("stg_flights_msp_daily", engine, if_exists="replace", index=False)

    output_path = PROJECT_ROOT / "data" / "processed" / "stg_flights_msp_daily.csv"
    daily.to_csv(output_path, index=False)

    print(f"Built stg_flights_msp_daily with {len(daily):,} rows")
    print(f"Wrote processed CSV: {output_path}")
    return len(daily)


def main():
    build_flight_daily()


if __name__ == "__main__":
    main()
