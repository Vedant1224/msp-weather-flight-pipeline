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
    (PROJECT_ROOT / "data" / "processed").mkdir(parents=True, exist_ok=True)


def build_airline_monthly():
    ensure_output_dirs()
    engine = get_engine()

    flights = pd.read_sql("SELECT * FROM raw_flights_msp_2024", engine)
    if flights.empty:
        raise ValueError("raw_flights_msp_2024 is empty.")

    required_columns = [
        "flight_date",
        "month",
        "reporting_airline",
        "cancelled",
        "dep_delay_minutes",
        "dep_del15",
        "carrier_delay",
        "weather_delay",
        "nas_delay",
        "security_delay",
        "late_aircraft_delay",
    ]
    missing_columns = [column for column in required_columns if column not in flights.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns in raw_flights_msp_2024: {missing_columns}")

    flights["flight_date"] = pd.to_datetime(flights["flight_date"], errors="coerce")
    flights["month"] = pd.to_numeric(flights["month"], errors="coerce")
    flights["reporting_airline"] = flights["reporting_airline"].astype(str).str.strip().str.upper()

    numeric_columns = [
        "cancelled",
        "dep_delay_minutes",
        "dep_del15",
        "carrier_delay",
        "weather_delay",
        "nas_delay",
        "security_delay",
        "late_aircraft_delay",
    ]
    for column in numeric_columns:
        flights[column] = pd.to_numeric(flights[column], errors="coerce")

    flights = flights[flights["reporting_airline"] != ""].copy()
    flights["cancelled_flag"] = np.where(flights["cancelled"].fillna(0) >= 1, 1, 0)
    flights["completed_flag"] = 1 - flights["cancelled_flag"]
    flights["delayed_15_completed_flag"] = np.where(
        (flights["cancelled_flag"] == 0) & (flights["dep_del15"].fillna(0) >= 1),
        1,
        0,
    )
    flights["month_name"] = pd.to_datetime(flights["month"], format="%m", errors="coerce").dt.strftime("%B")

    monthly = (
        flights.groupby(["month", "month_name", "reporting_airline"], dropna=False)
        .apply(
            lambda group: pd.Series(
                {
                    "scheduled_departures": int(len(group)),
                    "completed_departures": int(group["completed_flag"].sum()),
                    "cancelled_departures": int(group["cancelled_flag"].sum()),
                    "cancellation_rate_pct": round(
                        (group["cancelled_flag"].sum() / len(group)) * 100, 2
                    )
                    if len(group) > 0
                    else 0.0,
                    "avg_dep_delay_minutes": round(
                        group.loc[group["cancelled_flag"] == 0, "dep_delay_minutes"].mean(), 2
                    ),
                    "delayed_15_count": int(group["delayed_15_completed_flag"].sum()),
                    "delayed_15_rate_pct": round(
                        (
                            group["delayed_15_completed_flag"].sum()
                            / max(group["completed_flag"].sum(), 1)
                        )
                        * 100,
                        2,
                    )
                    if group["completed_flag"].sum() > 0
                    else 0.0,
                    "total_carrier_delay_minutes": round(group["carrier_delay"].fillna(0).sum(), 2),
                    "total_weather_delay_minutes": round(group["weather_delay"].fillna(0).sum(), 2),
                    "total_nas_delay_minutes": round(group["nas_delay"].fillna(0).sum(), 2),
                    "total_security_delay_minutes": round(group["security_delay"].fillna(0).sum(), 2),
                    "total_late_aircraft_delay_minutes": round(
                        group["late_aircraft_delay"].fillna(0).sum(), 2
                    ),
                }
            )
        )
        .reset_index()
        .sort_values(["month", "scheduled_departures"], ascending=[True, False])
    )

    monthly.to_sql(
        "mart_msp_airline_monthly_performance",
        engine,
        if_exists="replace",
        index=False,
    )

    output_path = PROJECT_ROOT / "data" / "processed" / "mart_msp_airline_monthly_performance.csv"
    monthly.to_csv(output_path, index=False)

    print(f"Built mart_msp_airline_monthly_performance with {len(monthly):,} rows")
    print(f"Wrote processed CSV: {output_path}")
    return len(monthly)


def main():
    build_airline_monthly()


if __name__ == "__main__":
    main()
