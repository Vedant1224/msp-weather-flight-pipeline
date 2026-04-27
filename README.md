# Weather and Flight Disruption Pipeline for MSP Airport, 2024

## Project Goal

This project builds a simple local data pipeline to analyze how 2024 weather at Minneapolis-St. Paul International Airport (`MSP` / `KMSP`) relates to flight delays and cancellations. The pipeline ingests BTS flight data and NOAA weather data, cleans and transforms both sources, loads the results into MySQL, and creates final daily and airline-level mart tables for Tableau.

## Hypotheses

1. Severe weather increases cancellation rate.
2. Severe weather increases average departure delay.

## Data Sources

- BTS On-Time Reporting Carrier Performance data for 2024 monthly flight records
- NOAA Local Climatological Data for Minneapolis-St. Paul International Airport for 2024

## Raw Data Layout

```text
data/
  raw/
    flights/
      bts_zips/
      extracted/
        On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv
        ...
        On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_12.csv
    weather/
      kmsp_lcd_2024.csv
```

## Scope

- Airport: `MSP` only
- Year: `2024` only
- Flight filter: departures only where `Origin == "MSP"`
- Final analysis grain: one row per `date`
- Final output: MySQL mart table for Tableau

## Pipeline Architecture

The project uses a simple manual pipeline:

1. Load filtered MSP departure flights from the 12 BTS monthly CSV files into `raw_flights_msp_2024`
2. Load MSP NOAA weather data into `raw_weather_kmsp_2024`
3. Aggregate flights to one row per day in `stg_flights_msp_daily`
4. Aggregate weather to one row per day in `stg_weather_kmsp_daily`
5. Join both daily tables into `mart_msp_daily_weather_flights`
6. Aggregate monthly MSP performance by `reporting_airline` into `mart_msp_airline_monthly_performance`

Python scripts live in:

- `src/ingestion/`
- `src/transformation/`
- `src/utils/`

SQL helper files live in:

- `sql/ddl/`
- `sql/staging/`
- `sql/marts/`

## Database Schema Overview

This project uses simple table prefixes rather than multiple schemas:

- `raw_flights_msp_2024`
- `raw_weather_kmsp_2024`
- `stg_flights_msp_daily`
- `stg_weather_kmsp_daily`
- `mart_msp_daily_weather_flights`
- `mart_msp_airline_monthly_performance`

### Table Roles

- `raw_*` tables store filtered, cleaned source-level data
- `stg_*` tables store daily aggregations for flights and weather
- `mart_*` tables store the final Tableau-ready analysis datasets

## Environment Setup

### 1. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install Python packages

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Copy `.env.example` to `.env` and update the values for your local MySQL setup.

Example:

```env
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=password
MYSQL_DATABASE=msp_weather_flights
```

### 4. Confirm MySQL is running

The pipeline expects a reachable local MySQL server.

## How to Run the Pipeline

From the project root:

```bash
python3 src/run_pipeline.py
```

The pipeline will:

- create the database if needed
- load filtered raw flights into MySQL
- load raw weather into MySQL
- build daily flight staging
- build daily weather staging
- build the final daily mart table
- build the airline-level monthly mart table
- write local debug/output CSVs to `data/staged/` and `data/processed/`

## Expected Final Tables

After a successful run, MySQL should contain:

- `raw_flights_msp_2024`
- `raw_weather_kmsp_2024`
- `stg_flights_msp_daily`
- `stg_weather_kmsp_daily`
- `mart_msp_daily_weather_flights`
- `mart_msp_airline_monthly_performance`

## Expected Local Output Files

`data/staged/`

- `raw_flights_msp_2024.csv`
- `raw_weather_kmsp_2024.csv`

`data/processed/`

- `stg_flights_msp_daily.csv`
- `stg_weather_kmsp_daily.csv`
- `mart_msp_daily_weather_flights.csv`
- `mart_msp_airline_monthly_performance.csv`

## Tableau / Dashboard Plan

Tableau Desktop should connect directly to the MySQL table:

- `mart_msp_daily_weather_flights`
- `mart_msp_airline_monthly_performance`

Suggested dashboards:

- delay and cancellation by weather category
- severe weather vs non-severe weather comparison
- monthly trend lines for delays and cancellations
- monthly airline comparison by `reporting_airline`
- top worst disruption days
- weather condition filters for interactive analysis

The `dashboard/` folder can later hold screenshots, Tableau workbook notes, or exported assets.

## Future Airflow / dbt Steps

Not included yet by design.

Later extensions could include:

- scheduling this pipeline with Airflow
- adding dbt models/tests for staging and marts
- adding validation checks and row-count tests
- adding more dashboard polish and documentation

## Project Status Checklist

- [x] Raw flight data added
- [x] Raw weather data added
- [x] Python ingestion scripts created
- [x] Python transformation scripts created
- [x] MySQL helper utilities created
- [x] DDL and SQL analysis files created
- [x] Final mart table design created
- [x] README and docs added
- [ ] Local MySQL pipeline run completed
- [ ] Tableau workbook connected to mart table
- [ ] Final report and presentation completed
