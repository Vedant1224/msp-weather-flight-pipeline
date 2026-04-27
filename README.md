# Weather and Flight Disruption Pipeline for MSP Airport, 2024

**Course:** CSDS 397: Data Pipelines and Visualization  
**Student:** Vedant Gupta

## Overview

This repository contains an end-to-end data pipeline that analyzes the relationship between local airport weather conditions and flight disruptions at Minneapolis-St. Paul International Airport, also known as MSP/KMSP, during the 2024 calendar year. The project combines Bureau of Transportation Statistics flight performance data with NOAA local weather observations to study how weather conditions relate to delays, cancellations, and airline-level disruption patterns.

The pipeline ingests raw data, cleans and transforms it in Python, stores intermediate and final results in MySQL, orchestrates the workflow with Apache Airflow, and supports visualization in Tableau Desktop. The final outputs are database tables designed for direct analysis rather than manual spreadsheet preparation.

## Business Question and Hypotheses

The central question in this project is how local airport weather conditions at MSP are associated with flight disruptions during 2024.

The project tests three hypotheses:

- Severe weather days at MSP are associated with higher cancellation rates.
- Severe weather days at MSP are associated with higher average departure delays.
- Airline disruption patterns differ by carrier because MSP has uneven carrier volume and hub effects.

## Data Sources

This project uses two public data sources.

1. **Bureau of Transportation Statistics On-Time Reporting Carrier On-Time Performance data**

   This dataset contains detailed flight-level records for U.S. airline operations, including scheduled and actual times, delays, cancellations, and delay causes. This project uses all 12 monthly files for calendar year 2024, then filters them to scheduled departures from MSP only.

2. **NOAA Local Climatological Data for the KMSP station**

   This dataset contains weather observations recorded at Minneapolis-St. Paul International Airport, including hourly temperature, precipitation, visibility, wind, and coded weather conditions. This project uses the 2024 KMSP file and aggregates hourly observations into daily weather indicators.

The raw source files are large and are not intended to be committed to version control. The repository documents the expected raw data layout and uses `.gitignore` to exclude raw and generated data folders.

## Data Volume from Completed Run

| Table Name | Layer | Grain | Row Count |
| --- | --- | --- | ---: |
| `raw_flights_msp_2024` | Raw | One row per MSP departure flight record | 126,779 |
| `raw_weather_kmsp_2024` | Raw | One row per NOAA weather observation record | 14,173 |
| `stg_flights_msp_daily` | Staging | One row per date | 366 |
| `stg_weather_kmsp_daily` | Staging | One row per date | 366 |
| `mart_msp_daily_weather_flights` | Mart | One row per date | 366 |
| `mart_msp_airline_monthly_performance` | Mart | One row per month and reporting airline | 165 |

## Repository Structure

```text
airflow/dags/
dashboard/
data/raw/
data/staged/
data/processed/
docs/dashboard_screenshots/
notebooks/
sql/ddl/
sql/staging/
sql/marts/
src/ingestion/
src/transformation/
src/utils/
README.md
requirements.txt
.env.example
.gitignore
```

- `airflow/dags/`: Apache Airflow DAG for orchestrating the pipeline.
- `dashboard/`: Tableau workbook used for the final visual analysis.
- `data/raw/`: Expected location for BTS and NOAA source files.
- `data/staged/`: Local debug exports from ingestion steps.
- `data/processed/`: Final processed CSV exports produced by the pipeline.
- `docs/dashboard_screenshots/`: Dashboard and Airflow screenshots from the completed project.
- `notebooks/`: Optional exploratory notebook space.
- `sql/ddl/`: Database creation and table setup SQL.
- `sql/staging/`: SQL queries for previewing staging outputs.
- `sql/marts/`: SQL analysis queries used for reporting and validation.
- `src/ingestion/`: Python ingestion scripts for flights and weather.
- `src/transformation/`: Python transformations for daily and airline-level marts.
- `src/utils/`: Shared database helpers and utility logic.
- `README.md`: Project overview and run instructions.
- `requirements.txt`: Python dependencies.
- `.env.example`: Template for local MySQL environment variables.
- `.gitignore`: Git exclusions for raw data, generated outputs, and local environment files.

## Pipeline Architecture

```text
BTS monthly CSV files + NOAA KMSP weather CSV
        |
        v
Python ingestion scripts
        |
        v
MySQL raw tables
        |
        v
Daily flight and weather staging tables
        |
        v
Final mart tables
        |
        v
Tableau dashboards
```

```text
create_database_and_tables
        |
load_raw_msp_flights
        |
load_raw_kmsp_weather
        |
build_flight_daily
        |
build_weather_daily
        |
build_mart_daily
        |
build_airline_monthly
```

## Cleaning and Transformation Summary

This project does not simply load source files at face value. The pipeline performs focused cleaning and transformation so the final tables are usable for analysis and visualization.

For flights, the pipeline:

- Filters the national BTS monthly files to MSP scheduled departures only.
- Selects only useful flight performance columns for this project.
- Handles the extra blank trailing BTS column found in the raw files.
- Converts dates and delay/cancellation fields into usable numeric and date types.
- Aggregates flight records into daily metrics such as cancellations, average delays, delay-cause totals, and delayed-15 rates.
- Builds monthly airline performance metrics using `reporting_airline` carrier codes as the primary airline identifier.

For weather, the pipeline:

- Uses KMSP station data only.
- Cleans source column names into consistent snake_case fields.
- Handles mixed NOAA report types rather than assuming every record is at the same grain.
- Uses hourly observations as the main input for daily aggregation.
- Converts numeric weather strings and trace precipitation values into usable numeric fields.
- Creates weather flags for snow, fog, thunder, low visibility, high wind, precipitation, severe weather day, and a daily weather category.

## Database Tables

The MySQL database uses a simple layered design.

- **Raw tables** store filtered source-level flight and weather records after ingestion.
- **Staging tables** store cleaned daily aggregations for flights and weather separately.
- **Mart tables** store final analysis-ready outputs used by SQL queries and Tableau.

Final tables:

- `raw_flights_msp_2024`: Flight-level MSP departures for 2024 from the BTS source files.
- `raw_weather_kmsp_2024`: KMSP weather observations for 2024 from the NOAA LCD file.
- `stg_flights_msp_daily`: Daily flight performance summary for MSP departures.
- `stg_weather_kmsp_daily`: Daily weather summary and weather severity indicators for MSP/KMSP.
- `mart_msp_daily_weather_flights`: Final daily weather-flight mart with one row per date.
- `mart_msp_airline_monthly_performance`: Final airline-level mart with one row per month and `reporting_airline`.

## How to Run Locally

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` with local MySQL credentials. The local MySQL environment used for this project was:

- `MYSQL_HOST=127.0.0.1`
- `MYSQL_PORT=3307`
- `MYSQL_USER=root`
- `MYSQL_DATABASE=msp_weather_flights`

Do not place the actual password in the README. Copy `.env.example` to `.env` and fill in your local credentials there.

Run the manual pipeline from the project root:

```bash
python3 src/run_pipeline.py
```

## How to Verify MySQL Output

After the pipeline completes, connect to MySQL and run:

```sql
SHOW TABLES;
SELECT COUNT(*) FROM raw_flights_msp_2024;
SELECT COUNT(*) FROM raw_weather_kmsp_2024;
SELECT COUNT(*) FROM mart_msp_daily_weather_flights;
SELECT COUNT(*) FROM mart_msp_airline_monthly_performance;
```

Successful tables created by the pipeline:

- `raw_flights_msp_2024`
- `raw_weather_kmsp_2024`
- `stg_flights_msp_daily`
- `stg_weather_kmsp_daily`
- `mart_msp_daily_weather_flights`
- `mart_msp_airline_monthly_performance`

## Airflow Orchestration

Apache Airflow orchestrates the already validated pipeline functions defined in the existing Python scripts. The DAG file is:

- `airflow/dags/msp_weather_flight_pipeline_dag.py`

DAG details:

- DAG id: `msp_weather_flight_pipeline`
- Manual trigger only
- `schedule=None`
- `catchup=False`

To run Airflow locally:

```bash
export AIRFLOW_HOME=/path/to/CSDS397_FinalProject/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db migrate
airflow standalone
```

Then:

1. Open [http://localhost:8080](http://localhost:8080).
2. Log in to the Airflow UI.
3. Unpause the DAG `msp_weather_flight_pipeline`.
4. Trigger the DAG manually.
5. Confirm that all tasks complete successfully and turn green in the graph or grid view.

Task order:

1. `create_database_and_tables`
2. `load_raw_msp_flights`
3. `load_raw_kmsp_weather`
4. `build_flight_daily`
5. `build_weather_daily`
6. `build_mart_daily`
7. `build_airline_monthly`

Airflow and dashboard screenshots are stored in `docs/dashboard_screenshots/`.

## Tableau Dashboards

Tableau Desktop was connected directly to the MySQL database, not to manually edited CSV files. The completed workbook is:

- `dashboard/CSDS_397_Final_Project.twb`

The `.twb` workbook stores the Tableau workbook and database connection structure, and it expects the local MySQL database to be running when reopened.

The workbook uses these database tables:

- `mart_msp_daily_weather_flights`
- `mart_msp_airline_monthly_performance`

The final workbook includes three dashboards:

- `MSP 2024 Weather Impact Overview`
- `Weather Severity and Flight Disruption`
- `MSP 2024 Airline Performance`

Dashboard screenshots are stored in `docs/dashboard_screenshots/`.

## Key Findings from SQL and Dashboard Analysis

- Severe weather days: 226 days, average departure delay 14.28 minutes, average cancellation rate 0.88%, delayed 15+ rate 18.24%.
- Non-severe weather days: 140 days, average departure delay 11.51 minutes, average cancellation rate 0.82%, delayed 15+ rate 15.78%.
- Thunderstorm days had the highest average departure delay at 24.06 minutes and the highest average cancellation rate at 1.92%.
- Snow days had average departure delay of 17.32 minutes and average cancellation rate of 1.36%.
- July had the highest average monthly departure delay at 26.81 minutes and highest average cancellation rate at 4.04%.
- Delta had the largest number of MSP departures with 69,402 scheduled departures. Delta’s cancellation rate was 0.83%, while smaller carriers such as Frontier had higher disruption rates.

These results support the main interpretation that weather severity is associated with more disruption at MSP, while also showing that disruption patterns vary across carriers because carrier scale and operating context at MSP are not uniform.

## Notes and Limitations

- Weather is associated with disruption, but this analysis does not prove that weather is the sole cause of each delay or cancellation.
- Delay can also be caused by airline operations, NAS/air traffic conditions, aircraft rotation, and late arriving aircraft.
- The analysis is daily, so it does not model exact hourly cause-and-effect for each individual flight.
- MSP/KMSP is a strong match for this project because the weather station is located in the same airport environment as the flights being studied.

## Final Project Status

This repository contains the completed pipeline code, SQL files, Airflow DAG, Tableau workbook, screenshots, and documentation needed to reproduce the final project locally.
