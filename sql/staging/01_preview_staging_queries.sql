SELECT COUNT(*) AS raw_flights_rows
FROM raw_flights_msp_2024;

SELECT MIN(flight_date) AS min_flight_date, MAX(flight_date) AS max_flight_date
FROM raw_flights_msp_2024;

SELECT flight_date, COUNT(*) AS departures
FROM raw_flights_msp_2024
GROUP BY flight_date
ORDER BY flight_date
LIMIT 20;

SELECT COUNT(*) AS raw_weather_rows
FROM raw_weather_kmsp_2024;

SELECT MIN(date) AS min_weather_ts, MAX(date) AS max_weather_ts
FROM raw_weather_kmsp_2024;

SELECT *
FROM stg_flights_msp_daily
ORDER BY date
LIMIT 20;

SELECT *
FROM stg_weather_kmsp_daily
ORDER BY date
LIMIT 20;

SELECT f.date
FROM stg_flights_msp_daily f
LEFT JOIN stg_weather_kmsp_daily w
    ON f.date = w.date
WHERE w.date IS NULL;
