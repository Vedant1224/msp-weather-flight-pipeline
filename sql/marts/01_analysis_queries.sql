SELECT
    weather_category,
    COUNT(*) AS days,
    AVG(avg_dep_delay_minutes) AS avg_dep_delay_minutes,
    AVG(cancellation_rate_pct) AS avg_cancellation_rate_pct
FROM mart_msp_daily_weather_flights
GROUP BY weather_category
ORDER BY avg_dep_delay_minutes DESC;

SELECT
    severe_weather_day,
    COUNT(*) AS days,
    AVG(avg_dep_delay_minutes) AS avg_dep_delay_minutes,
    AVG(cancellation_rate_pct) AS avg_cancellation_rate_pct
FROM mart_msp_daily_weather_flights
GROUP BY severe_weather_day
ORDER BY severe_weather_day DESC;

SELECT
    temp_bucket,
    COUNT(*) AS days,
    AVG(avg_dep_delay_minutes) AS avg_dep_delay_minutes,
    AVG(cancellation_rate_pct) AS avg_cancellation_rate_pct
FROM mart_msp_daily_weather_flights
GROUP BY temp_bucket
ORDER BY
    CASE temp_bucket
        WHEN 'Freezing' THEN 1
        WHEN 'Cold' THEN 2
        WHEN 'Mild' THEN 3
        WHEN 'Warm' THEN 4
        WHEN 'Hot' THEN 5
        ELSE 6
    END;

SELECT
    month,
    month_name,
    AVG(avg_dep_delay_minutes) AS avg_dep_delay_minutes,
    AVG(cancellation_rate_pct) AS avg_cancellation_rate_pct,
    AVG(precip_total_inches) AS avg_precip_total_inches,
    SUM(cancelled_departures) AS total_cancelled_departures
FROM mart_msp_daily_weather_flights
GROUP BY month, month_name
ORDER BY month;

SELECT
    date,
    weather_category,
    severe_weather_day,
    scheduled_departures,
    cancelled_departures,
    cancellation_rate_pct,
    avg_dep_delay_minutes,
    precip_total_inches,
    max_wind_gust_mph,
    min_visibility_miles
FROM mart_msp_daily_weather_flights
ORDER BY cancellation_rate_pct DESC, avg_dep_delay_minutes DESC
LIMIT 10;

SELECT
    date,
    severe_weather_day,
    weather_category,
    avg_temp_f,
    precip_total_inches,
    max_wind_gust_mph,
    min_visibility_miles,
    hours_with_snow,
    hours_with_fog,
    hours_with_thunder,
    scheduled_departures,
    cancelled_departures,
    cancellation_rate_pct,
    avg_dep_delay_minutes,
    delayed_15_rate_pct
FROM mart_msp_daily_weather_flights
ORDER BY date;

SELECT
    reporting_airline,
    SUM(scheduled_departures) AS annual_departures
FROM mart_msp_airline_monthly_performance
GROUP BY reporting_airline
ORDER BY annual_departures DESC;

SELECT
    reporting_airline,
    SUM(cancelled_departures) AS annual_cancellations
FROM mart_msp_airline_monthly_performance
GROUP BY reporting_airline
ORDER BY annual_cancellations DESC;

SELECT
    reporting_airline,
    SUM(scheduled_departures) AS annual_departures,
    SUM(cancelled_departures) AS annual_cancellations,
    ROUND(SUM(cancelled_departures) / SUM(scheduled_departures) * 100, 2) AS cancellation_rate_pct
FROM mart_msp_airline_monthly_performance
GROUP BY reporting_airline
HAVING SUM(scheduled_departures) >= 1000
ORDER BY cancellation_rate_pct DESC, annual_departures DESC;

SELECT
    reporting_airline,
    SUM(completed_departures) AS annual_completed_departures,
    SUM(delayed_15_count) AS annual_delayed_15_count,
    ROUND(SUM(delayed_15_count) / NULLIF(SUM(completed_departures), 0) * 100, 2) AS delayed_15_rate_pct
FROM mart_msp_airline_monthly_performance
GROUP BY reporting_airline
HAVING SUM(scheduled_departures) >= 1000
ORDER BY delayed_15_rate_pct DESC, annual_completed_departures DESC;

SELECT
    month,
    month_name,
    reporting_airline,
    scheduled_departures
FROM mart_msp_airline_monthly_performance
ORDER BY month, scheduled_departures DESC, reporting_airline;

SELECT
    month,
    month_name,
    reporting_airline,
    scheduled_departures,
    cancelled_departures,
    cancellation_rate_pct,
    avg_dep_delay_minutes,
    delayed_15_rate_pct,
    total_weather_delay_minutes,
    total_nas_delay_minutes,
    total_late_aircraft_delay_minutes
FROM mart_msp_airline_monthly_performance
WHERE scheduled_departures >= 100
ORDER BY cancellation_rate_pct DESC, avg_dep_delay_minutes DESC, scheduled_departures DESC
LIMIT 20;
