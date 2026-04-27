# Data Dictionary

Main final table: `mart_msp_daily_weather_flights`

| Column | Description |
| --- | --- |
| `date` | Calendar date in 2024 |
| `month` | Numeric month |
| `month_name` | Month name |
| `season` | Winter, Spring, Summer, or Fall |
| `temp_bucket` | Temperature bucket based on average daily temperature |
| `delay_bucket` | Delay severity bucket based on average departure delay |
| `scheduled_departures` | Total scheduled MSP departures for the day |
| `completed_departures` | Scheduled departures that were not cancelled |
| `cancelled_departures` | Cancelled MSP departures |
| `cancellation_rate` | Cancelled departures divided by scheduled departures |
| `cancellation_rate_pct` | Cancellation rate as a percentage |
| `diverted_departures` | Departures marked as diverted |
| `avg_dep_delay_minutes` | Average departure delay minutes for non-cancelled departures |
| `avg_arr_delay_minutes` | Average arrival delay minutes for non-cancelled departures |
| `delayed_15_count` | Number of departures delayed by 15 or more minutes |
| `delayed_15_rate` | Share of completed departures delayed by 15 or more minutes |
| `delayed_15_rate_pct` | Delayed 15 rate as a percentage |
| `total_carrier_delay_minutes` | Sum of carrier delay minutes |
| `total_weather_delay_minutes` | Sum of weather delay minutes |
| `total_nas_delay_minutes` | Sum of NAS delay minutes |
| `total_security_delay_minutes` | Sum of security delay minutes |
| `total_late_aircraft_delay_minutes` | Sum of late aircraft delay minutes |
| `avg_taxi_out_minutes` | Average taxi out minutes for non-cancelled departures |
| `weather_obs_count` | Number of hourly weather observations used that day |
| `avg_temp_f` | Average hourly dry bulb temperature in Fahrenheit |
| `min_temp_f` | Minimum hourly temperature in Fahrenheit |
| `max_temp_f` | Maximum hourly temperature in Fahrenheit |
| `precip_total_inches` | Total hourly precipitation for the day in inches |
| `avg_wind_speed_mph` | Average hourly wind speed |
| `max_wind_gust_mph` | Maximum hourly wind gust speed |
| `min_visibility_miles` | Lowest observed visibility for the day |
| `hours_with_precip` | Hourly observations with measurable precipitation |
| `hours_with_snow` | Hourly observations containing snow codes |
| `hours_with_fog` | Hourly observations containing fog or mist codes |
| `hours_with_thunder` | Hourly observations containing thunderstorm codes |
| `snow_day_flag` | Daily snow indicator |
| `fog_day_flag` | Daily fog indicator |
| `thunder_day_flag` | Daily thunder indicator |
| `low_visibility_day_flag` | Flag for visibility below 3 miles |
| `high_wind_day_flag` | Flag for gusts at or above 30 mph |
| `precip_day_flag` | Flag for precipitation that day |
| `severe_weather_day` | Combined severe weather indicator |
| `weather_category` | Main daily weather label used in analysis |
