# Project Plan

## Scope

- Analyze 2024 MSP departures only
- Join BTS flight data with NOAA MSP weather data
- Build daily flight, weather, and final mart tables in MySQL
- Use Tableau to visualize the final mart table

## Hypotheses

1. Severe weather increases cancellation rate.
2. Severe weather increases average departure delay.

## Pipeline Steps

1. Load MSP-only raw flight records from monthly BTS files
2. Load raw MSP NOAA weather records
3. Aggregate flights to one row per date
4. Aggregate weather to one row per date
5. Join daily flights and daily weather into a final mart

## Dashboard Plan

- KPI view for delays and cancellations
- comparison of severe vs non-severe weather days
- weather category breakdown
- monthly trends
- worst disruption days table

## Report Plan

- intro and motivation
- data description
- pipeline design
- hypotheses and methods
- results and visuals
- conclusions and limitations

## Presentation Plan

- project problem
- data sources
- pipeline flow
- database tables
- key findings
- Tableau dashboard screenshots
- future improvements
