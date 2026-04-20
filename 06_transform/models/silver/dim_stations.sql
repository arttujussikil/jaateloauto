{{ config(materialized='view') }}

/*
    dim_stations — Asemien dimensiotaulu (silver)
    Lukee raw-skeemasta, ei muunnoksia — toimii "siltana" dbt-mallien välillä.
*/

SELECT
    station_code,
    station_name,
    latitude,
    longitude,
    passenger_traffic,
    country_code
FROM {{ source('raw', 'raw_dim_stations') }}
