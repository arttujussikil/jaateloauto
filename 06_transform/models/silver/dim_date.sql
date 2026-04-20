{{ config(materialized='view') }}

/*
    dim_date — Päivädimensio (silver)
*/

SELECT
    date_day,
    year,
    month,
    day,
    day_of_week,
    day_name,
    week_number,
    is_weekend
FROM {{ source('raw', 'raw_dim_date') }}
