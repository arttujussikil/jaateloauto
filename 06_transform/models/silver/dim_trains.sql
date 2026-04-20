{{ config(materialized='view') }}

/*
    dim_trains — Junavuorojen dimensiotaulu (silver)
*/

SELECT
    train_key,
    train_number,
    departure_date,
    train_type,
    train_category,
    commuter_line_id,
    operator,
    cancelled,
    timetable_type
FROM {{ source('raw', 'raw_dim_trains') }}
