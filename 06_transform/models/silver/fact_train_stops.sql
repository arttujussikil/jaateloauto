{{ config(materialized='table') }}

/*
    fact_train_stops — Silver-kerroksen faktataulukko

    Lukee raw-skeemasta ja rikastaa datan lisäämällä is_late-lippukentän.

    is_late-logiikka:
        - NULL jos juna ei ole vielä ohittanut asemaa (actual_time IS NULL)
        - TRUE jos myöhässä yli 3 minuuttia (VR:n virallinen raja)
        - FALSE muuten

    Yksi rivi edustaa yhtä pysähdystä (saapumista tai lähtöä).
*/

SELECT
    stop_id,
    train_key,
    station_code,
    departure_date,
    stop_type,
    scheduled_time,
    actual_time,
    difference_minutes,

    -- is_late-lippu: yli 3 minuutin myöhästyminen katsotaan myöhässä-tilaksi
    CASE
        WHEN actual_time IS NULL              THEN NULL
        WHEN difference_minutes IS NULL       THEN NULL
        WHEN difference_minutes > 3           THEN TRUE
        ELSE FALSE
    END AS is_late,

    cancelled,
    commercial_stop,
    track

FROM {{ source('raw', 'raw_fact_train_stops') }}
