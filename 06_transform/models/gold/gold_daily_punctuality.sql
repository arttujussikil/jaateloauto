{{ config(materialized='table') }}

/*
    gold_daily_punctuality — Päivittäinen täsmällisyyskehitys

    Vastaa kysymykseen: "Miten täsmällisyys on kehittynyt päivittäin?"

    Käyttöesimerkki:
        SELECT departure_date, punctuality_pct, total_trains
        FROM gold.gold_daily_punctuality
        ORDER BY departure_date;
*/

SELECT
    f.departure_date,
    d.day_name,
    d.is_weekend,

    COUNT(DISTINCT f.train_key)                         AS total_trains,
    COUNT(*)                                            AS total_stops,
    SUM(CASE WHEN f.is_late = FALSE THEN 1 ELSE 0 END)  AS on_time_stops,
    SUM(CASE WHEN f.is_late = TRUE  THEN 1 ELSE 0 END)  AS late_stops,
    SUM(CASE WHEN f.cancelled = TRUE THEN 1 ELSE 0 END) AS cancelled_stops,

    ROUND(
        100.0 * SUM(CASE WHEN f.is_late = FALSE THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
        1
    ) AS punctuality_pct,

    ROUND(AVG(CASE WHEN f.difference_minutes > 0
                   THEN f.difference_minutes END), 1) AS avg_delay_minutes

FROM {{ ref('fact_train_stops') }} f
LEFT JOIN {{ ref('dim_date') }} d ON f.departure_date = d.date_day
WHERE f.stop_type = 'ARRIVAL'
  AND f.commercial_stop = TRUE
  AND f.actual_time IS NOT NULL
GROUP BY
    f.departure_date,
    d.day_name,
    d.is_weekend
ORDER BY f.departure_date
