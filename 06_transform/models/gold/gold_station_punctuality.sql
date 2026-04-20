{{ config(materialized='table') }}

/*
    gold_station_punctuality — Asemittainen täsmällisyysraportti

    Vastaa kysymykseen: "Kuinka täsmällisiä junat ovat kullakin asemalla?"

    Suodatukset:
        - Vain saapumiset (stop_type = 'ARRIVAL')
        - Vain kaupalliset pysähdykset (läpiajot pois)
        - Vain toteutuneet (actual_time IS NOT NULL)
        - Vain asemat joilla >= 10 pysähdystä (merkitsevyys)

    Käyttöesimerkki loppukäyttäjälle:
        SELECT station_name, punctuality_pct, avg_delay_minutes
        FROM gold.gold_station_punctuality
        ORDER BY punctuality_pct ASC
        LIMIT 10;
*/

WITH stops AS (
    SELECT
        station_code,
        difference_minutes,
        is_late
    FROM {{ ref('fact_train_stops') }}
    WHERE stop_type = 'ARRIVAL'
      AND commercial_stop = TRUE
      AND cancelled = FALSE
      AND actual_time IS NOT NULL
),

aggregated AS (
    SELECT
        s.station_code,
        st.station_name,
        st.latitude,
        st.longitude,

        COUNT(*)                                           AS stop_count,
        SUM(CASE WHEN s.is_late = FALSE THEN 1 ELSE 0 END) AS on_time_count,
        SUM(CASE WHEN s.is_late = TRUE  THEN 1 ELSE 0 END) AS late_count,

        ROUND(
            100.0 * SUM(CASE WHEN s.is_late = FALSE THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(*), 0),
            1
        ) AS punctuality_pct,

        ROUND(AVG(CASE WHEN s.difference_minutes > 0
                       THEN s.difference_minutes END), 1) AS avg_delay_minutes,
        MAX(s.difference_minutes)                         AS max_delay_minutes,
        MEDIAN(s.difference_minutes)                      AS median_delay_minutes

    FROM stops s
    LEFT JOIN {{ ref('dim_stations') }} st
        ON s.station_code = st.station_code
    GROUP BY
        s.station_code,
        st.station_name,
        st.latitude,
        st.longitude
)

SELECT *
FROM aggregated
WHERE stop_count >= 10
ORDER BY punctuality_pct DESC
