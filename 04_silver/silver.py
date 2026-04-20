"""
04_silver/silver.py
===================
Silver-kerroksen ensimmäinen vaihe: lataa bronze-Parquet-tiedostot
DuckDB-tietokantaan "raw"-skeemaan.

Skeemajako (tärkeä!):
    raw.*      ← Tämä skripti kirjoittaa tänne. Raaka tähtimalli.
    silver.*   ← dbt luo tänne puhdistetun tähtimallin (ks. 06_transform/)
    gold.*     ← dbt luo tänne aggregaatit loppukäyttäjälle

Miksi näin?
    dbt on vastuussa silver- ja gold-skeemoista, joten Python-skriptit eivät
    saa kirjoittaa niihin. Näin dbt hallitsee täysin muunnoksia ja testejä.
    Raw-skeema on "lähde", jonka dbt lukee `source()`-funktiolla.

Tähtimalli (raw-skeemassa):
    Faktataulu: raw_fact_train_stops
    Dimensiot:  raw_dim_trains, raw_dim_stations, raw_dim_date

Käyttö:
    python 04_silver/silver.py

Vaatimukset:
    uv sync --extra dev
"""

import logging
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Asetukset
# ---------------------------------------------------------------------------

load_dotenv()

BRONZE_DIR = Path(os.getenv("BRONZE_DIR", "03_bronze/data"))
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", "04_silver/vr_warehouse.duckdb"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tietokannan alustus ja tähtimallin luonti
# ---------------------------------------------------------------------------


def create_raw_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Luo raw-skeeman ja sen taulut DuckDB-tietokantaan.

    Taulut (raw-skeemassa):
        raw_dim_stations    — asemien perustiedot
        raw_dim_trains      — junavuorojen perustiedot
        raw_dim_date        — päivädimensio
        raw_fact_train_stops — faktat: pysähdykset

    Suunnitteluperiaate: Python hoitaa vain raaka-tähtimallin — kaikki
    bisneslogiikka (esim. is_late-kenttä) kuuluu dbt:lle.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_dim_stations (
            station_code     VARCHAR PRIMARY KEY,
            station_name     VARCHAR,
            latitude         DOUBLE,
            longitude        DOUBLE,
            passenger_traffic BOOLEAN,
            country_code     VARCHAR
        );

        CREATE TABLE IF NOT EXISTS raw.raw_dim_trains (
            train_key        VARCHAR PRIMARY KEY,
            train_number     INTEGER,
            departure_date   DATE,
            train_type       VARCHAR,
            train_category   VARCHAR,
            commuter_line_id VARCHAR,
            operator         VARCHAR,
            cancelled        BOOLEAN,
            timetable_type   VARCHAR
        );

        CREATE TABLE IF NOT EXISTS raw.raw_dim_date (
            date_day         DATE PRIMARY KEY,
            year             INTEGER,
            month            INTEGER,
            day              INTEGER,
            day_of_week      INTEGER,
            day_name         VARCHAR,
            week_number      INTEGER,
            is_weekend       BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS raw.raw_fact_train_stops (
            stop_id             VARCHAR PRIMARY KEY,
            train_key           VARCHAR,
            station_code        VARCHAR,
            departure_date      DATE,
            stop_type           VARCHAR,
            scheduled_time      TIMESTAMPTZ,
            actual_time         TIMESTAMPTZ,
            difference_minutes  INTEGER,
            cancelled           BOOLEAN,
            commercial_stop     BOOLEAN,
            track               VARCHAR
        );
    """)
    log.info("Raw-skeeman taulut varmistettu.")


# ---------------------------------------------------------------------------
# Datan lataus DuckDB:hen
# ---------------------------------------------------------------------------


def load_stations(conn: duckdb.DuckDBPyConnection) -> int:
    """Lataa asemametatiedot bronze-Parquetista raw.raw_dim_stations-tauluun."""
    stations_file = BRONZE_DIR / "stations.parquet"
    if not stations_file.exists():
        log.warning("stations.parquet puuttuu bronzesta — ohitetaan.")
        return 0

    # DuckDB:n INSERT OR REPLACE vaatii PRIMARY KEY -konfliktin käsittelyä.
    # Varmin ja yksinkertaisin tapa: tyhjennetään taulu ja täytetään uudelleen.
    conn.execute("DELETE FROM raw.raw_dim_stations;")
    conn.execute(f"""
        INSERT INTO raw.raw_dim_stations
        SELECT
            stationShortCode          AS station_code,
            stationName               AS station_name,
            latitude,
            longitude,
            passengerTraffic          AS passenger_traffic,
            countryCode               AS country_code
        FROM read_parquet('{stations_file}')
        WHERE countryCode = 'FI'
    """)
    count = conn.execute("SELECT COUNT(*) FROM raw.raw_dim_stations").fetchone()[0]
    log.info("raw.raw_dim_stations: %d asemaa.", count)
    return count


def load_bronze_trains(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Lataa bronze-Parquet-tiedostot (trains_*.parquet) raw-skeemaan.

    Täyttää:
        raw.raw_dim_trains, raw.raw_dim_date, raw.raw_fact_train_stops

    Käyttää DuckDB:n read_parquet-funktiota glob-polulla — ei Python-silmukkaa.
    """
    parquet_glob = str(BRONZE_DIR / "trains_*.parquet")
    files = list(BRONZE_DIR.glob("trains_*.parquet"))

    if not files:
        log.warning("Bronze-kansiosta ei löydy trains_*.parquet -tiedostoja.")
        return 0

    log.info("Ladataan %d Parquet-tiedostoa DuckDB:hen ...", len(files))

    # Väliaikaisnäkymä bronze-dataan
    conn.execute(f"""
        CREATE OR REPLACE VIEW bronze_stops AS
        SELECT * FROM read_parquet('{parquet_glob}')
        WHERE departureDate IS NOT NULL
          AND stationShortCode IS NOT NULL
    """)

    # 1. dim_trains — tyhjennys ja uudelleentäyttö
    conn.execute("DELETE FROM raw.raw_dim_trains;")
    conn.execute("""
        INSERT INTO raw.raw_dim_trains
        SELECT DISTINCT
            CAST(trainNumber AS VARCHAR) || '_' || CAST(departureDate AS VARCHAR)
                AS train_key,
            trainNumber           AS train_number,
            departureDate         AS departure_date,
            trainType             AS train_type,
            trainCategory         AS train_category,
            commuterLineID        AS commuter_line_id,
            operatorShortCode     AS operator,
            trainCancelled        AS cancelled,
            timetableType         AS timetable_type
        FROM bronze_stops
    """)
    train_count = conn.execute("SELECT COUNT(*) FROM raw.raw_dim_trains").fetchone()[0]
    log.info("raw.raw_dim_trains: %d junavuoroa.", train_count)

    # 2. dim_date
    conn.execute("DELETE FROM raw.raw_dim_date;")
    conn.execute("""
        INSERT INTO raw.raw_dim_date
        SELECT DISTINCT
            departureDate                              AS date_day,
            EXTRACT(YEAR FROM departureDate)::INTEGER  AS year,
            EXTRACT(MONTH FROM departureDate)::INTEGER AS month,
            EXTRACT(DAY FROM departureDate)::INTEGER   AS day,
            EXTRACT(ISODOW FROM departureDate)::INTEGER AS day_of_week,
            dayname(departureDate)                     AS day_name,
            EXTRACT(WEEK FROM departureDate)::INTEGER  AS week_number,
            EXTRACT(ISODOW FROM departureDate) >= 6    AS is_weekend
        FROM bronze_stops
        WHERE departureDate IS NOT NULL
    """)
    date_count = conn.execute("SELECT COUNT(*) FROM raw.raw_dim_date").fetchone()[0]
    log.info("raw.raw_dim_date: %d päivää.", date_count)

    # 3. fact_train_stops
    conn.execute("DELETE FROM raw.raw_fact_train_stops;")
    conn.execute("""
        INSERT INTO raw.raw_fact_train_stops
        SELECT
            CAST(trainNumber AS VARCHAR) || '_' || CAST(departureDate AS VARCHAR)
                || '_' || stationShortCode || '_' || type  AS stop_id,
            CAST(trainNumber AS VARCHAR) || '_' || CAST(departureDate AS VARCHAR)
                                                           AS train_key,
            stationShortCode   AS station_code,
            departureDate      AS departure_date,
            type               AS stop_type,
            scheduledTime      AS scheduled_time,
            actualTime         AS actual_time,
            differenceInMinutes AS difference_minutes,
            cancelled          AS cancelled,
            commercialStop     AS commercial_stop,
            commercialTrack    AS track
        FROM bronze_stops
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY trainNumber, departureDate, stationShortCode, type
            ORDER BY trainNumber
        ) = 1
    """)
    fact_count = conn.execute(
        "SELECT COUNT(*) FROM raw.raw_fact_train_stops"
    ).fetchone()[0]
    log.info("raw.raw_fact_train_stops: %d pysähdystä.", fact_count)
    return fact_count


# ---------------------------------------------------------------------------
# Pääohjelma
# ---------------------------------------------------------------------------


def main() -> None:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    log.info("Avataan DuckDB: %s", DUCKDB_PATH)
    with duckdb.connect(str(DUCKDB_PATH)) as conn:
        create_raw_schema(conn)
        load_stations(conn)
        load_bronze_trains(conn)

    size_mb = DUCKDB_PATH.stat().st_size / 1024 / 1024
    log.info("Silver raw-kerros valmis. Tietokanta: %s (%.1f MB)", DUCKDB_PATH, size_mb)
    log.info("Seuraava vaihe: cd 06_transform && dbt run && dbt test")


if __name__ == "__main__":
    main()
