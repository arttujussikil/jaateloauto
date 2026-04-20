"""
03_bronze/bronze.py
===================
Bronze-kerros: muuntaa staging-kansion raaka-JSON-tiedostot Parquet-muotoon
mahdollisimman vähäisillä muunnoksilla.

Mitä tämä kerros tekee (ja mitä se EI tee):
    ✓ Purkaa timeTableRows-listan rivitasolle (yksi rivi = yksi pysähdys)
    ✓ Muuntaa aikaleimakentät oikeaan tyyppiin
    ✓ Tallentaa Parquet-tiedostoiksi (tehokas hakuformaatti)
    ✗ EI korjaa puuttuvia arvoja
    ✗ EI suodata tai poista rivejä
    ✗ EI yhdistä eri päivien dataa
    ✗ EI liitä asemametatietoja

Periaate: Bronze on "as raw as needed" — dataa muokataan juuri sen verran,
että se on käyttökelpoinen seuraavassa vaiheessa (Silver).

Käyttö:
    python 03_bronze/bronze.py

    # Tietty päivä:
    python 03_bronze/bronze.py --date 2024-03-15

    # Kaikki staging-kansion tiedostot:
    python 03_bronze/bronze.py --all

Vaatimukset:
    uv sync --extra dev
"""

import argparse
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Asetukset
# ---------------------------------------------------------------------------

load_dotenv()

STAGING_DIR = Path(os.getenv("STAGING_DIR", "02_staging/data"))
BRONZE_DIR = Path(os.getenv("BRONZE_DIR", "03_bronze/data"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Muunnosfunktiot
# ---------------------------------------------------------------------------


def load_trains_json(filepath: Path) -> pl.DataFrame:
    """
    Lataa staging-tiedoston (JSON) Polars DataFrameen.

    Staging-tiedostossa data on hierarkkisessa muodossa:
        juna → timeTableRows[] → pysähdys

    Tämä funktio purkaa rakenteen niin, että jokainen pysähdys on oma rivinsä.
    Junatason kentät (trainNumber, trainType, jne.) kopioidaan jokaiselle riville.

    Args:
        filepath: Luettava JSON-tiedosto.

    Returns:
        DataFrame, jossa yksi rivi per pysähdys.
        Sarakkeet: ks. alla oleva schema-kommentti.
    """
    log.info("Ladataan: %s", filepath)

    # Polars lukee JSON:n laiskasti — muistinkäyttö pysyy hallinnassa
    # Kasvatetaan infer_schema_length kattamaan kaikki mahdolliset strukturit
    # estämään "extra field in struct" virheitä myöhemmissä tietueissa
    raw = pl.read_json(filepath, infer_schema_length=10000)

    # Puretaan timeTableRows: jokainen pysähdys omalle rivilleen.
    # explode() tekee tämän tehokkaasti ilman Python-silmukoita.
    exploded = raw.explode("timeTableRows")

    # Puretaan sisäkkäinen rakenne (timeTableRows on dict-sarake)
    ttr = exploded.select(pl.col("timeTableRows").struct.unnest())

    # Yhdistetään junatason kentät pysähdystason kenttiin
    train_cols = exploded.select(
        pl.col(
            "trainNumber",
            "departureDate",
            "operatorShortCode",
            "trainType",
            "trainCategory",
            "commuterLineID",
            "cancelled",
            "timetableType",
        )
    )

    # Nimetään cancelled uudelleen selkeyden vuoksi (juna vs. pysähdys)
    train_cols = train_cols.rename({"cancelled": "trainCancelled"})

    combined = pl.concat([train_cols, ttr], how="horizontal")

    # ---------------------------------------------------------------------------
    # Tyyppimuunnokset
    # ---------------------------------------------------------------------------
    # Aikaleimakentät tulevat merkkijonoina (ISO 8601 UTC). Muunnetaan
    # Polars Datetime-tyypiksi myöhempää laskentaa varten.
    # Huom: null-arvot (actualTime puuttuu tulevista pysähdyksistä) säilytetään.

    time_cols = ["scheduledTime", "actualTime"]
    for col in time_cols:
        if col in combined.columns:
            combined = combined.with_columns(
                pl.col(col)
                .str.strptime(pl.Datetime("us", "UTC"), "%+", strict=False)
                .alias(col)
            )

    if "departureDate" in combined.columns:
        combined = combined.with_columns(
            pl.col("departureDate").str.to_date("%Y-%m-%d").alias("departureDate")
        )

    return combined


def load_stations_json(filepath: Path) -> pl.DataFrame:
    """
    Lataa asemametatiedot (stations.json) DataFrameen.

    Returns:
        DataFrame asematiedoilla. Sarakkeet:
        stationShortCode, stationName, latitude, longitude,
        passengerTraffic, type, countryCode, stationUICCode
    """
    log.info("Ladataan asemametatiedot: %s", filepath)
    return pl.read_json(filepath)


def save_bronze(df: pl.DataFrame, output_path: Path) -> None:
    """
    Tallentaa DataFramen Parquet-tiedostona bronze-kansioon.

    Parquet valitaan JSON:n sijaan, koska:
    - ~10x pienempi tiedostokoko (sarakemuotoinen pakkaus)
    - Huomattavasti nopeampi lukeminen suurilla dataseteillä
    - Tyyppitiedot säilyvät (ei merkkijonoiksi muuttumista)

    Args:
        df: Tallennettava DataFrame.
        output_path: Kohdetiedoston polku (.parquet).
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path, compression="zstd")
    log.info(
        "Tallennettu: %s (%d riviä, %d saraketta)",
        output_path,
        len(df),
        len(df.columns),
    )


# ---------------------------------------------------------------------------
# Orkestraattori
# ---------------------------------------------------------------------------


def process_date(target_date: date) -> bool:
    """
    Käsittelee yhden päivän: staging → bronze.

    Args:
        target_date: Käsiteltävä päivä.

    Returns:
        True jos onnistui, False jos staging-tiedostoa ei löydy.
    """
    date_str = target_date.strftime("%Y-%m-%d")
    staging_file = STAGING_DIR / f"trains_{date_str}.json"
    bronze_file = BRONZE_DIR / f"trains_{date_str}.parquet"

    if not staging_file.exists():
        log.warning("Staging-tiedostoa ei löydy: %s — ohitetaan.", staging_file)
        return False

    df = load_trains_json(staging_file)
    save_bronze(df, bronze_file)
    return True


def process_stations() -> None:
    """Käsittelee asemametatiedot: staging → bronze."""
    staging_file = STAGING_DIR / "stations.json"
    bronze_file = BRONZE_DIR / "stations.parquet"

    if not staging_file.exists():
        log.warning("stations.json puuttuu stagingista — ohitetaan.")
        return

    df = load_stations_json(staging_file)
    save_bronze(df, bronze_file)


def process_all() -> None:
    """Käsittelee kaikki staging-kansion trains_*.json -tiedostot."""
    files = sorted(STAGING_DIR.glob("trains_*.json"))
    if not files:
        log.warning("Staging-kansiosta ei löydy trains_*.json -tiedostoja.")
        return

    log.info("Löytyi %d tiedostoa käsiteltäväksi.", len(files))
    for f in files:
        date_str = f.stem.replace("trains_", "")
        try:
            target_date = date.fromisoformat(date_str)
            process_date(target_date)
        except ValueError:
            log.warning("Epäkelpo päivämäärä tiedostonimessä: %s", f.name)


# ---------------------------------------------------------------------------
# Pääohjelma
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Muunna staging-JSON bronze-Parquet-tiedostoiksi."
    )
    parser.add_argument("--date", type=date.fromisoformat, help="Käsiteltävä päivä")
    parser.add_argument(
        "--all", action="store_true", help="Käsittele kaikki staging-tiedostot"
    )
    args = parser.parse_args()

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    process_stations()

    if args.all:
        process_all()
    elif args.date:
        process_date(args.date)
    else:
        # Oletuksena: eilinen
        yesterday = date.today() - timedelta(days=1)
        process_date(yesterday)

    log.info("Bronze-muunnos valmis.")


if __name__ == "__main__":
    main()
