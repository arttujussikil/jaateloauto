"""
01_fetch/fetch_trains.py
========================
Hakee junaliikenteen toteuma- ja aikataulutiedot Digitrafficin avoimesta
REST-rajapinnasta (rata.digitraffic.fi) ja tallentaa ne staging-kansioon
raakoina JSON-tiedostoina.

Mitä tämä skripti tekee:
    1. Lukee asetukset .env-tiedostosta tai ympäristömuuttujista
    2. Hakee päivittäiset junatiedot halutulle aikavälille
    3. Tallentaa jokaisen päivän tiedot omaksi JSON-tiedostokseen
    4. Siivoo staging-kansion TTL:n mukaan (oletuksena 14 päivää)

Käyttö:
    python 01_fetch/fetch_trains.py

    # Haetaan tietty päivä:
    python 01_fetch/fetch_trains.py --date 2024-03-15

    # Haetaan useampi päivä taaksepäin:
    python 01_fetch/fetch_trains.py --days-back 14

Vaatimukset:
    uv sync --extra dev

Datalähde:
    https://rata.digitraffic.fi/api/v1/trains/{departure_date}
    Lisenssi: Creative Commons Attribution 4.0 (Fintraffic Oy)
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Asetukset
# ---------------------------------------------------------------------------

load_dotenv()

# Digitraffic edellyttää User-otsikkoa korkean kuorman estämiseksi.
# Ilman otsikkoa rajapinta saattaa palauttaa 429 Too Many Requests.
DIGITRAFFIC_USER = os.getenv(
    "DIGITRAFFIC_USER", "vr-data-platform/0.1 opiskelija@example.com"
)

BASE_URL = "https://rata.digitraffic.fi/api/v1"
STAGING_DIR = Path(os.getenv("STAGING_DIR", "02_staging/data"))
STAGING_TTL_DAYS = int(os.getenv("STAGING_TTL_DAYS", "14"))
FETCH_DAYS_BACK = int(os.getenv("FETCH_DAYS_BACK", "7"))

# Viive pyyntöjen välissä sekunteina — ollaan kohteliaat rajapinnan kanssa
REQUEST_DELAY_SECONDS = 0.5

# ---------------------------------------------------------------------------
# Lokitus
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# API-asiakasluokka
# ---------------------------------------------------------------------------


class DigitrafficClient:
    """
    Yksinkertainen asiakas Digitrafficin rautatieliikenteen REST-rajapinnalle.

    Rajapinnan dokumentaatio: https://www.digitraffic.fi/rautatieliikenne/
    Swagger-UI: https://rata.digitraffic.fi/swagger/

    Huomio autentikoinnista:
        Rajapinta on avoin — API-avainta ei tarvita. Digitraffic-User -otsikko
        on kuitenkin pakollinen 1.12.2024 alkaen. Ilman sitä rajapinta saattaa
        palauttaa 429-virheen ruuhka-aikoina.
    """

    def __init__(self, user_agent: str = DIGITRAFFIC_USER):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Digitraffic-User": user_agent,
                "Accept-Encoding": "gzip",  # pakkaus vähentää siirrettävää dataa
                "Accept": "application/json",
            }
        )

    def get_trains_by_date(self, departure_date: date) -> list[dict]:
        """
        Hakee kaikki junat halutulle lähtöpäivämäärälle.

        Args:
            departure_date: Päivä, jonka junat haetaan.

        Returns:
            Lista junatietue-sanakirjoja. Jokainen sanakirja sisältää mm.:
            - trainNumber: junanumero (int)
            - departureDate: lähtöpäivä (str, YYYY-MM-DD)
            - trainType: junatyyppi (esim. "IC", "S", "P")
            - trainCategory: kategoria ("Long-distance" tai "Commuter")
            - cancelled: peruttu (bool)
            - timeTableRows: lista pysähdyksistä aikatauluineen
              - stationShortCode: aseman lyhenne (esim. "HKI")
              - scheduledTime: aikataulun mukainen aika (ISO 8601 UTC)
              - actualTime: toteutunut aika (ISO 8601 UTC), puuttuu jos ei vielä ohitettu
              - differenceInMinutes: myöhästyminen minuutteina (int)
              - type: "ARRIVAL" tai "DEPARTURE"

        Raises:
            requests.HTTPError: Jos rajapinta palauttaa 4xx tai 5xx -vastauksen.
            requests.ConnectionError: Jos yhteys rajapintaan epäonnistuu.
        """
        date_str = departure_date.strftime("%Y-%m-%d")
        url = f"{BASE_URL}/trains/{date_str}"

        log.info("Haetaan junat päivälle %s ...", date_str)
        response = self.session.get(url, timeout=30)
        response.raise_for_status()

        trains = response.json()
        log.info("  → %d junaa löytyi.", len(trains))
        return trains

    def get_live_trains(self) -> list[dict]:
        """
        Hakee tällä hetkellä liikennöivät junat reaaliaikaisesti.

        Returns:
            Lista junatietueita (sama rakenne kuin get_trains_by_date).
        """
        url = f"{BASE_URL}/live-trains"
        log.info("Haetaan reaaliaikaiset junat ...")
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        trains = response.json()
        log.info("  → %d junaa ajossa.", len(trains))
        return trains

    def get_stations(self) -> list[dict]:
        """
        Hakee kaikkien rautatieasemien metatiedot.

        Returns:
            Lista asematietueita, joissa mm.:
            - stationShortCode: lyhenne (esim. "HKI")
            - stationName: nimi suomeksi (esim. "Helsinki")
            - latitude / longitude: koordinaatit
            - countryCode: "FI"
        """
        url = f"{BASE_URL}/metadata/stations"
        log.info("Haetaan asemien metatiedot ...")
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return response.json()


# ---------------------------------------------------------------------------
# Staging-hallinta
# ---------------------------------------------------------------------------


def ensure_staging_dir(path: Path) -> None:
    """Luo staging-kansio, jos sitä ei vielä ole."""
    path.mkdir(parents=True, exist_ok=True)


def staging_filepath(staging_dir: Path, record_date: date, data_type: str) -> Path:
    """
    Palauttaa staging-tiedoston polun.

    Nimeämislogiikka: {data_type}_{YYYY-MM-DD}.json
    Esimerkki: trains_2024-03-15.json

    Yhtenäinen nimeäminen helpottaa TTL-siivousta ja inkrementaalista latausta:
    jos tiedosto on jo olemassa ja tuoreempi kuin TTL, sitä ei haeta uudelleen.
    """
    filename = f"{data_type}_{record_date.strftime('%Y-%m-%d')}.json"
    return staging_dir / filename


def file_is_fresh(filepath: Path, ttl_days: int) -> bool:
    """
    Tarkistaa, onko tiedosto tuoreempi kuin TTL sallii.

    Args:
        filepath: Tarkistettava tiedosto.
        ttl_days: Vanhenemisaika päivinä.

    Returns:
        True jos tiedosto on olemassa ja riittävän tuore.
    """
    if not filepath.exists():
        return False
    age = datetime.now() - datetime.fromtimestamp(filepath.stat().st_mtime)
    return age.days < ttl_days


def purge_old_files(staging_dir: Path, ttl_days: int) -> int:
    """
    Poistaa staging-kansiosta tiedostot, jotka ovat vanhempia kuin ttl_days.

    Args:
        staging_dir: Siivottava kansio.
        ttl_days: Säilytettävä aika päivinä.

    Returns:
        Poistettujen tiedostojen lukumäärä.
    """
    removed = 0
    cutoff = datetime.now() - timedelta(days=ttl_days)

    for filepath in staging_dir.glob("*.json"):
        mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
        if mtime < cutoff:
            filepath.unlink()
            log.info("Poistettu vanhentunut tiedosto: %s", filepath.name)
            removed += 1

    return removed


def save_to_staging(data: list[dict], filepath: Path) -> None:
    """
    Tallentaa datan JSON-tiedostoon staging-kansioon.

    Data tallennetaan muuttumattomana (raaka API-vastaus) — ei muunnoksia.
    Näin staging toimii "totuuden lähteenä" ja muunnokset voidaan toistaa.

    Args:
        data: Tallennettava data (lista sanakirjoja).
        filepath: Kohdetiedoston polku.
    """
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info("Tallennettu: %s (%d tietuetta)", filepath, len(data))


# ---------------------------------------------------------------------------
# Pääohjelma
# ---------------------------------------------------------------------------


def fetch_date_range(
    client: DigitrafficClient,
    start_date: date,
    end_date: date,
    staging_dir: Path,
) -> None:
    """
    Hakee junatiedot päivä kerrallaan annetulla aikavälillä.

    Inkrementaalinen lataus: jos päivän tiedosto on jo olemassa ja tuoreempi
    kuin STAGING_TTL_DAYS, sitä ei haeta uudelleen. Tämä säästää rajapintaan
    kohdistuvaa kuormaa.

    Args:
        client: DigitrafficClient-instanssi.
        start_date: Aikavälin ensimmäinen päivä.
        end_date: Aikavälin viimeinen päivä (mukaan lukien).
        staging_dir: Staging-kansion polku.
    """
    current = start_date
    while current <= end_date:
        filepath = staging_filepath(staging_dir, current, "trains")

        if file_is_fresh(filepath, STAGING_TTL_DAYS):
            log.info("Ohitetaan %s — tiedosto on tuore.", current)
        else:
            try:
                trains = client.get_trains_by_date(current)
                save_to_staging(trains, filepath)
                # Kohteliaisuusviive — ei rasiteta rajapintaa turhaan
                time.sleep(REQUEST_DELAY_SECONDS)
            except requests.HTTPError as e:
                log.error("HTTP-virhe päivälle %s: %s", current, e)
            except requests.ConnectionError as e:
                log.error("Yhteysvirhe: %s", e)
                sys.exit(1)

        current += timedelta(days=1)


def fetch_stations(client: DigitrafficClient, staging_dir: Path) -> None:
    """
    Hakee asemien metatiedot — haetaan vain kerran, koska muuttuvat harvoin.

    Tallennetaan tiedostoon stations.json (ei päiväkohtainen).
    """
    filepath = staging_dir / "stations.json"
    if file_is_fresh(filepath, ttl_days=30):
        log.info("Ohitetaan asemat — tiedosto on tuore (TTL 30 pv).")
        return

    try:
        stations = client.get_stations()
        save_to_staging(stations, filepath)
    except requests.HTTPError as e:
        log.error("Asemien haku epäonnistui: %s", e)


def main() -> None:
    """Komentorivirajapinta fetch-skriptille."""
    parser = argparse.ArgumentParser(
        description="Hae VR-junatiedot Digitrafficin API:sta staging-kansioon."
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        help="Haettava päivä muodossa YYYY-MM-DD (oletus: eilinen)",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=FETCH_DAYS_BACK,
        help=f"Kuinka monta päivää taaksepäin haetaan (oletus: {FETCH_DAYS_BACK})",
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        default=STAGING_DIR,
        help=f"Staging-kansion polku (oletus: {STAGING_DIR})",
    )
    args = parser.parse_args()

    staging_dir = args.staging_dir
    ensure_staging_dir(staging_dir)

    # Siivoaa vanhat tiedostot ennen hakua
    removed = purge_old_files(staging_dir, STAGING_TTL_DAYS)
    if removed:
        log.info("Poistettu %d vanhentunutta staging-tiedostoa.", removed)

    client = DigitrafficClient()

    # Haetaan asemat (metatiedot)
    fetch_stations(client, staging_dir)

    # Haetaan junatiedot
    if args.date:
        fetch_date_range(client, args.date, args.date, staging_dir)
    else:
        end_date = date.today() - timedelta(days=1)  # eilinen = viimeisin valmis päivä
        start_date = end_date - timedelta(days=args.days_back - 1)
        log.info("Haetaan aikaväli %s – %s", start_date, end_date)
        fetch_date_range(client, start_date, end_date, staging_dir)

    log.info("Haku valmis. Staging: %s", staging_dir)


if __name__ == "__main__":
    main()
