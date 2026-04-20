"""
tests/test_fetch.py
===================
Yksikkötestit 01_fetch/fetch_trains.py -moduulille.

Testit käyttävät `responses`-kirjastoa HTTP-pyyntöjen mockkaamiseen —
testit eivät tee oikeita verkkoyhteyksiä, joten ne toimivat ilman nettiä
ja eivät kuormita Digitrafficin rajapintaa.

Aja:
    pytest tests/test_fetch.py -v
"""

import json
from datetime import date, datetime, timedelta

import pytest
import responses as resp_mock

# fetch_trains löytyy conftest.py:n asettaman sys.path-konfiguraation kautta
from fetch_trains import (
    DigitrafficClient,
    file_is_fresh,
    purge_old_files,
    save_to_staging,
    staging_filepath,
)

# ---------------------------------------------------------------------------
# Testidata
# ---------------------------------------------------------------------------

SAMPLE_TRAIN = {
    "trainNumber": 1,
    "departureDate": "2024-03-15",
    "operatorShortCode": "vr",
    "trainType": "IC",
    "trainCategory": "Long-distance",
    "commuterLineID": None,
    "runningCurrently": False,
    "cancelled": False,
    "version": 123456,
    "timetableType": "REGULAR",
    "timeTableRows": [
        {
            "stationShortCode": "HKI",
            "type": "DEPARTURE",
            "scheduledTime": "2024-03-15T06:24:00.000Z",
            "actualTime": "2024-03-15T06:24:30.000Z",
            "differenceInMinutes": 0,
            "cancelled": False,
        },
        {
            "stationShortCode": "TPE",
            "type": "ARRIVAL",
            "scheduledTime": "2024-03-15T08:31:00.000Z",
            "actualTime": "2024-03-15T08:35:00.000Z",
            "differenceInMinutes": 4,
            "cancelled": False,
        },
    ],
}

SAMPLE_STATION = {
    "stationShortCode": "HKI",
    "stationName": "Helsinki",
    "latitude": 60.172097,
    "longitude": 24.941249,
    "passengerTraffic": True,
    "type": "STATION",
    "countryCode": "FI",
}


# ---------------------------------------------------------------------------
# API-asiakkaan testit
# ---------------------------------------------------------------------------


class TestDigitrafficClient:
    """Testaa DigitrafficClient-luokan toimintaa mockattuja HTTP-vastauksia käyttäen."""

    @resp_mock.activate
    def test_get_trains_by_date_returns_list(self):
        """Onnistunut haku palauttaa listan junatietueita."""
        target_date = date(2024, 3, 15)
        resp_mock.add(
            resp_mock.GET,
            "https://rata.digitraffic.fi/api/v1/trains/2024-03-15",
            json=[SAMPLE_TRAIN],
            status=200,
        )

        client = DigitrafficClient()
        trains = client.get_trains_by_date(target_date)

        assert isinstance(trains, list)
        assert len(trains) == 1
        assert trains[0]["trainNumber"] == 1
        assert trains[0]["trainType"] == "IC"

    @resp_mock.activate
    def test_get_trains_sends_digitraffic_user_header(self):
        """Pyyntö sisältää vaaditun Digitraffic-User -otsikon."""
        target_date = date(2024, 3, 15)
        resp_mock.add(
            resp_mock.GET,
            "https://rata.digitraffic.fi/api/v1/trains/2024-03-15",
            json=[SAMPLE_TRAIN],
            status=200,
        )

        client = DigitrafficClient(user_agent="test-agent/1.0")
        client.get_trains_by_date(target_date)

        assert len(resp_mock.calls) == 1
        sent_header = resp_mock.calls[0].request.headers.get("Digitraffic-User")
        assert sent_header == "test-agent/1.0"

    @resp_mock.activate
    def test_get_trains_raises_on_http_error(self):
        """HTTP 429 (Too Many Requests) nostaa poikkeuksen."""
        import requests

        target_date = date(2024, 3, 15)
        resp_mock.add(
            resp_mock.GET,
            "https://rata.digitraffic.fi/api/v1/trains/2024-03-15",
            status=429,
        )

        client = DigitrafficClient()
        with pytest.raises(requests.HTTPError):
            client.get_trains_by_date(target_date)

    @resp_mock.activate
    def test_get_stations_returns_list(self):
        """Asemahaku palauttaa listan asematietueita."""
        resp_mock.add(
            resp_mock.GET,
            "https://rata.digitraffic.fi/api/v1/metadata/stations",
            json=[SAMPLE_STATION],
            status=200,
        )

        client = DigitrafficClient()
        stations = client.get_stations()

        assert isinstance(stations, list)
        assert stations[0]["stationShortCode"] == "HKI"


# ---------------------------------------------------------------------------
# Staging-apufunktioiden testit
# ---------------------------------------------------------------------------


class TestStagingHelpers:
    """Testaa staging-tiedostojen hallintafunktioita."""

    def test_staging_filepath_format(self, tmp_path):
        """Tiedostonimi on oikeanmuotoinen."""
        staging_dir = tmp_path / "staging"
        target_date = date(2024, 3, 15)
        fp = staging_filepath(staging_dir, target_date, "trains")
        assert fp.name == "trains_2024-03-15.json"
        assert fp.parent == staging_dir

    def test_file_is_fresh_nonexistent(self, tmp_path):
        """Olematon tiedosto ei ole tuore."""
        fp = tmp_path / "nonexistent.json"
        assert file_is_fresh(fp, ttl_days=14) is False

    def test_file_is_fresh_new_file(self, tmp_path):
        """Juuri luotu tiedosto on tuore."""
        fp = tmp_path / "fresh.json"
        fp.write_text("{}")
        assert file_is_fresh(fp, ttl_days=14) is True

    def test_file_is_fresh_old_file(self, tmp_path):
        """Vanha tiedosto (muutettu yli TTL sitten) ei ole tuore."""
        fp = tmp_path / "old.json"
        fp.write_text("{}")
        # Muutetaan tiedoston muokkausaikaa menneisyyteen
        old_time = (datetime.now() - timedelta(days=20)).timestamp()
        import os
        os.utime(fp, (old_time, old_time))
        assert file_is_fresh(fp, ttl_days=14) is False

    def test_save_to_staging_creates_valid_json(self, tmp_path):
        """Tallennettu tiedosto on kelvollista JSON:ia."""
        fp = tmp_path / "trains_2024-03-15.json"
        data = [SAMPLE_TRAIN]
        save_to_staging(data, fp)

        assert fp.exists()
        with open(fp, encoding="utf-8") as f:
            loaded = json.load(f)
        assert len(loaded) == 1
        assert loaded[0]["trainNumber"] == 1

    def test_purge_old_files_removes_expired(self, tmp_path):
        """purge_old_files poistaa vanhentuneet tiedostot."""
        # Luodaan kaksi tiedostoa: tuore ja vanha
        fresh = tmp_path / "trains_2024-03-15.json"
        old = tmp_path / "trains_2024-02-01.json"
        fresh.write_text("[]")
        old.write_text("[]")

        # Vanhennettaan toinen tiedosto
        old_time = (datetime.now() - timedelta(days=20)).timestamp()
        import os
        os.utime(old, (old_time, old_time))

        removed = purge_old_files(tmp_path, ttl_days=14)

        assert removed == 1
        assert fresh.exists()
        assert not old.exists()

    def test_purge_old_files_empty_dir(self, tmp_path):
        """Tyhjä kansio ei aiheuta virheitä."""
        removed = purge_old_files(tmp_path, ttl_days=14)
        assert removed == 0
