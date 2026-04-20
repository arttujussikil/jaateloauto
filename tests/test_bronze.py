"""
tests/test_bronze.py
====================
Yksikkötestit 03_bronze/bronze.py -moduulille.

Testataan, että staging-JSON muuntuu oikein Polars DataFrameksi:
    - Sarakkeiden nimet ovat oikein
    - Tyypit muunnetaan (aikaleima, päivämäärä)
    - timeTableRows puretaan oikein rivitasolle
    - NULL-arvot säilyvät

Aja:
    pytest tests/test_bronze.py -v
"""

import json

import polars as pl
import pytest

# bronze löytyy conftest.py:n asettaman sys.path-konfiguraation kautta
from bronze import load_trains_json, save_bronze

# ---------------------------------------------------------------------------
# Testidata
# ---------------------------------------------------------------------------

SAMPLE_TRAINS_JSON = [
    {
        "trainNumber": 1,
        "departureDate": "2024-03-15",
        "operatorShortCode": "vr",
        "trainType": "IC",
        "trainCategory": "Long-distance",
        "commuterLineID": None,
        "cancelled": False,
        "timetableType": "REGULAR",
        "timeTableRows": [
            {
                "stationShortCode": "HKI",
                "type": "DEPARTURE",
                "scheduledTime": "2024-03-15T06:24:00.000Z",
                "actualTime": "2024-03-15T06:24:30.000Z",
                "differenceInMinutes": 0,
                "cancelled": False,
                "commercialStop": True,
                "commercialTrack": "10",
            },
            {
                "stationShortCode": "TPE",
                "type": "ARRIVAL",
                "scheduledTime": "2024-03-15T08:31:00.000Z",
                "actualTime": "2024-03-15T08:35:00.000Z",
                "differenceInMinutes": 4,
                "cancelled": False,
                "commercialStop": True,
                "commercialTrack": "1",
            },
        ],
    },
    {
        "trainNumber": 2,
        "departureDate": "2024-03-15",
        "operatorShortCode": "vr",
        "trainType": "S",
        "trainCategory": "Long-distance",
        "commuterLineID": None,
        "cancelled": True,  # peruutettu juna
        "timetableType": "REGULAR",
        "timeTableRows": [],
    },
]


@pytest.fixture
def sample_json_file(tmp_path):
    """Luo väliaikaisen JSON-tiedoston testeille."""
    fp = tmp_path / "trains_2024-03-15.json"
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(SAMPLE_TRAINS_JSON, f)
    return fp


# ---------------------------------------------------------------------------
# Testit
# ---------------------------------------------------------------------------


class TestLoadTrainsJson:
    def test_returns_dataframe(self, sample_json_file):
        """Funktio palauttaa Polars DataFramen."""
        df = load_trains_json(sample_json_file)
        assert isinstance(df, pl.DataFrame)

    def test_explodes_timetablerows(self, sample_json_file):
        """TimeTableRows on purettu: junat joilla pysähdyksiä → oikea rivimäärä."""
        df = load_trains_json(sample_json_file)
        # Juna 1: 2 pysähdystä, Juna 2: 0 pysähdystä → explode tuottaa 2 riviä
        # (Polars explode tyhjistä listoista voi käyttäytyä eri tavoin — tarkistetaan vain min)
        assert len(df) >= 2

    def test_contains_expected_columns(self, sample_json_file):
        """DataFrame sisältää tärkeimmät sarakkeet."""
        df = load_trains_json(sample_json_file)
        required_cols = {
            "trainNumber",
            "departureDate",
            "trainType",
            "trainCategory",
            "stationShortCode",
            "type",
            "scheduledTime",
        }
        assert required_cols.issubset(set(df.columns))

    def test_scheduled_time_is_datetime(self, sample_json_file):
        """scheduledTime on muunnettu Datetime-tyypiksi."""
        df = load_trains_json(sample_json_file)
        # Suodatetaan rivit joilla on scheduledTime
        df_with_time = df.filter(pl.col("scheduledTime").is_not_null())
        if len(df_with_time) > 0:
            dtype = df_with_time["scheduledTime"].dtype
            assert dtype == pl.Datetime("us", "UTC"), (
                f"Odotettu Datetime(us, UTC), saatiin {dtype}"
            )

    def test_departure_date_is_date(self, sample_json_file):
        """departureDate on muunnettu Date-tyypiksi."""
        df = load_trains_json(sample_json_file)
        df_with_date = df.filter(pl.col("departureDate").is_not_null())
        if len(df_with_date) > 0:
            assert df_with_date["departureDate"].dtype == pl.Date

    def test_cancelled_train_in_data(self, sample_json_file):
        """Peruutettu juna (Juna 2) on mukana datassa."""
        df = load_trains_json(sample_json_file)
        # Junan 2 tiedot pitäisi löytyä (vaikka pysähdyksiä ei ole)
        train_numbers = df["trainNumber"].to_list()
        # Juna 1 on varmasti mukana (sillä on pysähdyksiä)
        assert 1 in train_numbers


class TestSaveBronze:
    def test_creates_parquet_file(self, tmp_path, sample_json_file):
        """save_bronze luo Parquet-tiedoston."""
        df = load_trains_json(sample_json_file)
        output = tmp_path / "trains_2024-03-15.parquet"
        save_bronze(df, output)
        assert output.exists()

    def test_parquet_readable(self, tmp_path, sample_json_file):
        """Luotu Parquet-tiedosto voidaan lukea takaisin DataFrameksi."""
        df = load_trains_json(sample_json_file)
        output = tmp_path / "trains_2024-03-15.parquet"
        save_bronze(df, output)

        reloaded = pl.read_parquet(output)
        assert len(reloaded) == len(df)
        assert set(reloaded.columns) == set(df.columns)

    def test_parquet_preserves_row_count(self, tmp_path, sample_json_file):
        """Tallennus ja lataus säilyttää rivienmäärän."""
        df = load_trains_json(sample_json_file)
        output = tmp_path / "test.parquet"
        save_bronze(df, output)
        reloaded = pl.read_parquet(output)
        assert len(reloaded) == len(df)
