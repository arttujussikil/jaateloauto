# 02_staging — Skeema ja rakenne

## Tarkoitus

Staging-kerros tallentaa API:sta saadun datan **muuttumattomana**. Ei muunnoksia,
ei siivouksia — vain raaka JSON sellaisenaan. Näin voidaan aina palata
alkuperäiseen dataan ja toistaa muunnokset uudelleen.

## Tiedostorakenne

```
02_staging/data/
├── trains_2024-03-15.json    ← päiväkohtaiset junatiedot
├── trains_2024-03-16.json
├── ...
└── stations.json             ← asemien metatiedot
```

## TTL (Time-to-Live)

- Junatiedostot: **14 päivää** (muutettavissa `.env`-tiedostossa: `STAGING_TTL_DAYS`)
- Asemametatiedot: **30 päivää**
- Vanhat tiedostot poistetaan automaattisesti seuraavan `fetch_trains.py`-ajon yhteydessä.

## trains_YYYY-MM-DD.json — skeema

Tiedosto on lista JSON-objekteja. Jokainen objekti edustaa yhtä junavuoroa.

```json
[
  {
    "trainNumber": 1,
    "departureDate": "2024-03-15",
    "operatorShortCode": "vr",
    "trainType": "IC",
    "trainCategory": "Long-distance",
    "commuterLineID": null,
    "runningCurrently": false,
    "cancelled": false,
    "version": 296213584963,
    "timetableType": "REGULAR",
    "timetableAcceptanceDate": "2023-12-13T09:41:00.000Z",
    "timeTableRows": [
      {
        "stationShortCode": "HKI",
        "stationUICCode": 1,
        "countryCode": "FI",
        "type": "DEPARTURE",
        "trainStopping": true,
        "commercialStop": true,
        "commercialTrack": "10",
        "cancelled": false,
        "scheduledTime": "2024-03-15T06:24:00.000Z",
        "actualTime": "2024-03-15T06:24:30.000Z",
        "differenceInMinutes": 0,
        "causes": [],
        "trainReady": {
          "source": "LIIKE_AUTOMATIC",
          "accepted": true,
          "timestamp": "2024-03-15T06:24:00.000Z"
        }
      }
    ]
  }
]
```

### Tärkeimmät kentät

| Kenttä | Tyyppi | Kuvaus |
|--------|--------|--------|
| `trainNumber` | int | Junanumero |
| `departureDate` | string (YYYY-MM-DD) | Lähtöpäivä |
| `trainType` | string | Esim. "IC", "S", "P", "MUS" |
| `trainCategory` | string | "Long-distance" tai "Commuter" |
| `cancelled` | bool | Onko juna peruttu |
| `timeTableRows[].stationShortCode` | string | Aseman lyhenne (esim. "HKI") |
| `timeTableRows[].type` | string | "ARRIVAL" tai "DEPARTURE" |
| `timeTableRows[].scheduledTime` | string (ISO 8601 UTC) | Aikataulu |
| `timeTableRows[].actualTime` | string (ISO 8601 UTC) | Toteutunut aika |
| `timeTableRows[].differenceInMinutes` | int | Myöhästyminen minuutteina |

## stations.json — skeema

```json
[
  {
    "passengerTraffic": true,
    "type": "STATION",
    "stationName": "Helsinki",
    "stationShortCode": "HKI",
    "stationUICCode": 1,
    "countryCode": "FI",
    "longitude": 24.941249,
    "latitude": 60.172097
  }
]
```
