# 01_fetch — Tietojen haku

## Tarkoitus

Tämä moduuli hakee junaliikenteen raakadatan Digitrafficin REST-rajapinnasta
ja tallentaa sen staging-kansioon (`02_staging/data/`).

## Käytetty rajapinta

- **URL:** `https://rata.digitraffic.fi/api/v1/trains/{departure_date}`
- **Muoto:** JSON
- **Autentikointi:** Ei API-avainta. `Digitraffic-User`-otsikko on pakollinen.
- **Lisenssi:** CC BY 4.0 (Fintraffic Oy)

## Inkrementaalinen lataus

Skripti tarkistaa ennen hakua, onko päivän tiedosto jo olemassa ja tuoreempi
kuin `STAGING_TTL_DAYS` (oletus 14 pv). Jos on, sitä ei haeta uudelleen.
Vanhat tiedostot poistetaan automaattisesti TTL:n umpeuduttua.

## Käyttö

```bash
# Haetaan viimeiset 7 päivää (oletus)
python 01_fetch/fetch_trains.py

# Haetaan tietty päivä
python 01_fetch/fetch_trains.py --date 2024-03-15

# Haetaan 14 päivää
python 01_fetch/fetch_trains.py --days-back 14
```

## Tulostetiedostot (staging)

| Tiedosto | Sisältö |
|----------|---------|
| `trains_YYYY-MM-DD.json` | Kaikki junat kyseiseltä päivältä |
| `stations.json` | Asemien metatiedot (päivittyy 30 pv välein) |

## Tiedostorakenne

```
01_fetch/
├── fetch_trains.py   ← pääskripti
└── README.md         ← tämä tiedosto
```
