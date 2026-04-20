# 05_gold — Gold-kerros (loppukäyttäjän data)

## Tarkoitus

Gold-kerros sisältää **valmiit aggregaattitaulut**, jotka on optimoitu
loppukäyttäjän kyselyitä varten. Taulut luodaan dbt:llä (ks. `06_transform/`).

**Tätä kerrosta käytetään visualisoinneissa ja raporteissa.**

## Saatavilla olevat taulut

### `gold_station_punctuality` — Täsmällisyys asemittain

Vastaa kysymykseen: *"Millä asemilla junat ovat täsmällisimpiä?"*

```sql
-- Epätäsmällisimmät asemat (vähintään 100 pysähdystä)
SELECT station_name, punctuality_pct, avg_delay_minutes
FROM gold_station_punctuality
WHERE stop_count >= 100
ORDER BY punctuality_pct ASC
LIMIT 10;
```

| Sarake | Kuvaus |
|--------|--------|
| `station_name` | Aseman nimi suomeksi |
| `station_code` | Lyhenne (esim. HKI) |
| `punctuality_pct` | Täsmällisyysprosentti 0–100 |
| `avg_delay_minutes` | Myöhässä olleiden junien keskiviive (min) |
| `max_delay_minutes` | Suurin yksittäinen myöhästyminen |
| `stop_count` | Tarkasteltujen pysähdysten kokonaismäärä |
| `first_date` / `last_date` | Analysoitu aikaväli |

### `gold_daily_punctuality` — Päivittäinen trendi

Vastaa kysymykseen: *"Miten täsmällisyys on kehittynyt viime viikkoina?"*

```sql
SELECT departure_date, punctuality_pct, total_trains, avg_delay_minutes
FROM gold_daily_punctuality
ORDER BY departure_date;
```

| Sarake | Kuvaus |
|--------|--------|
| `departure_date` | Päivämäärä |
| `day_name` | Viikonpäivä englanniksi |
| `is_weekend` | TRUE = viikonloppu |
| `punctuality_pct` | Päivän täsmällisyysprosentti |
| `total_trains` | Päivän junamäärä |
| `avg_delay_minutes` | Keskimääräinen myöhästyminen |

## Täsmällisyyden määritelmä

> Juna katsotaan **myöhässä olevaksi**, jos se saapuu asemalle  
> **yli 3 minuuttia** myöhemmin kuin aikataulu.  
> (VR:n virallinen täsmällisyyskriteeri kaukoliikenteessä.)

Lähiliikenteessä raja on tiukempi (alle 3 min), mutta tässä analyysissa
käytetään yhtenäisesti 3 min rajaa vertailukelpoisuuden vuoksi.
