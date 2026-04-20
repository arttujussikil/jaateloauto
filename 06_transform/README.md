# 06_transform — dbt-projekti

## Tarkoitus

Tämä kansio sisältää dbt (data build tool) -projektin, joka muuntaa
silver-kerroksen tähtimallin gold-aggregaateiksi ja testaa tiedon laadun.

## Mallit

### Silver-kerros (views)
| Malli | Kuvaus |
|-------|--------|
| `silver/fact_train_stops.sql` | Pysähdysfaktat rikastettuna `is_late`-kentällä |

### Gold-kerros (taulut)
| Malli | Kuvaus |
|-------|--------|
| `gold/gold_station_punctuality.sql` | Täsmällisyystilasto asemittain |
| `gold/gold_daily_punctuality.sql` | Päivittäinen täsmällisyystrendi |

## Testit (dbt test)

Automaattiset tiedon laadun testit on määritelty `models/schema.yml`-tiedostossa:

| Taulu | Sarake | Testi |
|-------|--------|-------|
| `fact_train_stops` | `stop_id` | unique, not_null |
| `fact_train_stops` | `stop_type` | accepted_values (ARRIVAL/DEPARTURE) |
| `dim_stations` | `station_code` | unique, not_null |
| `dim_trains` | `train_key` | unique, not_null |
| `gold_station_punctuality` | `station_code` | unique, not_null |
| `gold_daily_punctuality` | `departure_date` | unique, not_null |

## Käyttö

```bash
cd 06_transform

# Aja kaikki mallit
dbt run

# Aja testit
dbt test

# Generoi ja avaa dokumentaatio selaimessa
dbt docs generate
dbt docs serve   # → http://localhost:8080
```

## Yhteyden konfigurointi

Tietokantayhteys on määritelty `profiles.yml`-tiedostossa.
DuckDB-tietokannan polku: `../04_silver/vr_warehouse.duckdb`
