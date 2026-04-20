# Demo-käsikirjoitus — VR Rautatieliikenne -tietoalusta

**Kohderyhmä:** Sekayleisö — business-osaajia, data-engineerejä, kehittäjiä, product ownereita  
**Kesto:** 10 min (video) / 15 min (lähiopetus)  
**Esitysväline:** README + live-koodi + Jupyter Notebook (ei PowerPointia tarvita)

---

## Avaus (1 min)

> "Oletetaan, että olet töissä logistiikkayrityksessä. Tiimisi haluaa tietää:
> **kuinka täsmällisesti junat pysähtyvät Helsingin asemalla maanantaisin?**
> Tähän kysymykseen pitäisi pystyä vastaamaan SQL:llä — ilman, että kenenkään
> tarvitsee kaivaa raaka-API-dataa käsin.
> Tänään näytän, miten rakensimme putken, joka tekee sen automaattisesti."

---

## 1. Projektin tavoite (1 min)

Näytä `README.md` selaimessa tai terminaalissa.

- Datalähde: Digitrafficin avoin rajapinta (rata.digitraffic.fi)
- Kysymykset joihin vastataan: täsmällisyys asemittain, päivittäin, viikonpäivittäin
- Lisenssi: CC BY 4.0 — vapaa kaupallinen käyttö

---

## 2. Arkkitehtuuri (2 min)

Näytä hakemistorakenne (`ls` tai tiedostonhallinta):

```
01_fetch → 02_staging → 03_bronze → 04_silver → 05_gold → 07_visualisation
```

Selitä lyhyesti jokaisesta:
- **Fetch:** Python hakee JSON:ia API:sta
- **Staging:** Raaka data sellaisenaan, 14 pv TTL
- **Bronze:** Parquet-muunto Polarsilla — nopea, ei muutoksia dataan
- **Silver:** DuckDB, tähtimalli — tässä syntyy `fact_train_stops`
- **Gold:** dbt laskee aggregaatit — tästä loppukäyttäjä kyselee

> "Tärkeää: jokainen kerros on tarkoituksella oma vaiheensa.
> Jos API muuttuu, korjataan vain fetch. Jos bisneslogiikka muuttuu,
> korjataan vain gold. Ei tarvitse ajaa kaikkea uudelleen."

---

## 3. Live-demo: dataputki (3 min)

### 3a. Fetch (30 s)
```bash
python 01_fetch/fetch_trains.py --days-back 3
ls 02_staging/data/
```
> "Tähän kansioon ilmestyy JSON-tiedosto per päivä. Raaka API-vastaus,
> ei muunnoksia."

### 3b. Bronze (30 s)
```bash
python 03_bronze/bronze.py --all
ls 03_bronze/data/
```
> "Parquet on JSON:ia ~10x pienempi ja paljon nopeampi lukea."

### 3c. Silver (30 s)
```bash
python 04_silver/silver.py
```

### 3d. dbt run + SQL-kysely (1,5 min)
```bash
cd 06_transform
dbt run
dbt test
```

Avaa DuckDB ja näytä SQL — **tämä on kohta joka puhuttelee SQL-osaajia:**
```sql
-- Epätäsmällisimmät asemat
SELECT station_name, punctuality_pct, avg_delay_minutes, stop_count
FROM gold.gold_station_punctuality
WHERE stop_count > 100
ORDER BY punctuality_pct ASC
LIMIT 5;
```

> "Huomaatte, että tulos on pelkkää SQL:ää — ei Python-koodia, ei Pandasia.
> Kuka tahansa teidän tiimistä, joka osaa SQL:ää, pystyy kyselemään tästä."

---

## 4. Visualisointi (2 min)

Avaa `07_visualisation/analysis.ipynb` Jupyterissa.

1. Näytä **päivittäinen täsmällisyyskaavio** — selitä mikä on VR:n 3 min raja
2. Näytä **dropdown-widget** — valitse asema, näytä päivittäinen trendi
   > "Loppukäyttäjä voi itse valita aseman pudotusvalikosta. Ei vaadi Python-osaamista."

---

## 5. Haasteet ja löydökset (1 min)

**Tekniset haasteet:**
- Digitraffic-User -otsikko pakollinen → dokumentoitu `.env.example`iin
- `actualTime` on NULL tulevilla pysähdyksillä → käsitelty silver-kerroksessa

**Havaintoja datasta (muuta omien löydöstesi mukaan):**
- Viikonloput yleensä täsmällisempiä kuin arkipäivät
- Suurimpien kaupunkien päätermit eivät ole välttämättä täsmällisimpiä
- Myöhästymiset kertautuvat reitillä: jos lähtöasemalla myöhässä, koko reitti myöhässä

---

## 6. Tulevat parannukset (1 min)

- Reaaliaikainen syöte (MQTT-websocket Digitrafficista)
- Häiriösyy-analyysi (API:ssa on `causes`-kenttä myöhästymisille)
- Karttavisualisointi asemista (koordinaatit löytyy `dim_stations`-taulusta)
- Automaattinen ajastus (cron tai Airflow)
- dbt docs julkaistuna sisäiseen wikiin

---

## Lopetus

> "Kaikki koodi on GitHubissa, README:ssä on asennusohjeet.
> Jos haluatte kokeilla itse tai lisätä uuden data-lähteen,
> ottakaa yhteyttä. Kysymyksiä?"

---

## Muistilista ennen demoa

- [ ] Aja putki läpi kerran etukäteen (`python run_pipeline.py`)
- [ ] Varmista, että DuckDB-tietokannassa on dataa (`cd 06_transform && dbt run`)
- [ ] Testaa Jupyter Notebook toimii (`jupyter notebook 07_visualisation/analysis.ipynb`)
- [ ] Pidä terminaali auki hakemistossa `vr-data-platform/`
