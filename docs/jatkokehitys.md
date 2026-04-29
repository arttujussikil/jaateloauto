# Jatkokehitys — haasteet, rajoitukset ja mahdollisuudet

## Tunnetut haasteet ja ratkaisut

### `actualTime` on null tulevilla pysähdyksillä

Digitraffic palauttaa kaikki junan pysähdykset yhdessä vastauksessa — sekä jo toteutuneet että tulevat. Tuleville pysähdyksille `actualTime` on `null`. Aluksi nämä nullit korvattiin nolilla, mikä oli väärin: nolla tulkittiin täsmälleen ajoissa olevaksi. Oikea ratkaisu on pitää `null` nullina ja käyttää `is_late = NULL` merkitsemään "ei vielä tiedossa".

### Duplikaattirivit

Sama asemapysähdys voi esiintyä kahdesti: ensin ennusteena ja myöhemmin toteutumana. Bronze-vaiheessa otetaan aina se rivi, jossa `actualTime` on olemassa — jos molemmat ovat null, otetaan myöhäisempi.

### `Digitraffic-User`-otsikko

1.12.2024 alkaen Digitraffic hylkää pyynnöt ilman tätä otsikkoa (palauttaa 429). Vaatimus ei ole kovin näkyvästi dokumentoitu. Otsikko asetetaan automaattisesti `.env`-tiedoston arvosta.

### Rate limiting

API palauttaa 429 jos pyyntöjä tulee liikaa. Fetch-skripti odottaa 0,5 s pyyntöjen välissä ja yrittää automaattisesti uudelleen (3 kertaa, odotukset 5 s → 10 s → 15 s). Isolla `--days-back`-arvolla (>30) kannattaa ajaa yön yli.

### DuckDB:n kirjoituslukko

DuckDB-tiedostoa voi kirjoittaa vain yksi prosessi kerrallaan. Jos Streamlit pyörii taustalla ja yrittää samanaikaisesti ajaa putken, silver-vaihe kaatuu lukkovirheeseen. Ratkaisu: sammuta Streamlit ennen putken ajoa tai muuta DuckDB read-only-tilaan visualisointia varten.

---

## Nykyiset rajoitukset

| Rajoitus | Syy | Kiertotie |
|----------|-----|-----------|
| Data vanhenee 14 päivän kuluttua | Staging TTL poistaa vanhat tiedostot | Kasvata `STAGING_TTL_DAYS` tai arkistoi Parquet-tiedostot |
| Putki ajetaan käsin | Ei ajastusta | Ks. jatkokehitys alla |
| Live-kartta ei päivity automaattisesti | Streamlit ei tue natiivisti push-päivityksiä | MQTT-integraatio tai `st.rerun(interval=...)` |
| Vain saapumisdata | Digitrafficin `/trains/`-rajapinta palauttaa kaikki pysähdykset, mutta myöhästyminen lasketaan vain toteutuneista | — |
| Ei lähiliikenteen erottelua | Kaukoliikenne ja lähiliikenne sekoitettu | Suodata `train_category`-sarakkeella |
| Ei historiaa ennen käyttöönottoa | Putki hakee vain eteenpäin | Digitraffic säilyttää dataa, voi hakea retroaktiivisesti |

---

## Jatkokehitysmahdollisuudet

### 1. Automaattinen ajastus

Putki pyörii tällä hetkellä käsin. Vaihtoehdot:

- **Windows Task Scheduler / cron** — yksinkertaisin: `python run_pipeline.py` kerran vuorokaudessa
- **Prefect / Airflow** — jos putki kasvaa tai tarvitaan uudelleenajologiikkaa ja seurantaa
- **GitHub Actions** — ilmainen vaihtoehto jos repo on GitHubissa; scheduled workflow ajaa putken ja commitoi tulokset

### 2. Reaaliaikainen MQTT-syöte

Digitraffic tarjoaa MQTT-websocket-rajapinnan (`rata.digitraffic.fi/mqtt`), joka lähettää sijaintipäivitykset automaattisesti. Tämä korvaisi Live-välilehden manuaalisen napinpainalluksen ja mahdollistaisi kartan päivittymisen sekunteina.

Tekninen vaatimus: websocket-yhteys Streamlit-sessiossa tai erillinen taustapalvelu joka kirjoittaa sijainteja tietokantaan.

### 3. Häiriöanalyysi syykoodien pohjalta

Digitraffic palauttaa häiriöille syykoodit (`causeCode`, `categoryCode`). Tällä hetkellä ne näytetään raakoina koodeina häiriötaulukossa. Jatkokehityksenä:

- Lisää koodisto `dim_cause`-dimensiotauluun
- Gold-tasolle aggregaatti: mitkä syyt aiheuttavat eniten myöhästymisiä, mihin kellonaikaan, millä reiteillä

### 4. Reitti- ja linja-analyysi

Tällä hetkellä analyysi on asemakeskeinen. Lisäarvo tulisi reittikohtaisesta analyysista:

- IC-linja Helsinki–Oulu: missä kohtaa reittiä myöhästyminen tyypillisesti alkaa?
- Myöhästyminen kertautuu reitillä — missä pisteessä se on suurimmillaan?

Vaatii `stop_sequence`-sarakkeen lisäämisen silver-tauluun (tieto löytyy Digitrafficin datasta).

### 5. Säädatan korrelaatio

Ilmatieteen laitos tarjoaa avoimen API:n (`opendata.fmi.fi`). Yhdistämällä junadata ja säädata voisi testata hypoteesia: vaikuttaako pakkas, lumi tai sade täsmällisyyteen, ja millä asemilla eniten.

### 6. Vertailu VR:n virallisiin tilastoihin

VR julkaisee kuukausittaiset täsmällisyystilastot. Vertailu tähän järjestelmään laskemaan samoihin lukuihin auttaisi validoimaan putken oikeellisuuden ja paljastamaan mahdolliset erot määritelmissä.

### 7. Docker-kontainerisointi

Tällä hetkellä asennus vaatii `uv`-työkalun ja Python-ympäristön. Docker-imagen avulla putki ja Streamlit käynnistyisivät yhdellä komennolla ilman asennusvaiheita.

```dockerfile
# Hahmotus — ei vielä toteutettu
FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install uv && uv sync
CMD ["python", "run_pipeline.py", "--visualise"]
```

### 8. Datan laadun laajennettu seuranta

dbt testaa tällä hetkellä perustasot (unique, not_null, accepted_values). Lisäarvo tulisi liiketoimintalogiikan testeistä:

- Myöhästyminen ei voi olla alle −60 min tai yli 600 min
- Jokaisen `fact_train_stops`-rivin pitää löytyä `dim_stations`-taulusta
- Päivittäinen junamäärä ei saa pudota alle järkevän alarajan (viittaisi hakuvirheeseenseen)

---

## Tekninen velka

| Asia | Kuvaus |
|------|--------|
| Silver-kerros Pythonissa | `04_silver/silver.py` rakentaa tähtimallin suoraan Pythonilla. Pidempiaikaisessa kehityksessä tämä kannattaisi siirtää dbt:hen yhtenäisyyden vuoksi. |
| Ei inkrementaalista silver-ajoa | Silver ajaa koko tietokannan uudelleen joka kerta. Isoilla datamäärillä tämä hidastuu; dbt:n inkrementaalimallit ratkaisevat ongelman. |
| Streamlit-tila ei persistoi | Sivupalkissa tehdyt valinnat (asema, päiväväli) nollautuvat sivun päivityksessä. `st.session_state` auttaisi. |
| Testikattavuus | Bronze ja fetch on testattu; silver, gold ja visualisointi ei. |
