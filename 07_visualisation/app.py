"""VR Täsmällisyys — Streamlit-dashboard"""

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium

import requests
import streamlit as st
from streamlit_folium import st_folium
from pathlib import Path

DUCKDB_PATH = Path(__file__).parent.parent / "04_silver" / "vr_warehouse.duckdb"

st.set_page_config(
    page_title="VR Täsmällisyys",
    page_icon="🚆",
    layout="wide",
)


@st.cache_resource
def get_conn():
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


@st.cache_data(ttl=300)
def load_daily(_conn, late_threshold: int) -> pd.DataFrame:
    return _conn.execute("""
        SELECT departure_date, punctuality_pct, avg_delay_minutes,
               total_trains, day_name, is_weekend
        FROM gold.gold_daily_punctuality
        ORDER BY departure_date
    """).df()


@st.cache_data(ttl=300)
def load_stations(_conn, min_stops: int) -> pd.DataFrame:
    return _conn.execute(f"""
        SELECT station_name, station_code, punctuality_pct,
               avg_delay_minutes, stop_count, max_delay_minutes,
               median_delay_minutes, latitude, longitude
        FROM gold.gold_station_punctuality
        WHERE stop_count >= {min_stops}
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY punctuality_pct DESC
    """).df()


@st.cache_data(ttl=300)
def load_delays(_conn) -> pd.DataFrame:
    return _conn.execute("""
        SELECT difference_minutes
        FROM silver.fact_train_stops
        WHERE stop_type = 'ARRIVAL'
          AND actual_time IS NOT NULL
          AND difference_minutes IS NOT NULL
          AND difference_minutes BETWEEN -15 AND 90
    """).df()


@st.cache_data(ttl=300)
def load_station_daily(_conn, station_code: str) -> pd.DataFrame:
    return _conn.execute(f"""
        SELECT
            departure_date,
            ROUND(AVG(CASE WHEN is_late = FALSE THEN 100.0 ELSE 0 END), 1) AS punctuality_pct,
            COUNT(*) AS stops,
            ROUND(AVG(CASE WHEN difference_minutes > 0 THEN difference_minutes END), 1) AS avg_delay
        FROM silver.fact_train_stops
        WHERE station_code = '{station_code}'
          AND stop_type = 'ARRIVAL'
          AND actual_time IS NOT NULL
          AND commercial_stop = TRUE
        GROUP BY departure_date
        ORDER BY departure_date
    """).df()


LIVE_LOCATIONS_API = "https://rata.digitraffic.fi/api/v1/train-locations/latest/"
LIVE_TRAINS_API = "https://rata.digitraffic.fi/api/v1/trains/{date}"
CAUSE_CODES_API = "https://rata.digitraffic.fi/api/v1/metadata/cause-category-codes"
DETAILED_CAUSE_CODES_API = "https://rata.digitraffic.fi/api/v1/metadata/detailed-cause-category-codes"
STATION_META_API = "https://rata.digitraffic.fi/api/v1/metadata/stations"
DISRUPTION_MIN_DELAY = 10  # minuuttia — tätä myöhäisempi juna näytetään häiriönä
STOP_TRAIN_TYPES = {"IC", "S", "P", "MUS", "AE"}  # junatyypit joille piirretään pysäkit

_DT_HEADERS = {"Accept-Encoding": "gzip", "Digitraffic-User": "VR-Tasmallisyys-Dashboard/1.0"}


@st.cache_data(ttl=86400)
def load_cause_codes() -> tuple[dict, dict]:
    """Palauttaa (kategoria_map, tarkka_map) koodeista nimiin."""
    try:
        cat = requests.get(CAUSE_CODES_API, timeout=10, headers=_DT_HEADERS).json()
        det = requests.get(DETAILED_CAUSE_CODES_API, timeout=10, headers=_DT_HEADERS).json()
        cat_map = {c["categoryCode"]: c["categoryName"] for c in cat}
        det_map = {c["detailedCategoryCode"]: c["detailedCategoryName"] for c in det}
        return cat_map, det_map
    except Exception:
        return {}, {}


@st.cache_data(ttl=120)
def load_disruptions(cat_map: dict, det_map: dict) -> tuple[pd.DataFrame, str]:
    """Hae tänään merkittävästi myöhässä olevat junat Digitrafficista.

    Palauttaa (df, tila) missä tila on 'ok', 'warning' tai 'alert'.
    """
    try:
        today = pd.Timestamp.now(tz="Europe/Helsinki").strftime("%Y-%m-%d")
        resp = requests.get(
            LIVE_TRAINS_API.format(date=today),
            timeout=15,
            headers=_DT_HEADERS,
        )
        resp.raise_for_status()

        delayed = []
        for train in resp.json():
            train_type = train.get("trainType", "")
            train_num = train.get("trainNumber")
            label = f"{train_type}{train_num}"
            rows = train.get("timeTableRows", [])
            if not rows:
                continue

            origin_code = rows[0].get("stationShortCode", "")
            dest_code = rows[-1].get("stationShortCode", "")

            # Skip trains that have already completed their journey
            if rows[-1].get("actualTime"):
                continue

            # Skip trains that haven't departed yet (no row with actualTime at all)
            if not any(r.get("actualTime") for r in rows):
                continue

            max_diff = 0
            worst_station = ""
            cause_name = ""

            for row in rows:
                if not row.get("actualTime"):
                    continue
                diff = row.get("differenceInMinutes") or 0
                if diff > max_diff:
                    max_diff = diff
                    worst_station = row.get("stationShortCode", "")
                    causes = row.get("causes", [])
                    if causes:
                        c = causes[0]
                        det_code = c.get("detailedCategoryCode", "")
                        cat_code = c.get("categoryCode", "")
                        cause_name = det_map.get(det_code) or cat_map.get(cat_code, "")

            if max_diff >= DISRUPTION_MIN_DELAY:
                delayed.append({
                    "Juna": label,
                    "Myöhässä (min)": max_diff,
                    "Syy": cause_name,
                    "Viimeksi mitattu": worst_station,
                    "Lähtö": origin_code,
                    "Määränpää": dest_code,
                })

        if not delayed:
            return pd.DataFrame(), "ok"

        df = (
            pd.DataFrame(delayed)
            .sort_values("Myöhässä (min)", ascending=False)
            .reset_index(drop=True)
        )
        severe = int((df["Myöhässä (min)"] >= 30).sum())
        status = "alert" if severe >= 3 else "warning"
        return df, status

    except Exception:
        return pd.DataFrame(), "unknown"


@st.cache_data(ttl=86400)
def load_station_coords() -> dict:
    """Palauttaa dict stationShortCode → (lat, lon) Digitrafficin metadata-rajapinnasta."""
    try:
        resp = requests.get(STATION_META_API, timeout=10, headers=_DT_HEADERS)
        resp.raise_for_status()
        return {
            s["stationShortCode"]: (s["latitude"], s["longitude"])
            for s in resp.json()
            if s.get("latitude") and s.get("longitude")
        }
    except Exception:
        return {}


@st.cache_data(ttl=86400)
def load_station_names(_conn) -> dict:
    df = _conn.execute("""
        SELECT DISTINCT station_code, station_name
        FROM gold.gold_station_punctuality
    """).df()
    return dict(zip(df["station_code"], df["station_name"]))


def load_live_trains(station_names: dict, station_coords: dict) -> tuple[pd.DataFrame, dict]:
    """Palauttaa (live_df, stops_dict).

    stops_dict: train_label → lista pysäkeistä
    {code, name, lat, lon, scheduled, actual, diff, is_past}
    Rakennetaan vain STOP_TRAIN_TYPES-junatyypeille.
    """
    try:
        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        loc_resp = requests.get(LIVE_LOCATIONS_API, timeout=10, headers=_DT_HEADERS)
        loc_resp.raise_for_status()
        train_resp = requests.get(LIVE_TRAINS_API.format(date=today), timeout=15, headers=_DT_HEADERS)
        train_resp.raise_for_status()

        train_meta = {}
        stops_dict = {}

        for t in train_resp.json():
            tt_rows = t.get("timeTableRows", [])
            train_type = t.get("trainType", "")
            train_num = t.get("trainNumber")
            label = f"{train_type}{train_num}" if train_type else str(train_num)

            origin_code = tt_rows[0]["stationShortCode"] if tt_rows else ""
            last = tt_rows[-1] if tt_rows else {}
            dest_code = last.get("stationShortCode", "")
            sched = last.get("scheduledTime", "")
            if sched:
                sched = pd.Timestamp(sched).tz_convert("Europe/Helsinki").strftime("%H:%M")
            train_meta[train_num] = {
                "type": train_type,
                "label": label,
                "origin": station_names.get(origin_code, origin_code),
                "destination": station_names.get(dest_code, dest_code),
                "arrival": sched,
            }

            if train_type in STOP_TRAIN_TYPES:
                seen = set()
                stops = []
                for row in tt_rows:
                    if not row.get("commercialStop"):
                        continue
                    code = row["stationShortCode"]
                    if code in seen:
                        continue
                    seen.add(code)
                    coords = station_coords.get(code)
                    if not coords:
                        continue
                    sched_t = row.get("scheduledTime", "")
                    if sched_t:
                        sched_t = pd.Timestamp(sched_t).tz_convert("Europe/Helsinki").strftime("%H:%M")
                    actual_t = row.get("actualTime")
                    if actual_t:
                        actual_t = pd.Timestamp(actual_t).tz_convert("Europe/Helsinki").strftime("%H:%M")
                    stops.append({
                        "code": code,
                        "name": station_names.get(code, code),
                        "lat": coords[0],
                        "lon": coords[1],
                        "scheduled": sched_t,
                        "actual": actual_t,
                        "diff": row.get("differenceInMinutes") or 0,
                        "is_past": bool(actual_t),
                    })
                if stops:
                    stops_dict[label] = stops

        rows = []
        for t in loc_resp.json():
            num = t.get("trainNumber")
            coords = t.get("location", {}).get("coordinates", [None, None])
            meta = train_meta.get(num, {})
            train_type = meta.get("type", "")
            label = meta.get("label") or (f"{train_type}{num}" if train_type else str(num))
            rows.append({
                "Juna": label,
                "Nopeus (km/h)": t.get("speed"),
                "Lähtöasema": meta.get("origin", ""),
                "Määränpää": meta.get("destination", ""),
                "Saapuu": meta.get("arrival", ""),
                "lat": coords[1],
                "lon": coords[0],
            })

        df = pd.DataFrame(rows).dropna(subset=["lat", "lon"])
        return df, stops_dict
    except Exception as e:
        st.error(f"Virhe haettaessa live-dataa: {e}")
        return pd.DataFrame(), {}


# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🚆 VR Täsmällisyys")
    st.divider()
    late_threshold = st.slider("Myöhässä-raja (min)", 1, 10, 3)
    min_stops = st.slider("Min. pysähdyksiä asemalla", 10, 200, 50)
    top_n = st.slider("Näytettävien asemien määrä", 5, 20, 10)
    st.divider()
    st.caption("Datalähde: VR Avoin data")

conn = get_conn()

# ── Datat ────────────────────────────────────────────────────────────────────

daily_df = load_daily(conn, late_threshold)
station_df = load_stations(conn, min_stops)
delay_df = load_delays(conn)
station_names = load_station_names(conn)
station_coords = load_station_coords()

# ── KPI-kortit ───────────────────────────────────────────────────────────────

st.header("VR Junien täsmällisyys", divider="gray")

col1, col2, col3, col4 = st.columns(4)
avg_pct = daily_df["punctuality_pct"].mean()
avg_delay = daily_df["avg_delay_minutes"].mean()
total_trains = daily_df["total_trains"].sum()
days = len(daily_df)

col1.metric("Keskimääräinen täsmällisyys", f"{avg_pct:.1f}%",
            delta=f"{avg_pct - 90:.1f}% vs. tavoite 90%",
            delta_color="normal")
col2.metric("Keskim. myöhästyminen", f"{avg_delay:.1f} min")
col3.metric("Junavuoroja yhteensä", f"{total_trains:,}")
col4.metric("Päiviä datassa", str(days))

st.divider()

# ── VR Poikkeustilanteet ──────────────────────────────────────────────────────

cat_map, det_map = load_cause_codes()
disrupt_df, disrupt_status = load_disruptions(cat_map, det_map)

_STATUS_CONFIG = {
    "ok":      ("🟢", "Liikenne sujuu normaalisti", "success"),
    "warning": ("🟡", "Joitakin myöhästymisiä liikenteessä", "warning"),
    "alert":   ("🔴", "Merkittäviä häiriöitä liikenteessä", "error"),
    "unknown": ("⚪", "Häiriötietoja ei saatu haettua", "info"),
}
icon, msg, alert_type = _STATUS_CONFIG[disrupt_status]

with st.expander(f"{icon} Liikennetilanne nyt — {msg}", expanded=(disrupt_status == "alert")):
    st.caption("Lähde: Digitraffic / Fintraffic · Päivittyy 2 min välein · Vain tällä hetkellä ajavat junat · Jos juna puuttuu Live-kartalta, se ei raportoi GPS-sijaintiaan")
    if disrupt_df.empty:
        if disrupt_status == "ok":
            st.success("Ei merkittäviä myöhästymisiä tällä hetkellä.")
        else:
            st.info("Tietoja ei saatavilla.")
    else:
        severe_count = int((disrupt_df["Myöhässä (min)"] >= 30).sum())
        mild_count = len(disrupt_df) - severe_count
        c1, c2 = st.columns(2)
        c1.metric("Vakavasti myöhässä (≥30 min)", severe_count)
        c2.metric("Lievästi myöhässä (10–29 min)", mild_count)

        display = disrupt_df.copy()
        display["Viimeksi mitattu"] = display["Viimeksi mitattu"].map(
            lambda c: station_names.get(c, c)
        )
        display["Lähtö"] = display["Lähtö"].map(lambda c: station_names.get(c, c))
        display["Määränpää"] = display["Määränpää"].map(lambda c: station_names.get(c, c))

        row_height = 35
        header_height = 38
        max_height = 400
        table_height = min(max_height, header_height + row_height * len(display))

        event = st.dataframe(
            display,
            hide_index=True,
            width="stretch",
            height=table_height,
            selection_mode="single-row",
            on_select="rerun",
            column_config={
                "Myöhässä (min)": st.column_config.ProgressColumn(
                    "Myöhässä (min)", min_value=0, max_value=120, format="%d min"
                )
            },
        )

        if event.selection.rows:
            sel_idx = event.selection.rows[0]
            sel_train = disrupt_df.iloc[sel_idx]["Juna"]
            st.session_state.highlighted_train = sel_train
            st.info(f"Juna **{sel_train}** valittu — avaa **Live**-välilehti nähdäksesi sijainnin kartalla.")

st.divider()

# ── Välilehdet ────────────────────────────────────────────────────────────────

tab_map, tab_daily, tab_stations, tab_dist, tab_station, tab_live = st.tabs([
    "🗺️ Kartta", "📅 Päivittäinen", "🏢 Asemat", "📊 Jakauma", "🔍 Asema-analyysi", "🔴 Live"
])


# ── Kartta ────────────────────────────────────────────────────────────────────

with tab_map:
    st.subheader("Asemien täsmällisyys kartalla")
    st.caption("Ympyrän koko = pysähdysten määrä · väri = täsmällisyysprosentti")

    fig_map = px.scatter_map(
        station_df,
        lat="latitude",
        lon="longitude",
        color="punctuality_pct",
        size="stop_count",
        hover_name="station_name",
        hover_data={
            "punctuality_pct": ":.1f",
            "avg_delay_minutes": ":.1f",
            "stop_count": True,
            "latitude": False,
            "longitude": False,
        },
        labels={
            "punctuality_pct": "Täsmällisyys (%)",
            "avg_delay_minutes": "Keskim. myöhästyminen (min)",
            "stop_count": "Pysähdyksiä",
        },
        color_continuous_scale=[
            (0.0, "#d9534f"),
            (0.5, "#f0ad4e"),
            (0.8, "#5cb85c"),
            (1.0, "#1a7a1a"),
        ],
        range_color=[50, 100],
        size_max=25,
        zoom=5,
        center={"lat": 64.5, "lon": 26.0},
        map_style="carto-positron",
        height=620,
    )
    fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0},
                          coloraxis_colorbar_title="Täsmällisyys %")
    st.plotly_chart(fig_map, width='stretch')

    with st.expander("Näytä data-taulukko"):
        st.dataframe(
            station_df[["station_name", "punctuality_pct", "avg_delay_minutes",
                         "stop_count", "max_delay_minutes"]].rename(columns={
                "station_name": "Asema",
                "punctuality_pct": "Täsmällisyys %",
                "avg_delay_minutes": "Keskim. myöhästyminen",
                "stop_count": "Pysähdyksiä",
                "max_delay_minutes": "Suurin myöhästyminen",
            }),
            width='stretch',
            hide_index=True,
        )


# ── Päivittäinen ──────────────────────────────────────────────────────────────

with tab_daily:
    st.subheader("Päivittäinen täsmällisyys")

    fig_daily = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.6, 0.4],
        subplot_titles=("Täsmällisyysprosentti", "Keskimääräinen myöhästyminen"),
    )

    colors = ["#5bc0de" if wknd else "#d9534f" for wknd in daily_df["is_weekend"]]

    fig_daily.add_trace(
        go.Bar(
            x=daily_df["departure_date"],
            y=daily_df["punctuality_pct"],
            marker_color=colors,
            name="Täsmällisyys %",
            hovertemplate="%{x}<br>Täsmällisyys: %{y:.1f}%<extra></extra>",
        ),
        row=1, col=1,
    )
    fig_daily.add_hline(
        y=90, line_dash="dash", line_color="#2ecc71",
        annotation_text="Tavoite 90%", row=1, col=1,
    )

    fig_daily.add_trace(
        go.Scatter(
            x=daily_df["departure_date"],
            y=daily_df["avg_delay_minutes"],
            mode="lines+markers",
            line={"color": "#e67e22", "width": 2},
            fill="tozeroy",
            fillcolor="rgba(230,126,34,0.15)",
            name="Keskim. myöhästyminen",
            hovertemplate="%{x}<br>Myöhästyminen: %{y:.1f} min<extra></extra>",
        ),
        row=2, col=1,
    )

    fig_daily.update_yaxes(title_text="Täsmällisyys (%)", row=1, col=1)
    fig_daily.update_yaxes(title_text="Minuuttia", row=2, col=1)
    fig_daily.update_layout(
        height=500, showlegend=False,
        hovermode="x unified",
    )
    st.plotly_chart(fig_daily, width='stretch')

    st.caption("🔵 Viikonloppu · 🔴 Arkipäivä")

    st.subheader("Junamäärä viikonpäivittäin")
    st.caption("Vaikuttaako junamäärä täsmällisyyteen?")

    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_fi = {
        "Monday": "Maanantai", "Tuesday": "Tiistai", "Wednesday": "Keskiviikko",
        "Thursday": "Torstai", "Friday": "Perjantai", "Saturday": "Lauantai", "Sunday": "Sunnuntai",
    }

    weekday_df = (
        daily_df.groupby("day_name", as_index=False)
        .agg(total_trains=("total_trains", "sum"), avg_punctuality=("punctuality_pct", "mean"),
             is_weekend=("is_weekend", "first"))
        .assign(day_name=lambda d: pd.Categorical(d["day_name"], categories=weekday_order, ordered=True))
        .sort_values("day_name")
    )
    weekday_df["label"] = weekday_df["day_name"].map(weekday_fi)
    weekday_df["color"] = weekday_df["is_weekend"].map({True: "#5bc0de", False: "#e67e22"})

    fig_wk = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Junavuoroja yhteensä", "Täsmällisyys (%)"),
    )
    fig_wk.add_trace(
        go.Bar(
            x=weekday_df["label"], y=weekday_df["total_trains"],
            marker_color=weekday_df["color"],
            text=weekday_df["total_trains"],
            textposition="outside",
            hovertemplate="%{x}<br>Junavuoroja: %{y:,}<extra></extra>",
            name="Junavuoroja",
        ),
        row=1, col=1,
    )
    fig_wk.add_trace(
        go.Bar(
            x=weekday_df["label"], y=weekday_df["avg_punctuality"].round(1),
            marker_color=weekday_df["color"],
            text=weekday_df["avg_punctuality"].round(1),
            texttemplate="%{text:.1f}%",
            textposition="outside",
            hovertemplate="%{x}<br>Täsmällisyys: %{y:.1f}%<extra></extra>",
            name="Täsmällisyys",
        ),
        row=1, col=2,
    )
    fig_wk.add_hline(y=90, line_dash="dash", line_color="#2ecc71",
                     annotation_text="Tavoite 90%", row=1, col=2)
    fig_wk.update_yaxes(title_text="Junavuoroja", row=1, col=1)
    fig_wk.update_yaxes(title_text="Täsmällisyys (%)", range=[0, 110], row=1, col=2)
    fig_wk.update_layout(height=380, showlegend=False)
    st.plotly_chart(fig_wk, width='stretch')
    st.caption("🔵 Viikonloppu · 🟠 Arkipäivä")


# ── Asemat ────────────────────────────────────────────────────────────────────

with tab_stations:
    st.subheader(f"Parhaat ja heikoiten suoriutuvat asemat – top {top_n}")

    best = station_df.head(top_n)
    worst = station_df.tail(top_n).sort_values("punctuality_pct")

    col_best, col_worst = st.columns(2)

    with col_best:
        fig_best = px.bar(
            best,
            x="punctuality_pct",
            y="station_name",
            orientation="h",
            color="punctuality_pct",
            color_continuous_scale=["#f0ad4e", "#5cb85c", "#1a7a1a"],
            range_color=[80, 100],
            text="punctuality_pct",
            hover_data={"avg_delay_minutes": ":.1f", "stop_count": True},
            labels={
                "punctuality_pct": "Täsmällisyys %",
                "station_name": "",
                "avg_delay_minutes": "Keskim. myöhästyminen",
                "stop_count": "Pysähdyksiä",
            },
            title=f"✅ Täsmällisimmät {top_n} asemaa",
        )
        fig_best.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_best.update_layout(coloraxis_showscale=False, height=max(400, top_n * 38),
                               xaxis_range=[0, 105])
        st.plotly_chart(fig_best, width='stretch')

    with col_worst:
        fig_worst = px.bar(
            worst,
            x="punctuality_pct",
            y="station_name",
            orientation="h",
            color="punctuality_pct",
            color_continuous_scale=["#d9534f", "#f0ad4e", "#f5e642"],
            range_color=[50, 90],
            text="punctuality_pct",
            hover_data={"avg_delay_minutes": ":.1f", "stop_count": True},
            labels={
                "punctuality_pct": "Täsmällisyys %",
                "station_name": "",
                "avg_delay_minutes": "Keskim. myöhästyminen",
                "stop_count": "Pysähdyksiä",
            },
            title=f"❌ Myöhästyvimmät {top_n} asemat",
        )
        fig_worst.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_worst.update_layout(coloraxis_showscale=False, height=max(400, top_n * 38),
                                xaxis_range=[0, 105])
        st.plotly_chart(fig_worst, width='stretch')


# ── Myöhästymisjakauma ────────────────────────────────────────────────────────

with tab_dist:
    st.subheader("Myöhästymisjakauma")

    col_hist, col_cdf = st.columns(2)

    with col_hist:
        fig_hist = px.histogram(
            delay_df,
            x="difference_minutes",
            nbins=60,
            color_discrete_sequence=["#5bc0de"],
            labels={"difference_minutes": "Myöhästyminen (min)", "count": "Pysähdyksiä"},
            title="Myöhästymisjakauma",
        )
        fig_hist.add_vline(x=late_threshold, line_dash="dash", line_color="#d9534f",
                           annotation_text=f"Raja {late_threshold} min")
        fig_hist.add_vline(x=0, line_color="#555", line_width=1)
        st.plotly_chart(fig_hist, width='stretch')

    with col_cdf:
        sorted_d = delay_df["difference_minutes"].sort_values().reset_index(drop=True)
        cdf_y = (sorted_d.index + 1) / len(sorted_d) * 100
        fig_cdf = go.Figure()
        fig_cdf.add_trace(go.Scatter(
            x=sorted_d, y=cdf_y,
            mode="lines", line={"color": "#e67e22", "width": 2},
            hovertemplate="≤%{x:.1f} min: %{y:.1f}%<extra></extra>",
        ))
        fig_cdf.add_vline(x=late_threshold, line_dash="dash", line_color="#d9534f",
                          annotation_text=f"Raja {late_threshold} min")
        fig_cdf.add_vline(x=0, line_color="#555", line_width=1)
        fig_cdf.update_layout(
            title="Kumulatiivinen jakauma",
            xaxis_title="Myöhästyminen (min)",
            yaxis_title="Kumulatiivinen osuus (%)",
            yaxis_ticksuffix="%",
        )
        st.plotly_chart(fig_cdf, width='stretch')

    med = delay_df["difference_minutes"].median()
    p95 = delay_df["difference_minutes"].quantile(0.95)
    pct_late = (delay_df["difference_minutes"] > late_threshold).mean() * 100
    c1, c2, c3 = st.columns(3)
    c1.metric("Mediaanimyöhästyminen", f"{med:.1f} min")
    c2.metric("95. prosenttipiste", f"{p95:.1f} min")
    c3.metric(f"Myöhässä (>{late_threshold} min)", f"{pct_late:.1f}%")


# ── Asema-analyysi ────────────────────────────────────────────────────────────

with tab_station:
    st.subheader("Asemakohtainen analyysi")

    station_options = {
        f"{row['station_name']} ({row['station_code']})": row["station_code"]
        for _, row in station_df.sort_values("station_name").iterrows()
    }
    selected_label = st.selectbox("Valitse asema", sorted(station_options.keys()))
    selected_code = station_options[selected_label]

    sdf = load_station_daily(conn, selected_code)

    if sdf.empty:
        st.warning("Ei dataa valitulle asemalle.")
    else:
        row = station_df[station_df["station_code"] == selected_code].iloc[0]

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Täsmällisyys (kaikki)", f"{row['punctuality_pct']:.1f}%")
        m2.metric("Keskim. myöhästyminen", f"{row['avg_delay_minutes']:.1f} min")
        m3.metric("Mediaanimyöhästyminen", f"{row['median_delay_minutes']:.1f} min")
        m4.metric("Suurin myöhästyminen", f"{row['max_delay_minutes']:.0f} min")

        fig_s = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            row_heights=[0.6, 0.4],
            subplot_titles=("Täsmällisyys %", "Keskim. myöhästyminen (min)"),
        )
        fig_s.add_trace(
            go.Scatter(
                x=sdf["departure_date"], y=sdf["punctuality_pct"],
                mode="lines+markers", fill="tozeroy",
                line={"color": "#3498db", "width": 2},
                fillcolor="rgba(52,152,219,0.15)",
                name="Täsmällisyys %",
            ),
            row=1, col=1,
        )
        fig_s.add_hline(y=90, line_dash="dash", line_color="#2ecc71",
                        annotation_text="90%", row=1, col=1)
        fig_s.add_trace(
            go.Bar(
                x=sdf["departure_date"], y=sdf["avg_delay"],
                marker_color="#e67e22", name="Myöhästyminen",
            ),
            row=2, col=1,
        )
        fig_s.update_layout(height=450, showlegend=False, hovermode="x unified")
        st.plotly_chart(fig_s, width='stretch')


# ── Live-seuranta ─────────────────────────────────────────────────────────────

def live_tab_content():
    st.subheader("Junien sijaintitiedot reaaliajassa")
    st.caption("Datalähde: Digitraffic / Fintraffic · Päivitä manuaalisesti alla olevalla napilla")

    if "live_df" not in st.session_state:
        st.session_state.live_df = pd.DataFrame()
        st.session_state.live_fetched_at = None
    if "live_stops" not in st.session_state:
        st.session_state.live_stops = {}

    col_btn, col_filter, col_search = st.columns([1, 2, 3])
    with col_btn:
        refresh = st.button("🔄 Päivitä sijainnit")
    with col_filter:
        hide_stationary = st.checkbox("Piilota pysähtyneet junat (0 km/h)", value=True)

    if refresh:
        df_new, stops_new = load_live_trains(station_names, station_coords)
        st.session_state.live_df = df_new
        st.session_state.live_stops = stops_new
        st.session_state.live_fetched_at = pd.Timestamp.now()

    live_df = st.session_state.live_df
    live_stops = st.session_state.live_stops

    # Sovelletaan karttaklikkauksesta tuleva valinta ennen widgetin luontia
    if "pending_train_select" in st.session_state:
        st.session_state["live_search_select"] = st.session_state.pop("pending_train_select")

    # Selectbox hakukenttänä — Streamlit filtteroi natiivisti kun kirjoitat
    all_train_options = sorted(live_df["Juna"].tolist()) if not live_df.empty else []
    with col_search:
        chosen_train = st.selectbox(
            "Hae junaa",
            all_train_options,
            index=None,
            placeholder="Hae junaa (esim. IC, S, 123)...",
            label_visibility="collapsed",
            key="live_search_select",
        )

    # Selectbox päivittää session staten heti kun käyttäjä valitsee
    if chosen_train:
        st.session_state.highlighted_train = chosen_train

    highlighted = st.session_state.get("highlighted_train")

    if live_df.empty:
        st.info("Paina **Päivitä sijainnit** ladataksesi junat kartalle.")
    else:
        display_df = live_df[live_df["Nopeus (km/h)"] > 0] if hide_stationary else live_df

        fetched_at = st.session_state.live_fetched_at.strftime("%H:%M:%S")
        st.caption(f"{len(display_df)} junaa kartalla · haettu {fetched_at}")

        # Kartan keskipiste ja zoom
        if highlighted:
            hl_row = live_df[live_df["Juna"] == highlighted]
            if not hl_row.empty:
                map_center = [hl_row.iloc[0]["lat"], hl_row.iloc[0]["lon"]]
                map_zoom = 9
                st.success(
                    f"Korostettu juna: **{highlighted}** · "
                    f"{hl_row.iloc[0]['Lähtöasema']} → {hl_row.iloc[0]['Määränpää']}"
                )
            else:
                map_center = [64.5, 26.0]
                map_zoom = 5
                st.warning(f"Junaa **{highlighted}** ei löydy live-datasta — päivitä sijainnit.")
        else:
            map_center = [64.5, 26.0]
            map_zoom = 5

        def speed_color(kmh):
            if kmh is None or kmh < 60:
                return "blue"
            if kmh < 140:
                return "green"
            return "red"

        def stop_color(diff):
            if diff <= 0:
                return "#2ecc71"
            if diff < 10:
                return "#f0ad4e"
            return "#d9534f"

        m = folium.Map(location=map_center, zoom_start=map_zoom, tiles="CartoDB positron")

        # Reitin pysäkit korostetulle junalle
        if highlighted and highlighted in live_stops:
            stops = live_stops[highlighted]
            coords_line = [[s["lat"], s["lon"]] for s in stops]
            if len(coords_line) >= 2:
                folium.PolyLine(
                    coords_line, color="#e67e22", weight=3, opacity=0.7, dash_array="6"
                ).add_to(m)
            for stop in stops:
                sched = stop["scheduled"]
                actual = stop["actual"]
                diff = stop["diff"]
                time_str = f"{actual} ({'+' if diff > 0 else ''}{diff} min)" if actual else sched
                popup_html = (
                    f"<b>{stop['name']}</b><br>"
                    f"Aik: {sched}"
                    + (f"<br>Tot: {actual} ({'+' if diff > 0 else ''}{diff} min)" if actual else "")
                )
                folium.CircleMarker(
                    location=[stop["lat"], stop["lon"]],
                    radius=6 if stop["is_past"] else 5,
                    color=stop_color(diff) if stop["is_past"] else "#888",
                    fill=True,
                    fill_opacity=0.85,
                    popup=folium.Popup(popup_html, max_width=180),
                    tooltip=f"{stop['name']} · {time_str}",
                ).add_to(m)

        def build_train_popup(row, stops):
            base = (
                f"<b>{row['Juna']}</b><br>"
                f"Nopeus: {row['Nopeus (km/h)']} km/h<br>"
                f"{row['Lähtöasema']} → {row['Määränpää']} (saa. {row['Saapuu']})"
            )
            if not stops:
                return base
            rows_html = ""
            for s in stops:
                diff = s["diff"]
                if s["is_past"]:
                    color = "#c0392b" if diff >= 10 else "#e67e22" if diff > 0 else "#27ae60"
                    time_col = f"<span style='color:{color}'>{s['actual']} ({'+' if diff>0 else ''}{diff})</span>"
                else:
                    time_col = f"<span style='color:#888'>{s['scheduled']}</span>"
                rows_html += f"<tr><td style='padding:1px 6px'>{s['name']}</td><td>{time_col}</td></tr>"
            return (
                base
                + "<details style='margin-top:6px;cursor:pointer'>"
                + "<summary style='font-size:0.85em;color:#555'>▶ Pysäkit</summary>"
                + f"<table style='font-size:0.82em;margin-top:4px'>{rows_html}</table>"
                + "</details>"
            )

        for _, row in display_df.iterrows():
            is_highlighted = highlighted and row["Juna"] == highlighted
            train_stops = live_stops.get(row["Juna"], [])
            popup_html = build_train_popup(row, train_stops)
            if is_highlighted:
                folium.Marker(
                    location=[row["lat"], row["lon"]],
                    icon=folium.Icon(icon="star", prefix="fa", color="orange"),
                    popup=folium.Popup(popup_html, max_width=280),
                    tooltip=f"⭐ {row['Juna']}",
                ).add_to(m)
            else:
                folium.Marker(
                    location=[row["lat"], row["lon"]],
                    icon=folium.Icon(icon="train", prefix="fa", color=speed_color(row["Nopeus (km/h)"])),
                    popup=folium.Popup(popup_html, max_width=280),
                    tooltip=row["Juna"],
                ).add_to(m)
        map_result = st_folium(m, height=640, width="100%")

        # Karttaklikkauksesta korostus — tooltip sisältää junan nimen
        if map_result:
            clicked = (map_result.get("last_object_clicked_tooltip") or "").replace("⭐ ", "").strip()
            train_labels = set(live_df["Juna"])
            if clicked in train_labels and st.session_state.get("highlighted_train") != clicked:
                st.session_state.highlighted_train = clicked
                st.session_state["pending_train_select"] = clicked  # sovelletaan ennen seuraavaa renderöintiä
                st.rerun()


with tab_live:
    live_tab_content()
