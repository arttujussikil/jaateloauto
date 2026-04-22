"""VR Täsmällisyys — Streamlit-dashboard"""

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from folium.plugins import MarkerCluster
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


@st.cache_data(ttl=86400)
def load_station_names(_conn) -> dict:
    df = _conn.execute("""
        SELECT DISTINCT station_code, station_name
        FROM gold.gold_station_punctuality
    """).df()
    return dict(zip(df["station_code"], df["station_name"]))


def load_live_trains(station_names: dict) -> pd.DataFrame:
    try:
        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        loc_resp = requests.get(LIVE_LOCATIONS_API, timeout=10, headers={"Accept-Encoding": "gzip"})
        loc_resp.raise_for_status()
        train_resp = requests.get(LIVE_TRAINS_API.format(date=today), timeout=15, headers={"Accept-Encoding": "gzip"})
        train_resp.raise_for_status()

        train_meta = {}
        for t in train_resp.json():
            tt_rows = t.get("timeTableRows", [])
            origin_code = tt_rows[0]["stationShortCode"] if tt_rows else ""
            last = tt_rows[-1] if tt_rows else {}
            dest_code = last.get("stationShortCode", "")
            sched = last.get("scheduledTime", "")
            if sched:
                sched = pd.Timestamp(sched).tz_convert("Europe/Helsinki").strftime("%H:%M")
            train_meta[t["trainNumber"]] = {
                "type": t.get("trainType", ""),
                "origin": station_names.get(origin_code, origin_code),
                "destination": station_names.get(dest_code, dest_code),
                "arrival": sched,
            }

        rows = []
        for t in loc_resp.json():
            num = t.get("trainNumber")
            coords = t.get("location", {}).get("coordinates", [None, None])
            meta = train_meta.get(num, {})
            train_type = meta.get("type", "")
            label = f"{train_type}{num}" if train_type else str(num)
            rows.append({
                "Juna": label,
                "Nopeus (km/h)": t.get("speed"),
                "Lähtöasema": meta.get("origin", ""),
                "Määränpää": meta.get("destination", ""),
                "Saapuu": meta.get("arrival", ""),
                "lat": coords[1],
                "lon": coords[0],
            })
        return pd.DataFrame(rows).dropna(subset=["lat", "lon"])
    except Exception as e:
        st.error(f"Virhe haettaessa live-dataa: {e}")
        return pd.DataFrame()


# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🚆 VR Täsmällisyys")
    st.divider()
    late_threshold = st.slider("Myöhässä-raja (min)", 1, 10, 3)
    min_stops = st.slider("Min. pysähdyksiä asemalla", 10, 200, 50)
    top_n = st.slider("Top/Bottom N asemaa", 5, 20, 10)
    st.divider()
    st.caption("Datalähde: VR Avoin data")

conn = get_conn()

# ── Datat ────────────────────────────────────────────────────────────────────

daily_df = load_daily(conn, late_threshold)
station_df = load_stations(conn, min_stops)
delay_df = load_delays(conn)
station_names = load_station_names(conn)

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

# ── Välilehdet ────────────────────────────────────────────────────────────────

tab_map, tab_daily, tab_stations, tab_dist, tab_station, tab_live = st.tabs([
    "🗺️ Kartta", "📅 Päivittäinen", "🏢 Asemat", "📊 Jakauma", "🔍 Asema-analyysi", "🔴 Live"
])


# ── Kartta ────────────────────────────────────────────────────────────────────

with tab_map:
    st.subheader("Asemien täsmällisyys kartalla")
    st.caption("Ympyrän koko = pysähdysten määrä · väri = täsmällisyysprosentti")

    fig_map = px.scatter_mapbox(
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
        mapbox_style="carto-positron",
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
    st.caption("Selittääkö suurempi junamäärä perjantain myöhästymiset?")

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
    st.subheader(f" {top_n} täsmällisintä ja vähiten täsmällisintä asemaa")

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
            title=f"✅ {top_n} Eniten täsmällisintä",
        )
        fig_best.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_best.update_layout(coloraxis_showscale=False, height=400,
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
            title=f"❌ {top_n} Vähiten täsmällisintä",
        )
        fig_worst.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_worst.update_layout(coloraxis_showscale=False, height=400,
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
        for _, row in station_df.iterrows()
    }
    selected_label = st.selectbox("Valitse asema", list(station_options.keys()))
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

    col_btn, col_filter = st.columns([1, 3])
    with col_btn:
        refresh = st.button("🔄 Päivitä sijainnit")
    with col_filter:
        hide_stationary = st.checkbox("Piilota pysähtyneet junat (0 km/h)", value=True)

    if refresh:
        st.session_state.live_df = load_live_trains(station_names)
        st.session_state.live_fetched_at = pd.Timestamp.now()

    live_df = st.session_state.live_df

    if live_df.empty:
        st.info("Paina **Päivitä sijainnit** ladataksesi junat kartalle.")
    else:
        display_df = live_df[live_df["Nopeus (km/h)"] > 0] if hide_stationary else live_df
        fetched_at = st.session_state.live_fetched_at.strftime("%H:%M:%S")
        st.caption(f"{len(display_df)} junaa kartalla · haettu {fetched_at}")

        def speed_color(kmh):
            if kmh is None or kmh < 60:
                return "blue"
            if kmh < 140:
                return "green"
            return "red"

        m = folium.Map(location=[64.5, 26.0], zoom_start=5, tiles="CartoDB positron")
        mc = MarkerCluster(
            options={"zoomToBoundsOnClick": True, "maxClusterRadius": 50}
        )
        for _, row in display_df.iterrows():
            popup_html = (
                f"<b>{row['Juna']}</b><br>"
                f"Nopeus: {row['Nopeus (km/h)']} km/h<br>"
                f"Lähtöasema: {row['Lähtöasema']}<br>"
                f"Määränpää: {row['Määränpää']} (saa. {row['Saapuu']})"
            )
            folium.Marker(
                location=[row["lat"], row["lon"]],
                icon=folium.Icon(icon="train", prefix="fa", color=speed_color(row["Nopeus (km/h)"])),
                popup=folium.Popup(popup_html, max_width=220),
                tooltip=row["Juna"],
            ).add_to(mc)
        mc.add_to(m)
        st_folium(m, height=640, use_container_width=True)


with tab_live:
    live_tab_content()
