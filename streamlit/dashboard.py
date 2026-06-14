import os
import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd
import sqlalchemy
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

#  Configuration 

st.set_page_config(
    page_title="Flight Tracking France",
    page_icon="✈️",
    layout="wide",
)

REFRESH_INTERVAL = 60

DATABASE_URL = (
    f"snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}"
    f"@{os.getenv('SNOWFLAKE_ACCOUNT')}/{os.getenv('SNOWFLAKE_DATABASE', 'FLIGHT_TRACKING')}/RAW_MARTS"
    f"?warehouse={os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')}"
    f"&role={os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')}"
)

COLOR_MAP = {
    "CROISIERE":    "#1a73e8",
    "MONTEE":       "#34a853",
    "DECOLLAGE":    "#34a853",
    "DESCENTE":     "#ea4335",
    "ATTERRISSAGE": "#ea4335",
    "TRANSITION":   "#fbbc04",
    "SOL":          "#9e9e9e",
}


#  Connexion 

@st.cache_resource
def get_engine():
    return sqlalchemy.create_engine(DATABASE_URL)


# Données 

@st.cache_data(ttl=REFRESH_INTERVAL)
def load_positions() -> pd.DataFrame:
    """Dernière position par vol sur la dernière heure."""
    query = """
        SELECT
            dv.icao24,
            dv.callsign_principal,
            dv.compagnie_nom,
            dv.origine_pays,
            fp.latitude,
            fp.longitude,
            fp.altitude_baro_ft,
            fp.vitesse_kmh,
            fp.cap_degres,
            fp.phase_vol,
            fp.taux_montee_calcule_ms,
            fp.taux_montee_declare_ms,
            fp.flag_changement_altitude_suspect,
            fp.timestamp_utc
        FROM FACT_POSITIONS fp
        JOIN DIM_VOLS dv ON fp.vol_sk = dv.vol_sk
        WHERE fp.timestamp_utc >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
          AND fp.au_sol = FALSE
          AND fp.latitude IS NOT NULL
          AND fp.longitude IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY fp.vol_sk
            ORDER BY fp.timestamp_utc DESC
        ) = 1
        ORDER BY fp.altitude_baro_ft DESC NULLS LAST
    """
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"❌ Erreur Snowflake : {e}")
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_anomalies_24h() -> pd.DataFrame:
    """Anomalies détectées sur les dernières 24h."""
    query = """
        SELECT
            dv.callsign_principal,
            dv.compagnie_nom,
            dv.origine_pays,
            fp.timestamp_utc,
            fp.altitude_baro_ft,
            fp.vitesse_kmh,
            fp.taux_montee_calcule_ms,
            fp.taux_montee_declare_ms,
            fp.phase_vol
        FROM FACT_POSITIONS fp
        JOIN DIM_VOLS dv ON fp.vol_sk = dv.vol_sk
        WHERE fp.flag_changement_altitude_suspect = TRUE
          AND fp.timestamp_utc >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
        ORDER BY fp.timestamp_utc DESC
    """
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        return pd.DataFrame()


# Sidebar 

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.title(" Filtres")

    compagnies = ["Toutes"] + sorted(df["compagnie_nom"].dropna().unique().tolist())
    selected = st.sidebar.selectbox(" Compagnie", compagnies)
    if selected != "Toutes":
        df = df[df["compagnie_nom"] == selected]

    st.sidebar.divider()
    st.sidebar.metric("Avions filtrés", len(df))

    return df


#  KPIs 

def render_kpis(df: pd.DataFrame, df_anom: pd.DataFrame):
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(" Avions en vol", len(df))
    with col2:
        st.metric(" Compagnies", df["compagnie_nom"].nunique())
    with col3:
        avg_alt = int(df["altitude_baro_ft"].mean()) if not df.empty else 0
        st.metric(" Altitude moy.", f"{avg_alt:,} ft")
    with col4:
        avg_speed = int(df["vitesse_kmh"].mean()) if not df.empty else 0
        st.metric(" Vitesse moy.", f"{avg_speed:,} km/h")
    with col5:
        n_anom = int(df["flag_changement_altitude_suspect"].sum()) if not df.empty else 0
        st.metric(
            " Anomalies actives",
            n_anom,
            delta=f"{len(df_anom)} sur 24h" if not df_anom.empty else None,
            delta_color="inverse",
        )


#  Carte 

def build_map(df: pd.DataFrame) -> folium.Map:
    m = folium.Map(location=[46.5, 2.5], zoom_start=6, tiles="CartoDB positron")

    for _, row in df.iterrows():
        if pd.isna(row.latitude) or pd.isna(row.longitude):
            continue

        color    = COLOR_MAP.get(str(row.phase_vol).upper(), "#fbbc04")
        cap      = float(row.cap_degres) if pd.notna(row.cap_degres) else 0
        alt_ft   = int(row.altitude_baro_ft) if pd.notna(row.altitude_baro_ft) else 0
        speed    = int(row.vitesse_kmh) if pd.notna(row.vitesse_kmh) else 0
        anomalie = row.flag_changement_altitude_suspect
        callsign = row.callsign_principal or row.icao24 or "?"
        taux_info = ""
        if pd.notna(row.taux_montee_calcule_ms):
            taux_info += f"📊 Taux calculé : {row.taux_montee_calcule_ms:.1f} m/s<br>"
        if pd.notna(row.taux_montee_declare_ms):
            taux_info += f"📊 Taux déclaré : {row.taux_montee_declare_ms:.1f} m/s<br>"

        icon_html = f"""
            <div style="
                transform: rotate({cap}deg);
                font-size: 20px;
                color: {color};
                text-shadow: 0 0 4px white;
                {'outline: 2px solid red; border-radius: 50%;' if anomalie else ''}
                line-height: 1;
            ">✈</div>
        """

        popup_html = f"""
            <div style="font-family: monospace; font-size: 12px; min-width: 200px;">
                <b style="font-size: 14px;">{callsign}</b><br>
                <hr style="margin: 4px 0;">
                🏢 Compagnie : {row.compagnie_nom or '?'}<br>
                🌍 Pavillon : {row.origine_pays or '?'}<br>
                📏 Altitude : <b>{alt_ft:,} ft</b><br>
                💨 Vitesse : <b>{speed:,} km/h</b><br>
                🧭 Cap : {int(cap)}&deg;<br>
                🛫 Phase : {row.phase_vol}<br>
                {taux_info}
                {'<br>⚠️ <b style="color:red;">ANOMALIE ALTITUDE</b>' if anomalie else ''}
            </div>
        """

        folium.Marker(
            location=[row.latitude, row.longitude],
            icon=folium.DivIcon(html=icon_html, icon_size=(24, 24), icon_anchor=(12, 12)),
            popup=folium.Popup(popup_html, max_width=240),
            tooltip=f"{'⚠️ ' if anomalie else ''}{callsign} — {alt_ft:,} ft — {speed:,} km/h",
        ).add_to(m)

    return m


#  Anomalies 

def render_anomalies(df: pd.DataFrame, df_anom: pd.DataFrame):
    anomalies_actives = df[df["flag_changement_altitude_suspect"] == True]

    if anomalies_actives.empty:
        st.success(" Aucune anomalie active en ce moment")
    else:
        st.error(f"⚠️ {len(anomalies_actives)} anomalie(s) active(s) — variation d'altitude > 100 m/s")
        for _, row in anomalies_actives.iterrows():
            callsign  = row.callsign_principal or row.icao24 or "?"
            alt_ft    = int(row.altitude_baro_ft) if pd.notna(row.altitude_baro_ft) else 0
            taux_calc = f"{row.taux_montee_calcule_ms:.1f} m/s" if pd.notna(row.taux_montee_calcule_ms) else "N/A"
            taux_dec  = f"{row.taux_montee_declare_ms:.1f} m/s" if pd.notna(row.taux_montee_declare_ms) else "N/A"

            with st.expander(f"⚠️ {callsign} — {row.compagnie_nom or '?'} — {alt_ft:,} ft"):
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.metric("Altitude", f"{alt_ft:,} ft")
                    st.metric("Vitesse", f"{int(row.vitesse_kmh) if pd.notna(row.vitesse_kmh) else 0:,} km/h")
                with c2:
                    st.metric("Taux calculé", taux_calc)
                    st.metric("Taux déclaré", taux_dec)
                with c3:
                    st.metric("Phase", row.phase_vol or "?")
                    st.metric("Pays", row.origine_pays or "?")
                if pd.notna(row.taux_montee_calcule_ms) and pd.notna(row.taux_montee_declare_ms):
                    diff = abs(row.taux_montee_calcule_ms - row.taux_montee_declare_ms)
                    st.warning(f"📐 Écart : **{diff:.1f} m/s** entre taux calculé et déclaré")

    st.subheader(" Historique des anomalies — 24h")
    if df_anom.empty:
        st.info("Aucune anomalie sur les dernières 24h")
    else:
        display = df_anom.copy()
        display["Alt (ft)"]        = display["altitude_baro_ft"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "—")
        display["Vit (km/h)"]      = display["vitesse_kmh"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "—")
        display["Taux calc (m/s)"] = display["taux_montee_calcule_ms"].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "—")
        display["Taux décl (m/s)"] = display["taux_montee_declare_ms"].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "—")

        st.dataframe(
            display[[
                "callsign_principal", "compagnie_nom", "origine_pays",
                "timestamp_utc", "Alt (ft)", "Vit (km/h)",
                "Taux calc (m/s)", "Taux décl (m/s)", "phase_vol"
            ]].rename(columns={
                "callsign_principal": "Callsign",
                "compagnie_nom": "Compagnie",
                "origine_pays": "Pays",
                "timestamp_utc": "Timestamp",
                "phase_vol": "Phase",
            }),
            use_container_width=True,
            height=300,
            hide_index=True,
        )


#  Layout 

st.title("✈️ Flight Tracking France")
st.caption(
    f"🟢 LIVE · OpenSky Network via Snowflake · "
    f"Rafraîchissement toutes les {REFRESH_INTERVAL}s"
)

with st.spinner("Chargement des données Snowflake..."):
    df_raw  = load_positions()
    df_anom = load_anomalies_24h()

if df_raw.empty:
    st.warning("⚠️ Aucune donnée. Vérifiez que le pipeline Airflow tourne.")
    st.stop()

df = apply_filters(df_raw)

# 1. KPIs
render_kpis(df, df_anom)
st.divider()

# 2. Carte
st.subheader(f" Positions temps réel — {len(df)} avions")
st.markdown(
    """<div style="font-size: 11px; margin-bottom: 6px;">
    🔵 Croisière &nbsp;|&nbsp; 🟢 Montée &nbsp;|&nbsp;
    🔴 Descente &nbsp;|&nbsp; 🟡 Transition &nbsp;|&nbsp;
    ⚠️ Contour rouge = anomalie</div>""",
    unsafe_allow_html=True,
)
st_folium(build_map(df), width=None, height=550, returned_objects=[])
st.divider()

# 3. Anomalies
st.subheader(" Anomalies détectées")
render_anomalies(df, df_anom)
st.divider()

# Refresh
col_btn, col_time = st.columns([1, 3])
with col_btn:
    if st.button(" Rafraîchir maintenant"):
        st.cache_data.clear()
        st.rerun()
with col_time:
    st.caption(
        f"Dernière actualisation : **{datetime.now().strftime('%H:%M:%S')}** | "
        f"{len(df)} avions filtrés / {len(df_raw)} total | "
        f"Source : OpenSky Network"
    )
