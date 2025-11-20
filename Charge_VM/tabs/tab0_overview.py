

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

from tabs.context import get_context

TAB_CODE = """


df_alertes = pd.DataFrame()
df_defauts_actifs = pd.DataFrame()

try:
    CONFIG_KPI = {
        "host": "141.94.31.144",
        "port": 3306,
        "user": "AdminNidec",
        "password": "u6Ehe987XBSXxa4",
        "database": "indicator",
    }

    engine = create_engine(
        "mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(**CONFIG_KPI)
    )

    query_alertes = \"\"\"
        SELECT
            Site,
            PDC,
            type_erreur,
            detection,
            occurrences_12h,
            moment,
            evi_code,
            downstream_code_pc
        FROM kpi_alertes
        ORDER BY detection DESC
    \"\"\"

    df_alertes = pd.read_sql(query_alertes, con=engine)

    query_defauts_actifs = \"\"\"
        SELECT
            site,
            date_debut,
            defaut,
            eqp
        FROM kpi_defauts_log
        WHERE date_fin IS NULL
        ORDER BY date_debut DESC
    \"\"\"

    df_defauts_actifs = pd.read_sql(query_defauts_actifs, con=engine)
    engine.dispose()

except Exception as e:
    st.error(f"Erreur de connexion: {str(e)}")
st.markdown("### Défauts Actifs")

if not df_defauts_actifs.empty:
    df_defauts_actifs["date_debut"] = pd.to_datetime(df_defauts_actifs["date_debut"], errors="coerce")

    if site_sel:
        df_defauts_actifs = df_defauts_actifs[df_defauts_actifs["site"].isin(site_sel)]

nb_defauts_actifs = len(df_defauts_actifs) if not df_defauts_actifs.empty else 0
nb_sites_concernes = (
    df_defauts_actifs["site"].nunique() if not df_defauts_actifs.empty else 0
)
card_color = "#dc3545" if nb_defauts_actifs > 5 else "#ffc107" if nb_defauts_actifs > 0 else "#28a745"
st.markdown(
    f'''
<div style='padding: 20px; background: {card_color}; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 2.3em;'>{nb_defauts_actifs}</h1>
    <p style='color: white; margin: 5px 0 0 0; font-size: 1em;'>défaut{'s' if nb_defauts_actifs > 1 else ''} en cours</p>
    <p style='color: white; margin: 5px 0 0 0; font-size: 0.95em;'>sur {nb_sites_concernes} site{'s' if nb_sites_concernes > 1 else ''}</p>
</div>
''',
    unsafe_allow_html=True,
)

filter_pdc_only = st.checkbox("PDC uniquement", value=False, key="defauts_actifs_pdc_filter")

if not df_defauts_actifs.empty:
    if filter_pdc_only:
        df_defauts_actifs = df_defauts_actifs[df_defauts_actifs["eqp"].str.contains("PDC", case=False, na=False)]

if nb_defauts_actifs > 0 and not df_defauts_actifs.empty:
    now = pd.Timestamp.now()

    delta = now - df_defauts_actifs["date_debut"]

    df_defauts_actifs["Depuis (jours)"] = delta.dt.days
    df_defauts_actifs["is_recent"] = delta < pd.Timedelta(days=1)

    # KPI GLOBAL "Sites à regarder" (défauts < 24h)
    sites_recent = df_defauts_actifs.groupby("site")["is_recent"].any()
    nb_sites_recent = int(sites_recent.sum())

    if nb_sites_recent > 0:
        suffix = "s" if nb_sites_recent > 1 else ""
        # Liste des sites concernés
        sites_to_watch = list(sites_recent[sites_recent].index)
        sites_str = ", ".join(sites_to_watch)

        kpi_html = (
            "<div style='padding: 15px; background: #17a2b8; border-radius: 10px; "
            "margin: 10px 0 15px 0; text-align: center;'>"
            "<p style='color: white; margin: 0; font-size: 1.05em;'>"
            "&#128269; <b>"
            + str(nb_sites_recent) +
            "</b> site" + suffix + " à regarder (défauts &lt; 24h)"
            "</p>"
            "<p style='color: white; margin: 5px 0 0 0; font-size: 0.9em;'>"
            "Sites : " + sites_str +
            "</p>"
            "</div>"
        )
        st.markdown(kpi_html, unsafe_allow_html=True)

    sites_groupes = df_defauts_actifs.groupby("site")

    for site_name, df_site in sites_groupes:
        nb_defauts_site = len(df_site)

        with st.expander(f" {site_name} ({nb_defauts_site} défaut{'s' if nb_defauts_site > 1 else ''})", expanded=False):
            num_cols = 3

            # Ordre d'affichage des "lignes" d'équipements
            equip_patterns = [
                ("PDC1", r"PDC1"),
                ("PDC2", r"PDC2"),
                ("PDC3", r"PDC3"),
                ("PDC4", r"PDC4"),
                ("Variateur HC1", r"Variateur.*HC1|HC1.*Variateur"),
                ("Variateur HC2", r"Variateur.*HC2|HC2.*Variateur"),
            ]

            handled_mask = pd.Series(False, index=df_site.index)

            for label, pattern in equip_patterns:
                mask = df_site["eqp"].str.contains(pattern, case=False, na=False, regex=True)
                df_eqp = df_site[mask].sort_values("date_debut")

                if df_eqp.empty:
                    continue

                handled_mask |= mask

                st.markdown(f"**{label}**")

                for i in range(0, len(df_eqp), num_cols):
                    cols = st.columns(num_cols)
                    for j, col in enumerate(cols):
                        idx = i + j
                        if idx < len(df_eqp):
                            row = df_eqp.iloc[idx]
                            with col:
                                defaut_color = "#dc3545" if row["Depuis (jours)"] > 7 else "#ffc107"
                                st.markdown(f'''
<div style='padding: 15px; background: {defaut_color}; border-radius: 10px; margin-bottom: 10px;'>
    <p style='color: white; margin: 0; font-weight: bold; font-size: 1.1em;'>⚠️ {row["defaut"]}</p>
    <p style='color: white; margin: 5px 0; font-size: 0.9em;'>🔧 {row["eqp"]}</p>
    <p style='color: white; margin: 5px 0 0 0; font-size: 0.8em; font-style: italic;'>Depuis {row["Depuis (jours)"]} jours</p>
</div>
''', unsafe_allow_html=True)


else:
    st.markdown('''
<div style='padding: 30px; background: #28a745; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 3em;'>0</h1>
    <p style='color: white; margin: 10px 0 0 0; font-size: 1.2em;'>Aucun défaut actif</p>
</div>
''', unsafe_allow_html=True)

st.markdown("---")

col_kpi1, col_kpi2 = st.columns(2)

with col_kpi1:
    suspicious = tables.get("suspicious_under_1kwh", pd.DataFrame())
    nb_suspicious = 0
    if not suspicious.empty:
        df_s_temp = suspicious.copy()
        if "Datetime start" in df_s_temp.columns:
            ds = pd.to_datetime(df_s_temp["Datetime start"], errors="coerce")
            mask = ds.ge(pd.Timestamp(d1)) & ds.lt(pd.Timestamp(d2) + pd.Timedelta(days=1))
            df_s_temp = df_s_temp[mask]
        if site_sel and "Site" in df_s_temp.columns:
            df_s_temp = df_s_temp[df_s_temp["Site"].isin(site_sel)]
        nb_suspicious = len(df_s_temp)

    st.markdown("### Transactions suspectes")
    color = "#dc3545" if nb_suspicious > 5 else "#ffc107" if nb_suspicious > 0 else "#28a745"
    st.markdown(f'''
<div style='padding: 20px; background: {color}; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 2.5em;'>{nb_suspicious}</h1>
    <p style='color: white; margin: 10px 0 0 0; font-size: 1em;'>Transactions <1 kWh</p>
</div>
''', unsafe_allow_html=True)

with col_kpi2:
    multi_attempts = tables.get("multi_attempts_hour", pd.DataFrame())
    nb_multi_attempts = 0
    if not multi_attempts.empty:
        dfm_temp = multi_attempts.copy()
        if "Date_heure" in dfm_temp.columns:
            dfm_temp["Date_heure"] = pd.to_datetime(dfm_temp["Date_heure"], errors="coerce")
            d1_ts = pd.Timestamp(d1)
            d2_ts = pd.Timestamp(d2) + pd.Timedelta(days=1)
            mask = dfm_temp["Date_heure"].between(d1_ts, d2_ts)
            dfm_temp = dfm_temp[mask]
        if site_sel and "Site" in dfm_temp.columns:
            dfm_temp = dfm_temp[dfm_temp["Site"].isin(site_sel)]
        nb_multi_attempts = len(dfm_temp)

    st.markdown("### Analyse tentatives multiples")
    color = "#dc3545" if nb_multi_attempts > 5 else "#ffc107" if nb_multi_attempts > 0 else "#28a745"
    st.markdown(f'''
<div style='padding: 20px; background: {color}; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 2.5em;'>{nb_multi_attempts}</h1>
    <p style='color: white; margin: 10px 0 0 0; font-size: 1em;'>Utilisateurs multiples tentatives</p>
</div>
''', unsafe_allow_html=True)

st.markdown("---")

if not df_alertes.empty:
    df_alertes["detection"] = pd.to_datetime(df_alertes["detection"], errors="coerce")

    start_dt = pd.to_datetime(d1)
    end_dt = pd.to_datetime(d2) + pd.Timedelta(days=1)
    df_alertes = df_alertes[df_alertes["detection"].between(start_dt, end_dt)]

    if site_sel:
        df_alertes = df_alertes[df_alertes["Site"].isin(site_sel)]

nb_alertes_actives = len(df_alertes) if not df_alertes.empty else 0

col_alert1, col_alert2 = st.columns(2)

with col_alert1:
    st.markdown("### Alertes Actives")
    if nb_alertes_actives > 0:
        alert_color = "#dc3545" if nb_alertes_actives > 10 else "#ffc107"
        st.markdown(f'''
<div style='padding: 30px; background: {alert_color}; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 3em;'>{nb_alertes_actives}</h1>
    <p style='color: white; margin: 10px 0 0 0; font-size: 1.2em;'>Alertes détectées</p>
</div>
''', unsafe_allow_html=True)
    else:
        st.markdown('''
<div style='padding: 30px; background: #28a745; border-radius: 10px; text-align: center;'>
    <h1 style='color: white; margin: 0; font-size: 3em;'>0</h1>
    <p style='color: white; margin: 10px 0 0 0; font-size: 1.2em;'>Aucune alerte</p>
</div>
''', unsafe_allow_html=True)

with col_alert2:
    st.markdown("### Top 5 Sites en Alerte")
    if not df_alertes.empty:
        top_sites_alertes = (
            df_alertes.groupby("Site")
            .size()
            .sort_values(ascending=False)
            .head(5)
        )

        fig_sites = go.Figure(go.Bar(
            x=top_sites_alertes.values,
            y=top_sites_alertes.index,
            orientation='h',
            marker=dict(
                color=top_sites_alertes.values,
                colorscale='Reds',
                showscale=False
            ),
            text=top_sites_alertes.values,
            textposition='outside'
        ))

        fig_sites.update_layout(
            height=300,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title="Nombre d'alertes",
            yaxis_title="",
            showlegend=False
        )

        st.plotly_chart(fig_sites, use_container_width=True)
    else:
        st.info("Aucun site en alerte")


st.markdown("---")


stat_global = (
    sess_kpi.groupby(SITE_COL)
    .agg(
        Total=("is_ok", "count"),
        Total_OK=("is_ok", "sum"),
    )
    .reset_index()
)
stat_global["Total_NOK"] = stat_global["Total"] - stat_global["Total_OK"]
stat_global["% OK"] = (
    np.where(stat_global["Total"].gt(0), stat_global["Total_OK"] / stat_global["Total"] * 100, 0)
).round(1)

if not stat_global.empty:
    top_charges = stat_global.sort_values("Total", ascending=False).head(10)
    by_site_success = top_charges.sort_values("% OK", ascending=False)
    by_site_fails = stat_global.sort_values("Total_NOK", ascending=False).head(10)

    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        st.markdown("### Top 10 Sites avec le plus de charges - Taux de Réussite")
        fig_success = go.Figure(go.Bar(
            x=by_site_success["% OK"],
            y=by_site_success[SITE_COL],
            orientation='h',
            marker=dict(
                color=by_site_success["% OK"],
                colorscale='RdYlGn',
                showscale=False,
                cmin=0,
                cmax=100
            ),
            text=by_site_success["% OK"].apply(lambda x: f"{x:.1f}%"),
            textposition='outside'
        ))

        fig_success.update_layout(
            height=400,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title="Taux de réussite (%)",
            yaxis_title="",
            xaxis=dict(range=[0, 105])
        )

        st.plotly_chart(fig_success, use_container_width=True)

    with col_chart2:
        st.markdown("### Top 10 Sites - Nombre d'Échecs")
        fig_fails = go.Figure(go.Bar(
            x=by_site_fails["Total_NOK"],
            y=by_site_fails[SITE_COL],
            orientation='h',
            marker=dict(
                color=by_site_fails["Total_NOK"],
                colorscale='Reds',
                showscale=False
            ),
            text=by_site_fails["Total_NOK"],
            textposition='outside'
        ))

        fig_fails.update_layout(
            height=400,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title="Nombre d'échecs",
            yaxis_title=""
        )

        st.plotly_chart(fig_fails, use_container_width=True)
"""

def render():
    ctx = get_context()
    globals_dict = {
        "np": np,
        "pd": pd,
        "px": px,
        "go": go,
        "st": st,
        "create_engine": create_engine
    }
    local_vars = dict(ctx.__dict__)
    local_vars.setdefault('plot', getattr(ctx, 'plot', None))
    local_vars.setdefault('hide_zero_labels', getattr(ctx, 'hide_zero_labels', None))
    local_vars.setdefault('with_charge_link', getattr(ctx, 'with_charge_link', None))
    local_vars.setdefault('evi_counts_pivot', getattr(ctx, 'evi_counts_pivot', None))
    local_vars = {k: v for k, v in local_vars.items() if v is not None}
    exec(TAB_CODE, globals_dict, local_vars)