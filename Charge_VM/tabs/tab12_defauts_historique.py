import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

from tabs.context import get_context

TAB_CODE = """
st.markdown("### 📋 Historique des Défauts")

try:
    # Configuration identique à CONFIG_KPI
    engine = create_engine("mysql+pymysql://AdminNidec:u6Ehe987XBSXxa4@141.94.31.144:3306/indicator")

    # Récupérer les dates de filtrage depuis le contexte
    d1_filter = d1 if 'd1' in dir() else None
    d2_filter = d2 if 'd2' in dir() else None

    # Requête pour récupérer les défauts filtrés par période
    if d1_filter and d2_filter:
        # Convertir les dates en format SQL
        d1_str = pd.Timestamp(d1_filter).strftime('%Y-%m-%d')
        d2_str = (pd.Timestamp(d2_filter) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

        query_defauts = f\"\"\"
            SELECT
                site,
                date_debut,
                date_fin,
                defaut,
                eqp
            FROM kpi_defauts_log
            WHERE (date_debut >= '{d1_str}' AND date_debut < '{d2_str}')
               OR (date_fin >= '{d1_str}' AND date_fin < '{d2_str}')
               OR (date_debut <= '{d1_str}' AND (date_fin >= '{d2_str}' OR date_fin IS NULL))
            ORDER BY date_debut DESC
        \"\"\"
    else:
        # Si pas de filtre, récupérer tous les défauts
        query_defauts = \"\"\"
            SELECT
                site,
                date_debut,
                date_fin,
                defaut,
                eqp
            FROM kpi_defauts_log
            ORDER BY date_debut DESC
        \"\"\"

    df_defauts = pd.read_sql(query_defauts, con=engine)
    engine.dispose()

    if not df_defauts.empty:
        df_defauts["date_debut"] = pd.to_datetime(df_defauts["date_debut"], errors="coerce")
        df_defauts["date_fin"] = pd.to_datetime(df_defauts["date_fin"], errors="coerce")

        # Calculer la durée du défaut
        now = pd.Timestamp.now()
        df_defauts["Durée (jours)"] = ((df_defauts["date_fin"].fillna(now)) - df_defauts["date_debut"]).dt.days

        # Statut du défaut
        df_defauts["Statut"] = df_defauts["date_fin"].apply(lambda x: "En cours" if pd.isna(x) else "Résolu")

        # Obtenir la liste unique des sites
        sites_disponibles = sorted(df_defauts["site"].dropna().unique().tolist())

        # Filtre par site
        st.markdown("### 🔍 Filtres")
        col_filtre1, col_filtre2 = st.columns([2, 2])

        with col_filtre1:
            site_selectionne = st.selectbox(
                "Sélectionner un site",
                options=["Tous les sites"] + sites_disponibles,
                index=0,
                key="defauts_site_filter"
            )

        with col_filtre2:
            statut_selectionne = st.multiselect(
                "Statut du défaut",
                options=["En cours", "Résolu"],
                default=["En cours", "Résolu"],
                key="defauts_statut_filter"
            )

        # Appliquer les filtres
        df_filtree = df_defauts.copy()

        if site_selectionne != "Tous les sites":
            df_filtree = df_filtree[df_filtree["site"] == site_selectionne]

        if statut_selectionne:
            df_filtree = df_filtree[df_filtree["Statut"].isin(statut_selectionne)]

        st.markdown("---")

        # KPIs
        col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

        nb_total = len(df_filtree)
        nb_en_cours = len(df_filtree[df_filtree["Statut"] == "En cours"])
        nb_resolus = len(df_filtree[df_filtree["Statut"] == "Résolu"])
        duree_moyenne = df_filtree["Durée (jours)"].mean() if not df_filtree.empty else 0

        with col_kpi1:
            st.metric("Total Défauts", nb_total)

        with col_kpi2:
            st.metric("En cours", nb_en_cours)

        with col_kpi3:
            st.metric("Résolus", nb_resolus)

        with col_kpi4:
            st.metric("Durée moyenne (jours)", f"{duree_moyenne:.1f}")

        st.markdown("---")

        # Graphiques
        st.markdown("### Répartition par Statut")
        defauts_par_statut = df_filtree.groupby("Statut").size()

        fig_statut = go.Figure(go.Pie(
            labels=defauts_par_statut.index,
            values=defauts_par_statut.values,
            marker=dict(colors=['#dc3545', '#28a745']),
            textinfo='label+percent+value'
        ))

        fig_statut.update_layout(
            height=400,
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=True
        )

        st.plotly_chart(fig_statut, use_container_width=True)

        st.markdown("---")

        # Tableau complet
        st.markdown("### 📊 Tableau des Défauts")

        df_display = df_filtree.copy()

        df_display = df_display.rename(columns={
            "site": "Site",
            "date_debut": "Date début",
            "date_fin": "Date fin",
            "defaut": "Défaut",
            "eqp": "Equipement"
        })

        # Formatter les dates pour l'affichage
        df_display["Date début"] = df_display["Date début"].dt.strftime("%Y-%m-%d %H:%M")
        df_display["Date fin"] = df_display["Date fin"].apply(
            lambda x: x.strftime("%Y-%m-%d %H:%M") if pd.notna(x) else "En cours"
        )

        display_cols = ["Site", "Date début", "Date fin", "Durée (jours)", "Statut", "Défaut", "Equipement"]

        # Tri par défaut : décroissant par date début
        df_display_sorted = df_display.sort_values(
            by="Date début",
            ascending=False,
            key=lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M", errors="coerce")
        )

        # Calculer la hauteur dynamique du tableau (30 pixels par ligne + header)
        num_rows = len(df_display_sorted)
        table_height = min(max(num_rows * 35 + 38, 100), 600)  # Min 100px, max 600px

        st.dataframe(
            df_display_sorted[display_cols],
            use_container_width=True,
            hide_index=True,
            height=table_height
        )

        # Stats supplémentaires
        st.markdown("---")
        st.markdown("### 📈 Statistiques")

        col_stat1, col_stat2 = st.columns(2)

        with col_stat1:
            st.markdown("#### Top 5 Equipements")
            top_equipements = df_filtree["eqp"].value_counts().head(5)
            st.dataframe(
                top_equipements.reset_index().rename(columns={"eqp": "Equipement", "count": "Nombre"}),
                use_container_width=True,
                hide_index=True
            )

        with col_stat2:
            st.markdown("#### Top 5 Défauts")
            top_defauts = df_filtree["defaut"].value_counts().head(5)
            st.dataframe(
                top_defauts.reset_index().rename(columns={"defaut": "Défauts", "count": "Nombre"}),
                use_container_width=True,
                hide_index=True
            )

    else:
        st.info("Aucun défaut trouvé dans l'historique.")

except Exception as e:
    st.error(f"Erreur lors de la récupération des défauts: {str(e)}")
"""

def render():
    ctx = get_context()
    globals_dict = {
        "pd": pd,
        "px": px,
        "go": go,
        "st": st,
        "create_engine": create_engine
    }
    local_vars = dict(ctx.__dict__)
    local_vars = {k: v for k, v in local_vars.items() if v is not None}
    exec(TAB_CODE, globals_dict, local_vars)
