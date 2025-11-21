import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from tabs.context import get_context

BASE_CHARGE_URL = "https://elto.nidec-asi-online.com/Charge/detail?id="

TAB_CODE = """
st.markdown("### 🔍 Analyse Erreur Spécifique")

def _format_soc_evolution(row):
    s0 = row.get("SOC Start", pd.NA)
    s1 = row.get("SOC End", pd.NA)
    if pd.notna(s0) and pd.notna(s1):
        try:
            return f"{int(round(s0))}% → {int(round(s1))}%"
        except Exception:
            return ""
    return ""

with st.expander("🔍 Filtrer par Mac adresse", expanded=False):
    st.caption("Renseignez tout ou partie d'une adresse MAC pour lister les charges associées")

    if "BASE_CHARGE_URL" not in globals():
        globals()["BASE_CHARGE_URL"] = "https://elto.nidec-asi-online.com/Charge/detail?id="
    BASE_CHARGE_URL = globals()["BASE_CHARGE_URL"]

    if "charges_mac" not in locals() or not isinstance(charges_mac, pd.DataFrame) or charges_mac.empty:
        st.info("Données 'charges_mac' indisponibles pour la recherche par MAC.")
    else:
        with st.form("mac_filter_form_tab9"):
            mac_query = st.text_input(
                "Adresse MAC ou préfixe",
                value="",
                placeholder="ex : 4E:5D",
                help="Saisir une adresse complète ou un préfixe (ex : 4E:5D pour filtrer toutes les MAC qui commencent par 4E:5D)",
                key="mac_filter_query_tab9",
            )
            submit_mac = st.form_submit_button("Rechercher", type="primary")

        if submit_mac:
            import re

            mac_norm = (
                mac_query.strip()
                .lower()
                .replace("0x", "", 1)
            )
            mac_norm = re.sub(r"[^0-9a-f]", "", mac_norm)

            if not mac_norm:
                st.warning("Saisissez une adresse ou un préfixe MAC valide.")
            else:
                df_mac = charges_mac.copy()

                if "mac" in df_mac.columns:
                    df_mac["_mac_norm"] = df_mac["mac"].astype(str)
                elif "MAC Address" in df_mac.columns:
                    df_mac["_mac_norm"] = df_mac["MAC Address"].astype(str)
                else:
                    df_mac["_mac_norm"] = ""

                df_mac["_mac_norm"] = (
                    df_mac["_mac_norm"]
                    .str.lower()
                    .str.replace("0x", "", regex=False)
                    .str.replace(r"[^0-9a-f]", "", regex=True)
                )

                mask_mac = df_mac["_mac_norm"].str.startswith(mac_norm)
                df_mac = df_mac[mask_mac].copy()

                if "Site" in df_mac.columns and st.session_state.get("site_sel"):
                    df_mac = df_mac[df_mac["Site"].isin(st.session_state.site_sel)]

                if (
                    "Datetime start" in df_mac.columns
                    and st.session_state.get("d1")
                    and st.session_state.get("d2")
                ):
                    df_mac["Datetime start"] = pd.to_datetime(df_mac["Datetime start"], errors="coerce")
                    d1 = pd.Timestamp(st.session_state.get("d1"))
                    d2 = pd.Timestamp(st.session_state.get("d2")) + pd.Timedelta(days=1)
                    df_mac = df_mac[df_mac["Datetime start"].ge(d1) & df_mac["Datetime start"].lt(d2)]

                if df_mac.empty:
                    st.info("Aucune charge trouvée pour ce préfixe MAC.")
                else:
                    if (
                        "sessions" in locals()
                        and isinstance(sessions, pd.DataFrame)
                        and not sessions.empty
                        and "ID" in df_mac.columns
                    ):
                        sess_lookup = sessions[["ID", "Datetime end"]].copy()
                        sess_lookup["ID"] = sess_lookup["ID"].astype(str).str.strip()
                        df_mac["ID"] = df_mac["ID"].astype(str).str.strip()
                        df_mac = df_mac.merge(
                            sess_lookup,
                            on="ID",
                            how="left",
                            suffixes=("", "_from_sessions"),
                        )
                        if "Datetime end_from_sessions" in df_mac.columns:
                            df_mac["Datetime end"] = df_mac.get("Datetime end").fillna(
                                df_mac.pop("Datetime end_from_sessions")
                            )

                if "Datetime end" in df_mac.columns:
                    df_mac["Datetime end"] = pd.to_datetime(df_mac["Datetime end"], errors="coerce")
                if "Energy (Kwh)" in df_mac.columns:
                    df_mac["Energy (Kwh)"] = pd.to_numeric(df_mac["Energy (Kwh)"], errors="coerce")
                for c in ("SOC Start", "SOC End"):
                    if c in df_mac.columns:
                        df_mac[c] = pd.to_numeric(df_mac[c], errors="coerce")
                if {"SOC Start", "SOC End"}.issubset(df_mac.columns):
                    df_mac["Évolution SOC"] = df_mac.apply(_format_soc_evolution, axis=1)
                if "MAC Address" in df_mac.columns:
                    df_mac["MAC Address"] = df_mac["MAC Address"].apply(_fmt_mac)
                if "is_ok" in df_mac.columns:
                    is_ok_series = df_mac["is_ok"]
                    if not pd.api.types.is_bool_dtype(is_ok_series):
                        is_ok_series = pd.to_numeric(is_ok_series, errors="coerce").fillna(0).astype(int).astype(bool)
                        df_mac["_is_ok_bool"] = is_ok_series
                    else:
                        df_mac["_is_ok_bool"] = is_ok_series.fillna(False)

                    total_mac = len(df_mac)
                    nb_ok = int(df_mac["_is_ok_bool"].sum())
                    taux_ok = (nb_ok / total_mac * 100) if total_mac else 0

                    st.metric(
                        "Taux de réussite MAC",
                        f"{taux_ok:.1f} %",
                        help="Part des charges associées à ce préfixe MAC marquées comme réussies",
                    )

                    def _render_mac_table(df_source: pd.DataFrame, title: str):
                        if df_source.empty:
                            st.info(f"Aucune {title.lower()} pour ce préfixe MAC.")
                            return

                        if "ID" in df_source.columns:
                            if "with_charge_link" in locals():
                                df_source = with_charge_link(df_source, id_col="ID", link_col="Lien Elto")
                            elif "Lien Elto" not in df_source.columns:
                                df_source = df_source.copy()
                                df_source["Lien Elto"] = BASE_CHARGE_URL + df_source["ID"].astype(str).str.strip()

                        display_cols = [
                            "Site",
                            "PDC",
                            "Datetime start",
                            "Datetime end",
                            "Évolution SOC",
                            "Error etiquette",
                            "MAC Address",
                            "Lien Elto",
                            "Vehicle",
                            "Energy (Kwh)",
                        ]
                        display_cols = [c for c in display_cols if c in df_source.columns]

                        df_out = df_source[display_cols].copy()
                        if "Datetime start" in df_out.columns:
                            df_out = df_out.sort_values("Datetime start", ascending=False)

                        df_out.insert(0, "#", range(1, len(df_out) + 1))

                        st.subheader(title)
                        st.data_editor(
                            df_out,
                            hide_index=True,
                            use_container_width=True,
                            column_config={
                                "Lien Elto": st.column_config.LinkColumn(
                                    "Lien Elto",
                                    help="Ouvrir la session dans Elto",
                                    display_text="🔗 Ouvrir",
                                ),
                                "Datetime start": st.column_config.DatetimeColumn("Start time", format="YYYY-MM-DD HH:mm:ss"),
                                "Datetime end": st.column_config.DatetimeColumn("End time", format="YYYY-MM-DD HH:mm:ss"),
                                "Energy (Kwh)": st.column_config.NumberColumn("Energy (kWh)", format="%.3f"),
                            },
                        )

                    df_ok = df_mac[df_mac["_is_ok_bool"]].copy()
                    df_nok = df_mac[~df_mac["_is_ok_bool"]].copy()

                    def _with_error_etiquette(df_source: pd.DataFrame) -> pd.DataFrame:
                        if (
                            df_source.empty
                            or "ID" not in df_source.columns
                            or "sessions" not in locals()
                            or not isinstance(sessions, pd.DataFrame)
                            or sessions.empty
                            or not {"type_erreur", "moment"}.issubset(sessions.columns)
                        ):
                            return df_source

                        df_out = df_source.copy()
                        df_out["ID"] = df_out["ID"].astype(str).str.strip()

                        errs = sessions[["ID", "type_erreur", "moment"]].copy()
                        errs["ID"] = errs["ID"].astype(str).str.strip()
                        errs["Error etiquette"] = errs.apply(
                            lambda r: f"{r['type_erreur']} - {r['moment']}"
                            if pd.notna(r.get("type_erreur")) and pd.notna(r.get("moment"))
                            else pd.NA,
                            axis=1,
                        )
                        errs = errs[["ID", "Error etiquette"]]

                        df_out = df_out.merge(errs, on="ID", how="left")
                        return df_out

                    df_nok = _with_error_etiquette(df_nok)

                    _render_mac_table(df_ok, "Charges OK")
                    _render_mac_table(df_nok, "Charges NOK")
with st.expander("🔍 Filtrer par code", expanded=False):
    code_raw_tab = st.text_input(
        "N° d'erreur / Code PC",
        value="",
        placeholder="ex : 73, 84, 90",
        key="code_filter_values_tab",
        help="Saisissez un ou plusieurs codes entiers séparés par virgules, espaces ou ;"
    )
    code_type_tab = st.selectbox(
        "Type du code à filtrer",
        options=["Tous", "Erreur_EVI", "Erreur_DownStream"],
        index=0,
        key="code_filter_type_tab"
    )

    def _mask_code_local(df: pd.DataFrame, code_raw: str, code_type: str):
        import re
        if not code_raw.strip():
            return pd.Series(False, index=df.index)

        parts = re.split(r"[,\s;]+", code_raw.strip())
        try:
            codes = [int(p) for p in parts if p.strip() != ""]
        except Exception:
            codes = []

        if not codes:
            return pd.Series(False, index=df.index)

        col_evi = "EVI Error Code" if "EVI Error Code" in df.columns else None
        col_ds  = "Downstream Code PC" if "Downstream Code PC" in df.columns else None

        masks = []
        if code_type == "Erreur_EVI" and col_evi:
            masks.append(pd.to_numeric(df[col_evi], errors="coerce").isin(codes))
        elif code_type == "Erreur_DownStream" and col_ds:
            masks.append(pd.to_numeric(df[col_ds], errors="coerce").isin(codes))
        elif code_type == "Tous":
            if col_evi:
                masks.append(pd.to_numeric(df[col_evi], errors="coerce").isin(codes))
            if col_ds:
                masks.append(pd.to_numeric(df[col_ds], errors="coerce").isin(codes))

        if not masks:
            return pd.Series(False, index=df.index)

        if len(masks) == 1:
            return masks[0]
        return masks[0] | masks[1]

    selected_codes = []
    err_sum = pd.DataFrame()

    if not code_raw_tab.strip():
        st.info("⏳ Saisissez un ou plusieurs codes dans le filtre")
    else:
        import re
        raw_parts = re.split(r"[,\s;]+", code_raw_tab.strip())
        try:
            selected_codes = [int(p) for p in raw_parts if p.strip() != ""]
        except Exception:
            selected_codes = []

        if not selected_codes:
            st.warning("Aucun code valide reconnu.")
        else:
            def code_match(key, codes=selected_codes, type_filter=code_type_tab):
                return key[2] in codes and (type_filter == "Tous" or key[3] == type_filter)

            if "tbl_all" not in locals() or not isinstance(tbl_all, pd.DataFrame):
                tbl_all = pd.DataFrame()
                if "sess_kpi" in locals():
                    from analyses.kpi_cal import EVI_MOMENT, EVI_CODE, DS_PC

                    err_ctx = sess_kpi.loc[~sess_kpi["is_ok_filt"]].copy()
                    if not err_ctx.empty:
                        evi_step = pd.to_numeric(err_ctx[EVI_MOMENT], errors="coerce").fillna(0).astype(int)
                        evi_code = pd.to_numeric(err_ctx[EVI_CODE], errors="coerce").fillna(0).astype(int)
                        ds_pc    = pd.to_numeric(err_ctx[DS_PC],    errors="coerce").fillna(0).astype(int)

                        def _local_map(step_val):
                            try:
                                step_val = int(step_val)
                            except Exception:
                                return "Unknown"
                            if step_val == 0:
                                return "Fin de charge"
                            if 1 <= step_val <= 2:
                                return "Init"
                            if 4 <= step_val <= 6:
                                return "Lock Connector"
                            if step_val == 7:
                                return "CableCheck"
                            if step_val == 8:
                                return "Charge"
                            if step_val > 8:
                                return "Fin de charge"
                            return "Unknown"

                        sub_evi = err_ctx.loc[(ds_pc.eq(8192)) | ((ds_pc.eq(0)) & (evi_code.ne(0)))].copy()
                        sub_evi["_step"]   = evi_step.loc[sub_evi.index]
                        sub_evi["_moment"] = sub_evi["_step"].map(_local_map)
                        sub_evi["_code"]   = evi_code.loc[sub_evi.index]
                        sub_evi["_type"]   = "Erreur_EVI"

                        sub_ds = err_ctx.loc[ds_pc.ne(0) & ds_pc.ne(8192)].copy()
                        sub_ds["_step"]   = evi_step.loc[sub_ds.index]
                        sub_ds["_moment"] = sub_ds["_step"].map(_local_map)
                        sub_ds["_code"]   = ds_pc.loc[sub_ds.index]
                        sub_ds["_type"]   = "Erreur_DownStream"

                        all_err = pd.concat([sub_evi, sub_ds], ignore_index=True)
                        if not all_err.empty:
                            tbl_all = (
                                all_err.assign(
                                    _key=lambda d: list(zip(
                                        d.get("_moment", [""] * len(d)),
                                        d.get("_step",   [0]  * len(d)),
                                        d.get("_code",   [0]  * len(d)),
                                        d.get("_type",   [""] * len(d)),
                                    ))
                                )
                                .groupby("_key")
                                .size()
                                .reset_index(name="Occurrences")
                                .sort_values("Occurrences", ascending=False)
                            )
                            total_err = tbl_all["Occurrences"].sum()
                            if total_err:
                                tbl_all["%"] = (tbl_all["Occurrences"] / total_err * 100).round(2)
                            else:
                                tbl_all["%"] = 0.0

            matched_rows = tbl_all[tbl_all.get("_key", pd.Series(dtype=object)).apply(code_match)] if not tbl_all.empty else pd.DataFrame()

            if matched_rows.empty:
                st.info("Aucune donnée pour les codes spécifiés.")
            else:
                total_pct = matched_rows["%"].sum().round(2)
                code_list_str = ", ".join(str(c) for c in selected_codes)
                st.markdown(
                    f"**Code {code_list_str} → {total_pct}% des erreurs totales**"
                )
        if not isinstance(sess, pd.DataFrame) or sess.empty:
            st.info("Aucune donnée disponible.")
        else:
            df_src = sess.copy()
            if "is_ok" not in df_src.columns:
                st.warning("Colonne 'is_ok' absente dans les sessions.")
            else:
                mask_type   = True
                mask_moment = True
                if "type_erreur" in df_src.columns and st.session_state.get("type_sel"):
                    mask_type = df_src["type_erreur"].isin(st.session_state.type_sel)
                if {"type_erreur", "moment"}.issubset(df_src.columns) and st.session_state.get("moment_sel"):
                    mask_moment = df_src["moment"].isin(st.session_state.moment_sel)

                df_src_f = df_src[mask_type & mask_moment].copy()
                mask_code_tab = _mask_code_local(df_src_f, code_raw_tab, code_type_tab)
                df_src_f = df_src_f[mask_code_tab].copy()
                if code_type_tab == "Erreur_EVI":
                    df_src_f = df_src_f[df_src_f["type_erreur"] == "Erreur_EVI"]
                elif code_type_tab == "Erreur_DownStream":
                    df_src_f = df_src_f[df_src_f["type_erreur"] == "Erreur_DownStream"]
                err_sum = df_src_f.loc[~df_src_f["is_ok"]].copy()

                if err_sum.empty:
                    st.info("Aucune charge en erreur pour le périmètre/filtre sélectionné.")
                else:
                    for c in ("Datetime start", "Datetime end"):
                        if c in err_sum.columns:
                            err_sum[c] = pd.to_datetime(err_sum[c], errors="coerce")
                    if "Energy (Kwh)" in err_sum.columns:
                        err_sum["Energy (Kwh)"] = pd.to_numeric(err_sum["Energy (Kwh)"], errors="coerce")

                    for c in ("SOC Start", "SOC End"):
                        if c in err_sum.columns:
                            err_sum[c] = pd.to_numeric(err_sum[c], errors="coerce")

                    if "MAC Address" in err_sum.columns:
                        err_sum["MAC Address"] = err_sum["MAC Address"].apply(_fmt_mac)

                    def _etiquette(row):
                        t = str(row.get("type_erreur", "") or "")
                        m = str(row.get("moment", "") or "")
                        return f"{t} — {m}" if m else t
                    err_sum["Erreur"] = err_sum.apply(_etiquette, axis=1)

                    err_sum["Évolution SOC"] = err_sum.apply(_format_soc_evolution, axis=1)

                    if "ID" not in err_sum.columns:
                        st.warning("Colonne 'ID' absente : les liens Elto ne seront pas affichés.")
                        err_sum["ELTO"] = ""
                    else:
                        err_sum["ELTO"] = BASE_CHARGE_URL + err_sum["ID"].astype(str).str.strip()

                    if "Vehicle" not in err_sum.columns and "ID" in err_sum.columns:
                        if (
                            "charges_mac" in locals()
                            and isinstance(charges_mac, pd.DataFrame)
                            and not charges_mac.empty
                            and {"ID", "Vehicle"}.issubset(charges_mac.columns)
                        ):
                            veh_map = charges_mac[["ID", "Vehicle"]].drop_duplicates("ID", keep="last")
                            err_sum = err_sum.merge(veh_map, on="ID", how="left")
                    cols_aff = [
                        "Site",
                        "Datetime start",
                        "Datetime end",
                        "Energy (Kwh)",
                        "MAC Address",
                        "Vehicle",
                        "Erreur",
                        "Évolution SOC",
                        "ELTO",
                    ]
                    cols_aff = [c for c in cols_aff if c in err_sum.columns]

                    out = err_sum[cols_aff].copy()
                    if "Datetime start" in out.columns:
                        out = out.sort_values("Datetime start", ascending=False)

                    out.insert(0, "#", range(1, len(out) + 1))

                    st.data_editor(
                        out,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "ELTO": st.column_config.LinkColumn(
                                "Lien Elto",
                                help="Ouvrir la session dans Elto",
                                display_text="🔗 Ouvrir"
                            ),
                            "Datetime start": st.column_config.DatetimeColumn("Start time", format="YYYY-MM-DD HH:mm:ss"),
                            "Datetime end":   st.column_config.DatetimeColumn("End time",   format="YYYY-MM-DD HH:mm:ss"),
                            "Energy (Kwh)":   st.column_config.NumberColumn("Energy (kWh)", format="%.3f"),
                            "MAC Address":    st.column_config.TextColumn("MacAdress"),
                            "Erreur":         st.column_config.TextColumn("Error etiquette"),
                            "Évolution SOC":  st.column_config.TextColumn("Evolution SOC"),
                        }
                    )

    st.divider()
    # Histogramme des occurrences par véhicule
    if not code_raw_tab.strip():
        st.info("⏳ Saisissez un ou plusieurs codes dans le filtre")
    else:
        if "Vehicle" in err_sum.columns:
            occ_vehicle = (
                err_sum.groupby("Vehicle")
                .size()
                .reset_index(name="Occurrences")
            )

            dfv_local = None
            if "dfv" in locals() and isinstance(dfv, pd.DataFrame):
                dfv_local = dfv
            elif "charges_mac" in locals() and isinstance(charges_mac, pd.DataFrame):
                dfv_local = charges_mac

            if isinstance(dfv_local, pd.DataFrame) and not dfv_local.empty:
                dfv_local = dfv_local.copy()

                if "Datetime start" in dfv_local.columns:
                    dfv_local["Datetime start"] = pd.to_datetime(dfv_local["Datetime start"], errors="coerce")

                vehicle_col = "Vehicle" if "Vehicle" in dfv_local.columns else None
                site_col = "Site" if "Site" in dfv_local.columns else (
                    "Name Project" if "Name Project" in dfv_local.columns else None
                )

                if vehicle_col is not None:
                    veh_series = dfv_local[vehicle_col].astype(str).str.strip()
                    veh_series = veh_series.replace({
                        "": np.nan,
                        "nan": np.nan,
                        "none": np.nan,
                        "NULL": np.nan,
                    }, regex=False)
                    dfv_local[vehicle_col] = veh_series

                if vehicle_col is not None:
                    mask_dfv = pd.Series(True, index=dfv_local.index)

                    if site_col and st.session_state.get("site_sel"):
                        mask_dfv &= dfv_local[site_col].isin(st.session_state.site_sel)

                    if (
                        "Datetime start" in dfv_local.columns
                        and st.session_state.get("d1")
                        and st.session_state.get("d2")
                    ):
                        d1 = pd.Timestamp(st.session_state.get("d1"))
                        d2 = pd.Timestamp(st.session_state.get("d2")) + pd.Timedelta(days=1)
                        mask_dfv &= dfv_local["Datetime start"].ge(d1)
                        mask_dfv &= dfv_local["Datetime start"].lt(d2)

                    dfv_filtered = dfv_local.loc[mask_dfv & dfv_local[vehicle_col].notna()].copy()
                    if not dfv_filtered.empty:
                        dfv_filtered[vehicle_col] = dfv_filtered[vehicle_col].astype(str).str.strip()
                        dfv_filtered = dfv_filtered[dfv_filtered[vehicle_col].str.len().gt(0)]
                        dfv_filtered = dfv_filtered[dfv_filtered[vehicle_col].str.lower() != "unknown"]

                    if not dfv_filtered.empty:
                        total_charges = (
                            dfv_filtered.groupby(vehicle_col)
                            .size()
                            .reset_index(name="Total Charges")
                        )

                        occ_vehicle = occ_vehicle.merge(
                            total_charges,
                            on="Vehicle",
                            how="left",
                        )

            if "Total Charges" in occ_vehicle.columns:
                occ_vehicle["Total Charges"] = occ_vehicle["Total Charges"].fillna(0).astype(int)
                occ_vehicle["Vehicle Label"] = occ_vehicle.apply(
                    lambda row: f"{row['Vehicle']} ({row['Total Charges']})",
                    axis=1
                )
            else:
                occ_vehicle["Vehicle Label"] = occ_vehicle["Vehicle"].astype(str)

            occ_vehicle = occ_vehicle.sort_values("Occurrences", ascending=True)

            bar_kwargs = dict(
                data_frame=occ_vehicle,
                x="Occurrences",
                y="Vehicle Label",
                orientation="h",
                title="Occurrences par véhicule (avec total charges)",
                labels={"Occurrences": "Nb d'occurrences", "Vehicle Label": "Véhicule"},
            )
            if "Total Charges" in occ_vehicle.columns:
                bar_kwargs["hover_data"] = {"Occurrences": True, "Total Charges": True}

            fig_vehicle = px.bar(**bar_kwargs)
            st.plotly_chart(fig_vehicle, use_container_width=True)
        else:
            st.info("Colonne 'Vehicle' absente pour compter les occurrences par véhicule.")

    st.divider()

    # Histogramme temporel par mois
    if not code_raw_tab.strip():
        st.info("⏳ Saisissez un ou plusieurs codes dans le filtre")
    else:
        if "Datetime start" in err_sum.columns and "Site" in err_sum.columns:
            err_sum["Datetime start"] = pd.to_datetime(err_sum["Datetime start"], errors="coerce")
            err_sum["month"] = err_sum["Datetime start"].dt.to_period("M").astype(str)
            fig_month = px.histogram(
                err_sum,
                x="month",
                color="Site",
                barmode="group",
                labels={"month": "Mois", "count": "Occurrences"},
                title="Histogramme mensuel des occurrences par site"
            )
            fig_month.update_layout(
                plot_bgcolor="#f9f9f9",
                bargap=0.2,
                xaxis=dict(type="category", tickangle=-45),
                yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.05)")
            )
            st.plotly_chart(fig_month, use_container_width=True)
        else:
            st.info("Colonnes 'Datetime start' ou 'Site' manquantes pour tracer l'histogramme.")
    # Nombre d'occurrences par site et PDC
    if not code_raw_tab.strip():
        st.info("⏳ Saisissez un ou plusieurs codes dans le filtre")
    else:
        if {"Site", "PDC"}.issubset(err_sum.columns):
            occ = (
                err_sum.groupby(["Site", "PDC"])
                    .size()
                    .reset_index(name="Occurrences")
                    .sort_values("Occurrences", ascending=False)
            )
            st.markdown("### Nombre d'occurrences par site et PDC")
            st.dataframe(occ, use_container_width=True, hide_index=True)
        else:
            st.info("Colonnes 'Site' ou 'PDC' absentes pour compter les occurrences.")
    st.divider()
    from datetime import date, timedelta

    if not code_raw_tab.strip():
        st.info("⏳ Saisissez un ou plusieurs codes dans le filtre")
    else:
        st.markdown("### Zoom site/mois/jour")

        if err_sum.empty or not {"Site", "Datetime start", "PDC"}.issubset(err_sum.columns):
            st.info("Données insuffisantes.")
        else:
            err_sum["Datetime start"] = pd.to_datetime(err_sum["Datetime start"], errors="coerce")
            err_sum = err_sum.dropna(subset=["Datetime start"])

            site_focus = st.selectbox("Site", sorted(err_sum["Site"].dropna().unique()), key="site_focus_tab9")
            df_site = err_sum[err_sum["Site"] == site_focus].copy()
            df_site["month"] = df_site["Datetime start"].dt.to_period("M").astype(str)

            months = sorted(df_site["month"].unique().tolist())
            if months:
                month_focus = st.selectbox("Mois", months, key="month_focus_tab9")
                df_month = df_site[df_site["month"] == month_focus].copy()
                df_month["day"] = df_month["Datetime start"].dt.date
                pdc_unique = sorted(df_month["PDC"].dropna().unique())
                palette = px.colors.qualitative.Plotly
                if not pdc_unique:
                    st.info("Aucun PDC disponible pour le filtre sélectionné.")
                else:
                    color_map = {pdc: palette[i % len(palette)] for i, pdc in enumerate(pdc_unique)}
                    year, month = map(int, month_focus.split("-"))
                    first_day = date(year, month, 1)
                    last_day = (first_day.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                    all_days = pd.date_range(first_day, last_day)

                    df_day_count = df_month.groupby(["day", "PDC"]).size().reset_index(name="Occurrences")
                    df_pivot = df_day_count.pivot(index="day", columns="PDC", values="Occurrences").reindex(all_days, fill_value=0)
                    df_full = df_pivot.stack().reset_index()
                    df_full.columns = ["day", "PDC", "Occurrences"]
                    df_full["day"] = pd.to_datetime(df_full["day"])

                    pdc_unique = sorted(df_full["PDC"].dropna().unique())

                    fig_jours = go.Figure()
                    for i, pdc in enumerate(pdc_unique):
                        df_sub = df_full[df_full["PDC"] == pdc]
                        df_sub = df_sub.sort_values("day")
                        fig_jours.add_trace(go.Bar(
                            x=df_sub["day"],
                            y=df_sub["Occurrences"],
                            name=str(pdc),
                            marker_color=color_map.get(pdc, palette[i % len(palette)])
                        ))

                    fig_jours.update_layout(
                        barmode='group',
                        title=f"{site_focus} — {month_focus} : Occurrences par jour et PDC",
                        xaxis=dict(title="Jour", tickangle=-45, type="date", tickformat="%Y-%m-%d"),
                        yaxis=dict(title="Occurrences"),
                        legend_title_text="PDC",
                        bargap=0.15,
                        showlegend=True
                    )

                    st.plotly_chart(fig_jours, use_container_width=True)
                    jours = sorted(df_month["day"].unique())
                    if jours:
                        jour_focus = st.selectbox("Jour", options=jours, key="day_focus_tab9")
                        df_jour = df_month[df_month["day"] == jour_focus].copy()
                        df_jour["hour"] = df_jour["Datetime start"].dt.hour

                        all_hours = list(range(24))
                        df_hour_count = df_jour.groupby(["hour", "PDC"]).size().reset_index(name="Occurrences")
                        df_pivot_hour = df_hour_count.pivot(index="hour", columns="PDC", values="Occurrences").reindex(all_hours, fill_value=0)
                        df_full_hour = df_pivot_hour.stack().reset_index()
                        df_full_hour.columns = ["hour", "PDC", "Occurrences"]

                        pdc_unique_hour = sorted(df_full_hour["PDC"].dropna().unique())

                        fig_heures = go.Figure()
                        for i, pdc in enumerate(pdc_unique_hour):
                            df_sub = df_full_hour[df_full_hour["PDC"] == pdc].sort_values("hour")
                            fig_heures.add_trace(go.Bar(
                                x=df_sub["hour"],
                                y=df_sub["Occurrences"],
                                name=str(pdc),
                                marker_color=color_map.get(pdc, palette[i % len(palette)])
                            ))

                        fig_heures.update_layout(
                            barmode='group',
                            title=f"{site_focus} — {jour_focus} : Occurrences par heure et PDC",
                            xaxis=dict(title="Heure", dtick=1, tickmode="linear"),
                            yaxis=dict(title="Occurrences"),
                            legend_title_text="PDC",
                            bargap=0.15,
                            showlegend=True
                        )
                        st.plotly_chart(fig_heures, use_container_width=True)

"""

def render():
    ctx = get_context()
    globals_dict = {"np": np, "pd": pd, "px": px, "go": go, "st": st, "BASE_CHARGE_URL": BASE_CHARGE_URL}
    local_vars = dict(ctx.__dict__)
    local_vars.setdefault('plot', getattr(ctx, 'plot', None))
    local_vars.setdefault('hide_zero_labels', getattr(ctx, 'hide_zero_labels', None))
    local_vars.setdefault('with_charge_link', getattr(ctx, 'with_charge_link', None))
    local_vars.setdefault('evi_counts_pivot', getattr(ctx, 'evi_counts_pivot', None))
    tables_ref = local_vars.get('tables')
    if isinstance(tables_ref, dict):
        local_vars.setdefault('charges_mac', tables_ref.get('charges_mac', pd.DataFrame()))
        local_vars.setdefault('sessions', tables_ref.get('sessions', pd.DataFrame()))
    local_vars = {k: v for k, v in local_vars.items() if v is not None}
    exec(TAB_CODE, globals_dict, local_vars)
