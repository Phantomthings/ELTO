"""
Router pour les sessions de charge
Inclut: Générale, Détails Site, Analyse Erreur
"""

from fastapi import APIRouter, Request, Query
from fastapi.templating import Jinja2Templates
from datetime import date
from typing import Any
from urllib.parse import urlencode
import pandas as pd
import numpy as np

from db import query_df
from routers.filters import MOMENT_ORDER

EVI_MOMENT = "EVI Status during error"
EVI_CODE = "EVI Error Code"
DS_PC = "Downstream Code PC"

PHASE_MAP = {
    "Avant charge": {"Init", "Lock Connector", "CableCheck"},
    "Charge": {"Charge"},
    "Fin de charge": {"Fin de charge"},
    "Unknown": {"Unknown"},
}

SITE_COLOR_PALETTE = [
    "#f97316",
    "#a855f7",
    "#eab308",
    "#ef4444",
    "#6366f1",
    "#3b82f6",
    "#06b6d4",
    "#10b981",
    "#0ea5e9",
    "#f43f5e",
]

router = APIRouter(tags=["sessions"])
templates = Jinja2Templates(directory="templates")


def _build_conditions(sites: str, date_debut: date | None, date_fin: date | None, table_alias: str | None = None):
    conditions = ["1=1"]
    params = {}

    datetime_col = f"{table_alias}.`Datetime start`" if table_alias else "`Datetime start`"
    site_col = f"{table_alias}.Site" if table_alias else "Site"

    if date_debut:
        conditions.append(f"{datetime_col} >= :date_debut")
        params["date_debut"] = str(date_debut)
    if date_fin:
        conditions.append(f"{datetime_col} < DATE_ADD(:date_fin, INTERVAL 1 DAY)")
        params["date_fin"] = str(date_fin)
    if sites:
        site_list = [s.strip() for s in sites.split(",") if s.strip()]
        if site_list:
            placeholders = ",".join([f":site_{i}" for i in range(len(site_list))])
            conditions.append(f"{site_col} IN ({placeholders})")
            for i, s in enumerate(site_list):
                params[f"site_{i}"] = s

    return " AND ".join(conditions), params


def _apply_status_filters(df: pd.DataFrame, error_type_list: list[str], moment_list: list[str]) -> pd.DataFrame:
    df["is_ok"] = pd.to_numeric(df["state"], errors="coerce").fillna(0).astype(int).eq(0)
    mask_nok = ~df["is_ok"]
    mask_type = (
        df["type_erreur"].isin(error_type_list)
        if error_type_list and "type_erreur" in df.columns
        else pd.Series(True, index=df.index)
    )
    mask_moment = (
        df["moment"].isin(moment_list)
        if moment_list and "moment" in df.columns
        else pd.Series(True, index=df.index)
    )
    df["is_ok_filt"] = np.where(mask_nok & mask_type & mask_moment, False, True)
    return df


def _map_moment_label(val: int) -> str:
    try:
        v = int(val)
    except Exception:
        return "Unknown"

    if v == 0:
        return "Fin de charge"
    if 1 <= v <= 2:
        return "Init"
    if 4 <= v <= 6:
        return "Lock Connector"
    if v == 7:
        return "CableCheck"
    if v == 8:
        return "Charge"
    if v > 8:
        return "Fin de charge"
    return "Unknown"


def _map_phase_label(moment: str | int | float | None) -> str:
    if pd.isna(moment):
        return "Unknown"

    if isinstance(moment, (list, tuple, set)):
        for value in moment:
            mapped = _map_phase_label(value)
            if mapped != "Unknown":
                return mapped
        return "Unknown"

    moment_str = str(moment)

    for phase, moments in PHASE_MAP.items():
        if moment_str in moments:
            return phase

    return "Unknown"


def _build_pivot_table(detail_df: pd.DataFrame, by_site: pd.DataFrame) -> dict[str, Any]:
    if detail_df.empty:
        return {"columns": [], "rows": []}

    if "Site" not in detail_df.columns:
        return {"columns": [], "rows": []}

    pivot_df = detail_df.assign(
        _site=detail_df["Site"],
        _type=detail_df.get("type", ""),
        _moment=detail_df["moment_label"],
        _step=detail_df["step"],
        _code=detail_df["code"],
    )

    pivot_table = pd.pivot_table(
        pivot_df,
        index="_site",
        columns=["_type", "_moment", "_step", "_code"],
        aggfunc="size",
        fill_value=0,
    ).sort_index(axis=1)

    pivot_table = pivot_table.reset_index()

    if isinstance(pivot_table.columns, pd.MultiIndex):
        pivot_table.columns = [
            col[0] if col[0] in ["_site", "Site"] else " | ".join(str(c) for c in col if c).strip()
            for col in pivot_table.columns
        ]

    if "_site" in pivot_table.columns:
        pivot_table = pivot_table.rename(columns={"_site": "Site"})

    new_columns = []
    for col in pivot_table.columns:
        if col == "Site":
            new_columns.append("Site")
        elif isinstance(col, tuple):
            new_columns.append(" | ".join(map(str, col)).strip())
        else:
            new_columns.append(str(col))
    pivot_table.columns = new_columns

    if "Site" not in pivot_table.columns:
        return {"columns": [], "rows": []}

    if "Site" not in by_site.columns:
        by_site = by_site.reset_index()

    pivot_table = pivot_table.merge(
        by_site[["Site", "Total_Charges"]].rename(columns={"Total_Charges": "Total Charges"}),
        on="Site",
        how="left",
    )

    ordered_columns = ["Site", "Total Charges"] + [
        col for col in pivot_table.columns if col not in {"Site", "Total Charges"}
    ]
    pivot_table = pivot_table[ordered_columns].fillna(0)

    numeric_cols = [col for col in pivot_table.columns if col != "Site"]
    pivot_table[numeric_cols] = pivot_table[numeric_cols].astype(int)

    return {
        "columns": pivot_table.columns.tolist(),
        "rows": pivot_table.to_dict("records"),
    }


def _format_soc(s0, s1):
    if pd.notna(s0) and pd.notna(s1):
        try:
            return f"{int(round(s0))}% → {int(round(s1))}%"
        except Exception:
            return ""
    return ""


def _prepare_query_params(request: Request) -> str:
    allowed = {"sites", "date_debut", "date_fin", "error_types", "moments"}
    data = {k: v for k, v in request.query_params.items() if k in allowed and v}
    return urlencode(data)


# ============================================================================
# GENERALE
# ============================================================================

@router.get("/sessions/general")
async def get_sessions_general(
    request: Request,
    sites: str = Query(default=""),
    date_debut: date = Query(default=None),
    date_fin: date = Query(default=None),
    error_types: str = Query(default=""),
    moments: str = Query(default=""),
):
    error_type_list = [e.strip() for e in error_types.split(",") if e.strip()] if error_types else []
    moment_list = [m.strip() for m in moments.split(",") if m.strip()] if moments else []

    where_clause, params = _build_conditions(sites, date_debut, date_fin)

    sql = f"""
        SELECT
            Site,
            PDC,
            `State of charge(0:good, 1:error)` as state,
            type_erreur,
            moment
        FROM kpi_sessions
        WHERE {where_clause}
    """

    df = query_df(sql, params)

    if df.empty:
        return templates.TemplateResponse(
            "partials/sessions_general.html",
            {
                "request": request,
                "total": 0,
                "ok": 0,
                "nok": 0,
                "taux_reussite": 0,
                "taux_echec": 0,
                "recap_columns": [],
                "recap_rows": [],
                "moment_distribution": [],
                "moment_total_errors": 0,
                "error_type_distribution": [],
                "error_type_total": 0,
                "site_success_cards": [],
                "site_success_bars": [],
            },
        )

    df = _apply_status_filters(df, error_type_list, moment_list)

    total = len(df)
    ok = int(df["is_ok_filt"].sum())
    nok = total - ok
    taux_reussite = round(ok / total * 100, 1) if total else 0
    taux_echec = round(nok / total * 100, 1) if total else 0

    df["PDC"] = df.get("PDC", "").astype(str)

    stats_site = (
        df.groupby("Site")
        .agg(
            total=("is_ok_filt", "count"),
            ok=("is_ok_filt", "sum"),
        )
        .reset_index()
    )
    stats_site["nok"] = stats_site["total"] - stats_site["ok"]
    stats_site["taux_ok"] = np.where(
        stats_site["total"] > 0,
        (stats_site["ok"] / stats_site["total"] * 100).round(1),
        0,
    )

    site_success_cards = [
        {
            "site": row["Site"],
            "ok": int(row["ok"]),
            "total": int(row["total"]),
            "taux_ok": float(row["taux_ok"]),
            "color": SITE_COLOR_PALETTE[idx % len(SITE_COLOR_PALETTE)],
        }
        for idx, row in stats_site.sort_values("Site").iterrows()
    ]

    site_success_bars = [
        {
            "site": row["Site"],
            "taux_ok": float(row["taux_ok"]),
            "total": int(row["total"]),
            "color": SITE_COLOR_PALETTE[idx % len(SITE_COLOR_PALETTE)],
        }
        for idx, row in stats_site.sort_values("taux_ok", ascending=False).iterrows()
    ]

    stats_pdc = (
        df.groupby(["Site", "PDC"])
        .agg(total=("is_ok_filt", "count"), ok=("is_ok_filt", "sum"))
        .reset_index()
    )
    stats_pdc["nok"] = stats_pdc["total"] - stats_pdc["ok"]
    stats_pdc["taux_ok"] = np.where(
        stats_pdc["total"] > 0,
        (stats_pdc["ok"] / stats_pdc["total"] * 100).round(1),
        0,
    )

    stat_global = stats_site.rename(columns={"Site": "Site", "total": "Total", "ok": "Total_OK"})
    stat_global["Total_NOK"] = stat_global["Total"] - stat_global["Total_OK"]
    stat_global["% OK"] = np.where(
        stat_global["Total"] > 0,
        (stat_global["Total_OK"] / stat_global["Total"] * 100).round(2),
        0,
    )
    stat_global["% NOK"] = np.where(
        stat_global["Total"] > 0,
        (stat_global["Total_NOK"] / stat_global["Total"] * 100).round(2),
        0,
    )

    err = df[~df["is_ok_filt"]].copy()

    recap_columns: list[str] = []
    recap_rows: list[dict] = []
    moment_distribution = []
    moment_total_errors = 0
    error_type_distribution: list[dict[str, int | float | str]] = []
    error_type_total = 0

    if not err.empty:
        err_grouped = (
            err.groupby(["Site", "moment"])
            .size()
            .reset_index(name="Nb")
            .pivot(index="Site", columns="moment", values="Nb")
            .fillna(0)
            .astype(int)
            .reset_index()
        )

        err_pdc_grouped = (
            err.groupby(["Site", "PDC", "moment"])
            .size()
            .reset_index(name="Nb")
            .pivot(index=["Site", "PDC"], columns="moment", values="Nb")
            .fillna(0)
            .astype(int)
            .reset_index()
        )

        err_moment_cols = [c for c in err_grouped.columns if c not in ["Site"]]
        err_pdc_moment_cols = [c for c in err_pdc_grouped.columns if c not in ["Site", "PDC"]]

        moment_cols = [m for m in MOMENT_ORDER if m in err_moment_cols or m in err_pdc_moment_cols]
        extra_moment_cols = [c for c in err_moment_cols + err_pdc_moment_cols if c not in moment_cols]
        moment_cols += [c for c in extra_moment_cols if c not in moment_cols]

        recap = (
            stat_global
            .merge(err_grouped, on="Site", how="left")
            .fillna(0)
            .sort_values("Total_NOK", ascending=False)
            .reset_index(drop=True)
        )

        recap_columns = [
            "Site / PDC",
            "Total",
            "Total_OK",
            "Total_NOK",
        ] + moment_cols + ["% OK", "% NOK"]

        recap["Site / PDC"] = recap["Site"]

        for col in moment_cols:
            if col not in recap.columns:
                recap[col] = 0

        numeric_moment_cols = [c for c in moment_cols if c in recap.columns]
        if numeric_moment_cols:
            recap[numeric_moment_cols] = recap[numeric_moment_cols].astype(int)

        pdc_recap = (
            stats_pdc.rename(
                columns={
                    "total": "Total",
                    "ok": "Total_OK",
                    "nok": "Total_NOK",
                    "taux_ok": "% OK",
                }
            )
            .assign(**{"% NOK": lambda d: np.where(d["Total"] > 0, (d["Total_NOK"] / d["Total"] * 100).round(2), 0)})
            .merge(err_pdc_grouped, on=["Site", "PDC"], how="left")
            .fillna(0)
        )

        numeric_moment_cols_pdc = [c for c in moment_cols if c in pdc_recap.columns]
        if numeric_moment_cols_pdc:
            pdc_recap[numeric_moment_cols_pdc] = pdc_recap[numeric_moment_cols_pdc].astype(int)

        for col in moment_cols:
            if col not in pdc_recap.columns:
                pdc_recap[col] = 0

        pdc_recap["Site / PDC"] = "↳ PDC " + pdc_recap["PDC"].astype(str)
        pdc_recap_display = pdc_recap[[c for c in recap_columns if c in pdc_recap.columns]].copy()

        recap_rows = []
        for _, row in recap.iterrows():
            row_dict = row.to_dict()
            label = row_dict.get("Site / PDC", "")
            row_dict["Site / PDC"] = f"{label} (Total)" if label else label
            row_dict.update({"row_type": "site", "site_key": row_dict.get("Site", "")})
            recap_rows.append(row_dict)

            site_pdcs = pdc_recap[pdc_recap["Site"].eq(row["Site"])].copy()
            site_pdcs = site_pdcs.sort_values("Total_NOK", ascending=False)

            for _, pdc_row in site_pdcs.iterrows():
                pdc_dict = pdc_row.to_dict()
                display_dict = {k: pdc_dict.get(k) for k in pdc_recap_display.columns}
                display_dict.update({"row_type": "pdc", "site_key": row_dict.get("Site", "")})
                recap_rows.append(display_dict)

        counts_moment = (
            err.groupby("moment")
            .size()
            .reindex(MOMENT_ORDER, fill_value=0)
            .reset_index(name="count")
        )
        counts_moment = counts_moment[counts_moment["count"] > 0]

        total_err = len(err)
        moment_total_errors = int(total_err)
        moment_distribution = [
            {
                "moment": row["moment"],
                "count": int(row["count"]),
                "percent": round(row["count"] / total_err * 100, 1) if total_err else 0,
            }
            for _, row in counts_moment.iterrows()
        ]

        error_type_order = ["Erreur_EVI", "Erreur_DownStream", "Erreur_Unknow_S"]
        error_type_labels = {
            "Erreur_EVI": "EVI",
            "Erreur_DownStream": "Downstream",
            "Erreur_Unknow_S": "Erreur_Unknow_S",
        }

        type_counts = (
            err[err["type_erreur"].isin(error_type_order)]
            .groupby("type_erreur")
            .size()
            .reindex(error_type_order, fill_value=0)
            .reset_index(name="count")
        )

        error_type_total = int(type_counts["count"].sum())

        error_type_distribution = [
            {
                "type_erreur": row["type_erreur"],
                "label": error_type_labels.get(row["type_erreur"], row["type_erreur"]),
                "count": int(row["count"]),
                "percent": round(row["count"] / error_type_total * 100, 1) if error_type_total else 0,
            }
            for _, row in type_counts.iterrows()
            if row["count"] > 0
        ]

    return templates.TemplateResponse(
        "partials/sessions_general.html",
        {
            "request": request,
            "total": total,
            "ok": ok,
            "nok": nok,
            "taux_reussite": taux_reussite,
            "taux_echec": taux_echec,
            "recap_columns": recap_columns,
            "recap_rows": recap_rows,
            "moment_distribution": moment_distribution,
            "moment_total_errors": moment_total_errors,
            "error_type_distribution": error_type_distribution,
            "error_type_total": error_type_total,
            "site_success_cards": site_success_cards,
            "site_success_bars": site_success_bars,
        },
    )


# ============================================================================
# ANALYSE ERREUR
# ============================================================================

@router.get("/sessions/error-analysis")
async def get_error_analysis(
    request: Request,
    sites: str = Query(default=""),
    date_debut: date = Query(default=None),
    date_fin: date = Query(default=None),
    error_types: str = Query(default=""),
    moments: str = Query(default=""),
):
    error_type_list = [e.strip() for e in error_types.split(",") if e.strip()] if error_types else []
    moment_list = [m.strip() for m in moments.split(",") if m.strip()] if moments else []

    where_clause, params = _build_conditions(sites, date_debut, date_fin, table_alias="k")

    sql = f"""
        SELECT
            k.Site,
            k.`State of charge(0:good, 1:error)` as state,
            k.type_erreur,
            k.moment,
            k.moment_avancee,
            k.`{EVI_MOMENT}`,
            k.`{EVI_CODE}`,
            k.`{DS_PC}`
        FROM kpi_sessions k
        WHERE {where_clause}
    """

    df = query_df(sql, params)

    if df.empty:
        return templates.TemplateResponse(
            "partials/error_analysis.html",
            {"request": request, "no_data": True},
        )

    df["is_ok"] = pd.to_numeric(df["state"], errors="coerce").fillna(0).astype(int).eq(0)
    df = _apply_status_filters(df, error_type_list, moment_list)
    df["Site"] = df.get("Site", "").fillna("")

    err = df[~df["is_ok_filt"]].copy()

    if err.empty:
        return templates.TemplateResponse(
            "partials/error_analysis.html",
            {"request": request, "no_errors": True},
        )

    evi_step = pd.to_numeric(err.get(EVI_MOMENT, pd.Series(np.nan, index=err.index)), errors="coerce")
    evi_code = pd.to_numeric(err.get(EVI_CODE, pd.Series(np.nan, index=err.index)), errors="coerce").fillna(0).astype(int)
    ds_pc = pd.to_numeric(err.get(DS_PC, pd.Series(np.nan, index=err.index)), errors="coerce").fillna(0).astype(int)
    moment_raw = err.get("moment", pd.Series(None, index=err.index))

    def resolve_moment_label(idx: int) -> str:
        label = None
        step_val = evi_step.loc[idx] if idx in evi_step.index else np.nan
        raw_val = moment_raw.loc[idx] if idx in moment_raw.index else None

        if pd.notna(step_val):
            label = _map_moment_label(step_val)
        if (not label or label == "Unknown") and isinstance(raw_val, str) and raw_val.strip():
            label = raw_val.strip()
        return label or "Unknown"

    err["moment_label"] = [resolve_moment_label(i) for i in err.index]

    sub_evi_mask = (ds_pc.eq(8192)) | (ds_pc.eq(0) & evi_code.ne(0))
    sub_ds_mask = ds_pc.ne(0) & ds_pc.ne(8192)

    sub_evi = err.loc[sub_evi_mask].copy()
    sub_evi["step"] = evi_step.loc[sub_evi.index]
    sub_evi["code"] = evi_code.loc[sub_evi.index]
    sub_evi["type"] = "Erreur_EVI"

    sub_ds = err.loc[sub_ds_mask].copy()
    sub_ds["step"] = evi_step.loc[sub_ds.index]
    sub_ds["code"] = ds_pc.loc[sub_ds.index]
    sub_ds["type"] = "Erreur_DownStream"

    evi_moment_code: list[dict[str, Any]] = []
    evi_moment_code_site: list[dict[str, Any]] = []
    if not sub_evi.empty:
        evi_moment_code_df = (
            sub_evi.groupby(["moment_label", "step", "code"])
            .size()
            .reset_index(name="Somme de Charge_NOK")
            .sort_values("Somme de Charge_NOK", ascending=False)
        )
        evi_total = int(evi_moment_code_df["Somme de Charge_NOK"].sum())
        total_row = pd.DataFrame(
            [
                {
                    "moment_label": "Total",
                    "step": "",
                    "code": "",
                    "Somme de Charge_NOK": evi_total,
                }
            ]
        )
        evi_moment_code_df = pd.concat([evi_moment_code_df, total_row], ignore_index=True)
        evi_moment_code_df.rename(
            columns={"moment_label": "Moment", "step": "Step", "code": "Code"}, inplace=True
        )
        evi_moment_code = evi_moment_code_df.to_dict("records")

        evi_moment_code_site_df = (
            sub_evi.groupby(["Site", "moment_label", "step", "code"])
            .size()
            .reset_index(name="Somme de Charge_NOK")
            .sort_values(["Site", "Somme de Charge_NOK"], ascending=[True, False])
        )
        evi_moment_code_site_df.rename(
            columns={"moment_label": "Moment", "step": "Step", "code": "Code"}, inplace=True
        )
        evi_moment_code_site = evi_moment_code_site_df.to_dict("records")

    ds_moment_code: list[dict[str, Any]] = []
    ds_moment_code_site: list[dict[str, Any]] = []
    if not sub_ds.empty:
        ds_moment_code_df = (
            sub_ds.groupby(["moment_label", "step", "code"])
            .size()
            .reset_index(name="Somme de Charge_NOK")
            .sort_values("Somme de Charge_NOK", ascending=False)
        )
        ds_total = int(ds_moment_code_df["Somme de Charge_NOK"].sum())
        ds_total_row = pd.DataFrame(
            [
                {
                    "moment_label": "Total",
                    "step": "",
                    "code": "",
                    "Somme de Charge_NOK": ds_total,
                }
            ]
        )
        ds_moment_code_df = pd.concat([ds_moment_code_df, ds_total_row], ignore_index=True)
        ds_moment_code_df.rename(
            columns={"moment_label": "Moment", "step": "Step", "code": "Code PC"}, inplace=True
        )
        ds_moment_code = ds_moment_code_df.to_dict("records")

        ds_moment_code_site_df = (
            sub_ds.groupby(["Site", "moment_label", "step", "code"])
            .size()
            .reset_index(name="Somme de Charge_NOK")
            .sort_values(["Site", "Somme de Charge_NOK"], ascending=[True, False])
        )
        ds_moment_code_site_df.rename(
            columns={"moment_label": "Moment", "step": "Step", "code": "Code PC"}, inplace=True
        )
        ds_moment_code_site = ds_moment_code_site_df.to_dict("records")

    by_site = (
        df.groupby("Site", as_index=False)
        .agg(Total_Charges=("is_ok_filt", "count"), Charges_OK=("is_ok_filt", "sum"))
        .assign(Charges_NOK=lambda d: d["Total_Charges"] - d["Charges_OK"])
    )

    all_err = pd.concat([sub_evi, sub_ds], ignore_index=True)

    top_all: list[dict[str, Any]] = []
    detail_all: list[dict[str, Any]] = []
    detail_all_pivot = {"columns": [], "rows": []}
    if not all_err.empty:
        tbl_all = (
            all_err.groupby(["moment_label", "step", "code", "type"])
            .size()
            .reset_index(name="Occurrences")
            .sort_values("Occurrences", ascending=False)
        )

        total_err = int(tbl_all["Occurrences"].sum()) or 1
        tbl_all["percent"] = (tbl_all["Occurrences"] / total_err * 100).round(2)

        top3_all = tbl_all.head(3)
        top_all = top3_all.to_dict("records")

        top_keys = top3_all[["moment_label", "step", "code", "type"]].to_records(index=False).tolist()
        detail_all_df = all_err[
            all_err[["moment_label", "step", "code", "type"]].apply(tuple, axis=1).isin(top_keys)
        ]
        detail_all = (
            detail_all_df.groupby(["moment_label", "step", "code", "type", "Site"])
            .size()
            .reset_index(name="Occurrences")
            .sort_values(
                ["type", "moment_label", "step", "code", "Occurrences"],
                ascending=[True, True, True, True, False],
            )
            .to_dict("records")
        )
        detail_all_pivot = _build_pivot_table(detail_all_df, by_site)

    top_evi: list[dict[str, Any]] = []
    detail_evi: list[dict[str, Any]] = []
    detail_evi_pivot = {"columns": [], "rows": []}
    if not sub_evi.empty:
        tbl_evi = (
            sub_evi.groupby(["moment_label", "step", "code"])
            .size()
            .reset_index(name="Occurrences")
            .sort_values("Occurrences", ascending=False)
        )
        total_evi = int(tbl_evi["Occurrences"].sum()) or 1
        tbl_evi["percent"] = (tbl_evi["Occurrences"] / total_evi * 100).round(2)
        top_evi = tbl_evi.head(3).to_dict("records")

        top_keys_evi = tbl_evi.head(3)[["moment_label", "step", "code"]].to_records(index=False).tolist()
        detail_evi_df = sub_evi[
            sub_evi[["moment_label", "step", "code"]].apply(tuple, axis=1).isin(top_keys_evi)
        ]
        detail_evi = (
            detail_evi_df.groupby(["moment_label", "step", "code", "Site"])
            .size()
            .reset_index(name="Occurrences")
            .sort_values(["moment_label", "step", "code", "Occurrences"], ascending=[True, True, True, False])
            .to_dict("records")
        )
        detail_evi_pivot = _build_pivot_table(detail_evi_df, by_site)

    top_ds: list[dict[str, Any]] = []
    detail_ds: list[dict[str, Any]] = []
    detail_ds_pivot = {"columns": [], "rows": []}
    if not sub_ds.empty:
        tbl_ds = (
            sub_ds.groupby(["moment_label", "step", "code"])
            .size()
            .reset_index(name="Occurrences")
            .sort_values("Occurrences", ascending=False)
        )
        total_ds = int(tbl_ds["Occurrences"].sum()) or 1
        tbl_ds["percent"] = (tbl_ds["Occurrences"] / total_ds * 100).round(2)
        top_ds = tbl_ds.head(3).to_dict("records")

        top_keys_ds = tbl_ds.head(3)[["moment_label", "step", "code"]].to_records(index=False).tolist()
        detail_ds_df = sub_ds[
            sub_ds[["moment_label", "step", "code"]].apply(tuple, axis=1).isin(top_keys_ds)
        ]
        detail_ds = (
            detail_ds_df.groupby(["moment_label", "step", "code", "Site"])
            .size()
            .reset_index(name="Occurrences")
            .sort_values(["moment_label", "step", "code", "Occurrences"], ascending=[True, True, True, False])
            .to_dict("records")
        )
        detail_ds_pivot = _build_pivot_table(detail_ds_df, by_site)

    err_phase = err.copy()
    err_phase["Phase"] = err_phase["moment_label"].map(_map_phase_label)

    err_by_phase = (
        err_phase.groupby(["Site", "Phase"])
        .size()
        .unstack("Phase", fill_value=0)
        .reset_index()
    )

    df_final = by_site.merge(err_by_phase, on="Site", how="left").fillna(0)

    for col in ["Avant charge", "Charge", "Fin de charge", "Unknown"]:
        if col not in df_final.columns:
            df_final[col] = 0

    df_final["% Réussite"] = np.where(
        df_final["Total_Charges"] > 0,
        (df_final["Charges_OK"] / df_final["Total_Charges"] * 100).round(2),
        0.0,
    )
    df_final["% Erreurs"] = np.where(
        df_final["Total_Charges"] > 0,
        (
            (
                df_final["Avant charge"]
                + df_final["Charge"]
                + df_final["Fin de charge"]
                + df_final["Unknown"]
            )
            / df_final["Total_Charges"]
            * 100
        ).round(2),
        0.0,
    )

    site_summary = df_final[
        [
            "Site",
            "Total_Charges",
            "Charges_OK",
            "Charges_NOK",
            "% Réussite",
            "% Erreurs",
            "Avant charge",
            "Charge",
            "Fin de charge",
            "Unknown",
        ]
    ].to_dict("records")

    error_type_counts: list[dict[str, Any]] = []
    err_nonempty = err.loc[err["type_erreur"].notna() & err["type_erreur"].ne("")].copy()
    if not err_nonempty.empty:
        counts_t = (
            err_nonempty.groupby("type_erreur")
            .size()
            .reset_index(name="Nb")
            .sort_values("Nb", ascending=False)
        )
        counts_t = pd.concat(
            [counts_t, pd.DataFrame([{"type_erreur": "Total", "Nb": int(counts_t["Nb"].sum())}])],
            ignore_index=True,
        )
        error_type_counts = counts_t.to_dict("records")

    moment_counts: list[dict[str, Any]] = []
    if "moment" in err.columns:
        counts_moment = (
            err.groupby("moment")
            .size()
            .reindex(MOMENT_ORDER, fill_value=0)
            .reset_index(name="Somme de Charge_NOK")
        )
        counts_moment = counts_moment[counts_moment["Somme de Charge_NOK"] > 0]
        if not counts_moment.empty:
            counts_moment = pd.concat(
                [
                    counts_moment,
                    pd.DataFrame(
                        [
                            {
                                "moment": "Total",
                                "Somme de Charge_NOK": int(counts_moment["Somme de Charge_NOK"].sum()),
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )
            moment_counts = counts_moment.to_dict("records")

    moment_adv_counts: list[dict[str, Any]] = []
    if "moment_avancee" in err.columns:
        counts_av = (
            err.groupby("moment_avancee")
            .size()
            .reset_index(name="Somme de Charge_NOK")
            .sort_values("Somme de Charge_NOK", ascending=False)
        )
        if not counts_av.empty:
            counts_av = pd.concat(
                [
                    counts_av,
                    pd.DataFrame(
                        [
                            {
                                "moment_avancee": "Total",
                                "Somme de Charge_NOK": int(counts_av["Somme de Charge_NOK"].sum()),
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )
            moment_adv_counts = counts_av.to_dict("records")

    err_evi = err[err["type_erreur"] == "Erreur_EVI"].copy()
    evi_moment_distribution: list[dict[str, Any]] = []
    evi_moment_adv_distribution: list[dict[str, Any]] = []
    if not err_evi.empty and "moment" in err_evi.columns:
        counts_moment = (
            err_evi.groupby("moment")
            .size()
            .reindex(MOMENT_ORDER, fill_value=0)
            .reset_index(name="Nb")
        )
        total_evi_err = int(counts_moment["Nb"].sum())
        if total_evi_err > 0:
            counts_moment["%"] = (counts_moment["Nb"] / total_evi_err * 100).round(2)
            counts_moment = counts_moment[counts_moment["Nb"] > 0]
            counts_moment = pd.concat(
                [
                    counts_moment,
                    pd.DataFrame(
                        [
                            {
                                "moment": "Total",
                                "Nb": total_evi_err,
                                "%": 100.0,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )
            evi_moment_distribution = counts_moment.to_dict("records")

        if "moment_avancee" in err_evi.columns:
            counts_ma = (
                err_evi.groupby("moment_avancee")
                .size()
                .reset_index(name="Nb")
                .sort_values("Nb", ascending=False)
            )
            if not counts_ma.empty:
                counts_ma = pd.concat(
                    [
                        counts_ma,
                        pd.DataFrame([
                            {"moment_avancee": "Total", "Nb": int(counts_ma["Nb"].sum())}
                        ]),
                    ],
                    ignore_index=True,
                )
                evi_moment_adv_distribution = counts_ma.to_dict("records")

    err_ds = err[err["type_erreur"] == "Erreur_DownStream"].copy()
    ds_moment_distribution: list[dict[str, Any]] = []
    ds_moment_adv_distribution: list[dict[str, Any]] = []
    if not err_ds.empty and "moment" in err_ds.columns:
        counts_moment_ds = (
            err_ds.groupby("moment")
            .size()
            .reindex(MOMENT_ORDER, fill_value=0)
            .reset_index(name="Nb")
        )
        total_ds_err = int(counts_moment_ds["Nb"].sum())
        if total_ds_err > 0:
            counts_moment_ds["%"] = (counts_moment_ds["Nb"] / total_ds_err * 100).round(2)
            counts_moment_ds = counts_moment_ds[counts_moment_ds["Nb"] > 0]
            counts_moment_ds = pd.concat(
                [
                    counts_moment_ds,
                    pd.DataFrame(
                        [
                            {
                                "moment": "Total",
                                "Nb": total_ds_err,
                                "%": 100.0,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )
            ds_moment_distribution = counts_moment_ds.to_dict("records")

        if "moment_avancee" in err_ds.columns:
            counts_ma_ds = (
                err_ds.groupby("moment_avancee")
                .size()
                .reset_index(name="Nb")
                .sort_values("Nb", ascending=False)
            )
            if not counts_ma_ds.empty:
                counts_ma_ds = pd.concat(
                    [
                        counts_ma_ds,
                        pd.DataFrame(
                            [
                                {
                                    "moment_avancee": "Total",
                                    "Nb": int(counts_ma_ds["Nb"].sum()),
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )
                ds_moment_adv_distribution = counts_ma_ds.to_dict("records")

    return templates.TemplateResponse(
        "partials/error_analysis.html",
        {
            "request": request,
            "top_all": top_all,
            "detail_all": detail_all,
            "detail_all_pivot": detail_all_pivot,
            "top_evi": top_evi,
            "detail_evi": detail_evi,
            "detail_evi_pivot": detail_evi_pivot,
            "top_ds": top_ds,
            "detail_ds": detail_ds,
            "detail_ds_pivot": detail_ds_pivot,
            "evi_moment_code": evi_moment_code,
            "evi_moment_code_site": evi_moment_code_site,
            "ds_moment_code": ds_moment_code,
            "ds_moment_code_site": ds_moment_code_site,
            "site_summary": site_summary,
            "error_type_counts": error_type_counts,
            "moment_counts": moment_counts,
            "moment_adv_counts": moment_adv_counts,
            "evi_moment_distribution": evi_moment_distribution,
            "evi_moment_adv_distribution": evi_moment_adv_distribution,
            "ds_moment_distribution": ds_moment_distribution,
            "ds_moment_adv_distribution": ds_moment_adv_distribution,
        },
    )


# ============================================================================
# DETAILS SITE
# ============================================================================

@router.get("/sessions/site-details")
async def get_sessions_site_details(
    request: Request,
    sites: str = Query(default=""),
    date_debut: date = Query(default=None),
    date_fin: date = Query(default=None),
    error_types: str = Query(default=""),
    moments: str = Query(default=""),
    site_focus: str = Query(default=""),
    pdc: str = Query(default=""),
):
    error_type_list = [e.strip() for e in error_types.split(",") if e.strip()] if error_types else []
    moment_list = [m.strip() for m in moments.split(",") if m.strip()] if moments else []

    where_clause, params = _build_conditions(sites, date_debut, date_fin)

    sql = f"""
        SELECT
            Site,
            PDC,
            ID,
            `Datetime start`,
            `Datetime end`,
            `Energy (Kwh)`,
            `MAC Address`,
            Vehicle,
            type_erreur,
            moment,
            moment_avancee,
            `SOC Start`,
            `SOC End`,
            `Downstream Code PC`,
            `EVI Error Code`,
            `State of charge(0:good, 1:error)` as state
        FROM kpi_sessions
        WHERE {where_clause}
    """

    df = query_df(sql, params)

    if df.empty:
        return templates.TemplateResponse(
            "partials/sessions_site_details.html",
            {
                "request": request,
                "site_options": [],
                "base_query": _prepare_query_params(request),
                "site_success_rate": 0.0,
                "site_total_charges": 0,
                "site_charges_ok": 0,
            },
        )

    df["PDC"] = df["PDC"].astype(str)
    df["is_ok"] = pd.to_numeric(df["state"], errors="coerce").fillna(0).astype(int).eq(0)

    mask_type = df["type_erreur"].isin(error_type_list) if error_type_list and "type_erreur" in df.columns else True
    mask_moment = df["moment"].isin(moment_list) if moment_list and "moment" in df.columns else True
    mask_nok = ~df["is_ok"]
    mask_filtered_error = mask_nok & mask_type & mask_moment
    df["is_ok_filt"] = np.where(mask_filtered_error, False, True)

    site_options = sorted(df["Site"].dropna().unique().tolist())
    site_value = site_focus if site_focus in site_options else (site_options[0] if site_options else "")

    df_site = df[df["Site"] == site_value].copy()
    if df_site.empty:
        return templates.TemplateResponse(
            "partials/sessions_site_details.html",
            {
                "request": request,
                "site_options": site_options,
                "site_focus": site_value,
                "pdc_options": [],
                "selected_pdc": [],
                "base_query": _prepare_query_params(request),
                "site_success_rate": 0.0,
                "site_total_charges": 0,
                "site_charges_ok": 0,
            },
        )

    pdc_options = sorted(df_site["PDC"].dropna().unique().tolist())
    selected_pdc = [p.strip() for p in pdc.split(",") if p.strip()] if pdc else pdc_options
    selected_pdc = [p for p in selected_pdc if p in pdc_options] or pdc_options

    df_site = df_site[df_site["PDC"].isin(selected_pdc)].copy()

    mask_type_site = (
        df_site["type_erreur"].isin(error_type_list)
        if error_type_list and "type_erreur" in df_site.columns
        else pd.Series(True, index=df_site.index)
    )
    mask_moment_site = (
        df_site["moment"].isin(moment_list)
        if moment_list and "moment" in df_site.columns
        else pd.Series(True, index=df_site.index)
    )
    df_filtered = df_site[mask_type_site & mask_moment_site].copy()

    for col in ["Datetime start", "Datetime end"]:
        if col in df_filtered.columns:
            df_filtered[col] = pd.to_datetime(df_filtered[col], errors="coerce")
    for col in ["Energy (Kwh)", "SOC Start", "SOC End"]:
        if col in df_filtered.columns:
            df_filtered[col] = pd.to_numeric(df_filtered[col], errors="coerce")

    err_rows = df_filtered[~df_filtered["is_ok"]].copy()
    err_rows["evolution_soc"] = err_rows.apply(lambda r: _format_soc(r.get("SOC Start"), r.get("SOC End")), axis=1)
    err_rows["elto"] = err_rows["ID"].apply(lambda x: f"https://elto.nidec-asi-online.com/Charge/detail?id={str(x).strip()}" if pd.notna(x) else "") if "ID" in err_rows.columns else ""
    err_display_cols = [
        "ID",
        "Datetime start",
        "Datetime end",
        "PDC",
        "Energy (Kwh)",
        "MAC Address",
        "Vehicle",
        "type_erreur",
        "moment",
        "evolution_soc",
        "elto",
    ]
    err_table = err_rows[err_display_cols].copy() if not err_rows.empty else pd.DataFrame(columns=err_display_cols)
    if "Datetime start" in err_table.columns:
        err_table = err_table.sort_values("Datetime start", ascending=False)

    ok_rows = df_filtered[df_filtered["is_ok"]].copy()
    ok_rows["evolution_soc"] = ok_rows.apply(lambda r: _format_soc(r.get("SOC Start"), r.get("SOC End")), axis=1)
    ok_rows["elto"] = ok_rows["ID"].apply(lambda x: f"https://elto.nidec-asi-online.com/Charge/detail?id={str(x).strip()}" if pd.notna(x) else "") if "ID" in ok_rows.columns else ""
    ok_display_cols = [
        "ID",
        "Datetime start",
        "Datetime end",
        "PDC",
        "Energy (Kwh)",
        "MAC Address",
        "Vehicle",
        "evolution_soc",
        "elto",
    ]
    ok_table = ok_rows[ok_display_cols].copy() if not ok_rows.empty else pd.DataFrame(columns=ok_display_cols)
    if "Datetime start" in ok_table.columns:
        ok_table = ok_table.sort_values("Datetime start", ascending=False)

    site_total_charges = int(df_site["is_ok_filt"].count())
    site_charges_ok = int(df_site["is_ok_filt"].sum())
    site_success_rate = round(site_charges_ok / site_total_charges * 100, 2) if site_total_charges else 0.0

    by_pdc = (
        df_site.groupby("PDC", as_index=False)
        .agg(Total_Charges=("is_ok_filt", "count"), Charges_OK=("is_ok_filt", "sum"))
        .assign(Charges_NOK=lambda d: d["Total_Charges"] - d["Charges_OK"])
    )
    by_pdc["% Réussite"] = np.where(
        by_pdc["Total_Charges"].gt(0),
        (by_pdc["Charges_OK"] / by_pdc["Total_Charges"] * 100).round(2),
        0.0,
    )
    by_pdc = by_pdc.sort_values(["% Réussite", "PDC"], ascending=[True, True])

    error_moment: list[dict] = []
    error_moment_grouped: list[dict] = []
    error_moment_adv: list[dict] = []
    error_type_distribution: list[dict] = []
    error_type_total = 0
    if not err_rows.empty:
        if "moment" in err_rows.columns:
            counts = err_rows.groupby("moment").size().reset_index(name="Nb")
            total = counts["Nb"].sum()
            if total:
                error_moment = (
                    counts.assign(percent=lambda d: (d["Nb"] / total * 100).round(2))
                    .sort_values("percent", ascending=False)
                    .to_dict("records")
                )
                error_moment_grouped = error_moment

        if "moment_avancee" in err_rows.columns:
            counts_adv = (
                err_rows.groupby("moment_avancee")
                .size()
                .reset_index(name="Nb")
                .sort_values("Nb", ascending=False)
            )

            total_adv = counts_adv["Nb"].sum()
            if total_adv:
                error_moment_adv = (
                    counts_adv.assign(percent=lambda d: (d["Nb"] / total_adv * 100).round(2))
                    .to_dict("records")
                )

        error_type_order = ["Erreur_EVI", "Erreur_DownStream", "Erreur_Unknow_S"]
        error_type_labels = {
            "Erreur_EVI": "EVI",
            "Erreur_DownStream": "Downstream",
            "Erreur_Unknow_S": "Erreur_Unknow_S",
        }

        type_counts = (
            err_rows[err_rows["type_erreur"].isin(error_type_order)]
            .groupby("type_erreur")
            .size()
            .reindex(error_type_order, fill_value=0)
            .reset_index(name="count")
        )

        error_type_total = int(type_counts["count"].sum())

        error_type_distribution = [
            {
                "type_erreur": row["type_erreur"],
                "label": error_type_labels.get(row["type_erreur"], row["type_erreur"]),
                "count": int(row["count"]),
                "percent": round(row["count"] / error_type_total * 100, 1) if error_type_total else 0,
            }
            for _, row in type_counts.iterrows()
            if row["count"] > 0
        ]

    downstream_occ: list[dict] = []
    downstream_moments: list[str] = []
    if not err_rows.empty:
        need_cols_ds = {"Downstream Code PC", "moment"}
        if need_cols_ds.issubset(err_rows.columns):
            ds_num = pd.to_numeric(err_rows["Downstream Code PC"], errors="coerce").fillna(0).astype(int)
            mask_downstream = (ds_num != 0) & (ds_num != 8192)
            sub = err_rows.loc[mask_downstream, ["Downstream Code PC", "moment"]].copy()

            if not sub.empty:
                sub["Code_PC"] = pd.to_numeric(sub["Downstream Code PC"], errors="coerce").fillna(0).astype(int)
                tmp = sub.groupby(["Code_PC", "moment"]).size().reset_index(name="Occurrences")
                downstream_moments = [m for m in MOMENT_ORDER if m in tmp["moment"].unique()]
                downstream_moments += [m for m in sorted(tmp["moment"].unique()) if m not in downstream_moments]

                table = (
                    tmp.pivot(index="Code_PC", columns="moment", values="Occurrences")
                    .reindex(columns=downstream_moments, fill_value=0)
                    .reset_index()
                )

                table[downstream_moments] = table[downstream_moments].fillna(0).astype(int)
                table["Total"] = table[downstream_moments].sum(axis=1).astype(int)
                table = table.sort_values("Total", ascending=False).reset_index(drop=True)

                total_all = int(table["Total"].sum())
                table["Percent"] = np.where(
                    total_all > 0,
                    (table["Total"] / total_all * 100).round(2),
                    0.0,
                )

                table.insert(0, "Rank", range(1, len(table) + 1))

                total_row = {
                    "Rank": "",
                    "Code_PC": "Total",
                    **{m: int(table[m].sum()) for m in downstream_moments},
                }
                total_row["Total"] = int(table["Total"].sum())
                total_row["Percent"] = 100.0 if total_all else 0.0

                downstream_occ = table.to_dict("records") + [total_row]

    evi_occ: list[dict] = []
    evi_occ_moments: list[str] = []
    if not err_rows.empty:
        need_cols_evi = {"EVI Error Code", "moment"}
        if need_cols_evi.issubset(err_rows.columns):
            ds_num = pd.to_numeric(err_rows.get("Downstream Code PC", 0), errors="coerce").fillna(0).astype(int)
            evi_code = pd.to_numeric(err_rows["EVI Error Code"], errors="coerce").fillna(0).astype(int)

            mask_evi = (ds_num == 8192) | ((ds_num == 0) & (evi_code != 0))
            sub = err_rows.loc[mask_evi, ["EVI Error Code", "moment"]].copy()

            if not sub.empty:
                sub["EVI_Code"] = pd.to_numeric(sub["EVI Error Code"], errors="coerce").astype(int)
                tmp = sub.groupby(["EVI_Code", "moment"]).size().reset_index(name="Occurrences")
                evi_occ_moments = [m for m in MOMENT_ORDER if m in tmp["moment"].unique()]
                evi_occ_moments += [m for m in sorted(tmp["moment"].unique()) if m not in evi_occ_moments]

                table = (
                    tmp.pivot(index="EVI_Code", columns="moment", values="Occurrences")
                    .reindex(columns=evi_occ_moments, fill_value=0)
                    .reset_index()
                )

                table[evi_occ_moments] = table[evi_occ_moments].fillna(0).astype(int)
                table["Total"] = table[evi_occ_moments].sum(axis=1).astype(int)
                table = table.sort_values("Total", ascending=False).reset_index(drop=True)

                total_all = int(table["Total"].sum())
                table["Percent"] = np.where(
                    total_all > 0,
                    (table["Total"] / total_all * 100).round(2),
                    0.0,
                )

                table.insert(0, "Rank", range(1, len(table) + 1))

                total_row = {
                    "Rank": "",
                    "EVI_Code": "Total",
                    **{m: int(table[m].sum()) for m in evi_occ_moments},
                }
                total_row["Total"] = int(table["Total"].sum())
                total_row["Percent"] = 100.0 if total_all else 0.0

                evi_occ = table.to_dict("records") + [total_row]

    return templates.TemplateResponse(
        "partials/sessions_site_details.html",
        {
            "request": request,
            "site_options": site_options,
            "site_focus": site_value,
            "pdc_options": pdc_options,
            "selected_pdc": selected_pdc,
            "err_rows": err_table.to_dict("records"),
            "ok_rows": ok_table.to_dict("records"),
            "by_pdc": by_pdc.to_dict("records"),
            "site_success_rate": site_success_rate,
            "site_total_charges": site_total_charges,
            "site_charges_ok": site_charges_ok,
            "error_moment": error_moment,
            "error_moment_grouped": error_moment_grouped,
            "error_moment_adv": error_moment_adv,
            "error_type_distribution": error_type_distribution,
            "error_type_total": error_type_total,
            "downstream_occ": downstream_occ,
            "downstream_moments": downstream_moments,
            "evi_occ": evi_occ,
            "evi_occ_moments": evi_occ_moments,
            "base_query": _prepare_query_params(request),
        },
    )
