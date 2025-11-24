from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
from sqlalchemy import bindparam, create_engine, inspect, text
from sqlalchemy.engine import Engine

CHARGES_DB_URI = "mysql+pymysql://nidec:MaV38f5xsGQp83@162.19.251.55:3306/Charges"
INDICATOR_DB_URI = "mysql+pymysql://AdminNidec:u6Ehe987XBSXxa4@141.94.31.144:3306/indicator"
BASE_ELTO_URL = "https://elto.nidec-asi-online.com/Charge/detail?id="
OUTPUT_FILE = Path("Ex.xlsx")
SITE_FILTER = "Pouilly-En-Auxois"


def build_engine(uri: str) -> Engine:
    return create_engine(uri)


def find_column(columns: Sequence[str], candidates: Sequence[str]) -> str | None:
    lower_map = {col.lower(): col for col in columns}
    for candidate in candidates:
        match = lower_map.get(candidate.lower())
        if match:
            return match
    return None


def normalize_mac(values: Iterable[object]) -> pd.Series:
    return (
        pd.Series(values)
        .astype(str)
        .str.strip()
        .str.replace("-", ":", regex=False)
        .str.upper()
    )


def quote_identifier(identifier: str) -> str:
    identifier = identifier.strip("`")
    return f"`{identifier}`"


def load_unique_macs(engine: Engine, schema: str) -> set[str]:
    inspector = inspect(engine)
    table_name = "kpi_id"
    if table_name not in inspector.get_table_names(schema=schema):
        raise ValueError(f"Table `{table_name}` absente du schéma {schema}.")

    df = pd.read_sql_table(table_name, con=engine, schema=schema)
    if df.empty:
        return set()

    mac_col = find_column(
        df.columns,
        ["mac", "mac_address", "adresse_mac", "macadresse", "kpi_id", "id"],
    )
    if mac_col is None:
        mac_col = df.columns[0]

    macs = normalize_mac(df[mac_col].dropna())
    macs = macs[macs.astype(bool)]
    return set(macs.tolist())


def resolve_kpi_session_table(engine: Engine) -> str:
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema="indicator")
    for name in ("kpi_session", "kpi_sessions"):
        if name in tables:
            return name
    raise ValueError("Aucune table kpi_session(s) trouvée dans indicator.")


def _pick_columns(columns: Sequence[str]) -> dict[str, str | None]:
    return {
        "mac": find_column(columns, ["mac", "mac_address", "adresse_mac", "macadresse", "adresse mac", "MAC"]),
        "site": find_column(columns, ["site", "Site"]),
        "start": find_column(columns, ["datetime start", "start_date", "date start", "start", "datedebut", "date debut"]),
        "end": find_column(columns, ["datetime end", "end_date", "date end", "end", "datefin", "date fin"]),
        "is_ok": find_column(columns, ["is_ok", "ok", "success", "is success", "status"]),
        "pdc": find_column(columns, ["pdc", "PDC"]),
        "error": find_column(columns, ["error etiquette", "error_etiquette", "erreur etiquette", "type_erreur", "error_label", "etiquette"]),
        "tr": find_column(columns, ["taux_reussite_global", "tr_global", "global_tr", "taux_global", "success_rate", "taux de reussite", "taux_reussite"]),
        "id": find_column(columns, ["id", "ID", "session_id", "charge_id"]),
    }


def load_latest_failed_sessions(engine: Engine, macs: set[str]) -> pd.DataFrame:
    if not macs:
        return pd.DataFrame()

    table_name = resolve_kpi_session_table(engine)
    inspector = inspect(engine)
    columns = [col["name"] for col in inspector.get_columns(table_name, schema="indicator")]
    column_map = _pick_columns(columns)

    mac_col = column_map["mac"]
    site_col = column_map["site"]
    start_col = column_map["start"]
    if not mac_col or not site_col or not start_col:
        raise ValueError("Colonnes MAC, Site ou Date de début introuvables dans kpi_session(s).")

    select_cols = [
        mac_col,
        site_col,
        start_col,
        column_map["end"],
        column_map["pdc"],
        column_map["error"],
        column_map["tr"],
        column_map["id"],
        column_map["is_ok"],
    ]
    select_cols = [c for c in select_cols if c]

    query_parts = [
        "SELECT ", ", ".join(quote_identifier(col) for col in select_cols),
        f" FROM indicator.{quote_identifier(table_name)}",
        f" WHERE {quote_identifier(site_col)} = :site",
        f"   AND {quote_identifier(mac_col)} IN :mac_list",
    ]

    if column_map["is_ok"]:
        query_parts.append(
            f"   AND ({quote_identifier(column_map['is_ok'])} IS NULL OR {quote_identifier(column_map['is_ok'])} <> 1)"
        )

    query_parts.append(f" ORDER BY {quote_identifier(start_col)} DESC")

    query = text("\n".join(query_parts)).bindparams(bindparam("mac_list", expanding=True))

    df = pd.read_sql_query(
        query,
        con=engine,
        params={"site": SITE_FILTER, "mac_list": list(macs)},
    )

    if df.empty:
        return df

    rename_map = {}
    if column_map["mac"]:
        rename_map[column_map["mac"]] = "Adresse MAC"
    if column_map["site"]:
        rename_map[column_map["site"]] = "Site"
    if column_map["start"]:
        rename_map[column_map["start"]] = "Date début"
    if column_map["end"]:
        rename_map[column_map["end"]] = "Date fin"
    if column_map["pdc"]:
        rename_map[column_map["pdc"]] = "PDC"
    if column_map["error"]:
        rename_map[column_map["error"]] = "Error etiquette"
    if column_map["tr"]:
        rename_map[column_map["tr"]] = "Taux de réussite global"
    if column_map["id"]:
        rename_map[column_map["id"]] = "ID session"
    if column_map["is_ok"]:
        rename_map[column_map["is_ok"]] = "is_ok"

    df = df.rename(columns=rename_map)

    for col in ["Date début", "Date fin"]:
        if col in df:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "Adresse MAC" in df:
        df["Adresse MAC"] = normalize_mac(df["Adresse MAC"]).str.replace(":", "-", regex=False)

    if "ID session" in df:
        df["Lien vers ELTO"] = df["ID session"].apply(
            lambda x: f"{BASE_ELTO_URL}{x}" if pd.notna(x) else None
        )

    df = df.sort_values(by=["Date début"], ascending=False)
    df = df.drop_duplicates(subset=["Adresse MAC"], keep="first")

    desired_order = [
        "Adresse MAC",
        "Taux de réussite global",
        "Site",
        "Date début",
        "Date fin",
        "PDC",
        "Error etiquette",
        "Lien vers ELTO",
    ]
    existing_cols = [col for col in desired_order if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in existing_cols]
    df = df[existing_cols + remaining_cols]

    if "is_ok" in df:
        df = df.drop(columns=["is_ok"])

    return df


def main() -> None:
    print("🔎 Récupération des adresses MAC depuis les tables kpi_id...")
    charges_engine = build_engine(CHARGES_DB_URI)
    indicator_engine = build_engine(INDICATOR_DB_URI)

    macs_charges = load_unique_macs(charges_engine, "Charges")
    macs_indicator = load_unique_macs(indicator_engine, "indicator")

    all_macs = macs_charges.union(macs_indicator)
    print(f"📡 {len(all_macs)} adresses MAC uniques trouvées.")

    print(f"\n📍 Recherche des charges non OK les plus récentes sur {SITE_FILTER}...")
    sessions = load_latest_failed_sessions(indicator_engine, all_macs)

    if sessions.empty:
        print("Aucune session trouvée correspondant aux critères.")
        return

    sessions.to_excel(OUTPUT_FILE, index=False)
    print(f"✅ Export terminé : {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()
