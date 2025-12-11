"""
Connexion MySQL avec pool de connexions SQLAlchemy
Version exportable sans authentification
"""

import os
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import pandas as pd

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "141.94.31.144"),
    "port": os.getenv("DB_PORT", "3306"),
    "user": os.getenv("DB_USER", "AdminNidec"),
    "password": os.getenv("DB_PASSWORD", "u6Ehe987XBSXxa4"),
    "database": os.getenv("DB_NAME", "indicator"),
}

DATABASE_URL = (
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_recycle=3600,
    pool_pre_ping=True,
)


def get_sites() -> list[str]:
    """Récupère la liste des sites disponibles"""
    query = """
        SELECT DISTINCT Site
        FROM kpi_sessions
        WHERE Site IS NOT NULL
        ORDER BY Site
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [row[0] for row in result]


def get_date_range() -> dict:
    """Récupère les dates min/max des sessions"""
    query = """
        SELECT
            MIN(DATE(`Datetime start`)) as date_min,
            MAX(DATE(`Datetime start`)) as date_max
        FROM kpi_sessions
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
        return {
            "min": result[0] or date.today() - timedelta(days=365),
            "max": result[1] or date.today(),
        }


def query_df(sql: str, params: dict = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)
