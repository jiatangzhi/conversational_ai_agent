"""ETL Load: persist transformed DataFrames to SQLite warehouse."""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "warehouse.db"


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def load_table(df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> None:
    """Write a DataFrame to SQLite. Defaults to replacing the table."""
    with get_connection() as conn:
        # Serialize datetime columns to ISO strings so SQLite can store them
        df_copy = df.copy()
        for col in df_copy.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]):
            df_copy[col] = df_copy[col].dt.strftime("%Y-%m-%d")

        df_copy.to_sql(table_name, conn, if_exists=if_exists, index=False)
        logger.info("Loaded table '%s': %d rows", table_name, len(df_copy))


def load_all(tables: dict[str, pd.DataFrame]) -> None:
    """Load all transformed tables into the warehouse."""
    for table_name, df in tables.items():
        load_table(df, table_name)
    logger.info("All tables loaded to %s", DB_PATH)


def table_exists(table_name: str) -> bool:
    """Check if a table exists in the warehouse."""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cursor.fetchone() is not None


def list_tables() -> list[str]:
    """Return list of all table names in the warehouse."""
    with get_connection() as conn:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return [row[0] for row in cursor.fetchall()]


def run_query(sql: str) -> pd.DataFrame:
    """Execute a read-only SQL query and return results as DataFrame."""
    with get_connection() as conn:
        return pd.read_sql_query(sql, conn)
