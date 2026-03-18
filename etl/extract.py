"""ETL Extract: Load CSVs from the data/ directory."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"


def load_csv(filename: str) -> pd.DataFrame:
    """Load a single CSV file and return a DataFrame."""
    path = DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")
    df = pd.read_csv(path)
    logger.info("Loaded %s: %d rows, %d columns", filename, len(df), len(df.columns))
    return df


def extract_all() -> dict[str, pd.DataFrame]:
    """Load all source CSVs. Returns dict of {table_name: DataFrame}."""
    sources = {
        "users": "users.csv",
        "sales": "sales.csv",
        "products": "products.csv",
    }
    raw: dict[str, pd.DataFrame] = {}
    for name, filename in sources.items():
        try:
            raw[name] = load_csv(filename)
        except FileNotFoundError as exc:
            logger.warning("Skipping missing file: %s", exc)
    return raw
