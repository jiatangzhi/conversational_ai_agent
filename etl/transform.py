"""ETL Transform: clean, normalize, and aggregate raw data."""
from __future__ import annotations

import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)


def to_snake_case(name: str) -> str:
    """Convert any column name to snake_case."""
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.strip().lower().replace(" ", "_").replace("-", "_")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all columns to snake_case."""
    df = df.copy()
    df.columns = [to_snake_case(c) for c in df.columns]
    return df


def drop_null_user_ids(df: pd.DataFrame, col: str = "user_id") -> pd.DataFrame:
    """Remove rows where user_id is null or empty."""
    before = len(df)
    df = df[df[col].notna() & (df[col].astype(str).str.strip() != "")]
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d rows with null/empty %s", dropped, col)
    return df


def build_curated(raw: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Join users + sales + products, normalize, and return the curated dataset.
    Rows with null user_id are excluded.
    """
    users = normalize_columns(raw["users"])
    sales = normalize_columns(raw["sales"])
    products = normalize_columns(raw["products"])

    # Drop bad user rows
    users = drop_null_user_ids(users, "user_id")
    # Drop sales that reference invalid user_ids
    valid_users = set(users["user_id"])
    sales = sales[sales["user_id"].isin(valid_users)]
    logger.info("Sales after user filter: %d rows", len(sales))

    # Parse dates
    users["signup_date"] = pd.to_datetime(users["signup_date"], errors="coerce")
    sales["sale_date"] = pd.to_datetime(sales["sale_date"], errors="coerce")

    # Join sales ← users
    merged = sales.merge(users, on="user_id", how="left")
    # Join ← products
    merged = merged.merge(products, on="product_id", how="left")

    logger.info("Curated dataset: %d rows, %d columns", len(merged), len(merged.columns))
    return merged


def compute_dau(curated: pd.DataFrame) -> pd.DataFrame:
    """Daily Active Users: distinct users per sale_date."""
    dau = (
        curated.groupby("sale_date")["user_id"]
        .nunique()
        .reset_index()
        .rename(columns={"user_id": "dau"})
    )
    dau["sale_date"] = dau["sale_date"].dt.strftime("%Y-%m-%d")
    return dau


def compute_sales_by_product(curated: pd.DataFrame) -> pd.DataFrame:
    """Total revenue and units sold per product."""
    agg = (
        curated.groupby(["product_id", "product_name"])
        .agg(total_revenue=("amount", "sum"), total_units=("quantity", "sum"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )
    return agg


def transform(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Run all transformations. Returns dict of table_name → DataFrame."""
    curated = build_curated(raw)
    dau = compute_dau(curated)
    sales_by_product = compute_sales_by_product(curated)

    return {
        "curated_data": curated,
        "dau": dau,
        "sales_by_product": sales_by_product,
    }
