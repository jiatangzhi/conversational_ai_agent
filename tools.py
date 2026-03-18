"""
Agent tools: ETL pipeline, warehouse queries, memory retrieval, and metrics.
All tools use the @beta_tool decorator for automatic schema generation.
"""
from __future__ import annotations

import json
import logging

import pandas as pd
from anthropic import beta_tool

from etl.extract import extract_all
from etl.load import list_tables, run_query
from etl.transform import transform
from etl.load import load_all
from guardrails.validator import GuardrailError, check_data_available, sanitize_dataframe, validate_sql
from memory.store import get_last_context, get_recent

logger = logging.getLogger(__name__)


def _df_to_markdown(df: pd.DataFrame, max_rows: int = 30) -> str:
    """Convert a DataFrame to a compact markdown table string."""
    if df.empty:
        return "_No data returned._"
    df = df.head(max_rows)
    header = " | ".join(str(c) for c in df.columns)
    sep = " | ".join(["---"] * len(df.columns))
    rows = [" | ".join(str(v) for v in row) for row in df.itertuples(index=False)]
    return "\n".join([header, sep] + rows)


@beta_tool
def run_etl_pipeline() -> str:
    """
    Execute the full ETL pipeline: extract CSVs from data/, transform and clean
    the data (remove null user_ids, normalize columns, compute aggregates), and
    load results into the SQLite warehouse (warehouse.db). Returns a summary of
    tables created and row counts.
    """
    try:
        logger.info("Starting ETL pipeline")
        raw = extract_all()
        if not raw:
            return "ERROR: No source CSV files found in data/."

        tables = transform(raw)
        load_all(tables)

        summary_lines = ["ETL pipeline completed successfully.\n\nTables loaded:"]
        for name, df in tables.items():
            summary_lines.append(f"  - {name}: {len(df):,} rows, {len(df.columns)} columns")

        result = "\n".join(summary_lines)
        logger.info(result)
        return result
    except Exception as exc:
        logger.error("ETL pipeline failed: %s", exc, exc_info=True)
        return f"ETL pipeline error: {exc}"


@beta_tool
def query_warehouse(sql: str) -> str:
    """
    Execute a read-only SQL SELECT query against the SQLite warehouse (warehouse.db)
    and return results as a markdown table. Only SELECT statements are allowed —
    any attempt to run INSERT, UPDATE, DELETE, DROP, or other destructive operations
    will be rejected. The main table is 'curated_data'. Other tables: 'dau',
    'sales_by_product'.

    Args:
        sql: A valid SQL SELECT statement to run against warehouse.db.
    """
    try:
        safe_sql = validate_sql(sql)
        df = run_query(safe_sql)
        df = sanitize_dataframe(df)
        return _df_to_markdown(df)
    except GuardrailError as exc:
        return f"GUARDRAIL VIOLATION: {exc}"
    except Exception as exc:
        logger.error("Query failed: %s | SQL: %s", exc, sql)
        return f"Query error: {exc}"


@beta_tool
def get_daily_active_users(start_date: str = "", end_date: str = "") -> str:
    """
    Return the Daily Active Users (DAU) — number of distinct users who made a
    purchase each day. Optionally filter by date range (YYYY-MM-DD format).

    Args:
        start_date: Start date in YYYY-MM-DD format. Leave empty for all dates.
        end_date: End date in YYYY-MM-DD format. Leave empty for all dates.
    """
    try:
        check_data_available("dau")
        conditions = []
        if start_date:
            conditions.append("sale_date >= ?")
        if end_date:
            conditions.append("sale_date <= ?")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT sale_date, dau FROM dau {where} ORDER BY sale_date"
        params = tuple(d for d in [start_date, end_date] if d)
        df = run_query(sql, params)
        if df.empty:
            return "No DAU data found for the specified range."
        total = df["dau"].sum()
        avg = df["dau"].mean()
        table = _df_to_markdown(df)
        return f"**Daily Active Users**\nTotal unique users across period: {total}\nAverage DAU: {avg:.1f}\n\n{table}"
    except GuardrailError as exc:
        return f"GUARDRAIL: {exc}"
    except Exception as exc:
        return f"Error retrieving DAU: {exc}"


@beta_tool
def get_top_products(n: int = 5, date: str = "") -> str:
    """
    Return the top N products ranked by total revenue. Optionally filter by a
    specific date (YYYY-MM-DD). Uses the sales_by_product pre-aggregated table
    if no date filter is applied, otherwise queries curated_data directly.

    Args:
        n: Number of top products to return (default 5).
        date: Optional date filter in YYYY-MM-DD format. Leave empty for all time.
    """
    try:
        if date:
            check_data_available("curated_data")
            sql = f"""
                SELECT product_name, SUM(amount) AS total_revenue, SUM(quantity) AS total_units
                FROM curated_data
                WHERE sale_date = ?
                GROUP BY product_name
                ORDER BY total_revenue DESC
                LIMIT {int(n)}
            """
        else:
            check_data_available("sales_by_product")
            sql = f"""
                SELECT product_name, total_revenue, total_units
                FROM sales_by_product
                ORDER BY total_revenue DESC
                LIMIT {int(n)}
            """
        df = run_query(sql, (date,) if date else ())
        if df.empty:
            return f"No sales data found{' for ' + date if date else ''}."
        table = _df_to_markdown(df)
        label = f" on {date}" if date else " (all time)"
        return f"**Top {n} Products by Revenue{label}**\n\n{table}"
    except GuardrailError as exc:
        return f"GUARDRAIL: {exc}"
    except Exception as exc:
        return f"Error retrieving top products: {exc}"


@beta_tool
def get_sales_by_region(date: str = "") -> str:
    """
    Return total sales revenue and number of transactions grouped by customer
    region. Optionally filter to a specific date (YYYY-MM-DD).

    Args:
        date: Optional date filter in YYYY-MM-DD format. Leave empty for all time.
    """
    try:
        check_data_available("curated_data")
        where = "WHERE sale_date = ?" if date else ""
        sql = f"""
            SELECT region,
                   COUNT(*) AS transactions,
                   SUM(amount) AS total_revenue,
                   COUNT(DISTINCT user_id) AS unique_customers
            FROM curated_data
            {where}
            GROUP BY region
            ORDER BY total_revenue DESC
        """
        df = run_query(sql, (date,) if date else ())
        if df.empty:
            return "No regional data found."
        table = _df_to_markdown(df)
        label = f" on {date}" if date else " (all time)"
        return f"**Sales by Region{label}**\n\n{table}"
    except GuardrailError as exc:
        return f"GUARDRAIL: {exc}"
    except Exception as exc:
        return f"Error retrieving regional data: {exc}"


@beta_tool
def get_memory_context() -> str:
    """
    Retrieve the last 5 conversation interactions from memory — including previous
    user queries and agent responses. Use this to answer follow-up questions that
    reference earlier results (e.g., 'the same day', 'that product', 'yesterday's data').
    """
    return get_last_context()


@beta_tool
def list_warehouse_tables() -> str:
    """
    List all available tables in the SQLite warehouse (warehouse.db) along with
    their column names. Useful for understanding what data is available before
    constructing queries.
    """
    try:
        tables = list_tables()
        if not tables:
            return "Warehouse is empty. Run the ETL pipeline first."
        from etl.load import get_connection
        lines = ["**Warehouse Tables:**\n"]
        with get_connection() as conn:
            for table in tables:
                safe = table.replace('"', '""')
                cursor = conn.execute(f'PRAGMA table_info("{safe}")')
                cols = [row[1] for row in cursor.fetchall()]
                cursor2 = conn.execute(f'SELECT COUNT(*) FROM "{safe}"')
                count = cursor2.fetchone()[0]
                lines.append(f"**{table}** ({count:,} rows)")
                lines.append(f"  Columns: {', '.join(cols)}\n")
        return "\n".join(lines)
    except Exception as exc:
        return f"Error listing tables: {exc}"
