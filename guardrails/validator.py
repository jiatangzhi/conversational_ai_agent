"""Guardrails: validate SQL queries and filter sensitive data."""

import re

# Patterns for destructive SQL operations
_DESTRUCTIVE_PATTERNS = re.compile(
    r"\b(DROP|DELETE|UPDATE|INSERT|CREATE|ALTER|TRUNCATE|REPLACE|MERGE|UPSERT|EXEC|EXECUTE|PRAGMA)\b",
    re.IGNORECASE,
)

# Columns that should never be surfaced in responses
_SENSITIVE_COLUMNS = {"signup_date", "email", "phone", "password", "credit_card"}


class GuardrailError(ValueError):
    """Raised when a query violates safety rules."""


def validate_sql(sql: str) -> str:
    """
    Ensure SQL is read-only. Raises GuardrailError on violations.
    Returns the original SQL if safe.
    """
    stripped = sql.strip()

    # Must start with SELECT or WITH (CTEs)
    if not re.match(r"^\s*(SELECT|WITH)\b", stripped, re.IGNORECASE):
        raise GuardrailError(
            f"Only SELECT queries are allowed. Received: '{stripped[:60]}...'"
        )

    # No destructive keywords anywhere in the statement
    match = _DESTRUCTIVE_PATTERNS.search(stripped)
    if match:
        raise GuardrailError(
            f"Destructive SQL keyword detected: '{match.group()}'. "
            "Only read-only queries are permitted."
        )

    return stripped


def sanitize_dataframe(df) -> object:
    """Remove sensitive columns from a DataFrame before returning to user."""
    import pandas as pd

    if not isinstance(df, pd.DataFrame):
        return df

    cols_to_drop = [c for c in df.columns if c.lower() in _SENSITIVE_COLUMNS]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
    return df


def check_data_available(table_name: str) -> None:
    """Raise GuardrailError if the warehouse table doesn't exist."""
    from etl.load import table_exists

    if not table_exists(table_name):
        raise GuardrailError(
            f"Table '{table_name}' does not exist. Run the ETL pipeline first."
        )
