from __future__ import annotations

from collections.abc import Iterable, Sequence

import duckdb

FULL_STOCK_MASTER_COLUMNS = (
    "code",
    "company_name",
    "company_name_english",
    "market_code",
    "market_name",
    "sector_17_code",
    "sector_17_name",
    "sector_33_code",
    "sector_33_name",
    "scale_category",
    "listed_date",
    "created_at",
    "updated_at",
)


def materialize_stock_master_daily(
    conn: duckdb.DuckDBPyConnection,
    *,
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
) -> None:
    """Materialize caller-supplied PIT master payloads without database reads."""
    if not columns or any(not column.isidentifier() for column in columns):
        raise ValueError("stock_master_daily columns must be non-empty identifiers")
    materialized_rows = list(rows)
    expected_width = len(columns) + 1
    if any(len(row) != expected_width for row in materialized_rows):
        raise ValueError(
            f"stock_master_daily rows must contain {expected_width} values"
        )
    column_sql = ", ".join(f'"{column}" TEXT' for column in columns)
    conn.execute(f"CREATE TABLE stock_master_daily (date TEXT, {column_sql})")
    if materialized_rows:
        placeholders = ", ".join("?" for _ in range(expected_width))
        conn.executemany(
            f"INSERT INTO stock_master_daily VALUES ({placeholders})",
            materialized_rows,
        )
