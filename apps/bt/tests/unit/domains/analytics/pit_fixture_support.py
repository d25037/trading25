from __future__ import annotations

from collections.abc import Iterable

import duckdb


def materialize_stock_master_daily(
    conn: duckdb.DuckDBPyConnection,
    *,
    date_code_rows: Iterable[tuple[str, str]],
) -> None:
    """Materialize explicitly selected PIT master rows for an analytics fixture."""
    conn.execute(
        "CREATE TABLE stock_master_daily AS "
        "SELECT CAST(NULL AS TEXT) AS date, stocks.* FROM stocks WHERE FALSE"
    )
    conn.executemany(
        "INSERT INTO stock_master_daily "
        "SELECT ?, stocks.* FROM stocks WHERE code = ?",
        sorted(set(date_code_rows)),
    )
