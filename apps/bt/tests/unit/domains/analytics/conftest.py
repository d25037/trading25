"""Compatibility helpers for legacy analytics unit fixtures.

Product code requires market DB schema v3 tables. Many older analytics tests build
small synthetic DuckDBs with only `stocks` + `stock_data`; this shim upgrades those
test-only databases on demand without adding production fallbacks.
"""

from __future__ import annotations

from typing import Any

import duckdb
import pytest


class _AnalyticsDuckDBConnection:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def close(self) -> None:
        try:
            _ensure_stock_master_daily(self._conn)
            _ensure_index_membership_daily(self._conn)
        except Exception:  # noqa: BLE001 - not every analytics fixture has market tables
            pass
        self._conn.close()

    def execute(self, query: str, parameters: object | None = None) -> duckdb.DuckDBPyConnection:
        try:
            return self._execute(query, parameters)
        except duckdb.CatalogException as exc:
            message = str(exc)
            needs_master = "stock_master_daily" in message
            needs_membership = "index_membership_daily" in message
            if not needs_master and not needs_membership:
                raise
            if needs_master:
                _ensure_stock_master_daily(self._conn)
            if needs_membership:
                _ensure_index_membership_daily(self._conn)
            return self._execute(query, parameters)

    def _execute(self, query: str, parameters: object | None) -> duckdb.DuckDBPyConnection:
        if parameters is None:
            return self._conn.execute(query)
        return self._conn.execute(query, parameters)


def _table_or_view_exists(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        LIMIT 1
        """,
        (name,),
    ).fetchone()
    return row is not None


def _ensure_stock_master_daily(conn: duckdb.DuckDBPyConnection) -> None:
    if _table_or_view_exists(conn, "stock_master_daily"):
        return
    if not _table_or_view_exists(conn, "stocks"):
        return
    date_sources: list[str] = []
    if _table_or_view_exists(conn, "stock_data"):
        date_sources.append("SELECT DISTINCT date FROM stock_data")
    if _table_or_view_exists(conn, "stock_data_minute_raw"):
        date_sources.append("SELECT DISTINCT date FROM stock_data_minute_raw")
    if not date_sources:
        return
    date_sql = " UNION ".join(date_sources)
    conn.execute("""
        CREATE VIEW stock_master_daily AS
        SELECT d.date, s.*
        FROM (
            {date_sql}
        ) d
        CROSS JOIN stocks s
    """.format(date_sql=date_sql))


def _ensure_index_membership_daily(conn: duckdb.DuckDBPyConnection) -> None:
    if _table_or_view_exists(conn, "index_membership_daily"):
        return
    _ensure_stock_master_daily(conn)
    if not _table_or_view_exists(conn, "stock_master_daily"):
        return
    conn.execute("""
        CREATE VIEW index_membership_daily AS
        SELECT date, 'TOPIX500' AS index_code, code
        FROM stock_master_daily
        WHERE scale_category IN ('TOPIX Core30', 'TOPIX Large70', 'TOPIX Mid400')
    """)


@pytest.fixture(autouse=True)
def _legacy_analytics_market_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    original_connect = duckdb.connect

    def connect_with_schema_v3(*args: Any, **kwargs: Any) -> _AnalyticsDuckDBConnection:
        return _AnalyticsDuckDBConnection(original_connect(*args, **kwargs))

    monkeypatch.setattr(duckdb, "connect", connect_with_schema_v3)
