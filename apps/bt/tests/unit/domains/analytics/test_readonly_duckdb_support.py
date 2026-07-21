from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest

from tests.unit.domains.analytics.pit_fixture_support import (
    materialize_stock_master_daily,
)

from src.domains.analytics.readonly_duckdb_support import (
    _connect_duckdb,
    date_where_clause,
    fetch_date_range,
    normalize_code_sql,
    open_readonly_analysis_connection,
    require_market_v5_compatibility,
)


def test_materialize_stock_master_daily_uses_only_explicit_historical_rows() -> None:
    conn = duckdb.connect(":memory:")
    try:
        materialize_stock_master_daily(
            conn,
            columns=("code", "market_code", "market_name", "scale_category"),
            rows=(
                ("2024-01-05", "1111", "0112", "Standard", None),
                ("2025-01-06", "1111", "0111", "Prime", "TOPIX Mid400"),
            ),
        )

        assert conn.execute(
            "SELECT * FROM stock_master_daily ORDER BY date"
        ).fetchall() == [
            ("2024-01-05", "1111", "0112", "Standard", None),
            ("2025-01-06", "1111", "0111", "Prime", "TOPIX Mid400"),
        ]
    finally:
        conn.close()


def _market_compatibility_connection(
    *,
    version: int,
    adjustment_mode: str,
    include_stock_data: bool = True,
) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE market_schema_version (version INTEGER, applied_at TEXT, notes TEXT)"
    )
    conn.execute("INSERT INTO market_schema_version VALUES (?, NULL, NULL)", [version])
    conn.execute("CREATE TABLE sync_metadata (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)")
    conn.execute(
        "INSERT INTO sync_metadata VALUES ('stock_price_adjustment_mode', ?, NULL)",
        [adjustment_mode],
    )
    if include_stock_data:
        conn.execute("CREATE TABLE stock_data (code TEXT, date TEXT)")
    return conn


@pytest.mark.parametrize(
    ("version", "adjustment_mode"),
    [
        (4, "local_projection_v2_event_time"),
        (5, "local_projection_v2_event_time"),
        (5, "local_projection_v1"),
    ],
)
def test_require_market_v5_compatibility_rejects_incompatible_metadata(
    version: int,
    adjustment_mode: str,
) -> None:
    conn = _market_compatibility_connection(
        version=version,
        adjustment_mode=adjustment_mode,
    )
    try:
        with pytest.raises(RuntimeError, match="market-cutover cutover"):
            require_market_v5_compatibility(conn, required_tables=("stock_data",))
    finally:
        conn.close()


def test_require_market_v5_compatibility_rejects_missing_consumer_table() -> None:
    conn = _market_compatibility_connection(
        version=5,
        adjustment_mode="provider_adjusted_v1",
        include_stock_data=False,
    )
    try:
        with pytest.raises(RuntimeError, match="stock_data.*market-cutover cutover"):
            require_market_v5_compatibility(conn, required_tables=("stock_data",))
    finally:
        conn.close()


def test_require_market_v5_compatibility_accepts_exact_v5_provider_schema() -> None:
    conn = _market_compatibility_connection(
        version=5,
        adjustment_mode="provider_adjusted_v1",
    )
    try:
        assert require_market_v5_compatibility(
            conn, required_tables=("stock_data",)
        ) == 5
    finally:
        conn.close()


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    db_path = tmp_path / "market.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE sample_days (date TEXT PRIMARY KEY, value DOUBLE)")
    conn.executemany(
        "INSERT INTO sample_days VALUES (?, ?)",
        [
            ("2024-01-02", 1.0),
            ("2024-01-03", 2.0),
            ("2024-01-05", 3.0),
        ],
    )
    conn.close()
    return str(db_path)


def test_open_readonly_analysis_connection_reads_live_db(
    analytics_db_path: str,
) -> None:
    with open_readonly_analysis_connection(
        analytics_db_path,
        snapshot_prefix="readonly-duckdb-support-",
    ) as ctx:
        row = ctx.connection.execute(
            "SELECT COUNT(*) AS row_count FROM sample_days"
        ).fetchone()

    assert ctx.source_mode == "live"
    assert ctx.source_detail == f"live DuckDB: {analytics_db_path}"
    assert row[0] == 3


def test_open_readonly_analysis_connection_falls_back_to_snapshot(
    analytics_db_path: str,
) -> None:
    wal_path = Path(f"{analytics_db_path}.wal")
    wal_path.write_text("wal", encoding="utf-8")
    attempts = {"count": 0}

    def flaky_connect(db_path: str, *, read_only: bool = True) -> Any:
        assert read_only is True
        if db_path == analytics_db_path and attempts["count"] == 0:
            attempts["count"] += 1
            raise duckdb.IOException(
                'IO Error: Could not set lock on file "market.duckdb": Conflicting lock is held'
            )
        if db_path != analytics_db_path:
            assert Path(f"{db_path}.wal").exists()
        return _connect_duckdb(db_path, read_only=read_only)

    with open_readonly_analysis_connection(
        analytics_db_path,
        snapshot_prefix="readonly-duckdb-support-",
        connect_fn=flaky_connect,
    ) as ctx:
        row = ctx.connection.execute(
            "SELECT MAX(date) AS max_date FROM sample_days"
        ).fetchone()

    assert ctx.source_mode == "snapshot"
    assert "temporary snapshot copied from" in ctx.source_detail
    assert row[0] == "2024-01-05"


def test_open_readonly_analysis_connection_propagates_non_lock_errors(
    analytics_db_path: str,
) -> None:
    def failing_connect(db_path: str, *, read_only: bool = True) -> Any:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        with open_readonly_analysis_connection(
            analytics_db_path,
            snapshot_prefix="readonly-duckdb-support-",
            connect_fn=failing_connect,
        ):
            pass


def test_shared_sql_helpers_cover_common_date_and_code_logic(
    analytics_db_path: str,
) -> None:
    where_sql, params = date_where_clause("date", "2024-01-03", "2024-01-05")

    assert where_sql == " WHERE date >= ? AND date <= ?"
    assert params == ["2024-01-03", "2024-01-05"]
    assert normalize_code_sql("code") == (
        "CASE "
        "WHEN length(code) IN (5, 6) AND right(code, 1) = '0' "
        "THEN left(code, length(code) - 1) "
        "ELSE code "
        "END"
    )

    conn = duckdb.connect(analytics_db_path, read_only=True)
    try:
        assert fetch_date_range(
            conn,
            table_name="sample_days",
            start_date="2024-01-03",
            end_date="2024-01-04",
        ) == ("2024-01-03", "2024-01-03")
    finally:
        conn.close()
