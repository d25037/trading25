from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest

from src.domains.analytics.readonly_duckdb_support import (
    _connect_duckdb,
    date_where_clause,
    fetch_date_range,
    normalize_code_sql,
    open_readonly_analysis_connection,
)


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
