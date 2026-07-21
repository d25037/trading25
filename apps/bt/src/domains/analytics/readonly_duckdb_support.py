"""
Shared read-only DuckDB helpers for research analytics modules.
"""

from __future__ import annotations

import importlib
import shutil
import tempfile
from collections.abc import Collection, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol

if TYPE_CHECKING:
    import duckdb

SourceMode = Literal["live", "snapshot"]

_LOCK_ERROR_PATTERNS: tuple[str, ...] = (
    "conflicting lock is held",
    "could not set lock",
)
_MARKET_SCHEMA_VERSION = 5
_STOCK_PRICE_ADJUSTMENT_MODE = "provider_adjusted_v1"
_COMPATIBILITY_METADATA_TABLES = frozenset(
    {"market_schema_version", "sync_metadata"}
)


class DuckDbConnectFn(Protocol):
    def __call__(self, db_path: str, *, read_only: bool = True) -> Any: ...


@dataclass(frozen=True)
class ReadonlyAnalysisConnectionContext:
    connection: Any
    source_mode: SourceMode
    source_detail: str


def _connect_duckdb(db_path: str, *, read_only: bool = True) -> Any:
    duckdb = importlib.import_module("duckdb")
    return duckdb.connect(db_path, read_only=read_only)


def _is_lock_error(error: Exception) -> bool:
    message = str(error).lower()
    return any(pattern in message for pattern in _LOCK_ERROR_PATTERNS)


@contextmanager
def open_readonly_analysis_connection(
    db_path: str,
    *,
    snapshot_prefix: str,
    connect_fn: DuckDbConnectFn | None = None,
) -> Iterator[ReadonlyAnalysisConnectionContext]:
    connector = connect_fn or _connect_duckdb
    conn: Any | None = None
    tmpdir: tempfile.TemporaryDirectory[str] | None = None
    try:
        try:
            conn = connector(db_path, read_only=True)
            yield ReadonlyAnalysisConnectionContext(
                connection=conn,
                source_mode="live",
                source_detail=f"live DuckDB: {db_path}",
            )
            return
        except Exception as exc:
            if not _is_lock_error(exc):
                raise

        tmpdir = tempfile.TemporaryDirectory(prefix=snapshot_prefix, dir="/tmp")
        snapshot_dir = Path(tmpdir.name)
        db_path_obj = Path(db_path)
        snapshot_path = snapshot_dir / db_path_obj.name
        shutil.copy2(db_path_obj, snapshot_path)

        wal_path = Path(f"{db_path}.wal")
        if wal_path.exists():
            shutil.copy2(wal_path, Path(f"{snapshot_path}.wal"))

        conn = connector(str(snapshot_path), read_only=True)
        yield ReadonlyAnalysisConnectionContext(
            connection=conn,
            source_mode="snapshot",
            source_detail=f"temporary snapshot copied from {db_path}",
        )
    finally:
        if conn is not None:
            conn.close()
        if tmpdir is not None:
            tmpdir.cleanup()


def date_where_clause(
    column_name: str,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, list[str]]:
    conditions: list[str] = []
    params: list[str] = []
    if start_date:
        conditions.append(f"{column_name} >= ?")
        params.append(start_date)
    if end_date:
        conditions.append(f"{column_name} <= ?")
        params.append(end_date)
    if not conditions:
        return "", []
    return " WHERE " + " AND ".join(conditions), params


def fetch_date_range(
    conn: Any,
    *,
    table_name: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[str | None, str | None]:
    where_sql, params = date_where_clause("date", start_date, end_date)
    row = conn.execute(
        f"SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM {table_name}{where_sql}",
        params,
    ).fetchone()
    min_date = str(row[0]) if row and row[0] else None
    max_date = str(row[1]) if row and row[1] else None
    return min_date, max_date


def normalize_code_sql(column_name: str) -> str:
    return (
        "CASE "
        f"WHEN length({column_name}) IN (5, 6) AND right({column_name}, 1) = '0' "
        f"THEN left({column_name}, length({column_name}) - 1) "
        f"ELSE {column_name} "
        "END"
    )


def require_market_v5_compatibility(
    conn: duckdb.DuckDBPyConnection,
    *,
    required_tables: Collection[str],
) -> int:
    """Require the standalone analytics Market Data Plane contract."""
    existing_tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
        if row and row[0]
    }
    missing_tables = sorted(
        (_COMPATIBILITY_METADATA_TABLES | set(required_tables)) - existing_tables
    )
    if missing_tables:
        missing = ", ".join(missing_tables)
        raise RuntimeError(
            "Incompatible market.duckdb: missing required Market v5 tables "
            f"({missing}). Run bt market-cutover cutover to rebuild the "
            "Market Data Plane."
        )

    version_row = conn.execute(
        "SELECT MAX(version) FROM market_schema_version"
    ).fetchone()
    version = int(version_row[0]) if version_row and version_row[0] is not None else None
    mode_row = conn.execute(
        "SELECT value FROM sync_metadata WHERE key = 'stock_price_adjustment_mode'"
    ).fetchone()
    adjustment_mode = str(mode_row[0]) if mode_row and mode_row[0] is not None else None
    if (
        version != _MARKET_SCHEMA_VERSION
        or adjustment_mode != _STOCK_PRICE_ADJUSTMENT_MODE
    ):
        observed_version = "missing" if version is None else str(version)
        observed_mode = "missing" if adjustment_mode is None else adjustment_mode
        raise RuntimeError(
            "Incompatible market.duckdb metadata: required schema version 5 and "
            "stock_price_adjustment_mode=provider_adjusted_v1; observed "
            f"schema version {observed_version} and adjustment mode {observed_mode}. "
            "Run bt market-cutover cutover to rebuild the Market Data Plane."
        )
    return _MARKET_SCHEMA_VERSION
