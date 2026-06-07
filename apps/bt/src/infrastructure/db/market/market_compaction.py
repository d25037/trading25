"""Offline compaction helpers for market DuckDB files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, cast


@dataclass(frozen=True)
class MarketCompactionResult:
    source_path: Path
    output_path: Path
    source_bytes: int
    output_bytes: int
    table_count: int
    elapsed_ms: float


def _duckdb_path_bytes(path: Path) -> int:
    total = 0
    for candidate in (path, Path(f"{path}.wal")):
        try:
            if candidate.exists():
                total += int(candidate.stat().st_size)
        except OSError:
            continue
    return total


def _sql_string(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _unlink_duckdb_artifacts(path: Path) -> None:
    for candidate in (path, Path(f"{path}.wal")):
        try:
            if candidate.exists():
                candidate.unlink()
        except FileNotFoundError:
            continue


def compact_market_duckdb(
    source_path: str | Path,
    output_path: str | Path,
    *,
    overwrite: bool = False,
) -> MarketCompactionResult:
    """Create a compact copy of a market DuckDB file without replacing the source."""
    source = Path(source_path).expanduser()
    output = Path(output_path).expanduser()
    source_resolved = source.resolve(strict=True)
    output_resolved = output.resolve(strict=False)
    if source_resolved == output_resolved:
        raise ValueError("Compaction output path must differ from the source path")
    if output.exists() and not overwrite:
        raise FileExistsError(f"Compaction output already exists: {output}")

    output.parent.mkdir(parents=True, exist_ok=True)
    if overwrite:
        _unlink_duckdb_artifacts(output)

    duckdb = __import__("duckdb")
    started = perf_counter()
    conn = cast(Any, duckdb).connect(":memory:")
    try:
        conn.execute(f"ATTACH {_sql_string(source_resolved)} AS source_db (READ_ONLY)")
        conn.execute(f"ATTACH {_sql_string(output_resolved)} AS target_db")
        table_count = len(conn.execute("SHOW TABLES FROM source_db").fetchall())
        conn.execute("COPY FROM DATABASE source_db TO target_db")
        conn.execute("CHECKPOINT target_db")
    except Exception:
        conn.close()
        _unlink_duckdb_artifacts(output)
        raise
    else:
        conn.close()

    return MarketCompactionResult(
        source_path=source,
        output_path=output,
        source_bytes=_duckdb_path_bytes(source),
        output_bytes=_duckdb_path_bytes(output),
        table_count=table_count,
        elapsed_ms=(perf_counter() - started) * 1000,
    )
