"""Offline compaction helpers for market DuckDB files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, cast
from uuid import uuid4


@dataclass(frozen=True)
class MarketCompactionResult:
    source_path: Path
    output_path: Path
    source_bytes: int
    output_bytes: int
    table_count: int
    elapsed_ms: float


@dataclass(frozen=True)
class MarketInPlaceCompactionResult:
    source_path: Path
    compacted: bool
    reason: str
    before_bytes: int
    after_bytes: int
    before_free_bytes: int
    after_free_bytes: int
    before_free_ratio: float
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


def _move_duckdb_artifacts(source: Path, target: Path) -> None:
    for source_candidate, target_candidate in (
        (source, target),
        (Path(f"{source}.wal"), Path(f"{target}.wal")),
    ):
        if source_candidate.exists():
            source_candidate.replace(target_candidate)


def _read_duckdb_size_snapshot(path: Path) -> dict[str, int]:
    duckdb = __import__("duckdb")
    conn = cast(Any, duckdb).connect(str(path), read_only=True)
    try:
        row = conn.execute("PRAGMA database_size").fetchone()
        columns = [description[0] for description in conn.description]
    finally:
        conn.close()
    values = dict(zip(columns, row, strict=False))
    block_size = int(values.get("block_size") or 0)
    free_blocks = int(values.get("free_blocks") or 0)
    return {
        "block_size": block_size,
        "free_blocks": free_blocks,
        "free_bytes": block_size * free_blocks,
    }


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


def compact_market_duckdb_in_place_if_needed(
    source_path: str | Path,
    *,
    min_free_bytes: int,
    min_free_ratio: float,
) -> MarketInPlaceCompactionResult:
    """Replace a closed market DuckDB file with a compact copy when free space is high."""
    source = Path(source_path).expanduser()
    source_resolved = source.resolve(strict=True)
    before_bytes = _duckdb_path_bytes(source_resolved)
    before_size = _read_duckdb_size_snapshot(source_resolved)
    before_free_bytes = before_size["free_bytes"]
    before_free_ratio = before_free_bytes / before_bytes if before_bytes > 0 else 0.0
    if before_free_bytes < min_free_bytes or before_free_ratio < min_free_ratio:
        return MarketInPlaceCompactionResult(
            source_path=source,
            compacted=False,
            reason="below_threshold",
            before_bytes=before_bytes,
            after_bytes=before_bytes,
            before_free_bytes=before_free_bytes,
            after_free_bytes=before_free_bytes,
            before_free_ratio=before_free_ratio,
            table_count=0,
            elapsed_ms=0.0,
        )

    suffix = uuid4().hex
    compact_path = source_resolved.with_name(f"{source_resolved.name}.compact-{suffix}")
    backup_path = source_resolved.with_name(f"{source_resolved.name}.precompact-{suffix}")
    started = perf_counter()
    table_count = 0
    try:
        compact_result = compact_market_duckdb(source_resolved, compact_path, overwrite=True)
        table_count = compact_result.table_count
        _move_duckdb_artifacts(source_resolved, backup_path)
        _move_duckdb_artifacts(compact_path, source_resolved)
        after_bytes = _duckdb_path_bytes(source_resolved)
        after_size = _read_duckdb_size_snapshot(source_resolved)
    except Exception:
        if not source_resolved.exists() and backup_path.exists():
            _move_duckdb_artifacts(backup_path, source_resolved)
        _unlink_duckdb_artifacts(compact_path)
        raise
    else:
        _unlink_duckdb_artifacts(backup_path)

    return MarketInPlaceCompactionResult(
        source_path=source,
        compacted=True,
        reason="compacted",
        before_bytes=before_bytes,
        after_bytes=after_bytes,
        before_free_bytes=before_free_bytes,
        after_free_bytes=after_size["free_bytes"],
        before_free_ratio=before_free_ratio,
        table_count=table_count,
        elapsed_ms=(perf_counter() - started) * 1000,
    )
