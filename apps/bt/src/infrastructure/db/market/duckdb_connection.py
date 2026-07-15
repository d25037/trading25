"""DuckDB connection helpers for market data stores."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, cast


_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)?\s*$")
_SIZE_UNITS = {
    "bytes": 1,
    "byte": 1,
    "b": 1,
    "kib": 1024,
    "kb": 1000,
    "mib": 1024**2,
    "mb": 1000**2,
    "gib": 1024**3,
    "gb": 1000**3,
    "tib": 1024**4,
    "tb": 1000**4,
}


def connect_market_duckdb(
    db_path: str | Path,
    *,
    read_only: bool = False,
    temp_directory: str | Path | None = None,
) -> Any:
    """Connect to market DuckDB with a managed temp directory."""
    path = Path(db_path)
    duckdb = __import__("duckdb")
    conn = cast(Any, duckdb).connect(str(path), read_only=read_only)
    environment_temp_dir = os.environ.get("TRADING25_DUCKDB_TEMP_DIR")
    resolved_temp_dir = (
        Path(temp_directory)
        if temp_directory is not None
        else (
            Path(environment_temp_dir)
            if environment_temp_dir
            else path.parent / "duckdb-tmp"
        )
    )
    resolved_temp_dir.mkdir(parents=True, exist_ok=True)
    escaped = str(resolved_temp_dir).replace("'", "''")
    conn.execute(f"SET temp_directory='{escaped}'")
    return conn


def parse_duckdb_size_bytes(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return 0
    match = _SIZE_RE.match(text)
    if match is None:
        return 0
    number = float(match.group(1))
    unit = (match.group(2) or "bytes").lower()
    return int(number * _SIZE_UNITS.get(unit, 1))


def resolve_directory_size(path: Path) -> int:
    try:
        if not path.exists():
            return 0
        total = 0
        for file_path in path.rglob("*"):
            if file_path.is_file():
                total += int(file_path.stat().st_size)
        return total
    except OSError:
        return 0
