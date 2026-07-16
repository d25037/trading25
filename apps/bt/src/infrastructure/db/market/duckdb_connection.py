"""DuckDB connection helpers for market data stores."""

from __future__ import annotations

import os
import re
import errno
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
_OWNED_TEMP_CAPABILITY = "retained_market_smoke"
_OWNED_TEMP_PATH_RE = re.compile(
    r"^\.cutover-runtime-[A-Za-z0-9][A-Za-z0-9._-]{0,127}/duckdb-tmp$"
)
_DIRECTORY_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_MARKET_WRITER_SECRET = object()


class MarketWriterToken:
    """Unforgeable process-local capability issued only to the writer factory."""

    __slots__ = ("_secret",)

    def __init__(self, secret: object) -> None:
        if secret is not _MARKET_WRITER_SECRET:
            raise PermissionError("Market writer token is factory-owned")
        self._secret = secret


def _issue_market_writer_token() -> MarketWriterToken:  # pyright: ignore[reportUnusedFunction]
    """Issue the private capability consumed only by MarketWriterResourceFactory."""
    return MarketWriterToken(_MARKET_WRITER_SECRET)


def _require_market_writer_token(token: MarketWriterToken | None) -> None:
    if token is None or token._secret is not _MARKET_WRITER_SECRET:
        raise PermissionError("Writable Market open requires the writer resource factory")


def _create_real_directory_tree(path: Path) -> None:
    """Create *path* without following symlink or special-file components."""
    if path.is_absolute():
        directory_fd = os.open("/", _DIRECTORY_OPEN_FLAGS)
        parts = path.parts[1:]
    else:
        directory_fd = os.open(".", _DIRECTORY_OPEN_FLAGS)
        parts = path.parts
    try:
        for part in parts:
            if part in {"", ".", ".."}:
                raise ValueError("DuckDB temp directory path is not safe")
            try:
                child_fd = os.open(part, _DIRECTORY_OPEN_FLAGS, dir_fd=directory_fd)
            except FileNotFoundError:
                try:
                    os.mkdir(part, dir_fd=directory_fd)
                except FileExistsError:
                    pass
                try:
                    child_fd = os.open(
                        part,
                        _DIRECTORY_OPEN_FLAGS,
                        dir_fd=directory_fd,
                    )
                except OSError as exc:
                    raise ValueError(
                        "DuckDB temp directory must contain only real directories"
                    ) from exc
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise ValueError(
                        "DuckDB temp directory must contain only real directories"
                    ) from exc
                raise
            os.close(directory_fd)
            directory_fd = child_fd
    finally:
        os.close(directory_fd)


def _resolve_market_duckdb_temp_directory(
    db_path: Path,
    explicit: str | Path | None,
) -> Path:
    if explicit is not None:
        return Path(explicit)
    ambient = os.environ.get("TRADING25_DUCKDB_TEMP_DIR")
    capability = os.environ.get("TRADING25_RUNTIME_CAPABILITY")
    if capability != _OWNED_TEMP_CAPABILITY:
        return db_path.parent / "duckdb-tmp"
    if ambient is None or _OWNED_TEMP_PATH_RE.fullmatch(ambient) is None:
        raise ValueError("DuckDB temp directory override is not canonical")
    return db_path.parent / ambient


def connect_market_duckdb(
    db_path: str | Path,
    *,
    read_only: bool = True,
    temp_directory: str | Path | None = None,
    writer_token: MarketWriterToken | None = None,
) -> Any:
    """Connect to market DuckDB with a managed temp directory."""
    path = Path(db_path)
    resolved_temp_dir: Path | None = None
    if not read_only:
        _require_market_writer_token(writer_token)
        resolved_temp_dir = _resolve_market_duckdb_temp_directory(path, temp_directory)
        _create_real_directory_tree(resolved_temp_dir)
    duckdb = __import__("duckdb")
    conn = cast(Any, duckdb).connect(str(path), read_only=read_only)
    if read_only:
        return conn
    assert resolved_temp_dir is not None
    escaped = str(resolved_temp_dir).replace("'", "''")
    try:
        conn.execute(f"SET temp_directory='{escaped}'")
    except Exception:
        conn.close()
        raise
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
