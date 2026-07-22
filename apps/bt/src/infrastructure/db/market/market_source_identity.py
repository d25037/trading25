"""Validated identity of one active Market v5 DuckDB source."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import stat
from typing import Any, cast

from .market_schema import (
    MARKET_SCHEMA_VERSION,
    PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE,
    inspect_adjusted_daily_volume_physical_contract,
)


class MarketSourceIdentityError(RuntimeError):
    pass


@dataclass(frozen=True)
class MarketSourceIdentity:
    path: Path
    device: int
    inode: int
    size: int
    schema_version: int
    adjustment_mode: str


def inspect_market_source_identity(path: Path) -> MarketSourceIdentity:
    path = Path(os.path.abspath(os.fspath(path)))
    try:
        path_stat = path.lstat()
    except OSError as exc:
        raise MarketSourceIdentityError("Market source is missing") from exc
    if stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISREG(path_stat.st_mode):
        raise MarketSourceIdentityError("Market source must be a regular file")
    duckdb = __import__("duckdb")
    connection = cast(Any, duckdb).connect(str(path), read_only=True)
    physical_contract_issues: list[str] = []
    try:
        schema_row = connection.execute(
            "SELECT MAX(version) FROM market_schema_version"
        ).fetchone()
        mode_row = connection.execute(
            "SELECT value FROM sync_metadata WHERE key = 'stock_price_adjustment_mode'"
        ).fetchone()
        if schema_row and schema_row[0] == MARKET_SCHEMA_VERSION:
            physical_contract_issues = (
                inspect_adjusted_daily_volume_physical_contract(connection)
            )
    except Exception as exc:
        raise MarketSourceIdentityError(
            f"Market source must be schema v{MARKET_SCHEMA_VERSION}"
        ) from exc
    finally:
        connection.close()
    current = path.lstat()
    if (
        stat.S_ISLNK(current.st_mode)
        or not stat.S_ISREG(current.st_mode)
        or (path_stat.st_dev, path_stat.st_ino) != (current.st_dev, current.st_ino)
    ):
        raise MarketSourceIdentityError("Market source identity changed during validation")
    schema = int(schema_row[0]) if schema_row and schema_row[0] is not None else None
    mode = str(mode_row[0]) if mode_row and mode_row[0] is not None else None
    if schema != MARKET_SCHEMA_VERSION:
        raise MarketSourceIdentityError(
            f"Market source must be schema v{MARKET_SCHEMA_VERSION}"
        )
    if mode != PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE:
        raise MarketSourceIdentityError(
            "Market source adjustment mode must be "
            f"{PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE}"
        )
    if physical_contract_issues:
        raise MarketSourceIdentityError(
            "Incompatible Market v5 daily adjusted-volume physical contract "
            f"({', '.join(physical_contract_issues)}). Run RESET initial "
            "(mode='initial', resetBeforeSync=true) to rebuild; compatibility "
            "reads and in-place migration are not supported."
        )
    return MarketSourceIdentity(path, current.st_dev, current.st_ino, current.st_size, schema, mode)


def assert_same_market_source(expected: MarketSourceIdentity) -> None:
    try:
        current = expected.path.lstat()
    except OSError as exc:
        raise MarketSourceIdentityError("Active Market source is missing") from exc
    if (
        stat.S_ISLNK(current.st_mode)
        or not stat.S_ISREG(current.st_mode)
        or (current.st_dev, current.st_ino) != (expected.device, expected.inode)
    ):
        raise MarketSourceIdentityError("Active Market source inode changed")


def capture_market_source_identity(
    path: Path,
    *,
    schema_version: int,
    adjustment_mode: str,
) -> MarketSourceIdentity:
    path = Path(os.path.abspath(os.fspath(path)))
    current = path.lstat()
    if stat.S_ISLNK(current.st_mode) or not stat.S_ISREG(current.st_mode):
        raise MarketSourceIdentityError("Market source must be a regular file")
    if schema_version != MARKET_SCHEMA_VERSION:
        raise MarketSourceIdentityError(
            f"Market source must be schema v{MARKET_SCHEMA_VERSION}"
        )
    if adjustment_mode != PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE:
        raise MarketSourceIdentityError(
            "Market source adjustment mode must be "
            f"{PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE}"
        )
    return MarketSourceIdentity(
        path,
        current.st_dev,
        current.st_ino,
        current.st_size,
        schema_version,
        adjustment_mode,
    )
