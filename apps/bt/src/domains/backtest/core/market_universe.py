"""Backtest helpers for resolving market.duckdb-backed universes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

from src.infrastructure.db.market.universe_resolver import (
    UniverseResolutionError,
    resolve_universe,
)
from src.infrastructure.db.market.market_db import MarketDb
from src.shared.config.settings import get_settings


class DatasetSnapshotRuntimeError(ValueError):
    """Raised when a normal run tries to use a physical dataset snapshot as SoT."""


def _market_db_path() -> Path:
    settings = get_settings()
    market_timeseries_dir = str(getattr(settings, "market_timeseries_dir", "") or "").strip()
    if not market_timeseries_dir:
        raise FileNotFoundError("MARKET_TIMESERIES_DIR is not configured")
    return Path(market_timeseries_dir).resolve() / "market.duckdb"


def resolve_backtest_universe_codes(shared_config: dict[str, Any]) -> list[str] | None:
    """Resolve stock_codes=['all'] from market.duckdb when dataset is a universe preset.

    Phase 6 demotes physical dataset bundles to export/repro fixtures only.  Normal
    all-stock backtest/research execution must choose a market-backed universe preset.
    """
    stock_codes = shared_config.get("stock_codes", ["all"])
    if stock_codes != ["all"]:
        return None

    explicit_preset = str(shared_config.get("universe_preset") or "").strip()
    preset = explicit_preset or None
    if preset is None:
        if (
            shared_config.get("data_source") == "dataset_snapshot"
            and shared_config.get("static_universe") is True
        ):
            logger.warning(
                "Using archived/static dataset snapshot universe={} because static_universe=true",
                shared_config.get("dataset_snapshot"),
            )
            return None
        raise DatasetSnapshotRuntimeError(
            "Physical dataset snapshots are no longer supported as the normal universe SoT. "
            "Set shared_config.universe_preset (prime/standard/growth/topix100/primeExTopix500). "
            "For explicit archived reproducibility only, set data_source=dataset_snapshot, "
            "dataset_snapshot=<name>, and static_universe=true."
        )

    as_of_date = str(
        shared_config.get("universe_as_of_date")
        or shared_config.get("start_date")
        or shared_config.get("end_date")
        or ""
    ).strip()
    if not as_of_date:
        raise ValueError(
            "shared_config.start_date or shared_config.universe_as_of_date is required "
            "when resolving a market-backed universe"
        )

    market_db = MarketDb(str(_market_db_path()), read_only=True)
    try:
        resolved = resolve_universe(
            market_db,
            as_of_date=as_of_date,
            preset=preset,
            filters=shared_config.get("universe_filters") if isinstance(shared_config.get("universe_filters"), dict) else None,
        )
    except UniverseResolutionError as exc:
        raise ValueError(f"Failed to resolve universe preset '{preset}': {exc}") from exc
    finally:
        market_db.close()

    if not resolved.codes:
        warnings = "; ".join(resolved.provenance.warnings)
        raise ValueError(
            f"Universe preset '{preset}' resolved to 0 stocks for {as_of_date}. {warnings}"
        )

    logger.info(
        "Resolved market universe preset={} as_of={} count={} source={}",
        preset,
        as_of_date,
        len(resolved.codes),
        resolved.provenance.sourceTable,
    )
    shared_config["universe_preset"] = preset
    shared_config["universe_as_of_date"] = as_of_date
    shared_config["universe_provenance"] = {
        "sourceTable": resolved.provenance.sourceTable,
        "asOfDate": resolved.provenance.asOfDate,
        "preset": resolved.provenance.preset,
        "rowCount": resolved.provenance.rowCount,
        "resolvedCount": resolved.provenance.resolvedCount,
        "filters": resolved.provenance.filters,
        "warnings": resolved.provenance.warnings,
    }
    return resolved.codes
