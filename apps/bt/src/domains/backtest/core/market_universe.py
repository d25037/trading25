"""Backtest helpers for resolving market.duckdb-backed universes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.universe_resolver import (
    UniverseResolutionError,
    resolve_universe,
    resolve_universe_code_superset,
)
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
        filters = _universe_filters(shared_config)
        end_date = str(shared_config.get("end_date") or as_of_date).strip()
        if (
            shared_config.get("data_source", "market") == "market"
            and shared_config.get("static_universe") is not True
            and end_date
            and end_date >= as_of_date
        ):
            resolved = resolve_universe_code_superset(
                market_db,
                start_date=as_of_date,
                end_date=end_date,
                preset=preset,
                filters=filters,
            )
        else:
            resolved = resolve_universe(
                market_db,
                as_of_date=as_of_date,
                preset=preset,
                filters=filters,
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
        resolved.provenance.asOfDate,
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


def build_dynamic_universe_eligibility_frame(
    shared_config: dict[str, Any],
    *,
    index: pd.DatetimeIndex,
    columns: list[str],
) -> pd.DataFrame:
    """Build a bool frame marking whether each code is entry-eligible on each date."""
    if not _uses_dynamic_market_universe(shared_config) or index.empty or not columns:
        return pd.DataFrame(True, index=index, columns=columns, dtype=bool)

    preset = str(shared_config.get("universe_preset") or "").strip()
    start_date = pd.Timestamp(index.min()).strftime("%Y-%m-%d")
    end_date = pd.Timestamp(index.max()).strftime("%Y-%m-%d")
    filters = _universe_filters(shared_config)
    normalized_columns = [str(column) for column in columns]
    market_db = MarketDb(str(_market_db_path()), read_only=True)
    try:
        rows = _membership_rows_for_preset(
            market_db,
            preset=preset,
            start_date=start_date,
            end_date=end_date,
            codes=normalized_columns,
            filters=filters,
        )
    finally:
        market_db.close()

    eligibility = pd.DataFrame(False, index=index, columns=columns, dtype=bool)
    index_by_date: dict[str, list[pd.Timestamp]] = {}
    for ts in index:
        timestamp = pd.Timestamp(ts)
        index_by_date.setdefault(timestamp.strftime("%Y-%m-%d"), []).append(timestamp)
    column_set = set(normalized_columns)
    for date_key, code in rows:
        if code not in column_set:
            continue
        for ts in index_by_date.get(str(date_key), []):
            eligibility.loc[ts, code] = True
    return eligibility


def _uses_dynamic_market_universe(shared_config: dict[str, Any]) -> bool:
    return (
        shared_config.get("data_source", "market") == "market"
        and bool(str(shared_config.get("universe_preset") or "").strip())
        and shared_config.get("static_universe") is not True
    )


def _universe_filters(shared_config: dict[str, Any]) -> dict[str, Any] | None:
    filters = shared_config.get("universe_filters")
    return filters if isinstance(filters, dict) else None


def _membership_rows_for_preset(
    market_db: MarketDb,
    *,
    preset: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    filters: dict[str, Any] | None,
) -> list[tuple[str, str]]:
    # Currently only code filters are supported in dynamic entry masks.
    if filters:
        allowed_codes = filters.get("codes")
        if isinstance(allowed_codes, list) and allowed_codes:
            allowed = {str(code) for code in allowed_codes}
            codes = [code for code in codes if code in allowed]
    if preset in {"prime", "standard", "growth"}:
        from src.infrastructure.db.market.universe_resolver import _MARKET_CODES_BY_PRESET

        return market_db.get_stock_master_code_dates_for_date_range(
            start_date,
            end_date,
            codes=codes,
            market_codes=list(_MARKET_CODES_BY_PRESET[preset]),
        )
    if preset == "topix100":
        return market_db.get_stock_master_code_dates_for_date_range(
            start_date,
            end_date,
            codes=codes,
            scale_categories=["TOPIX Core30", "TOPIX Large70"],
        )
    if preset == "primeExTopix500":
        from src.infrastructure.db.market.universe_resolver import (
            _MARKET_CODES_BY_PRESET,
            _TOPIX500_SCALE_CATEGORIES,
        )

        return market_db.get_stock_master_code_dates_for_date_range(
            start_date,
            end_date,
            codes=codes,
            market_codes=list(_MARKET_CODES_BY_PRESET["prime"]),
            exclude_scale_categories=list(_TOPIX500_SCALE_CATEGORIES),
        )
    if preset == "custom":
        return [
            (pd.Timestamp(ts).strftime("%Y-%m-%d"), code)
            for ts in pd.date_range(start_date, end_date)
            for code in codes
        ]
    raise ValueError(f"Unsupported universe preset: {preset}")
