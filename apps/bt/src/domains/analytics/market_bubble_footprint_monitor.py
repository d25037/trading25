"""Runtime market-bubble footprint monitor backed by market.duckdb.

The research bundle remains the publication artifact. This module provides the
ranking-page monitor path: compute only the narrow footprint needed for the
current as-of date, and cache the historical percentile baseline.
"""

from __future__ import annotations

from functools import lru_cache
import json
import math
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from src.domains.analytics.market_bubble_footprint_support import (
    DEFAULT_FOOTPRINT_HORIZONS,
    BUBBLE_FOOTPRINT_ID,
    RERATING_BUBBLE_REGIME_ID,
    assert_footprint_required_tables as _assert_footprint_required_tables,
    build_footprint_table as _build_footprint_table,
    classify_footprint as _classify_footprint,
)
from src.shared.config.settings import get_settings
from src.shared.paths.resolver import get_cache_dir
from src.shared.utils.market_code_alias import canonicalize_market_list

_NEAR_BLOWOFF_RETURN_DISPERSION_PERCENTILE = 0.85
_BUBBLE_BASELINE_CACHE_VERSION = 2
_BUBBLE_BASELINE_START_DATE = "2016-01-01"
_BUBBLE_REGIME_BY_SCORE = {
    0: "normal",
    1: "normal",
    2: "narrowing",
    3: "crowded",
    4: "blowoff_watch",
    5: "blowoff_watch",
}


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float, str)):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _split_active_flags(value: Any) -> list[str]:
    if value is None:
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _is_near_blowoff(row: Any) -> bool:
    score = int(getattr(row, "bubble_score", 0) or 0)
    regime = str(getattr(row, "bubble_regime", "") or "")
    dispersion = _optional_float(getattr(row, "return_p90_p10_spread_pct_hist_pct", None))
    return (
        score == 3
        and regime != "blowoff_watch"
        and dispersion is not None
        and dispersion >= _NEAR_BLOWOFF_RETURN_DISPERSION_PERCENTILE
    )


def _normalize_footprint_markets(markets: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(canonicalize_market_list(markets))
    return normalized if normalized else ("prime", "standard", "growth")


def _market_duckdb_path() -> str:
    settings = get_settings()
    return str(Path(settings.market_timeseries_dir) / "market.duckdb")


@lru_cache(maxsize=8)
def _cached_market_bubble_footprint_baseline_from_db(
    db_path: str,
    db_mtime_ns: int,
    markets: tuple[str, ...],
) -> pd.DataFrame:
    cached = _load_market_bubble_footprint_baseline_cache(db_path, db_mtime_ns, markets)
    if cached is not None:
        return cached

    conn = duckdb.connect(db_path, read_only=True)
    try:
        _assert_footprint_required_tables(conn)
        baseline = _build_market_bubble_footprint_percentile_baseline(
            conn,
            start_date=_BUBBLE_BASELINE_START_DATE,
            end_date=None,
            return_horizons=DEFAULT_FOOTPRINT_HORIZONS,
            market_scopes=markets,
            table_name="bubble_footprint_baseline",
        )
    finally:
        conn.close()
    if "snapshot_date" in baseline.columns:
        baseline["snapshot_date"] = baseline["snapshot_date"].astype(str)
    _write_market_bubble_footprint_baseline_cache(db_path, db_mtime_ns, markets, baseline)
    return baseline


def _build_market_bubble_footprint_percentile_baseline(
    conn: Any,
    *,
    start_date: str,
    end_date: str | None,
    return_horizons: tuple[int, ...],
    market_scopes: tuple[str, ...],
    table_name: str,
) -> pd.DataFrame:
    return _build_footprint_table(
        conn,
        start_date=start_date,
        end_date=end_date,
        return_horizons=return_horizons,
        market_scopes=market_scopes,
        frequency="monthly",
        table_name=table_name,
    )


def _market_bubble_footprint_baseline_cache_paths(
    db_path: str,
    markets: tuple[str, ...],
) -> tuple[Path, Path]:
    market_key = "-".join(markets).replace("/", "_") or "default"
    cache_dir = get_cache_dir() / "market-bubble-footprint"
    stem = f"baseline-v{_BUBBLE_BASELINE_CACHE_VERSION}-{Path(db_path).stem}-{market_key}"
    return cache_dir / f"{stem}.csv", cache_dir / f"{stem}.meta.json"


def _load_market_bubble_footprint_baseline_cache(
    db_path: str,
    db_mtime_ns: int,
    markets: tuple[str, ...],
) -> pd.DataFrame | None:
    csv_path, meta_path = _market_bubble_footprint_baseline_cache_paths(db_path, markets)
    if not csv_path.is_file() or not meta_path.is_file():
        return None
    try:
        metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    expected_metadata = {
        "version": _BUBBLE_BASELINE_CACHE_VERSION,
        "db_path": str(Path(db_path)),
        "db_mtime_ns": int(db_mtime_ns),
        "markets": list(markets),
        "baseline_start_date": _BUBBLE_BASELINE_START_DATE,
        "frequency": "monthly",
        "return_horizons": list(DEFAULT_FOOTPRINT_HORIZONS),
    }
    if metadata != expected_metadata:
        return None
    try:
        baseline = pd.read_csv(csv_path)
    except (OSError, pd.errors.ParserError):
        return None
    if "snapshot_date" in baseline.columns:
        baseline["snapshot_date"] = baseline["snapshot_date"].astype(str)
    return baseline


def _write_market_bubble_footprint_baseline_cache(
    db_path: str,
    db_mtime_ns: int,
    markets: tuple[str, ...],
    baseline: pd.DataFrame,
) -> None:
    csv_path, meta_path = _market_bubble_footprint_baseline_cache_paths(db_path, markets)
    metadata = {
        "version": _BUBBLE_BASELINE_CACHE_VERSION,
        "db_path": str(Path(db_path)),
        "db_mtime_ns": int(db_mtime_ns),
        "markets": list(markets),
        "baseline_start_date": _BUBBLE_BASELINE_START_DATE,
        "frequency": "monthly",
        "return_horizons": list(DEFAULT_FOOTPRINT_HORIZONS),
    }
    try:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_csv_path = csv_path.with_suffix(".csv.tmp")
        tmp_meta_path = meta_path.with_suffix(".meta.json.tmp")
        baseline.to_csv(tmp_csv_path, index=False)
        tmp_meta_path.write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")
        tmp_csv_path.replace(csv_path)
        tmp_meta_path.replace(meta_path)
    except OSError:
        return


@lru_cache(maxsize=64)
def _cached_market_bubble_footprint_as_of(
    db_path: str,
    db_mtime_ns: int,
    markets: tuple[str, ...],
    date: str | None,
) -> dict[str, Any]:
    baseline = _cached_market_bubble_footprint_baseline_from_db(db_path, db_mtime_ns, markets)
    latest = _build_market_bubble_footprint_as_of_frame(
        db_path,
        baseline=baseline,
        markets=markets,
        date=date,
    )
    return _serialize_market_bubble_footprint(latest, markets=markets)


def _serialize_market_bubble_footprint(latest: pd.DataFrame, *, markets: tuple[str, ...]) -> dict[str, Any]:
    horizons: list[dict[str, Any]] = []
    for row in latest.itertuples(index=False):
        cap_weight = _optional_float(getattr(row, "cap_weight_return_pct", None))
        equal_weight = _optional_float(getattr(row, "equal_weight_return_pct", None))
        horizons.append(
            {
                "horizon": int(getattr(row, "horizon")),
                "score": int(getattr(row, "bubble_score", 0) or 0),
                "regime": str(getattr(row, "bubble_regime", "") or "normal"),
                "nearBlowoff": _is_near_blowoff(row),
                "breadthUpPct": _optional_float(getattr(row, "breadth_up_pct", None)),
                "pctAboveSma50": _optional_float(getattr(row, "pct_above_sma50", None)),
                "pctAboveSma200": _optional_float(getattr(row, "pct_above_sma200", None)),
                "expensiveMcapSharePct": _optional_float(
                    getattr(row, "expensive_mcap_share_pct", None)
                ),
                "returnP90P10SpreadPct": _optional_float(
                    getattr(row, "return_p90_p10_spread_pct", None)
                ),
                "returnDispersionPercentile": _optional_float(
                    getattr(row, "return_p90_p10_spread_pct_hist_pct", None)
                ),
                "capWeightLeadershipPct": (
                    cap_weight - equal_weight
                    if cap_weight is not None and equal_weight is not None
                    else None
                ),
                "activeFlags": _split_active_flags(getattr(row, "active_flags", None)),
            }
        )

    overall_score = max((int(item["score"]) for item in horizons), default=0)
    overall_regime = _BUBBLE_REGIME_BY_SCORE.get(overall_score, "blowoff_watch")
    return {
        "date": str(latest["snapshot_date"].iloc[0]) if not latest.empty else "",
        "markets": list(markets),
        "overallRegime": overall_regime,
        "overallScore": overall_score,
        "nearBlowoff": any(bool(item["nearBlowoff"]) for item in horizons)
        or overall_regime == "blowoff_watch",
        "researchExperimentId": BUBBLE_FOOTPRINT_ID,
        "reratingExperimentId": RERATING_BUBBLE_REGIME_ID,
        "horizons": horizons,
    }


def _build_market_bubble_footprint_as_of_frame(
    db_path: str,
    *,
    baseline: pd.DataFrame,
    markets: tuple[str, ...],
    date: str | None,
) -> pd.DataFrame:
    target_date = _resolve_bubble_footprint_target_date(db_path, date=date)
    conn = duckdb.connect(db_path, read_only=True)
    try:
        _assert_footprint_required_tables(conn)
        raw = _build_footprint_table(
            conn,
            start_date=target_date,
            end_date=target_date,
            return_horizons=DEFAULT_FOOTPRINT_HORIZONS,
            market_scopes=markets,
            frequency="monthly",
            table_name="bubble_footprint_as_of",
        )
    finally:
        conn.close()
    return _reclassify_footprint_against_baseline(raw, baseline=baseline, target_date=target_date)


def _resolve_bubble_footprint_target_date(db_path: str, *, date: str | None) -> str:
    conn = duckdb.connect(db_path, read_only=True)
    try:
        if date is None:
            row = conn.execute(
                "SELECT max(date) FROM stock_data_raw WHERE adjusted_close > 0"
            ).fetchone()
        else:
            target = pd.Timestamp(date).strftime("%Y-%m-%d")
            row = conn.execute(
                "SELECT max(date) FROM stock_data_raw "
                "WHERE adjusted_close > 0 AND date <= ?",
                [target],
            ).fetchone()
    finally:
        conn.close()
    value = row[0] if row is not None else None
    if value is None:
        raise FileNotFoundError("No market price data was available for the requested bubble footprint date.")
    return str(value)


def _reclassify_footprint_against_baseline(
    raw: pd.DataFrame,
    *,
    baseline: pd.DataFrame,
    target_date: str,
) -> pd.DataFrame:
    if raw.empty:
        return raw
    frame = raw.copy()
    frame["snapshot_date"] = frame["snapshot_date"].astype(str)
    history = baseline.copy()
    if not history.empty and "snapshot_date" in history.columns:
        history["snapshot_date"] = history["snapshot_date"].astype(str)
        history = history.loc[history["snapshot_date"] < target_date]

    classified = _classify_footprint(pd.concat([history, frame], ignore_index=True))
    return (
        classified.loc[classified["snapshot_date"].astype(str) == target_date]
        .sort_values(["snapshot_date", "horizon"])
        .reset_index(drop=True)
    )


def get_latest_market_bubble_footprint(
    *,
    markets: tuple[str, ...],
    date: str | None = None,
) -> dict[str, Any]:
    normalized_markets = _normalize_footprint_markets(markets)
    db_path = _market_duckdb_path()
    return _cached_market_bubble_footprint_as_of(
        db_path,
        Path(db_path).stat().st_mtime_ns,
        normalized_markets,
        date,
    )
