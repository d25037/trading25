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

from src.domains.analytics.earnings_holdthrough_expectancy import _table_exists
from src.domains.analytics.market_bubble_footprint import (
    DEFAULT_FOOTPRINT_HORIZONS,
    BUBBLE_FOOTPRINT_EXPERIMENT_ID,
    RERATING_BUBBLE_REGIME_EXPERIMENT_ID,
    _assert_footprint_required_tables,
    _build_footprint_table,
    _classify_footprint,
    _column_exists,
    _market_scope_case_sql,
    _optional_double_expr,
    _sql_string_list,
)
from src.domains.analytics.readonly_duckdb_support import normalize_code_sql
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
    _create_monitor_footprint_base_tables(
        conn,
        start_date=start_date,
        end_date=end_date,
        market_scopes=market_scopes,
    )
    horizons_sql = ", ".join(f"({int(horizon)})" for horizon in return_horizons)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE monitor_footprint_pairs AS
        WITH horizons(horizon) AS (VALUES {horizons_sql})
        SELECT
            s.snapshot_date,
            h.horizon,
            ca.date AS anchor_date
        FROM monitor_snapshot_dates s
        JOIN monitor_calendar cs ON cs.date = s.snapshot_date
        JOIN horizons h ON TRUE
        JOIN monitor_calendar ca ON ca.rn = cs.rn - h.horizon
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE monitor_footprint_observations AS
        SELECT
            p.snapshot_date,
            p.anchor_date,
            p.horizon,
            l.code,
            l.close / nullif(a.close, 0) - 1.0 AS return_decimal,
            va.market_cap AS anchor_market_cap,
            vl.market_cap AS latest_market_cap,
            vl.forward_per,
            vl.pbr
        FROM monitor_footprint_pairs p
        JOIN monitor_latest_stock l
          ON l.date = p.snapshot_date
        JOIN monitor_stock_price a
          ON a.code = l.code
         AND a.date = p.anchor_date
        LEFT JOIN monitor_daily_valuation va
          ON va.code = l.code
         AND va.date = a.date
        LEFT JOIN monitor_daily_valuation vl
          ON vl.code = l.code
         AND vl.date = l.date
        WHERE l.close > 0
          AND a.close > 0
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE monitor_footprint_contribution_base AS
        SELECT
            *,
            anchor_market_cap * return_decimal AS cap_return_contribution
        FROM monitor_footprint_observations
        WHERE return_decimal IS NOT NULL
          AND anchor_market_cap IS NOT NULL
          AND anchor_market_cap > 0
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE monitor_footprint_metric_base AS
        SELECT
            c.*,
            row_number() OVER (
                PARTITION BY snapshot_date, horizon
                ORDER BY cap_return_contribution DESC NULLS LAST
            ) AS positive_contribution_rank,
            row_number() OVER (
                PARTITION BY snapshot_date, horizon
                ORDER BY latest_market_cap DESC NULLS LAST
            ) AS mcap_rank
        FROM monitor_footprint_contribution_base c
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
        SELECT
            snapshot_date,
            anchor_date,
            horizon,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            avg(CASE WHEN return_decimal > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS breadth_up_pct,
            avg(return_decimal) * 100.0 AS equal_weight_return_pct,
            sum(anchor_market_cap * return_decimal) / nullif(sum(anchor_market_cap), 0)
                * 100.0 AS cap_weight_return_pct,
            (quantile_cont(return_decimal, 0.90) - quantile_cont(return_decimal, 0.10))
                * 100.0 AS return_p90_p10_spread_pct,
            sum(CASE WHEN positive_contribution_rank <= 10 AND cap_return_contribution > 0
                     THEN cap_return_contribution ELSE 0 END)
                / nullif(sum(CASE WHEN cap_return_contribution > 0
                                  THEN cap_return_contribution ELSE 0 END), 0)
                * 100.0 AS top10_positive_contribution_share_pct,
            sum(CASE WHEN mcap_rank <= 10 THEN latest_market_cap ELSE 0 END)
                / nullif(sum(latest_market_cap), 0) * 100.0 AS top10_mcap_share_pct,
            sum(CASE WHEN forward_per > 40 OR pbr > 5 THEN latest_market_cap ELSE 0 END)
                / nullif(sum(latest_market_cap), 0) * 100.0 AS expensive_mcap_share_pct,
            100.0 AS pct_above_sma50,
            100.0 AS pct_above_sma200,
            CAST(NULL AS DOUBLE) AS top5_positive_contribution_share_pct,
            CAST(NULL AS DOUBLE) AS top5_mcap_share_pct,
            CAST(NULL AS DOUBLE) AS expensive_count_share_pct,
            CAST(NULL AS DOUBLE) AS no_positive_earnings_count_share_pct,
            CAST(NULL AS DOUBLE) AS median_trading_value_ratio_20v232,
            CAST(NULL AS DOUBLE) AS p90_trading_value_ratio_20v232,
            CAST(NULL AS DOUBLE) AS topix_return_pct
        FROM monitor_footprint_metric_base
        GROUP BY snapshot_date, anchor_date, horizon
        ORDER BY snapshot_date, horizon
        """
    )
    return conn.execute(f"SELECT * FROM {table_name}").fetchdf()


def _create_monitor_footprint_base_tables(
    conn: Any,
    *,
    start_date: str,
    end_date: str | None,
    market_scopes: tuple[str, ...],
) -> None:
    query_start = (pd.Timestamp(start_date) - pd.Timedelta(days=900)).strftime("%Y-%m-%d")
    query_end_filter = "" if end_date is None else "AND date <= ?"
    query_end_params = [] if end_date is None else [end_date]
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE monitor_calendar AS
        SELECT date, row_number() OVER (ORDER BY date) AS rn
        FROM (
            SELECT DISTINCT date
            FROM topix_data
            WHERE close > 0
              AND date >= ?
              {query_end_filter}
        )
        ORDER BY date
        """,
        [query_start, *query_end_params],
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE monitor_snapshot_dates AS
        WITH ranged AS (
            SELECT date, substr(date, 1, 7) AS period_key
            FROM monitor_calendar
            WHERE date >= ?
        ),
        periodic AS (
            SELECT max(date) AS snapshot_date
            FROM ranged
            GROUP BY period_key
        ),
        latest AS (
            SELECT max(date) AS snapshot_date
            FROM ranged
        )
        SELECT DISTINCT snapshot_date
        FROM (
            SELECT snapshot_date FROM periodic
            UNION ALL
            SELECT snapshot_date FROM latest
        )
        WHERE snapshot_date IS NOT NULL
        ORDER BY snapshot_date
        """,
        [start_date],
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE monitor_required_dates AS
        SELECT snapshot_date AS date FROM monitor_snapshot_dates
        UNION
        SELECT ca.date
        FROM monitor_snapshot_dates s
        JOIN monitor_calendar cs ON cs.date = s.snapshot_date
        JOIN monitor_calendar ca
          ON ca.rn IN (
              cs.rn - 20,
              cs.rn - 60,
              cs.rn - 120,
              cs.rn - 252
          )
        WHERE ca.date IS NOT NULL
        """
    )
    _create_monitor_market_master_source(conn)
    stock_code = normalize_code_sql("sd.code")
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE monitor_stock_price AS
        SELECT code, date, close
        FROM (
            SELECT
                {stock_code} AS code,
                sd.date,
                CAST(sd.close AS DOUBLE) AS close,
                row_number() OVER (
                    PARTITION BY {stock_code}, sd.date
                    ORDER BY CASE WHEN length(sd.code) = 4 THEN 0 ELSE 1 END, sd.code
                ) AS row_rank
            FROM stock_data sd
            JOIN monitor_required_dates rd
              ON rd.date = sd.date
            WHERE sd.close > 0
        )
        WHERE row_rank = 1
        """
    )
    market_filter = (
        "TRUE"
        if "all" in market_scopes
        else f"mm.market IN ({_sql_string_list(market_scopes)})"
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE monitor_latest_stock AS
        SELECT p.code, p.date, p.close, mm.market
        FROM monitor_stock_price p
        JOIN monitor_market_master_source mm
          ON mm.code = p.code
         AND mm.date = p.date
        WHERE {market_filter}
        """
    )
    valuation_code = normalize_code_sql("dv.code")
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE monitor_daily_valuation AS
        SELECT code, date, market_cap, forward_per, pbr
        FROM (
            SELECT
                {valuation_code} AS code,
                dv.date,
                CAST(dv.market_cap AS DOUBLE) AS market_cap,
                {_optional_double_expr(conn, "daily_valuation", "dv", "forward_per")}
                    AS forward_per,
                {_optional_double_expr(conn, "daily_valuation", "dv", "pbr")} AS pbr,
                row_number() OVER (
                    PARTITION BY {valuation_code}, dv.date
                    ORDER BY dv.price_basis_date DESC NULLS LAST,
                             dv.basis_version DESC NULLS LAST,
                             CASE WHEN length(dv.code) = 4 THEN 0 ELSE 1 END,
                             dv.code
                ) AS row_rank
            FROM daily_valuation dv
            JOIN monitor_required_dates rd
              ON rd.date = dv.date
        )
        WHERE row_rank = 1
        """
    )


def _create_monitor_market_master_source(conn: Any) -> None:
    if _table_exists(conn, "stock_master_daily"):
        code = normalize_code_sql("smd.code")
        sector_expr = (
            "smd.sector_33_name"
            if _column_exists(conn, "stock_master_daily", "sector_33_name")
            else "'unknown'"
        )
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE monitor_market_master_source AS
            SELECT
                {code} AS code,
                smd.date,
                smd.company_name,
                {_market_scope_case_sql("smd.market_code")} AS market,
                {sector_expr} AS sector_33_name
            FROM stock_master_daily smd
            JOIN monitor_snapshot_dates d
              ON d.snapshot_date = smd.date
            """
        )
        return
    code = normalize_code_sql("s.code")
    sector_expr = (
        "s.sector_33_name" if _column_exists(conn, "stocks", "sector_33_name") else "'unknown'"
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE monitor_market_master_source AS
        SELECT
            d.snapshot_date AS date,
            {code} AS code,
            s.company_name,
            {_market_scope_case_sql("s.market_code")} AS market,
            {sector_expr} AS sector_33_name
        FROM stocks s
        CROSS JOIN monitor_snapshot_dates d
        """
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
        "researchExperimentId": BUBBLE_FOOTPRINT_EXPERIMENT_ID,
        "reratingExperimentId": RERATING_BUBBLE_REGIME_EXPERIMENT_ID,
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
            row = conn.execute("SELECT max(date) FROM stock_data WHERE close > 0").fetchone()
        else:
            target = pd.Timestamp(date).strftime("%Y-%m-%d")
            row = conn.execute(
                "SELECT max(date) FROM stock_data WHERE close > 0 AND date <= ?",
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
