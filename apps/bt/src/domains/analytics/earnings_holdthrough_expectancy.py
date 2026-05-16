"""Earnings disclosure hold-through expectancy research."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import numpy as np
import pandas as pd

from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.free_float_liquidity_adjustment import (
    apply_adjusted_free_float_market_cap,
    load_adjustment_events_by_code as load_liquidity_adjustment_events_by_code,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle
from src.shared.utils.market_code_alias import normalize_market_scope, resolve_market_codes
from src.shared.utils.share_adjustment import (
    ShareAdjustmentEvent,
    adjust_free_float_shares_to_price_basis,
)

EARNINGS_HOLDTHROUGH_EXPECTANCY_EXPERIMENT_ID = (
    "market-behavior/earnings-holdthrough-expectancy"
)
DEFAULT_PRE_WINDOWS: tuple[int, ...] = (20, 60)
DEFAULT_HORIZONS: tuple[int, ...] = (1, 5, 20)
DEFAULT_LIQUIDITY_WINDOW = 60
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
OVERHEAT_PRE_RETURN_20D_THRESHOLD_PCT = 30.0
OVERHEAT_STATE = "overheat"
NOT_OVERHEAT_STATE = "not_overheat"
MISSING_OVERHEAT_STATE = "missing"
_MARKET_SCOPE_ORDER: tuple[str, ...] = (
    "all",
    "prime",
    "standard",
    "growth",
    "unknown",
)
_STRENGTH_ORDER: tuple[str, ...] = ("positive", "neutral", "negative", "missing")
_LIQUIDITY_REGIME_ORDER: tuple[str, ...] = (
    "rerating_participation",
    "distribution_stress",
    "stale_liquidity",
    "neutral",
    "missing",
)
_LIQUIDITY_RESIDUAL_BUCKET_ORDER: tuple[str, ...] = (
    "low",
    "neutral",
    "high",
    "missing",
)
DEFAULT_LIQUIDITY_REGRESSION_MIN_OBSERVATIONS = 100


@dataclass(frozen=True)
class EarningsHoldthroughExpectancyResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    pre_windows: tuple[int, ...]
    horizons: tuple[int, ...]
    liquidity_window: int
    severe_loss_threshold_pct: float
    event_feature_df: pd.DataFrame
    precondition_outcome_df: pd.DataFrame
    bucket_expectancy_df: pd.DataFrame
    liquidity_interaction_df: pd.DataFrame
    signed_premove_df: pd.DataFrame
    holdthrough_return_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame


def run_earnings_holdthrough_expectancy_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    pre_windows: Iterable[int] = DEFAULT_PRE_WINDOWS,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    liquidity_window: int = DEFAULT_LIQUIDITY_WINDOW,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
) -> EarningsHoldthroughExpectancyResult:
    resolved_pre_windows = tuple(sorted({int(window) for window in pre_windows}))
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    _validate_params(
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        liquidity_window=liquidity_window,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    lookback_sessions = max(max(resolved_pre_windows), liquidity_window) + 5
    max_horizon = max(resolved_horizons)
    query_start = _offset_calendar_date(start_date, days=-(lookback_sessions * 3 + 30))
    query_end = _offset_calendar_date(end_date, days=max_horizon * 3 + 30)

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="earnings-holdthrough-expectancy-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = (
            "stock_master_daily_as_of_disclosed_date"
            if _table_exists(ctx.connection, "stock_master_daily")
            else "stocks_latest_fallback"
        )
        statement_df = _query_statement_rows(
            ctx.connection,
            start_date=query_start,
            end_date=query_end,
            market_source=market_source,
        )
        price_df = _query_price_rows(
            ctx.connection,
            codes=tuple(statement_df["code"].dropna().astype(str).unique()),
            start_date=query_start,
            end_date=query_end,
        )
        topix_df = _query_topix_rows(
            ctx.connection,
            start_date=query_start,
            end_date=query_end,
        )
        adjustment_events_by_code = _load_adjustment_events_by_code(
            ctx.connection,
            through_date=end_date or str(price_df["date"].max()) if not price_df.empty else end_date,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    event_feature_df = _build_event_feature_df(
        statement_df,
        price_df,
        topix_df,
        adjustment_events_by_code,
        start_date=start_date,
        end_date=end_date,
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        liquidity_window=liquidity_window,
    )
    event_feature_df = enrich_event_features_with_prime_liquidity_residuals(
        db_path_obj,
        event_feature_df,
        liquidity_window=liquidity_window,
    )
    scoped_event_df = _expand_market_scope(event_feature_df)
    precondition_outcome_df = _build_precondition_outcome_df(
        scoped_event_df,
        horizons=resolved_horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    bucket_expectancy_df = _build_bucket_expectancy_df(
        scoped_event_df,
        horizons=resolved_horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    liquidity_interaction_df = _build_liquidity_interaction_df(
        scoped_event_df,
        horizons=resolved_horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    signed_premove_df = _build_signed_premove_df(
        scoped_event_df,
        horizons=resolved_horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    holdthrough_return_df = _build_holdthrough_return_df(
        scoped_event_df,
        horizons=resolved_horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    coverage_diagnostics_df = _build_coverage_diagnostics_df(scoped_event_df)

    return EarningsHoldthroughExpectancyResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        market_source=market_source,
        analysis_start_date=_str_or_none(event_feature_df["disclosed_date"].min())
        if "disclosed_date" in event_feature_df
        else None,
        analysis_end_date=_str_or_none(event_feature_df["disclosed_date"].max())
        if "disclosed_date" in event_feature_df
        else None,
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        liquidity_window=liquidity_window,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        event_feature_df=event_feature_df,
        precondition_outcome_df=precondition_outcome_df,
        bucket_expectancy_df=bucket_expectancy_df,
        liquidity_interaction_df=liquidity_interaction_df,
        signed_premove_df=signed_premove_df,
        holdthrough_return_df=holdthrough_return_df,
        coverage_diagnostics_df=coverage_diagnostics_df,
    )


def write_earnings_holdthrough_expectancy_bundle(
    result: EarningsHoldthroughExpectancyResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=EARNINGS_HOLDTHROUGH_EXPECTANCY_EXPERIMENT_ID,
        module=__name__,
        function="run_earnings_holdthrough_expectancy_research",
        params={
            "pre_windows": list(result.pre_windows),
            "horizons": list(result.horizons),
            "liquidity_window": result.liquidity_window,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "event_count": int(len(result.event_feature_df)),
            "code_count": int(result.event_feature_df["code"].nunique())
            if "code" in result.event_feature_df
            else 0,
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "precondition_outcome_df": result.precondition_outcome_df,
            "bucket_expectancy_df": result.bucket_expectancy_df,
            "liquidity_interaction_df": result.liquidity_interaction_df,
            "signed_premove_df": result.signed_premove_df,
            "holdthrough_return_df": result.holdthrough_return_df,
            "event_feature_df": result.event_feature_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: EarningsHoldthroughExpectancyResult) -> str:
    diagnostics = _top_rows_for_markdown(result.coverage_diagnostics_df, limit=16)
    precondition = _top_rows_for_markdown(
        result.precondition_outcome_df,
        sort_columns=[
            "market_scope",
            "horizon",
            "is_fy",
            "pre_return_60d_bucket",
            "adv60_to_free_float_bucket",
        ],
        limit=30,
    )
    bucket = _top_rows_for_markdown(
        result.bucket_expectancy_df,
        sort_columns=[
            "market_scope",
            "horizon",
            "is_fy",
            "has_next_guidance",
            "event_strength",
            "pre_return_60d_bucket",
        ],
        limit=30,
    )
    liquidity = _top_rows_for_markdown(
        result.liquidity_interaction_df,
        sort_columns=["market_scope", "horizon", "liquidity_regime", "event_strength"],
        limit=30,
    )
    signed = _top_rows_for_markdown(
        result.signed_premove_df,
        sort_columns=["market_scope", "horizon", "event_strength", "signed_pre_move"],
        limit=24,
    )
    holdthrough = _top_rows_for_markdown(
        result.holdthrough_return_df,
        sort_columns=["market_scope", "horizon", "is_fy", "has_next_guidance"],
        limit=24,
    )
    return "\n".join(
        [
            "# Earnings Hold-Through Expectancy",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Pre windows: `{list(result.pre_windows)}`",
            f"- Forward horizons: `{list(result.horizons)}`",
            f"- Liquidity window: `{result.liquidity_window}`",
            "",
            "## Coverage Diagnostics",
            "",
            diagnostics,
            "",
            "## Precondition Outcome",
            "",
            precondition,
            "",
            "## Bucket Expectancy",
            "",
            bucket,
            "",
            "## Liquidity Interaction",
            "",
            liquidity,
            "",
            "## Signed Pre-Move",
            "",
            signed,
            "",
            "## Hold-Through Returns",
            "",
            holdthrough,
            "",
        ]
    )


def _validate_params(
    *,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    liquidity_window: int,
    severe_loss_threshold_pct: float,
) -> None:
    if not pre_windows or any(window <= 0 for window in pre_windows):
        raise ValueError("pre_windows must be positive")
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if liquidity_window <= 0:
        raise ValueError("liquidity_window must be positive")
    if severe_loss_threshold_pct >= 0.0:
        raise ValueError("severe_loss_threshold_pct must be negative")


def _assert_required_tables(conn: Any) -> None:
    missing = [
        table
        for table in ("statements", "stock_data", "topix_data")
        if not _table_exists(conn, table)
    ]
    if missing:
        raise RuntimeError(f"market.duckdb is missing required tables: {missing}")
    if not _table_exists(conn, "stocks") and not _table_exists(conn, "stock_master_daily"):
        raise RuntimeError("market.duckdb requires stocks or stock_master_daily")


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _has_column(conn: Any, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT count(*)
        FROM information_schema.columns
        WHERE lower(table_name) = lower(?)
          AND lower(column_name) = lower(?)
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _column_expr(conn: Any, table_name: str, column_name: str, fallback: str = "NULL") -> str:
    return column_name if _has_column(conn, table_name, column_name) else f"{fallback} AS {column_name}"


def _query_statement_rows(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    market_source: str,
) -> pd.DataFrame:
    normalized_code = normalize_code_sql("code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    date_clauses: list[str] = []
    params: list[str] = []
    if start_date:
        date_clauses.append("disclosed_date >= ?")
        params.append(start_date)
    if end_date:
        date_clauses.append("disclosed_date <= ?")
        params.append(end_date)
    date_sql = "WHERE " + " AND ".join(date_clauses) if date_clauses else ""
    statement_select = ",\n                        ".join(
        [
            f"{normalized_code} AS code",
            "disclosed_date",
            _column_expr(conn, "statements", "type_of_document"),
            _column_expr(conn, "statements", "type_of_current_period"),
            _column_expr(conn, "statements", "forecast_eps"),
            _column_expr(conn, "statements", "next_year_forecast_earnings_per_share"),
            _column_expr(conn, "statements", "next_year_forecast_profit"),
            _column_expr(conn, "statements", "profit"),
            _column_expr(conn, "statements", "earnings_per_share"),
            _column_expr(conn, "statements", "shares_outstanding"),
            _column_expr(conn, "statements", "treasury_shares", "0"),
        ]
    )

    if market_source == "stock_master_daily_as_of_disclosed_date":
        df = conn.execute(
            f"""
            WITH statements_canonical AS (
                SELECT *
                FROM (
                    SELECT
                        {statement_select},
                        ROW_NUMBER() OVER (
                            PARTITION BY {normalized_code}, disclosed_date
                            ORDER BY {prefer_4digit}, type_of_document NULLS LAST
                        ) AS rn
                    FROM statements
                    {date_sql}
                )
                WHERE rn = 1
            ),
            master_asof AS (
                SELECT *
                FROM (
                    SELECT
                        st.code AS event_code,
                        st.disclosed_date AS event_disclosed_date,
                        smd.company_name,
                        smd.market_code,
                        smd.market_name,
                        smd.scale_category,
                        ROW_NUMBER() OVER (
                            PARTITION BY st.code, st.disclosed_date
                            ORDER BY smd.date DESC
                        ) AS rn
                    FROM statements_canonical st
                    LEFT JOIN stock_master_daily smd
                      ON {normalize_code_sql("smd.code")} = st.code
                     AND smd.date <= st.disclosed_date
                )
                WHERE rn = 1
            )
            SELECT
                st.*,
                coalesce(m.company_name, st.code) AS company_name,
                m.market_code,
                m.market_name,
                m.scale_category
            FROM statements_canonical st
            LEFT JOIN master_asof m
              ON m.event_code = st.code AND m.event_disclosed_date = st.disclosed_date
            ORDER BY st.code, st.disclosed_date
            """,
            params,
        ).fetchdf()
    else:
        df = conn.execute(
            f"""
            WITH stocks_canonical AS (
                SELECT *
                FROM (
                    SELECT
                        {normalize_code_sql("code")} AS code,
                        company_name,
                        market_code,
                        market_name,
                        scale_category,
                        ROW_NUMBER() OVER (
                            PARTITION BY {normalize_code_sql("code")}
                            ORDER BY {prefer_4digit}
                        ) AS rn
                    FROM stocks
                )
                WHERE rn = 1
            ),
            statements_canonical AS (
                SELECT *
                FROM (
                    SELECT
                        {statement_select},
                        ROW_NUMBER() OVER (
                            PARTITION BY {normalized_code}, disclosed_date
                            ORDER BY {prefer_4digit}, type_of_document NULLS LAST
                        ) AS rn
                    FROM statements
                    {date_sql}
                )
                WHERE rn = 1
            )
            SELECT
                st.*,
                coalesce(s.company_name, st.code) AS company_name,
                s.market_code,
                s.market_name,
                s.scale_category
            FROM statements_canonical st
            LEFT JOIN stocks_canonical s ON s.code = st.code
            ORDER BY st.code, st.disclosed_date
            """,
            params,
        ).fetchdf()
    if df.empty:
        return _empty_statement_df()
    df["code"] = df["code"].astype(str)
    df["disclosed_date"] = df["disclosed_date"].astype(str)
    df["market"] = [
        normalize_market_scope(market_code, market_name=market_name, default="unknown")
        for market_code, market_name in zip(
            df["market_code"],
            df["market_name"],
            strict=False,
        )
    ]
    df["is_fy"] = df["type_of_current_period"].map(_is_fy_period)
    df["has_next_guidance"] = df.apply(_has_next_guidance, axis=1)
    df["actual_metric"] = df.apply(_resolve_actual_metric, axis=1)
    df["guidance_metric"] = df.apply(_resolve_guidance_metric, axis=1)
    df = df.sort_values(["code", "disclosed_date"], kind="stable").reset_index(drop=True)
    df["prior_actual_metric"] = df.groupby("code", sort=False)["actual_metric"].shift(1)
    df["prior_guidance_metric"] = df.groupby("code", sort=False)["guidance_metric"].shift(1)
    df["actual_metric_change_pct"] = [
        _safe_pct_change(current, previous)
        for current, previous in zip(
            df["actual_metric"],
            df["prior_actual_metric"],
            strict=True,
        )
    ]
    df["guidance_metric_change_pct"] = [
        _safe_pct_change(current, previous)
        for current, previous in zip(
            df["guidance_metric"],
            df["prior_guidance_metric"],
            strict=True,
        )
    ]
    df["actual_strength"] = df["actual_metric_change_pct"].map(_strength_from_change)
    df["guidance_strength"] = df["guidance_metric_change_pct"].map(_strength_from_change)
    df["event_strength"] = [
        guidance if guidance != "missing" else actual
        for guidance, actual in zip(df["guidance_strength"], df["actual_strength"], strict=True)
    ]
    return df


def _query_price_rows(
    conn: Any,
    *,
    codes: Sequence[str],
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    if not codes:
        return _empty_price_df()
    normalized_code = normalize_code_sql("code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    clauses = [f"{normalized_code} IN ({_placeholder_sql(len(codes))})"]
    params = [str(code) for code in codes]
    if start_date:
        clauses.append("date >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("date <= ?")
        params.append(end_date)
    df = conn.execute(
        f"""
        SELECT code, date, open, high, low, close, volume
        FROM (
            SELECT
                {normalized_code} AS code,
                date,
                open,
                high,
                low,
                close,
                volume,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code}, date
                    ORDER BY {prefer_4digit}
                ) AS rn
            FROM stock_data
            WHERE {" AND ".join(clauses)}
        )
        WHERE rn = 1
          AND open > 0 AND high > 0 AND low > 0 AND close > 0
          AND volume IS NOT NULL
        ORDER BY code, date
        """,
        params,
    ).fetchdf()
    if df.empty:
        return _empty_price_df()
    df["code"] = df["code"].astype(str)
    df["date"] = df["date"].astype(str)
    return df


def _query_topix_rows(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    clauses: list[str] = []
    params: list[str] = []
    if start_date:
        clauses.append("date >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("date <= ?")
        params.append(end_date)
    where_sql = "WHERE " + " AND ".join(clauses) if clauses else ""
    df = conn.execute(
        f"""
        SELECT date, close
        FROM topix_data
        {where_sql}
        ORDER BY date
        """,
        params,
    ).fetchdf()
    if df.empty:
        return pd.DataFrame(columns=["date", "close"])
    df["date"] = df["date"].astype(str)
    return df


def _load_adjustment_events_by_code(
    conn: Any,
    *,
    through_date: str | None,
) -> dict[str, list[ShareAdjustmentEvent]]:
    if not through_date or not _table_exists(conn, "stock_data_raw"):
        return {}
    if not _has_column(conn, "stock_data_raw", "adjustment_factor"):
        return {}
    raw_normalized = normalize_code_sql("raw.code")
    raw_prefer_4digit = "CASE WHEN length(raw.code) = 4 THEN 0 ELSE 1 END"
    rows = conn.execute(
        f"""
        WITH adjustment_canonical AS (
            SELECT
                {raw_normalized} AS code,
                raw.date,
                raw.adjustment_factor,
                ROW_NUMBER() OVER (
                    PARTITION BY {raw_normalized}, raw.date
                    ORDER BY {raw_prefer_4digit}
                ) AS rn
            FROM stock_data_raw raw
            WHERE raw.date <= ?
              AND raw.adjustment_factor IS NOT NULL
              AND raw.adjustment_factor != 1.0
        )
        SELECT code, date, adjustment_factor
        FROM adjustment_canonical
        WHERE rn = 1
        ORDER BY code, date
        """,
        [through_date],
    ).fetchall()
    grouped: dict[str, list[ShareAdjustmentEvent]] = {}
    for code, date, adjustment_factor in rows:
        grouped.setdefault(_normalize_equity_code(code), []).append(
            ShareAdjustmentEvent(date=str(date), adjustment_factor=float(adjustment_factor))
        )
    return grouped


def _build_event_feature_df(
    statement_df: pd.DataFrame,
    price_df: pd.DataFrame,
    topix_df: pd.DataFrame,
    adjustment_events_by_code: dict[str, list[ShareAdjustmentEvent]],
    *,
    start_date: str | None,
    end_date: str | None,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    liquidity_window: int,
) -> pd.DataFrame:
    columns = _event_feature_columns(pre_windows, horizons)
    if statement_df.empty or price_df.empty:
        return pd.DataFrame(columns=columns)

    price_panel = _add_price_features(price_df)
    price_by_code = {
        str(code): frame.reset_index(drop=True)
        for code, frame in price_panel.groupby("code", sort=False)
    }
    topix_panel = topix_df.sort_values("date").reset_index(drop=True)
    records: list[dict[str, Any]] = []
    for row in statement_df.itertuples(index=False):
        disclosed_date = str(row.disclosed_date)
        if start_date and disclosed_date < start_date:
            continue
        if end_date and disclosed_date > end_date:
            continue
        code = str(row.code)
        code_prices = price_by_code.get(code)
        if code_prices is None or code_prices.empty:
            records.append(_base_missing_record(row, pre_windows, horizons, "missing_price_history"))
            continue
        records.append(
            _build_single_event_record(
                row,
                code_prices,
                topix_panel,
                adjustment_events_by_code.get(code, []),
                pre_windows=pre_windows,
                horizons=horizons,
                liquidity_window=liquidity_window,
            )
        )
    if not records:
        return pd.DataFrame(columns=columns)
    event_df = pd.DataFrame.from_records(records)
    for column in columns:
        if column not in event_df.columns:
            event_df[column] = np.nan
    for column in ("is_fy", "has_next_guidance"):
        if column in event_df.columns:
            event_df[column] = event_df[column].astype(object)
    return event_df[columns].sort_values(["disclosed_date", "code"], kind="stable").reset_index(drop=True)


def _add_price_features(price_df: pd.DataFrame) -> pd.DataFrame:
    frame = price_df.copy()
    frame["trading_value"] = frame["close"].astype(float) * frame["volume"].astype(float)
    return frame.sort_values(["code", "date"], kind="stable").reset_index(drop=True)


def _build_single_event_record(
    row: Any,
    code_prices: pd.DataFrame,
    topix_panel: pd.DataFrame,
    adjustment_events: Sequence[ShareAdjustmentEvent],
    *,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    liquidity_window: int,
) -> dict[str, Any]:
    dates = code_prices["date"].astype(str).to_numpy()
    disclosed_date = str(row.disclosed_date)
    pre_idx = int(np.searchsorted(dates, disclosed_date, side="left")) - 1
    entry_idx = int(np.searchsorted(dates, disclosed_date, side="right"))
    record = _base_event_record(row)
    if pre_idx < 0 or entry_idx >= len(code_prices):
        record["status"] = "missing_entry_session"
        _fill_missing_feature_values(record, pre_windows, horizons)
        return record

    pre_row = code_prices.iloc[pre_idx]
    record["status"] = "realized"
    record["pre_event_date"] = str(pre_row["date"])
    record["entry_date"] = str(code_prices.iloc[entry_idx]["date"])
    record["pre_event_close"] = _float_or_nan(pre_row["close"])
    for window in pre_windows:
        price_lag_idx = pre_idx - window
        if price_lag_idx >= 0:
            price_lag_close = _float_or_nan(code_prices.iloc[price_lag_idx]["close"])
            pre_return_pct = _return_pct(record["pre_event_close"], price_lag_close)
            topix_return_pct = _topix_return_pct(
                topix_panel,
                start_date=str(code_prices.iloc[price_lag_idx]["date"]),
                end_date=record["pre_event_date"],
            )
            record[f"pre_return_{window}d_pct"] = pre_return_pct
            record[f"pre_topix_return_{window}d_pct"] = topix_return_pct
            record[f"pre_abret_{window}d_pct"] = (
                pre_return_pct - topix_return_pct
                if math.isfinite(pre_return_pct) and math.isfinite(topix_return_pct)
                else np.nan
            )
            record[f"pre_return_{window}d_bucket"] = _bucket_pre_return(pre_return_pct)
        else:
            record[f"pre_return_{window}d_pct"] = np.nan
            record[f"pre_topix_return_{window}d_pct"] = np.nan
            record[f"pre_abret_{window}d_pct"] = np.nan
            record[f"pre_return_{window}d_bucket"] = "missing"

    max_pre_window = max(pre_windows)
    record["overheat_state"] = _classify_overheat_state(
        record.get("pre_return_20d_pct")
    )
    record["signed_pre_move"] = _signed_pre_move(
        record[f"pre_abret_{max_pre_window}d_pct"],
        str(record["event_strength"]),
    )
    _append_liquidity_features(
        record,
        row,
        code_prices,
        adjustment_events,
        pre_idx=pre_idx,
        liquidity_window=liquidity_window,
    )

    for horizon in horizons:
        exit_idx = entry_idx + horizon - 1
        if exit_idx >= len(code_prices):
            record[f"forward_return_{horizon}d_pct"] = np.nan
            record[f"forward_topix_return_{horizon}d_pct"] = np.nan
            record[f"forward_excess_return_{horizon}d_pct"] = np.nan
            continue
        exit_close = _float_or_nan(code_prices.iloc[exit_idx]["close"])
        forward_return_pct = _return_pct(exit_close, record["pre_event_close"])
        topix_return_pct = _topix_return_pct(
            topix_panel,
            start_date=record["pre_event_date"],
            end_date=str(code_prices.iloc[exit_idx]["date"]),
        )
        record[f"forward_return_{horizon}d_pct"] = forward_return_pct
        record[f"forward_topix_return_{horizon}d_pct"] = topix_return_pct
        record[f"forward_excess_return_{horizon}d_pct"] = (
            forward_return_pct - topix_return_pct
            if math.isfinite(forward_return_pct) and math.isfinite(topix_return_pct)
            else np.nan
        )
    return record


def _append_liquidity_features(
    record: dict[str, Any],
    row: Any,
    code_prices: pd.DataFrame,
    adjustment_events: Sequence[ShareAdjustmentEvent],
    *,
    pre_idx: int,
    liquidity_window: int,
) -> None:
    liquidity_frame = code_prices.iloc[max(0, pre_idx - liquidity_window + 1) : pre_idx + 1]
    trading_value = pd.to_numeric(liquidity_frame["trading_value"], errors="coerce").dropna()
    adv_count = int(len(trading_value))
    med_adv = float(trading_value.median()) if adv_count >= liquidity_window else np.nan
    pre_close = _float_or_nan(code_prices.iloc[pre_idx]["close"])
    shares_outstanding = _float_or_none(getattr(row, "shares_outstanding", None))
    treasury_shares = _float_or_none(getattr(row, "treasury_shares", None))
    free_float_shares = adjust_free_float_shares_to_price_basis(
        shares_outstanding,
        treasury_shares,
        adjustment_events,
        from_date=str(row.disclosed_date),
        through_date=record["pre_event_date"],
    )
    free_float_market_cap = (
        pre_close * free_float_shares
        if free_float_shares is not None and math.isfinite(pre_close) and pre_close > 0
        else np.nan
    )
    adv_to_free_float = (
        med_adv / free_float_market_cap * 100.0
        if math.isfinite(med_adv)
        and math.isfinite(free_float_market_cap)
        and free_float_market_cap > 0
        else np.nan
    )
    record["med_adv60_mil_jpy"] = med_adv / 1_000_000.0 if math.isfinite(med_adv) else np.nan
    record["med_adv60_source_sessions"] = adv_count
    record["free_float_market_cap_mil_jpy"] = (
        free_float_market_cap / 1_000_000.0 if math.isfinite(free_float_market_cap) else np.nan
    )
    record["adv60_to_free_float_pct"] = adv_to_free_float
    record["adv60_to_free_float_bucket"] = _bucket_adv60_to_free_float(adv_to_free_float)
    record["liquidity_residual_z"] = np.nan
    record["liquidity_regime"] = _classify_liquidity_regime(
        adv_to_free_float=adv_to_free_float,
        recent_return_20d_pct=record.get("pre_return_20d_pct"),
        recent_return_60d_pct=record.get("pre_return_60d_pct"),
    )


def _classify_liquidity_regime(
    *,
    adv_to_free_float: float,
    recent_return_20d_pct: object,
    recent_return_60d_pct: object,
) -> str:
    if not math.isfinite(_float_or_nan(adv_to_free_float)):
        return "missing"
    if adv_to_free_float <= 1.0:
        return "stale_liquidity"
    valid_returns = [
        value
        for value in (_float_or_none(recent_return_20d_pct), _float_or_none(recent_return_60d_pct))
        if value is not None
    ]
    if len(valid_returns) == 2 and all(value >= 0 for value in valid_returns):
        return "rerating_participation"
    if len(valid_returns) == 2 and any(value < 0 for value in valid_returns):
        return "distribution_stress"
    return "neutral"


def _classify_overheat_state(recent_return_20d_pct: object) -> str:
    value = _float_or_nan(recent_return_20d_pct)
    if not math.isfinite(value):
        return MISSING_OVERHEAT_STATE
    if value >= OVERHEAT_PRE_RETURN_20D_THRESHOLD_PCT:
        return OVERHEAT_STATE
    return NOT_OVERHEAT_STATE


def enrich_event_features_with_prime_liquidity_residuals(
    db_path: str | Path,
    event_df: pd.DataFrame,
    *,
    liquidity_window: int,
    min_regression_observations: int = DEFAULT_LIQUIDITY_REGRESSION_MIN_OBSERVATIONS,
) -> pd.DataFrame:
    """Attach Daily Ranking style Prime liquidity residual z as of pre-event date."""
    if event_df.empty or "pre_event_date" not in event_df.columns:
        result = event_df.copy()
        if "liquidity_residual_z_bucket" not in result.columns:
            result["liquidity_residual_z_bucket"] = "missing"
        return result

    target_dates = sorted(
        {
            str(value)
            for value in event_df.loc[
                (event_df["market"].astype(str) == "prime")
                & event_df["pre_event_date"].notna(),
                "pre_event_date",
            ]
            if str(value)
        }
    )
    if not target_dates:
        return _attach_prime_liquidity_residual_panel(
            event_df,
            pd.DataFrame(),
        )

    db_path_obj = Path(db_path).expanduser().resolve()
    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="earnings-prime-liquidity-residual-",
    ) as ctx:
        source_df = _query_prime_liquidity_residual_source(
            ctx.connection,
            target_dates=target_dates,
            liquidity_window=liquidity_window,
        )
        if not source_df.empty:
            adjustment_events_by_code = load_liquidity_adjustment_events_by_code(
                ctx.connection,
                codes=sorted(source_df["code"].astype(str).unique().tolist()),
                end_date=max(target_dates),
            )
            source_df = apply_adjusted_free_float_market_cap(
                source_df,
                adjustment_events_by_code=adjustment_events_by_code,
            )

    panel_df = _build_prime_liquidity_residual_panel(
        source_df,
        liquidity_window=liquidity_window,
        min_regression_observations=min_regression_observations,
    )
    return _attach_prime_liquidity_residual_panel(
        event_df,
        panel_df,
    )


def _query_prime_liquidity_residual_source(
    conn: Any,
    *,
    target_dates: Sequence[str],
    liquidity_window: int,
) -> pd.DataFrame:
    if not target_dates:
        return pd.DataFrame()
    _, prime_market_codes = resolve_market_codes("prime")
    if not prime_market_codes:
        return pd.DataFrame()

    start_date = _offset_calendar_date(
        min(target_dates),
        days=-(liquidity_window * 4 + 30),
    )
    end_date = max(target_dates)
    market_placeholders = _placeholder_sql(len(prime_market_codes))
    target_placeholders = _placeholder_sql(len(target_dates))
    stock_code = normalize_code_sql("s.code")
    price_code = normalize_code_sql("sd.code")
    statement_code = normalize_code_sql("st.code")
    prefer_price = "CASE WHEN length(sd.code) = 4 THEN 0 ELSE 1 END"
    prefer_statement = "CASE WHEN length(st.code) = 4 THEN 0 ELSE 1 END"
    df = conn.execute(
        f"""
        WITH prime_codes AS (
            SELECT DISTINCT {stock_code} AS code
            FROM stocks s
            WHERE lower(trim(s.market_code)) IN ({market_placeholders})
        ),
        price_base AS (
            SELECT code, date, close, volume
            FROM (
                SELECT
                    {price_code} AS code,
                    sd.date,
                    sd.close,
                    sd.volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY {price_code}, sd.date
                        ORDER BY {prefer_price}
                    ) AS rn
                FROM stock_data sd
                WHERE sd.date >= ?
                  AND sd.date <= ?
                  AND sd.close > 0
                  AND sd.volume IS NOT NULL
            )
            WHERE rn = 1
        ),
        prime_price AS (
            SELECT price_base.*
            FROM price_base
            JOIN prime_codes USING (code)
        ),
        price_feature AS (
            SELECT
                *,
                MEDIAN(close * volume) OVER (
                    PARTITION BY code ORDER BY date
                    ROWS BETWEEN {liquidity_window - 1} PRECEDING AND CURRENT ROW
                ) AS adv_jpy,
                COUNT(*) OVER (
                    PARTITION BY code ORDER BY date
                    ROWS BETWEEN {liquidity_window - 1} PRECEDING AND CURRENT ROW
                ) AS adv_sessions
            FROM prime_price
        ),
        statement_base AS (
            SELECT *
            FROM (
                SELECT
                    {statement_code} AS code,
                    st.disclosed_date,
                    st.shares_outstanding,
                    st.treasury_shares,
                    ROW_NUMBER() OVER (
                        PARTITION BY {statement_code}, st.disclosed_date
                        ORDER BY {prefer_statement}
                    ) AS rn
                FROM statements st
                WHERE st.shares_outstanding > 0
            )
            WHERE rn = 1
        ),
        statement_interval AS (
            SELECT
                code,
                disclosed_date AS share_disclosed_date,
                LEAD(disclosed_date) OVER (
                    PARTITION BY code ORDER BY disclosed_date
                ) AS valid_to,
                shares_outstanding,
                treasury_shares
            FROM statement_base
        )
        SELECT
            pf.code,
            pf.date,
            pf.close,
            pf.adv_jpy,
            pf.adv_sessions,
            st.share_disclosed_date,
            st.shares_outstanding,
            st.treasury_shares
        FROM price_feature pf
        JOIN statement_interval st
          ON st.code = pf.code
         AND st.share_disclosed_date <= pf.date
         AND (st.valid_to IS NULL OR pf.date < st.valid_to)
        WHERE pf.date IN ({target_placeholders})
          AND st.shares_outstanding - coalesce(st.treasury_shares, 0) > 0
        ORDER BY pf.date, pf.code
        """,
        [*prime_market_codes, start_date, end_date, *target_dates],
    ).fetchdf()
    if df.empty:
        return pd.DataFrame()
    df["code"] = df["code"].astype(str)
    df["date"] = df["date"].astype(str)
    return df


def _build_prime_liquidity_residual_panel(
    source_df: pd.DataFrame,
    *,
    liquidity_window: int,
    min_regression_observations: int,
) -> pd.DataFrame:
    columns = [
        "code",
        "date",
        "liquidity_residual_z",
        "adv60_to_free_float_pct",
    ]
    if source_df.empty:
        return pd.DataFrame(columns=columns)
    frame = source_df.copy()
    frame["adv_jpy"] = pd.to_numeric(frame["adv_jpy"], errors="coerce")
    frame["adv_sessions"] = pd.to_numeric(frame["adv_sessions"], errors="coerce")
    frame["free_float_market_cap_jpy"] = pd.to_numeric(
        frame["free_float_market_cap_jpy"],
        errors="coerce",
    )
    frame = frame[
        (frame["adv_jpy"] > 0)
        & (frame["adv_sessions"] >= liquidity_window)
        & (frame["free_float_market_cap_jpy"] > 0)
    ].copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)
    frame["log_adv"] = np.log(frame["adv_jpy"].astype(float))
    frame["log_free_float_market_cap"] = np.log(
        frame["free_float_market_cap_jpy"].astype(float)
    )
    rows: list[dict[str, Any]] = []
    for date, group in frame.groupby("date", sort=True):
        valid = (
            group[
                [
                    "code",
                    "log_adv",
                    "log_free_float_market_cap",
                    "adv_jpy",
                    "free_float_market_cap_jpy",
                ]
            ]
            .replace([np.inf, -np.inf], np.nan)
            .dropna()
        )
        if (
            len(valid) < min_regression_observations
            or valid["log_free_float_market_cap"].nunique() < 2
        ):
            continue
        x = valid["log_free_float_market_cap"].to_numpy(dtype=float)
        y = valid["log_adv"].to_numpy(dtype=float)
        design = np.column_stack([np.ones(len(x)), x])
        intercept, beta = np.linalg.lstsq(design, y, rcond=None)[0]
        fitted = float(intercept) + float(beta) * x
        residuals = y - fitted
        residual_std = float(np.std(residuals, ddof=1))
        if not math.isfinite(residual_std) or residual_std <= 0:
            continue
        for row, residual in zip(valid.to_dict(orient="records"), residuals, strict=True):
            adv = _float_or_nan(row["adv_jpy"])
            free_float_cap = _float_or_nan(row["free_float_market_cap_jpy"])
            rows.append(
                {
                    "code": str(row["code"]),
                    "date": str(date),
                    "liquidity_residual_z": float(residual / residual_std),
                    "adv60_to_free_float_pct": (
                        adv / free_float_cap * 100.0
                        if math.isfinite(adv)
                        and math.isfinite(free_float_cap)
                        and free_float_cap > 0
                        else np.nan
                    ),
                }
            )
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame.from_records(rows, columns=columns)


def _attach_prime_liquidity_residual_panel(
    event_df: pd.DataFrame,
    panel_df: pd.DataFrame,
) -> pd.DataFrame:
    result = event_df.copy()
    if "liquidity_residual_z_bucket" not in result.columns:
        result["liquidity_residual_z_bucket"] = "missing"
    if result.empty or panel_df.empty:
        result["liquidity_residual_z"] = np.nan
        result["liquidity_residual_z_bucket"] = "missing"
        result["liquidity_regime"] = "missing"
        if "overheat_state" not in result.columns:
            if "pre_return_20d_pct" in result.columns:
                result["overheat_state"] = result["pre_return_20d_pct"].map(
                    _classify_overheat_state
                )
            else:
                result["overheat_state"] = MISSING_OVERHEAT_STATE
        return result

    merged = result.merge(
        panel_df.rename(
            columns={
                "date": "pre_event_date",
                "liquidity_residual_z": "_prime_liquidity_residual_z",
                "adv60_to_free_float_pct": "_prime_adv60_to_free_float_pct",
            }
        ),
        on=["code", "pre_event_date"],
        how="left",
    )
    z_values = pd.to_numeric(merged["_prime_liquidity_residual_z"], errors="coerce")
    prime_event_mask = (
        result["market"].astype(str).eq("prime")
        if "market" in result.columns
        else pd.Series(True, index=result.index)
    )
    z_values = z_values.where(prime_event_mask, np.nan)
    result["liquidity_residual_z"] = z_values.to_numpy()
    result["liquidity_residual_z_bucket"] = [
        _bucket_liquidity_residual_z(value) for value in z_values
    ]
    panel_adv_to_free_float = pd.to_numeric(
        merged["_prime_adv60_to_free_float_pct"],
        errors="coerce",
    ).where(prime_event_mask, np.nan)
    current_adv_to_free_float = pd.to_numeric(
        result["adv60_to_free_float_pct"],
        errors="coerce",
    )
    result["adv60_to_free_float_pct"] = current_adv_to_free_float.where(
        panel_adv_to_free_float.isna(),
        panel_adv_to_free_float,
    )
    result["adv60_to_free_float_bucket"] = result["adv60_to_free_float_pct"].map(
        _bucket_adv60_to_free_float
    )
    result["liquidity_regime"] = [
        _classify_liquidity_residual_regime(
            residual_z=z,
            recent_return_20d_pct=row.get("pre_return_20d_pct"),
            recent_return_60d_pct=row.get("pre_return_60d_pct"),
        )
        for z, row in zip(z_values, result.to_dict(orient="records"), strict=False)
    ]
    if "pre_return_20d_pct" in result.columns:
        result["overheat_state"] = result["pre_return_20d_pct"].map(
            _classify_overheat_state
        )
    elif "overheat_state" not in result.columns:
        result["overheat_state"] = MISSING_OVERHEAT_STATE
    return result


def _bucket_liquidity_residual_z(value: object) -> str:
    numeric = _float_or_nan(value)
    if not math.isfinite(numeric):
        return "missing"
    if numeric <= -1.0:
        return "low"
    if numeric >= 1.0:
        return "high"
    return "neutral"


def _classify_liquidity_residual_regime(
    *,
    residual_z: object,
    recent_return_20d_pct: object,
    recent_return_60d_pct: object,
) -> str:
    z_value = _float_or_nan(residual_z)
    if not math.isfinite(z_value):
        return "missing"
    valid_returns = [
        value
        for value in (_float_or_none(recent_return_20d_pct), _float_or_none(recent_return_60d_pct))
        if value is not None
    ]
    if z_value >= 1.0 and len(valid_returns) == 2:
        if all(value >= 0 for value in valid_returns):
            return "rerating_participation"
        if any(value < 0 for value in valid_returns):
            return "distribution_stress"
    if z_value <= -1.0:
        return "stale_liquidity"
    return "neutral"


def _expand_market_scope(event_df: pd.DataFrame) -> pd.DataFrame:
    if event_df.empty:
        expanded = event_df.copy()
        expanded["market_scope"] = pd.Series(dtype="object")
        return expanded
    frames = []
    actual = event_df.copy()
    actual["market_scope"] = actual["market"].astype(str)
    all_scope = event_df.copy()
    all_scope["market_scope"] = "all"
    frames.extend([all_scope, actual])
    expanded = pd.concat(frames, ignore_index=True)
    expanded["_market_order"] = expanded["market_scope"].map(
        {scope: idx for idx, scope in enumerate(_MARKET_SCOPE_ORDER)}
    ).fillna(len(_MARKET_SCOPE_ORDER))
    return expanded.sort_values(
        ["_market_order", "disclosed_date", "code"],
        kind="stable",
    ).drop(columns=["_market_order"]).reset_index(drop=True)


def _build_precondition_outcome_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        window_columns = [
            column
            for column in scoped_df.columns
            if column.startswith("pre_return_") and column.endswith("d_bucket")
        ]
        group_columns = [
            "market_scope",
            "is_fy",
            *window_columns,
            "adv60_to_free_float_bucket",
            "liquidity_residual_z_bucket",
            "liquidity_regime",
            "overheat_state",
        ]
        for horizon in horizons:
            return_col = f"forward_excess_return_{horizon}d_pct"
            for keys, frame in scoped_df.groupby(group_columns, sort=False, dropna=False):
                key_values = dict(zip(group_columns, keys, strict=True))
                rows.append(
                    {
                        **key_values,
                        "horizon": horizon,
                        **_return_summary(frame, return_col, severe_loss_threshold_pct),
                        "positive_event_rate_pct": _event_strength_rate_pct(
                            frame, "positive"
                        ),
                        "negative_event_rate_pct": _event_strength_rate_pct(
                            frame, "negative"
                        ),
                        "neutral_event_rate_pct": _event_strength_rate_pct(frame, "neutral"),
                        "missing_event_rate_pct": _event_strength_rate_pct(frame, "missing"),
                        "next_guidance_rate_pct": _bool_rate_pct(
                            frame["has_next_guidance"]
                        ),
                    }
                )
    columns = [
        "market_scope",
        "is_fy",
        *[f"pre_return_{window}d_bucket" for window in _infer_pre_windows(scoped_df)],
        "adv60_to_free_float_bucket",
        "liquidity_residual_z_bucket",
        "liquidity_regime",
        "overheat_state",
        "horizon",
        *_summary_columns(),
        "positive_event_rate_pct",
        "negative_event_rate_pct",
        "neutral_event_rate_pct",
        "missing_event_rate_pct",
        "next_guidance_rate_pct",
    ]
    return _sort_summary_df(pd.DataFrame(rows), columns=columns)


def _build_bucket_expectancy_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        window_columns = [column for column in scoped_df.columns if column.startswith("pre_return_") and column.endswith("d_bucket")]
        group_columns = [
            "market_scope",
            "is_fy",
            "has_next_guidance",
            "event_strength",
            "overheat_state",
            *window_columns,
        ]
        for horizon in horizons:
            return_col = f"forward_excess_return_{horizon}d_pct"
            for keys, frame in scoped_df.groupby(group_columns, sort=False, dropna=False):
                key_values = dict(zip(group_columns, keys, strict=True))
                rows.append(
                    {
                        **key_values,
                        "horizon": horizon,
                        **_return_summary(frame, return_col, severe_loss_threshold_pct),
                    }
                )
    columns = [
        "market_scope",
        "is_fy",
        "has_next_guidance",
        "event_strength",
        "overheat_state",
        *[f"pre_return_{window}d_bucket" for window in _infer_pre_windows(scoped_df)],
        "horizon",
        *_summary_columns(),
    ]
    return _sort_summary_df(pd.DataFrame(rows), columns=columns)


def _build_liquidity_interaction_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        group_columns = [
            "market_scope",
            "liquidity_regime",
            "liquidity_residual_z_bucket",
            "event_strength",
            "overheat_state",
            "is_fy",
            "has_next_guidance",
        ]
        for horizon in horizons:
            return_col = f"forward_excess_return_{horizon}d_pct"
            for keys, frame in scoped_df.groupby(group_columns, sort=False, dropna=False):
                key_values = dict(zip(group_columns, keys, strict=True))
                rows.append(
                    {
                        **key_values,
                        "horizon": horizon,
                        **_return_summary(frame, return_col, severe_loss_threshold_pct),
                        "median_adv60_mil_jpy": _median_or_nan(frame["med_adv60_mil_jpy"]),
                        "median_adv60_to_free_float_pct": _median_or_nan(
                            frame["adv60_to_free_float_pct"]
                        ),
                        "median_liquidity_residual_z": _median_or_nan(
                            frame["liquidity_residual_z"]
                        ),
                    }
                )
    columns = [
        "market_scope",
        "liquidity_regime",
        "liquidity_residual_z_bucket",
        "event_strength",
        "overheat_state",
        "is_fy",
        "has_next_guidance",
        "horizon",
        *_summary_columns(),
        "median_adv60_mil_jpy",
        "median_adv60_to_free_float_pct",
        "median_liquidity_residual_z",
    ]
    return _sort_summary_df(pd.DataFrame(rows), columns=columns)


def _build_signed_premove_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        group_columns = ["market_scope", "event_strength", "signed_pre_move"]
        for horizon in horizons:
            return_col = f"forward_excess_return_{horizon}d_pct"
            for keys, frame in scoped_df.groupby(group_columns, sort=False, dropna=False):
                key_values = dict(zip(group_columns, keys, strict=True))
                rows.append(
                    {
                        **key_values,
                        "horizon": horizon,
                        **_return_summary(frame, return_col, severe_loss_threshold_pct),
                    }
                )
    columns = ["market_scope", "event_strength", "signed_pre_move", "horizon", *_summary_columns()]
    return _sort_summary_df(pd.DataFrame(rows), columns=columns)


def _build_holdthrough_return_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        group_columns = ["market_scope", "is_fy", "has_next_guidance"]
        for horizon in horizons:
            raw_col = f"forward_return_{horizon}d_pct"
            excess_col = f"forward_excess_return_{horizon}d_pct"
            for keys, frame in scoped_df.groupby(group_columns, sort=False, dropna=False):
                key_values = dict(zip(group_columns, keys, strict=True))
                raw_summary = _return_summary(frame, raw_col, severe_loss_threshold_pct)
                excess_summary = _return_summary(frame, excess_col, severe_loss_threshold_pct)
                rows.append(
                    {
                        **key_values,
                        "horizon": horizon,
                        "event_count": excess_summary["event_count"],
                        "code_count": excess_summary["code_count"],
                        "mean_forward_return_pct": raw_summary["mean_forward_excess_return_pct"],
                        "median_forward_return_pct": raw_summary["median_forward_excess_return_pct"],
                        "mean_forward_excess_return_pct": excess_summary[
                            "mean_forward_excess_return_pct"
                        ],
                        "median_forward_excess_return_pct": excess_summary[
                            "median_forward_excess_return_pct"
                        ],
                        "win_rate_pct": excess_summary["win_rate_pct"],
                        "severe_loss_rate_pct": excess_summary["severe_loss_rate_pct"],
                    }
                )
    columns = [
        "market_scope",
        "is_fy",
        "has_next_guidance",
        "horizon",
        "event_count",
        "code_count",
        "mean_forward_return_pct",
        "median_forward_return_pct",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
    ]
    return _sort_summary_df(pd.DataFrame(rows), columns=columns)


def _build_coverage_diagnostics_df(scoped_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        for market_scope, frame in scoped_df.groupby("market_scope", sort=False):
            rows.append(
                {
                    "market_scope": market_scope,
                    "event_count": int(len(frame)),
                    "realized_count": int((frame["status"] == "realized").sum()),
                    "code_count": int(frame["code"].nunique()),
                    "fy_event_count": int((frame["is_fy"] == True).sum()),  # noqa: E712
                    "next_guidance_count": int((frame["has_next_guidance"] == True).sum()),  # noqa: E712
                    "overheat_count": int(
                        (frame["overheat_state"].astype(str) == OVERHEAT_STATE).sum()
                    ),
                    "med_adv60_coverage_pct": _coverage_rate_pct(frame["med_adv60_mil_jpy"]),
                    "liquidity_regime_coverage_pct": float(
                        (frame["liquidity_regime"] != "missing").mean() * 100.0
                    ),
                    "liquidity_residual_z_coverage_pct": _coverage_rate_pct(
                        frame["liquidity_residual_z"]
                    ),
                }
            )
    columns = [
        "market_scope",
        "event_count",
        "realized_count",
        "code_count",
        "fy_event_count",
        "next_guidance_count",
        "overheat_count",
        "med_adv60_coverage_pct",
        "liquidity_regime_coverage_pct",
        "liquidity_residual_z_coverage_pct",
    ]
    return _sort_summary_df(pd.DataFrame(rows), columns=columns)


def _return_summary(
    frame: pd.DataFrame,
    return_col: str,
    severe_loss_threshold_pct: float,
) -> dict[str, Any]:
    values = pd.to_numeric(frame[return_col], errors="coerce")
    valid = values.replace([np.inf, -np.inf], np.nan).dropna()
    return {
        "event_count": int(len(frame)),
        "code_count": int(frame["code"].nunique()),
        "valid_return_count": int(len(valid)),
        "mean_forward_excess_return_pct": float(valid.mean()) if not valid.empty else np.nan,
        "median_forward_excess_return_pct": float(valid.median()) if not valid.empty else np.nan,
        "win_rate_pct": float((valid > 0).mean() * 100.0) if not valid.empty else np.nan,
        "severe_loss_rate_pct": (
            float((valid <= severe_loss_threshold_pct).mean() * 100.0)
            if not valid.empty
            else np.nan
        ),
    }


def _event_strength_rate_pct(frame: pd.DataFrame, strength: str) -> float:
    if frame.empty:
        return np.nan
    return float((frame["event_strength"].astype(str) == strength).mean() * 100.0)


def _bool_rate_pct(values: pd.Series) -> float:
    if values.empty:
        return np.nan
    return float((values == True).mean() * 100.0)  # noqa: E712


def _summary_columns() -> list[str]:
    return [
        "event_count",
        "code_count",
        "valid_return_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
    ]


def _base_event_record(row: Any) -> dict[str, Any]:
    return {
        "event_id": f"{row.code}:{row.disclosed_date}",
        "code": str(row.code),
        "company_name": str(row.company_name),
        "market": str(row.market),
        "market_code": str(row.market_code) if pd.notna(row.market_code) else None,
        "scale_category": str(row.scale_category) if pd.notna(row.scale_category) else None,
        "disclosed_date": str(row.disclosed_date),
        "type_of_document": str(row.type_of_document) if pd.notna(row.type_of_document) else None,
        "type_of_current_period": str(row.type_of_current_period)
        if pd.notna(row.type_of_current_period)
        else None,
        "is_fy": bool(row.is_fy),
        "has_next_guidance": bool(row.has_next_guidance),
        "actual_metric": _float_or_nan(row.actual_metric),
        "prior_actual_metric": _float_or_nan(row.prior_actual_metric),
        "actual_metric_change_pct": _float_or_nan(row.actual_metric_change_pct),
        "actual_strength": str(row.actual_strength),
        "guidance_metric": _float_or_nan(row.guidance_metric),
        "prior_guidance_metric": _float_or_nan(row.prior_guidance_metric),
        "guidance_metric_change_pct": _float_or_nan(row.guidance_metric_change_pct),
        "guidance_strength": str(row.guidance_strength),
        "event_strength": str(row.event_strength),
        "status": "unknown",
        "pre_event_date": None,
        "entry_date": None,
        "pre_event_close": np.nan,
        "signed_pre_move": "unknown",
        "med_adv60_mil_jpy": np.nan,
        "med_adv60_source_sessions": 0,
        "free_float_market_cap_mil_jpy": np.nan,
        "adv60_to_free_float_pct": np.nan,
        "adv60_to_free_float_bucket": "missing",
        "liquidity_residual_z": np.nan,
        "liquidity_residual_z_bucket": "missing",
        "liquidity_regime": "missing",
        "overheat_state": MISSING_OVERHEAT_STATE,
    }


def _base_missing_record(
    row: Any,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    status: str,
) -> dict[str, Any]:
    record = _base_event_record(row)
    record["status"] = status
    _fill_missing_feature_values(record, pre_windows, horizons)
    return record


def _fill_missing_feature_values(
    record: dict[str, Any],
    pre_windows: Sequence[int],
    horizons: Sequence[int],
) -> None:
    for window in pre_windows:
        record[f"pre_return_{window}d_pct"] = np.nan
        record[f"pre_topix_return_{window}d_pct"] = np.nan
        record[f"pre_abret_{window}d_pct"] = np.nan
        record[f"pre_return_{window}d_bucket"] = "missing"
    for horizon in horizons:
        record[f"forward_return_{horizon}d_pct"] = np.nan
        record[f"forward_topix_return_{horizon}d_pct"] = np.nan
        record[f"forward_excess_return_{horizon}d_pct"] = np.nan


def _event_feature_columns(
    pre_windows: Sequence[int],
    horizons: Sequence[int],
) -> list[str]:
    columns = [
        "event_id",
        "code",
        "company_name",
        "market",
        "market_code",
        "scale_category",
        "disclosed_date",
        "type_of_document",
        "type_of_current_period",
        "is_fy",
        "has_next_guidance",
        "actual_metric",
        "prior_actual_metric",
        "actual_metric_change_pct",
        "actual_strength",
        "guidance_metric",
        "prior_guidance_metric",
        "guidance_metric_change_pct",
        "guidance_strength",
        "event_strength",
        "status",
        "pre_event_date",
        "entry_date",
        "pre_event_close",
        "signed_pre_move",
        "med_adv60_mil_jpy",
        "med_adv60_source_sessions",
        "free_float_market_cap_mil_jpy",
        "adv60_to_free_float_pct",
        "adv60_to_free_float_bucket",
        "liquidity_residual_z",
        "liquidity_residual_z_bucket",
        "liquidity_regime",
        "overheat_state",
    ]
    for window in pre_windows:
        columns.extend(
            [
                f"pre_return_{window}d_pct",
                f"pre_topix_return_{window}d_pct",
                f"pre_abret_{window}d_pct",
                f"pre_return_{window}d_bucket",
            ]
        )
    for horizon in horizons:
        columns.extend(
            [
                f"forward_return_{horizon}d_pct",
                f"forward_topix_return_{horizon}d_pct",
                f"forward_excess_return_{horizon}d_pct",
            ]
        )
    return columns


def _empty_statement_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "code",
            "company_name",
            "market_code",
            "market_name",
            "scale_category",
            "disclosed_date",
            "type_of_document",
            "type_of_current_period",
            "forecast_eps",
            "next_year_forecast_earnings_per_share",
            "next_year_forecast_profit",
            "profit",
            "earnings_per_share",
            "shares_outstanding",
            "treasury_shares",
            "market",
            "is_fy",
            "has_next_guidance",
            "actual_metric",
            "guidance_metric",
            "prior_actual_metric",
            "prior_guidance_metric",
            "actual_metric_change_pct",
            "guidance_metric_change_pct",
            "actual_strength",
            "guidance_strength",
            "event_strength",
        ]
    )


def _empty_price_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["code", "date", "open", "high", "low", "close", "volume", "trading_value"]
    )


def _is_fy_period(value: object) -> bool:
    return str(value or "").strip().upper() in {"FY", "FULL", "FULL_YEAR"}


def _has_next_guidance(row: pd.Series) -> bool:
    for column in ("next_year_forecast_earnings_per_share", "next_year_forecast_profit"):
        value = _float_or_none(row.get(column))
        if value is not None:
            return True
    return False


def _resolve_actual_metric(row: pd.Series) -> float:
    for column in ("earnings_per_share", "profit"):
        value = _float_or_nan(row.get(column))
        if math.isfinite(value):
            return value
    return np.nan


def _resolve_guidance_metric(row: pd.Series) -> float:
    for column in (
        "next_year_forecast_earnings_per_share",
        "next_year_forecast_profit",
        "forecast_eps",
    ):
        value = _float_or_nan(row.get(column))
        if math.isfinite(value):
            return value
    return np.nan


def _strength_from_change(change_pct: float) -> str:
    value = _float_or_nan(change_pct)
    if not math.isfinite(value):
        return "missing"
    if value > 0.0:
        return "positive"
    if value < 0.0:
        return "negative"
    return "neutral"


def _signed_pre_move(pre_abret_pct: float, event_strength: str) -> str:
    value = _float_or_nan(pre_abret_pct)
    if not math.isfinite(value) or event_strength not in {"positive", "negative"}:
        return "unknown"
    if (event_strength == "positive" and value > 0.0) or (
        event_strength == "negative" and value < 0.0
    ):
        return "aligned"
    return "opposed"


def _bucket_pre_return(value: float) -> str:
    numeric = _float_or_nan(value)
    if not math.isfinite(numeric):
        return "missing"
    if numeric >= 20.0:
        return "strong_runup"
    if numeric >= 5.0:
        return "runup"
    if numeric <= -20.0:
        return "strong_drawdown"
    if numeric <= -5.0:
        return "drawdown"
    return "flat"


def _bucket_adv60_to_free_float(value: float) -> str:
    numeric = _float_or_nan(value)
    if not math.isfinite(numeric):
        return "missing"
    if numeric < 0.1:
        return "lt0.1"
    if numeric < 0.5:
        return "0.1-0.5"
    if numeric < 1.0:
        return "0.5-1.0"
    if numeric < 2.0:
        return "1.0-2.0"
    return "ge2.0"


def _topix_return_pct(topix_panel: pd.DataFrame, *, start_date: str, end_date: str) -> float:
    if topix_panel.empty:
        return np.nan
    dates = topix_panel["date"].astype(str).to_numpy()
    start_idx = int(np.searchsorted(dates, start_date, side="left"))
    end_idx = int(np.searchsorted(dates, end_date, side="right")) - 1
    if start_idx < 0 or end_idx < 0 or start_idx >= len(topix_panel) or end_idx >= len(topix_panel):
        return np.nan
    start_close = _float_or_nan(topix_panel.iloc[start_idx]["close"])
    end_close = _float_or_nan(topix_panel.iloc[end_idx]["close"])
    return _return_pct(end_close, start_close)


def _return_pct(current: float, base: float) -> float:
    if not math.isfinite(current) or not math.isfinite(base) or base == 0:
        return np.nan
    return (current / base - 1.0) * 100.0


def _safe_pct_change(current: object, previous: object) -> float:
    current_value = _float_or_nan(current)
    previous_value = _float_or_nan(previous)
    if not math.isfinite(current_value) or not math.isfinite(previous_value) or previous_value == 0:
        return np.nan
    return (current_value - previous_value) / abs(previous_value) * 100.0


def _placeholder_sql(count: int) -> str:
    return ",".join("?" for _ in range(count))


def _normalize_equity_code(code: object) -> str:
    text = str(code).strip()
    if len(text) in {5, 6} and text.endswith("0"):
        return text[:-1]
    return text


def _offset_calendar_date(value: str | None, *, days: int) -> str | None:
    if value is None:
        return None
    return (pd.Timestamp(value) + pd.Timedelta(days=days)).strftime("%Y-%m-%d")


def _float_or_nan(value: object) -> float:
    try:
        numeric = float(cast(Any, value))
    except (TypeError, ValueError):
        return np.nan
    return numeric if math.isfinite(numeric) else np.nan


def _float_or_none(value: object) -> float | None:
    numeric = _float_or_nan(value)
    return numeric if math.isfinite(numeric) else None


def _str_or_none(value: object) -> str | None:
    if value is None or pd.isna(cast(Any, value)):
        return None
    return str(value)


def _median_or_nan(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    return float(numeric.median()) if not numeric.empty else np.nan


def _coverage_rate_pct(values: pd.Series) -> float:
    if values.empty:
        return np.nan
    numeric = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan)
    return float(numeric.notna().mean() * 100.0)


def _infer_pre_windows(scoped_df: pd.DataFrame) -> list[int]:
    windows: list[int] = []
    for column in scoped_df.columns:
        if column.startswith("pre_return_") and column.endswith("d_bucket"):
            raw = column.removeprefix("pre_return_").removesuffix("d_bucket")
            try:
                windows.append(int(raw))
            except ValueError:
                continue
    return sorted(windows)


def _sort_summary_df(pd_frame: pd.DataFrame, *, columns: Sequence[str]) -> pd.DataFrame:
    if pd_frame.empty:
        return pd.DataFrame(columns=list(columns))
    for column in columns:
        if column not in pd_frame.columns:
            pd_frame[column] = np.nan
    frame = pd_frame[list(columns)].copy()
    sort_columns = [column for column in ("market_scope", "horizon") if column in frame.columns]
    if sort_columns:
        frame["_market_order"] = frame["market_scope"].map(
            {scope: idx for idx, scope in enumerate(_MARKET_SCOPE_ORDER)}
        ).fillna(len(_MARKET_SCOPE_ORDER))
        sort_by = ["_market_order", *[column for column in sort_columns if column != "market_scope"]]
        frame = frame.sort_values(sort_by, kind="stable").drop(columns=["_market_order"])
    return frame.reset_index(drop=True)


def _top_rows_for_markdown(
    frame: pd.DataFrame,
    *,
    sort_columns: Sequence[str] | None = None,
    limit: int,
) -> str:
    if frame.empty:
        return "_No rows._"
    display = frame.copy()
    if sort_columns:
        existing = [column for column in sort_columns if column in display.columns]
        if existing:
            display = display.sort_values(existing, kind="stable")
    display = display.head(limit).copy()
    for column in display.columns:
        if pd.api.types.is_float_dtype(display[column]):
            display[column] = display[column].map(
                lambda value: round(float(value), 4) if pd.notna(value) else value
            )
    return _frame_to_markdown(display)


def _frame_to_markdown(frame: pd.DataFrame) -> str:
    headers = [str(column) for column in frame.columns]
    rows = [
        [_format_markdown_cell(value) for value in row]
        for row in frame.itertuples(index=False, name=None)
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def _format_markdown_cell(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(cast(Any, value)):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value).replace("|", "\\|")
