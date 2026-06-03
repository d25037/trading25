"""Pre-disclosure flow and volatility event study for local market.duckdb."""

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
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle
from src.domains.strategy.indicators.calculations import compute_atr
from src.shared.utils.market_code_alias import normalize_market_scope

PRE_DISCLOSURE_FLOW_VOLATILITY_EXPERIMENT_ID = (
    "market-behavior/pre-disclosure-flow-volatility"
)
DEFAULT_PRE_WINDOWS: tuple[int, ...] = (5, 20)
DEFAULT_HORIZONS: tuple[int, ...] = (1, 5, 20)
DEFAULT_ATR_PERIOD = 14
DEFAULT_BASELINE_WINDOW = 120
DEFAULT_BUCKET_COUNT = 5
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
_MARKET_SCOPE_ORDER: tuple[str, ...] = (
    "all",
    "prime",
    "standard",
    "growth",
)
_SCORE_BUCKET_LABELS: dict[int, str] = {
    1: "low",
    2: "mid_low",
    3: "middle",
    4: "mid_high",
    5: "high",
}


@dataclass(frozen=True)
class PreDisclosureFlowVolatilityResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    pre_windows: tuple[int, ...]
    horizons: tuple[int, ...]
    atr_period: int
    baseline_window: int
    bucket_count: int
    severe_loss_threshold_pct: float
    event_feature_df: pd.DataFrame
    market_score_bucket_forward_return_df: pd.DataFrame
    market_atr_volume_interaction_df: pd.DataFrame
    market_signed_move_df: pd.DataFrame
    market_sample_diagnostics_df: pd.DataFrame


def run_pre_disclosure_flow_volatility_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    pre_windows: Iterable[int] = DEFAULT_PRE_WINDOWS,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    atr_period: int = DEFAULT_ATR_PERIOD,
    baseline_window: int = DEFAULT_BASELINE_WINDOW,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
) -> PreDisclosureFlowVolatilityResult:
    resolved_pre_windows = tuple(sorted({int(window) for window in pre_windows}))
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    _validate_params(
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        atr_period=atr_period,
        baseline_window=baseline_window,
        bucket_count=bucket_count,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    lookback_sessions = max(max(resolved_pre_windows), atr_period, baseline_window) + 5
    max_horizon = max(resolved_horizons)
    query_start = _offset_calendar_date(start_date, days=-(lookback_sessions * 3 + 30))
    query_end = _offset_calendar_date(end_date, days=max_horizon * 3 + 30)

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="pre-disclosure-flow-volatility-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = "stock_master_daily_as_of_disclosed_date"
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
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    event_feature_df = _build_event_feature_df(
        statement_df,
        price_df,
        topix_df,
        start_date=start_date,
        end_date=end_date,
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        atr_period=atr_period,
        baseline_window=baseline_window,
    )
    scoped_event_df = _expand_market_scope(event_feature_df)
    scoped_event_df = _add_score_and_segments(
        scoped_event_df,
        pre_windows=resolved_pre_windows,
        bucket_count=bucket_count,
    )
    market_score_bucket_forward_return_df = _build_score_bucket_summary_df(
        scoped_event_df,
        horizons=resolved_horizons,
        bucket_count=bucket_count,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    market_atr_volume_interaction_df = _build_atr_volume_interaction_df(
        scoped_event_df,
        horizons=resolved_horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    market_signed_move_df = _build_signed_move_df(
        scoped_event_df,
        horizons=resolved_horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    market_sample_diagnostics_df = _build_sample_diagnostics_df(scoped_event_df)

    return PreDisclosureFlowVolatilityResult(
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
        atr_period=atr_period,
        baseline_window=baseline_window,
        bucket_count=bucket_count,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        event_feature_df=event_feature_df,
        market_score_bucket_forward_return_df=market_score_bucket_forward_return_df,
        market_atr_volume_interaction_df=market_atr_volume_interaction_df,
        market_signed_move_df=market_signed_move_df,
        market_sample_diagnostics_df=market_sample_diagnostics_df,
    )


def write_pre_disclosure_flow_volatility_bundle(
    result: PreDisclosureFlowVolatilityResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=PRE_DISCLOSURE_FLOW_VOLATILITY_EXPERIMENT_ID,
        module=__name__,
        function="run_pre_disclosure_flow_volatility_research",
        params={
            "pre_windows": list(result.pre_windows),
            "horizons": list(result.horizons),
            "atr_period": result.atr_period,
            "baseline_window": result.baseline_window,
            "bucket_count": result.bucket_count,
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
            "market_sample_diagnostics_df": result.market_sample_diagnostics_df,
            "market_score_bucket_forward_return_df": (
                result.market_score_bucket_forward_return_df
            ),
            "market_atr_volume_interaction_df": (
                result.market_atr_volume_interaction_df
            ),
            "market_signed_move_df": result.market_signed_move_df,
            "event_feature_df": result.event_feature_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: PreDisclosureFlowVolatilityResult) -> str:
    diagnostics = _top_rows_for_markdown(result.market_sample_diagnostics_df, limit=12)
    bucket = _top_rows_for_markdown(
        result.market_score_bucket_forward_return_df,
        sort_columns=["market_scope", "horizon", "score_bucket_rank"],
        ascending=[True, True, True],
        limit=24,
    )
    interaction = _top_rows_for_markdown(
        result.market_atr_volume_interaction_df,
        sort_columns=["market_scope", "horizon", "atr_segment", "volume_segment"],
        ascending=[True, True, True, True],
        limit=24,
    )
    signed = _top_rows_for_markdown(
        result.market_signed_move_df,
        sort_columns=["market_scope", "horizon", "event_direction", "signed_pre_move"],
        ascending=[True, True, True, True],
        limit=24,
    )
    return "\n".join(
        [
            "# Pre-Disclosure Flow/Volatility",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Pre windows: `{list(result.pre_windows)}`",
            f"- Forward horizons: `{list(result.horizons)}`",
            f"- ATR period: `{result.atr_period}`",
            "",
            "## Market Sample Diagnostics",
            "",
            diagnostics,
            "",
            "## Market Score Bucket Forward Returns",
            "",
            bucket,
            "",
            "## ATR x Volume Interaction",
            "",
            interaction,
            "",
            "## Signed Pre-Move",
            "",
            signed,
            "",
        ]
    )


def _validate_params(
    *,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    atr_period: int,
    baseline_window: int,
    bucket_count: int,
    severe_loss_threshold_pct: float,
) -> None:
    if not pre_windows or any(window <= 0 for window in pre_windows):
        raise ValueError("pre_windows must be positive")
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if atr_period < 2:
        raise ValueError("atr_period must be at least 2")
    if baseline_window < 2:
        raise ValueError("baseline_window must be at least 2")
    if bucket_count < 2:
        raise ValueError("bucket_count must be at least 2")
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
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


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

    if market_source != "stock_master_daily_as_of_disclosed_date":
        raise ValueError(f"Unsupported market_source for PIT research: {market_source}")
    df = conn.execute(
        f"""
        WITH statements_canonical AS (
            SELECT *
            FROM (
                SELECT
                    {normalized_code} AS code,
                    disclosed_date,
                    type_of_document,
                    type_of_current_period,
                    forecast_eps,
                    next_year_forecast_earnings_per_share,
                    profit,
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
            st.code,
            coalesce(m.company_name, st.code) AS company_name,
            m.market_code,
            m.market_name,
            m.scale_category,
            st.disclosed_date,
            st.type_of_document,
            st.type_of_current_period,
            st.forecast_eps,
            st.next_year_forecast_earnings_per_share,
            st.profit
        FROM statements_canonical st
        LEFT JOIN master_asof m
          ON m.event_code = st.code AND m.event_disclosed_date = st.disclosed_date
        ORDER BY st.code, st.disclosed_date
        """,
        params,
    ).fetchdf()
    if df.empty:
        return _empty_event_source_df()
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
    df["event_metric"] = df.apply(_resolve_event_metric, axis=1)
    df = df.sort_values(["code", "disclosed_date"], kind="stable").reset_index(drop=True)
    df["prior_event_metric"] = df.groupby("code", sort=False)["event_metric"].shift(1)
    df["event_metric_change_pct"] = [
        _safe_pct_change(current, previous)
        for current, previous in zip(
            df["event_metric"],
            df["prior_event_metric"],
            strict=True,
        )
    ]
    df["event_direction"] = df["event_metric_change_pct"].map(_event_direction)
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


def _build_event_feature_df(
    statement_df: pd.DataFrame,
    price_df: pd.DataFrame,
    topix_df: pd.DataFrame,
    *,
    start_date: str | None,
    end_date: str | None,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    atr_period: int,
    baseline_window: int,
) -> pd.DataFrame:
    columns = _event_feature_columns(pre_windows, horizons)
    if statement_df.empty or price_df.empty:
        return pd.DataFrame(columns=columns)

    price_panel = _add_price_features(price_df, atr_period=atr_period)
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
        record = _build_single_event_record(
            row,
            code_prices,
            topix_panel,
            pre_windows=pre_windows,
            horizons=horizons,
            baseline_window=baseline_window,
        )
        records.append(record)
    if not records:
        return pd.DataFrame(columns=columns)
    event_df = pd.DataFrame.from_records(records)
    for column in columns:
        if column not in event_df.columns:
            event_df[column] = np.nan
    return event_df[columns].sort_values(["disclosed_date", "code"], kind="stable").reset_index(drop=True)


def _add_price_features(price_df: pd.DataFrame, *, atr_period: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for _, frame in price_df.sort_values(["code", "date"]).groupby("code", sort=False):
        enriched = frame.copy().reset_index(drop=True)
        atr = compute_atr(
            enriched["high"].astype(float),
            enriched["low"].astype(float),
            enriched["close"].astype(float),
            period=atr_period,
        )
        enriched["atr"] = atr.to_numpy()
        enriched["atr_pct"] = np.where(
            enriched["close"].astype(float) > 0,
            enriched["atr"].astype(float) / enriched["close"].astype(float) * 100.0,
            np.nan,
        )
        frames.append(enriched)
    if not frames:
        return _empty_price_df()
    return pd.concat(frames, ignore_index=True)


def _build_single_event_record(
    row: Any,
    code_prices: pd.DataFrame,
    topix_panel: pd.DataFrame,
    *,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    baseline_window: int,
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
    record["pre_atr"] = _float_or_nan(pre_row["atr"])
    record["pre_atr_pct"] = _float_or_nan(pre_row["atr_pct"])

    atr_baseline = code_prices.iloc[max(0, pre_idx - baseline_window) : pre_idx]["atr_pct"]
    volume_baseline = code_prices.iloc[max(0, pre_idx - baseline_window) : pre_idx]["volume"]
    record["pre_atr_z"] = _zscore(record["pre_atr_pct"], atr_baseline)
    record["atr_expansion_ratio"] = _ratio(
        record["pre_atr_pct"],
        _mean_or_nan(atr_baseline),
    )
    max_pre_window = max(pre_windows)
    directional_return_pct = np.nan
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
            if window == max_pre_window:
                directional_return_pct = record[f"pre_abret_{window}d_pct"]
        else:
            record[f"pre_return_{window}d_pct"] = np.nan
            record[f"pre_topix_return_{window}d_pct"] = np.nan
            record[f"pre_abret_{window}d_pct"] = np.nan
        volume_window = code_prices.iloc[max(0, pre_idx - window + 1) : pre_idx + 1]["volume"]
        record[f"pre_volume_mean_{window}d"] = _mean_or_nan(volume_window)
        record[f"pre_volume_z_{window}d"] = _zscore(
            record[f"pre_volume_mean_{window}d"],
            volume_baseline,
        )

    record["directional_pre_abret_pct"] = _directional_pre_move(
        directional_return_pct,
        str(record["event_direction"]),
    )
    record["signed_pre_move"] = _signed_pre_move(
        directional_return_pct,
        str(record["event_direction"]),
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


def _add_score_and_segments(
    scoped_df: pd.DataFrame,
    *,
    pre_windows: Sequence[int],
    bucket_count: int,
) -> pd.DataFrame:
    if scoped_df.empty:
        for column in (
            "informed_flow_score",
            "score_bucket_rank",
            "score_bucket",
            "atr_segment",
            "volume_segment",
        ):
            scoped_df[column] = pd.Series(dtype="object")
        return scoped_df
    result = scoped_df.copy()
    pre_window = max(pre_windows)
    volume_col = f"pre_volume_z_{pre_window}d"
    components = ["directional_pre_abret_pct", volume_col, "pre_atr_z"]
    score_parts: list[pd.Series] = []
    for component in components:
        rank = result.groupby("market_scope", sort=False)[component].rank(pct=True)
        score_parts.append(rank)
    result["informed_flow_score"] = pd.concat(score_parts, axis=1).mean(axis=1, skipna=True)
    result["score_bucket_rank"] = _bucket_rank_by_scope(
        result,
        value_column="informed_flow_score",
        bucket_count=bucket_count,
    )
    result["score_bucket"] = [
        _bucket_label(rank, bucket_count=bucket_count)
        for rank in result["score_bucket_rank"]
    ]
    result["atr_segment"] = _tercile_segment_by_scope(result, "pre_atr_z")
    result["volume_segment"] = _tercile_segment_by_scope(result, volume_col)
    return result


def _build_score_bucket_summary_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    bucket_count: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        for horizon in horizons:
            return_col = f"forward_excess_return_{horizon}d_pct"
            for keys, frame in scoped_df.groupby(
                ["market_scope", "score_bucket_rank", "score_bucket"],
                sort=False,
                dropna=False,
            ):
                market_scope, bucket_rank, score_bucket = keys
                values = frame[return_col]
                rows.append(
                    {
                        "market_scope": market_scope,
                        "horizon": horizon,
                        "score_bucket_rank": int(bucket_rank)
                        if pd.notna(bucket_rank)
                        else None,
                        "score_bucket": score_bucket,
                        "event_count": int(len(frame)),
                        "code_count": int(frame["code"].nunique()),
                        "mean_forward_excess_return_pct": _mean_or_nan(values),
                        "median_forward_excess_return_pct": _median_or_nan(values),
                        "severe_loss_rate_pct": _rate_le_pct(
                            values,
                            severe_loss_threshold_pct,
                        ),
                        "mean_informed_flow_score": _mean_or_nan(
                            frame["informed_flow_score"]
                        ),
                        "mean_pre_atr_z": _mean_or_nan(frame["pre_atr_z"]),
                    }
                )
    columns = [
        "market_scope",
        "horizon",
        "score_bucket_rank",
        "score_bucket",
        "event_count",
        "code_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "severe_loss_rate_pct",
        "mean_informed_flow_score",
        "mean_pre_atr_z",
    ]
    return _sort_summary_df(pd.DataFrame(rows, columns=columns), bucket_count=bucket_count)


def _build_atr_volume_interaction_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        for horizon in horizons:
            return_col = f"forward_excess_return_{horizon}d_pct"
            for keys, frame in scoped_df.groupby(
                ["market_scope", "atr_segment", "volume_segment"],
                sort=False,
                dropna=False,
            ):
                market_scope, atr_segment, volume_segment = keys
                values = frame[return_col]
                rows.append(
                    {
                        "market_scope": market_scope,
                        "horizon": horizon,
                        "atr_segment": atr_segment,
                        "volume_segment": volume_segment,
                        "event_count": int(len(frame)),
                        "code_count": int(frame["code"].nunique()),
                        "mean_forward_excess_return_pct": _mean_or_nan(values),
                        "median_forward_excess_return_pct": _median_or_nan(values),
                        "severe_loss_rate_pct": _rate_le_pct(
                            values,
                            severe_loss_threshold_pct,
                        ),
                        "mean_pre_atr_z": _mean_or_nan(frame["pre_atr_z"]),
                    }
                )
    columns = [
        "market_scope",
        "horizon",
        "atr_segment",
        "volume_segment",
        "event_count",
        "code_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "severe_loss_rate_pct",
        "mean_pre_atr_z",
    ]
    return _sort_summary_df(pd.DataFrame(rows, columns=columns))


def _build_signed_move_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        for horizon in horizons:
            return_col = f"forward_excess_return_{horizon}d_pct"
            for keys, frame in scoped_df.groupby(
                ["market_scope", "event_direction", "signed_pre_move"],
                sort=False,
                dropna=False,
            ):
                market_scope, event_direction, signed_pre_move = keys
                values = frame[return_col]
                rows.append(
                    {
                        "market_scope": market_scope,
                        "horizon": horizon,
                        "event_direction": event_direction,
                        "signed_pre_move": signed_pre_move,
                        "event_count": int(len(frame)),
                        "code_count": int(frame["code"].nunique()),
                        "mean_forward_excess_return_pct": _mean_or_nan(values),
                        "median_forward_excess_return_pct": _median_or_nan(values),
                        "severe_loss_rate_pct": _rate_le_pct(
                            values,
                            severe_loss_threshold_pct,
                        ),
                        "mean_directional_pre_abret_pct": _mean_or_nan(
                            frame["directional_pre_abret_pct"]
                        ),
                    }
                )
    columns = [
        "market_scope",
        "horizon",
        "event_direction",
        "signed_pre_move",
        "event_count",
        "code_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "severe_loss_rate_pct",
        "mean_directional_pre_abret_pct",
    ]
    return _sort_summary_df(pd.DataFrame(rows, columns=columns))


def _build_sample_diagnostics_df(scoped_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        for market_scope, frame in scoped_df.groupby("market_scope", sort=False):
            rows.append(
                {
                    "market_scope": market_scope,
                    "event_count": int(len(frame)),
                    "realized_event_count": int((frame["status"] == "realized").sum()),
                    "code_count": int(frame["code"].nunique()),
                    "analysis_start_date": _str_or_none(frame["disclosed_date"].min()),
                    "analysis_end_date": _str_or_none(frame["disclosed_date"].max()),
                    "positive_event_count": int(
                        (frame["event_direction"] == "positive").sum()
                    ),
                    "negative_event_count": int(
                        (frame["event_direction"] == "negative").sum()
                    ),
                    "unknown_event_count": int(
                        (frame["event_direction"] == "unknown").sum()
                    ),
                }
            )
    columns = [
        "market_scope",
        "event_count",
        "realized_event_count",
        "code_count",
        "analysis_start_date",
        "analysis_end_date",
        "positive_event_count",
        "negative_event_count",
        "unknown_event_count",
    ]
    return _sort_summary_df(pd.DataFrame(rows, columns=columns))


def _base_event_record(row: Any) -> dict[str, Any]:
    return {
        "event_id": f"{row.code}:{row.disclosed_date}",
        "code": str(row.code),
        "company_name": str(row.company_name),
        "market": str(row.market),
        "market_code": str(row.market_code) if pd.notna(row.market_code) else None,
        "scale_category": str(row.scale_category) if pd.notna(row.scale_category) else None,
        "disclosed_date": str(row.disclosed_date),
        "type_of_document": str(row.type_of_document)
        if pd.notna(row.type_of_document)
        else None,
        "type_of_current_period": str(row.type_of_current_period)
        if pd.notna(row.type_of_current_period)
        else None,
        "event_metric": _float_or_nan(row.event_metric),
        "prior_event_metric": _float_or_nan(row.prior_event_metric),
        "event_metric_change_pct": _float_or_nan(row.event_metric_change_pct),
        "event_direction": str(row.event_direction),
        "status": "unknown",
        "pre_event_date": None,
        "entry_date": None,
        "pre_event_close": np.nan,
        "pre_atr": np.nan,
        "pre_atr_pct": np.nan,
        "pre_atr_z": np.nan,
        "atr_expansion_ratio": np.nan,
        "directional_pre_abret_pct": np.nan,
        "signed_pre_move": "unknown",
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
        record[f"pre_volume_mean_{window}d"] = np.nan
        record[f"pre_volume_z_{window}d"] = np.nan
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
        "event_metric",
        "prior_event_metric",
        "event_metric_change_pct",
        "event_direction",
        "status",
        "pre_event_date",
        "entry_date",
        "pre_event_close",
        "pre_atr",
        "pre_atr_pct",
        "pre_atr_z",
        "atr_expansion_ratio",
        "directional_pre_abret_pct",
        "signed_pre_move",
    ]
    for window in pre_windows:
        columns.extend(
            [
                f"pre_return_{window}d_pct",
                f"pre_topix_return_{window}d_pct",
                f"pre_abret_{window}d_pct",
                f"pre_volume_mean_{window}d",
                f"pre_volume_z_{window}d",
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


def _empty_event_source_df() -> pd.DataFrame:
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
            "profit",
            "market",
            "event_metric",
            "prior_event_metric",
            "event_metric_change_pct",
            "event_direction",
        ]
    )


def _empty_price_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["code", "date", "open", "high", "low", "close", "volume", "atr", "atr_pct"]
    )


def _resolve_event_metric(row: pd.Series) -> float:
    for column in ("forecast_eps", "next_year_forecast_earnings_per_share", "profit"):
        value = _float_or_nan(row.get(column))
        if math.isfinite(value):
            return value
    return np.nan


def _event_direction(change_pct: float) -> str:
    value = _float_or_nan(change_pct)
    if not math.isfinite(value):
        return "unknown"
    if value > 0.0:
        return "positive"
    if value < 0.0:
        return "negative"
    return "neutral"


def _signed_pre_move(pre_abret_pct: float, event_direction: str) -> str:
    value = _float_or_nan(pre_abret_pct)
    if not math.isfinite(value) or event_direction not in {"positive", "negative"}:
        return "unknown"
    if (event_direction == "positive" and value > 0.0) or (
        event_direction == "negative" and value < 0.0
    ):
        return "aligned"
    if value == 0.0:
        return "flat"
    return "opposed"


def _directional_pre_move(pre_abret_pct: float, event_direction: str) -> float:
    value = _float_or_nan(pre_abret_pct)
    if not math.isfinite(value):
        return np.nan
    if event_direction == "positive":
        return value
    if event_direction == "negative":
        return -value
    return abs(value)


def _topix_return_pct(topix_df: pd.DataFrame, *, start_date: str, end_date: str) -> float:
    if topix_df.empty:
        return np.nan
    start = topix_df[topix_df["date"].astype(str) <= start_date].tail(1)
    end = topix_df[topix_df["date"].astype(str) <= end_date].tail(1)
    if start.empty or end.empty:
        return np.nan
    return _return_pct(_float_or_nan(end.iloc[0]["close"]), _float_or_nan(start.iloc[0]["close"]))


def _bucket_rank_by_scope(
    df: pd.DataFrame,
    *,
    value_column: str,
    bucket_count: int,
) -> pd.Series:
    ranks = pd.Series(index=df.index, dtype="float64")
    for _, frame in df.groupby("market_scope", sort=False):
        valid = frame[value_column].replace([np.inf, -np.inf], np.nan).dropna()
        if valid.empty:
            continue
        pct = valid.rank(method="first", pct=True)
        ranks.loc[valid.index] = np.ceil(pct * bucket_count).clip(1, bucket_count)
    return ranks


def _tercile_segment_by_scope(df: pd.DataFrame, value_column: str) -> pd.Series:
    segments = pd.Series("missing", index=df.index, dtype="object")
    for _, frame in df.groupby("market_scope", sort=False):
        valid = frame[value_column].replace([np.inf, -np.inf], np.nan).dropna()
        if valid.empty:
            continue
        pct = valid.rank(method="first", pct=True)
        segments.loc[valid[pct <= (1.0 / 3.0)].index] = "low"
        segments.loc[valid[(pct > (1.0 / 3.0)) & (pct < (2.0 / 3.0))].index] = "middle"
        segments.loc[valid[pct >= (2.0 / 3.0)].index] = "high"
    return segments


def _bucket_label(rank: object, *, bucket_count: int) -> str:
    value = _float_or_nan(rank)
    if not math.isfinite(value):
        return "missing"
    rank_int = int(value)
    if bucket_count == 2:
        return "low" if rank_int == 1 else "high"
    if bucket_count == 3:
        return {1: "low", 2: "middle", 3: "high"}.get(rank_int, "missing")
    return _SCORE_BUCKET_LABELS.get(rank_int, f"q{rank_int}")


def _sort_summary_df(df: pd.DataFrame, *, bucket_count: int | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    ordered = df.copy()
    ordered["_market_order"] = ordered["market_scope"].map(
        {scope: idx for idx, scope in enumerate(_MARKET_SCOPE_ORDER)}
    ).fillna(len(_MARKET_SCOPE_ORDER))
    sort_columns = ["_market_order"]
    if "horizon" in ordered.columns:
        sort_columns.append("horizon")
    if "score_bucket_rank" in ordered.columns:
        sort_columns.append("score_bucket_rank")
    if "atr_segment" in ordered.columns:
        ordered["_atr_order"] = ordered["atr_segment"].map(
            {"low": 0, "middle": 1, "high": 2, "missing": 3}
        ).fillna(4)
        sort_columns.append("_atr_order")
    if "volume_segment" in ordered.columns:
        ordered["_volume_order"] = ordered["volume_segment"].map(
            {"low": 0, "middle": 1, "high": 2, "missing": 3}
        ).fillna(4)
        sort_columns.append("_volume_order")
    return ordered.sort_values(sort_columns, kind="stable").drop(
        columns=[column for column in ("_market_order", "_atr_order", "_volume_order") if column in ordered],
    ).reset_index(drop=True)


def _top_rows_for_markdown(
    df: pd.DataFrame,
    *,
    sort_columns: Sequence[str] | None = None,
    ascending: Sequence[bool] | bool = True,
    limit: int = 12,
) -> str:
    if df.empty:
        return "_No rows._"
    output = df.copy()
    if sort_columns:
        output = output.sort_values(list(sort_columns), ascending=ascending, kind="stable")
    return _dataframe_to_markdown(output.head(limit))


def _placeholder_sql(size: int) -> str:
    if size <= 0:
        raise ValueError("placeholder size must be positive")
    return ",".join("?" for _ in range(size))


def _offset_calendar_date(value: str | None, *, days: int) -> str | None:
    if value is None:
        return None
    return (pd.Timestamp(value) + pd.Timedelta(days=days)).strftime("%Y-%m-%d")


def _safe_pct_change(current: object, previous: object) -> float:
    current_value = _float_or_nan(current)
    previous_value = _float_or_nan(previous)
    if not math.isfinite(current_value) or not math.isfinite(previous_value):
        return np.nan
    if previous_value == 0.0:
        return np.nan
    return ((current_value / abs(previous_value)) - math.copysign(1.0, previous_value)) * 100.0


def _return_pct(end_value: object, start_value: object) -> float:
    end_float = _float_or_nan(end_value)
    start_float = _float_or_nan(start_value)
    if not math.isfinite(end_float) or not math.isfinite(start_float) or start_float <= 0:
        return np.nan
    return (end_float / start_float - 1.0) * 100.0


def _ratio(numerator: object, denominator: object) -> float:
    numerator_value = _float_or_nan(numerator)
    denominator_value = _float_or_nan(denominator)
    if not math.isfinite(numerator_value) or not math.isfinite(denominator_value):
        return np.nan
    if denominator_value == 0.0:
        return np.nan
    return numerator_value / denominator_value


def _zscore(value: object, baseline: pd.Series) -> float:
    value_float = _float_or_nan(value)
    baseline_values = pd.to_numeric(baseline, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if not math.isfinite(value_float) or len(baseline_values) < 2:
        return np.nan
    std = float(baseline_values.std(ddof=0))
    if std <= 0.0 or not math.isfinite(std):
        mean = float(baseline_values.mean())
        return 0.0 if value_float == mean else np.nan
    return (value_float - float(baseline_values.mean())) / std


def _dataframe_to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"
    columns = [str(column) for column in df.columns]
    rows = [
        [
            _format_markdown_cell(value)
            for value in row
        ]
        for row in df.itertuples(index=False, name=None)
    ]
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join([header, separator, *body])


def _format_markdown_cell(value: object) -> str:
    if _is_missing_scalar(value):
        return ""
    if isinstance(value, float):
        if not math.isfinite(value):
            return ""
        return f"{value:.4g}"
    return str(value)


def _rate_le_pct(values: pd.Series, threshold: float) -> float:
    numeric = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if numeric.empty:
        return np.nan
    return float((numeric <= threshold).mean() * 100.0)


def _mean_or_nan(values: pd.Series | object) -> float:
    if isinstance(values, pd.Series):
        numeric = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        return float(numeric.mean()) if not numeric.empty else np.nan
    value = _float_or_nan(values)
    return value if math.isfinite(value) else np.nan


def _median_or_nan(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    return float(numeric.median()) if not numeric.empty else np.nan


def _float_or_nan(value: object) -> float:
    try:
        if _is_missing_scalar(value):
            return np.nan
        return float(cast(Any, value))
    except (TypeError, ValueError):
        return np.nan


def _str_or_none(value: object) -> str | None:
    if _is_missing_scalar(value):
        return None
    return str(value)


def _is_missing_scalar(value: object) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    if isinstance(value, np.floating):
        return bool(np.isnan(value))
    return False


__all__ = [
    "DEFAULT_ATR_PERIOD",
    "DEFAULT_BASELINE_WINDOW",
    "DEFAULT_BUCKET_COUNT",
    "DEFAULT_HORIZONS",
    "DEFAULT_PRE_WINDOWS",
    "DEFAULT_SEVERE_LOSS_THRESHOLD_PCT",
    "PRE_DISCLOSURE_FLOW_VOLATILITY_EXPERIMENT_ID",
    "PreDisclosureFlowVolatilityResult",
    "build_summary_markdown",
    "run_pre_disclosure_flow_volatility_research",
    "write_pre_disclosure_flow_volatility_bundle",
]
