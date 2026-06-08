"""N-month rebalance portfolio lens for value-composite rankings."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    DEFAULT_ADV_WINDOW,
    DEFAULT_MARKETS,
    SourceMode,
    ShareAdjustmentEvent,
    _FEATURE_COLUMNS,
    _compute_adv,
    _empty_result_df as _annual_empty_result_df,
    _fetch_date_range,
    _latest_actual_fy_metric_statement,
    _latest_fy_statement,
    _market_query_codes,
    _open_analysis_connection,
    _query_adjustment_event_rows,
    _query_entry_stock_master,
    _query_price_rows,
    _query_statement_rows,
    _resolve_baseline_share_snapshot,
    _resolve_forward_eps,
    _to_nullable_float,
    _validate_adv_window,
    _normalize_selected_markets,
)
from src.domains.analytics.fundamental_ranking import adjust_per_share_value
from src.shared.utils.share_adjustment import is_valid_share_count
from src.domains.analytics.annual_fundamental_confounder_analysis import (
    DEFAULT_WINSOR_LOWER,
    DEFAULT_WINSOR_UPPER,
    _normalize_required_positive_columns,
)
from src.domains.analytics.annual_value_composite_selection import (
    DEFAULT_MIN_TRAIN_OBSERVATIONS,
    LIQUIDITY_SCENARIOS,
    PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
    STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
    EQUAL_VALUE_COMPOSITE_WEIGHTS,
    ScoreMethodSpec,
    _build_scored_panel_df,
    _build_walkforward_weight_df,
    _daily_stats,
    _frame_for_market_scope,
    _market_scope_sort,
    _series_mean,
    _walkforward_score_for_scope_year,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.analytics.research_core import build_event_portfolio_daily_df
from src.domains.analytics.value_composite_scoring import (
    build_value_composite_score_frame,
)

ANNUAL_VALUE_PERIODIC_REBALANCE_EXPERIMENT_ID = (
    "market-behavior/annual-value-periodic-rebalance"
)
DEFAULT_REBALANCE_MONTHS: tuple[int, ...] = (1, 3, 6, 12)
DEFAULT_SELECTION_COUNTS: tuple[int, ...] = (5, 10)
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth")
_CORE_SCORE_COLUMNS: tuple[str, ...] = (
    "low_pbr_score",
    "small_market_cap_score",
    "low_forward_per_score",
)
_PERIODIC_PORTFOLIO_GROUP_COLUMNS: tuple[str, ...] = (
    "market_scope",
    "score_method",
    "liquidity_scenario",
    "rebalance_months",
    "selection_count",
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "rebalance_calendar_df",
    "event_ledger_df",
    "scored_panel_df",
    "walkforward_weight_df",
    "selected_event_df",
    "selection_summary_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)


@dataclass(frozen=True)
class AnnualValuePeriodicRebalanceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    selected_markets: tuple[str, ...]
    rebalance_months: tuple[int, ...]
    selection_counts: tuple[int, ...]
    winsor_lower: float
    winsor_upper: float
    min_train_observations: int
    adv_window: int
    required_positive_columns: tuple[str, ...]
    current_market_snapshot_only: bool
    score_policy: str
    rebalance_calendar_df: pd.DataFrame
    event_ledger_df: pd.DataFrame
    scored_panel_df: pd.DataFrame
    walkforward_weight_df: pd.DataFrame
    selected_event_df: pd.DataFrame
    selection_summary_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


PERIODIC_SCORE_METHODS: tuple[ScoreMethodSpec, ...] = (
    ScoreMethodSpec(
        "equal_weight",
        "Equal-weight value composite",
        "equal_weight_score",
        "Average of low PBR, small market cap, and low forward PER percentile scores.",
    ),
    ScoreMethodSpec(
        "standard_pbr_tilt",
        "Standard PBR tilt ranking profile",
        "standard_pbr_tilt_score",
        "35% small market cap, 40% low PBR, and 25% low forward PER.",
    ),
    ScoreMethodSpec(
        "prime_size_tilt",
        "Prime size tilt ranking profile",
        "prime_size_tilt_score",
        "46.5% small market cap, 5% low PBR, and 48.5% low forward PER.",
    ),
    ScoreMethodSpec(
        "walkforward_regression_weight",
        "Walk-forward regression-weight composite",
        None,
        "Prior-period positive OLS coefficients over the three core scores.",
    ),
)


def _empty_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _normalize_rebalance_months(values: Sequence[int]) -> tuple[int, ...]:
    normalized: list[int] = []
    for raw_value in values:
        value = int(raw_value)
        if value < 1 or value > 12:
            raise ValueError("rebalance months must satisfy 1 <= months <= 12")
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError("at least one rebalance month value is required")
    return tuple(sorted(normalized))


def _normalize_selection_counts(values: Sequence[int]) -> tuple[int, ...]:
    normalized: list[int] = []
    for raw_value in values:
        value = int(raw_value)
        if value < 1:
            raise ValueError("selection counts must be positive")
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError("at least one selection count is required")
    return tuple(sorted(normalized))


def _query_trading_dates(
    conn: Any,
    *,
    start_year: int | None,
    end_year: int | None,
) -> list[str]:
    conditions: list[str] = []
    params: list[Any] = []
    if start_year is not None:
        conditions.append("date >= ?")
        params.append(f"{int(start_year):04d}-01-01")
    if end_year is not None:
        conditions.append("date <= ?")
        params.append(f"{int(end_year):04d}-12-31")
    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    df = conn.execute(
        f"""
        SELECT DISTINCT date
        FROM stock_data
        {where_sql}
        ORDER BY date
        """,
        params,
    ).fetchdf()
    return [str(value) for value in df["date"].tolist()]


def _first_trading_date_on_or_after(
    trading_dates: Sequence[str],
    boundary: str,
) -> str | None:
    for date_value in trading_dates:
        if date_value >= boundary:
            return str(date_value)
    return None


def _last_trading_date_before(
    trading_dates: Sequence[str],
    boundary: str,
) -> str | None:
    prior = [str(date_value) for date_value in trading_dates if str(date_value) < boundary]
    return prior[-1] if prior else None


def _build_rebalance_calendar_df(
    trading_dates: Sequence[str],
    *,
    rebalance_months: int,
    include_incomplete_last_period: bool,
) -> pd.DataFrame:
    columns = [
        "year",
        "calendar_year",
        "start_month",
        "rebalance_months",
        "entry_date",
        "exit_date",
        "market_trading_days",
    ]
    if not trading_dates:
        return _empty_df(columns)
    years = sorted({int(str(date_value)[:4]) for date_value in trading_dates})
    records: list[dict[str, Any]] = []
    for calendar_year in years:
        year_dates = [
            str(date_value)
            for date_value in trading_dates
            if str(date_value).startswith(f"{calendar_year:04d}-")
        ]
        if not year_dates:
            continue
        for start_month in range(1, 13, rebalance_months):
            start_boundary = f"{calendar_year:04d}-{start_month:02d}-01"
            entry_date = _first_trading_date_on_or_after(year_dates, start_boundary)
            if entry_date is None:
                continue
            next_month = start_month + rebalance_months
            if next_month <= 12:
                next_boundary = f"{calendar_year:04d}-{next_month:02d}-01"
                next_entry = _first_trading_date_on_or_after(year_dates, next_boundary)
                if next_entry is None:
                    continue
                exit_date = _last_trading_date_before(year_dates, next_entry)
            else:
                exit_date = year_dates[-1]
                if not include_incomplete_last_period and str(exit_date)[5:] < "12-15":
                    continue
            if exit_date is None or entry_date >= exit_date:
                continue
            path_dates = [
                date_value
                for date_value in year_dates
                if entry_date <= date_value <= exit_date
            ]
            if not path_dates:
                continue
            records.append(
                {
                    "year": f"{calendar_year:04d}-M{start_month:02d}-{rebalance_months}m",
                    "calendar_year": calendar_year,
                    "start_month": start_month,
                    "rebalance_months": rebalance_months,
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "market_trading_days": int(len(path_dates)),
                }
            )
    if not records:
        return _empty_df(columns)
    return pd.DataFrame(records, columns=columns).sort_values("year", kind="stable").reset_index(drop=True)


def _periodic_event_ledger_columns() -> list[str]:
    return [
        "event_id",
        "year",
        "code",
        "company_name",
        "market",
        "market_code",
        "sector_33_name",
        "scale_category",
        "listed_date",
        "status",
        "entry_date",
        "exit_date",
        "entry_open",
        "entry_close",
        "entry_previous_close",
        "exit_close",
        "holding_trading_days",
        "holding_calendar_days",
        "has_fy_as_of_entry",
        "latest_fy_disclosed_date",
        "latest_fy_period_end",
        "baseline_shares",
        "baseline_shares_source_date",
        "baseline_shares_source_period_type",
        "baseline_treasury_shares",
        "baseline_treasury_source_date",
        "baseline_treasury_source_period_type",
        "fy_shares_outstanding",
        "share_adjustment_ratio",
        "share_adjustment_applied",
        "forward_eps_source",
        "forward_eps_disclosed_date",
        "forward_eps_period_type",
        "preopen_per_prev_close",
        "preopen_forward_per_prev_close",
        "preopen_pbr_prev_close",
        "avg_trading_value_60d_source_sessions",
        "operating_profit_mil_jpy",
        "operating_cash_flow_mil_jpy",
        "simple_fcf_mil_jpy",
        "event_return",
        "event_return_pct",
        "cagr_pct",
        "max_drawdown_pct",
        "max_runup_pct",
        "annualized_volatility_pct",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        *_FEATURE_COLUMNS,
    ]


def _fast_period_return_metrics(
    *,
    entry_open: float,
    exit_close: float,
    entry_date: str,
    exit_date: str,
) -> dict[str, float | None]:
    total_return = exit_close / entry_open - 1.0
    period_days = (pd.Timestamp(exit_date) - pd.Timestamp(entry_date)).days
    cagr = None
    if period_days > 0 and total_return > -1.0:
        cagr_value = (1.0 + total_return) ** (365.25 / period_days) - 1.0
        cagr = float(cagr_value) if math.isfinite(cagr_value) else None
    return {
        "event_return": float(total_return),
        "event_return_pct": float(total_return * 100.0),
        "cagr_pct": cagr * 100.0 if cagr is not None else None,
        "max_drawdown_pct": None,
        "max_runup_pct": None,
        "annualized_volatility_pct": None,
        "sharpe_ratio": None,
        "sortino_ratio": None,
        "calmar_ratio": None,
    }


def _build_value_feature_values(
    *,
    statement_frame: pd.DataFrame,
    latest_fy: pd.Series | None,
    baseline: Any,
    as_of_date: str,
    entry_open: float | None,
    entry_previous_close: float | None,
    price_frame: pd.DataFrame,
    entry_idx: int | None,
    adv_window: int,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "has_fy_as_of_entry": latest_fy is not None,
        "latest_fy_disclosed_date": None,
        "latest_fy_period_end": None,
        "baseline_shares": baseline.shares,
        "baseline_shares_source_date": baseline.shares_source_date,
        "baseline_shares_source_period_type": baseline.shares_source_period_type,
        "baseline_treasury_shares": baseline.treasury_shares,
        "baseline_treasury_source_date": baseline.treasury_source_date,
        "baseline_treasury_source_period_type": baseline.treasury_source_period_type,
        "fy_shares_outstanding": None,
        "share_adjustment_ratio": None,
        "share_adjustment_applied": False,
        "forward_eps_source": None,
        "forward_eps_disclosed_date": None,
        "forward_eps_period_type": None,
        "preopen_per_prev_close": None,
        "preopen_forward_per_prev_close": None,
        "preopen_pbr_prev_close": None,
        "avg_trading_value_60d_source_sessions": 0,
        "operating_profit_mil_jpy": None,
        "operating_cash_flow_mil_jpy": None,
        "simple_fcf_mil_jpy": None,
    }
    for column in _FEATURE_COLUMNS:
        record[column] = None
    if latest_fy is None:
        return record

    def actual_per_share_metric(metric_column: str) -> float | None:
        row = _latest_actual_fy_metric_statement(
            statement_frame,
            as_of_date=as_of_date,
            metric_column=metric_column,
        )
        if row is None:
            return None
        return adjust_per_share_value(
            _to_nullable_float(row[metric_column]),
            _to_nullable_float(row["shares_outstanding"]),
            baseline.shares,
        )

    fy_shares = _to_nullable_float(latest_fy["shares_outstanding"])
    share_adjustment_ratio = (
        fy_shares / baseline.shares
        if fy_shares is not None
        and baseline.shares is not None
        and is_valid_share_count(fy_shares)
        and is_valid_share_count(baseline.shares)
        else None
    )
    bps = actual_per_share_metric("bps")
    forward_eps, forward_eps_source, forward_eps_date, forward_eps_period = (
        _resolve_forward_eps(
            statement_frame,
            latest_fy=latest_fy,
            baseline_shares=baseline.shares,
            as_of_date=as_of_date,
        )
    )
    market_cap = (
        entry_open * baseline.shares
        if entry_open is not None
        and baseline.shares is not None
        and entry_open > 0
        and baseline.shares > 0
        else None
    )
    adv, adv_sessions = (
        _compute_adv(price_frame, entry_idx=entry_idx, adv_window=adv_window)
        if entry_idx is not None
        else (None, 0)
    )
    record.update(
        {
            "latest_fy_disclosed_date": str(latest_fy["disclosed_date"]),
            "fy_shares_outstanding": fy_shares,
            "share_adjustment_ratio": share_adjustment_ratio,
            "share_adjustment_applied": (
                share_adjustment_ratio is not None
                and not math.isclose(
                    share_adjustment_ratio,
                    1.0,
                    rel_tol=1e-9,
                    abs_tol=1e-12,
                )
            ),
            "forward_eps": forward_eps,
            "forward_eps_source": forward_eps_source,
            "forward_eps_disclosed_date": forward_eps_date,
            "forward_eps_period_type": forward_eps_period,
            "bps": bps,
            "forward_per": (
                entry_open / forward_eps
                if entry_open is not None and forward_eps is not None and forward_eps != 0
                else None
            ),
            "pbr": (
                entry_open / bps
                if entry_open is not None and bps is not None and bps != 0
                else None
            ),
            "preopen_forward_per_prev_close": (
                entry_previous_close / forward_eps
                if entry_previous_close is not None
                and forward_eps is not None
                and forward_eps != 0
                else None
            ),
            "preopen_pbr_prev_close": (
                entry_previous_close / bps
                if entry_previous_close is not None and bps is not None and bps != 0
                else None
            ),
            "market_cap_bil_jpy": market_cap / 1_000_000_000.0
            if market_cap is not None
            else None,
            "avg_trading_value_60d_mil_jpy": adv / 1_000_000.0
            if adv is not None
            else None,
            "avg_trading_value_60d_source_sessions": adv_sessions,
            "market_cap_to_adv60": (
                market_cap / adv
                if market_cap is not None and adv is not None and adv != 0
                else None
            ),
            "adv60_to_market_cap_pct": (
                adv / market_cap * 100.0
                if adv is not None and market_cap is not None and market_cap != 0
                else None
            ),
        }
    )
    return record


def _build_periodic_event_ledger_fast(
    *,
    stock_df: pd.DataFrame,
    statement_df: pd.DataFrame,
    price_df: pd.DataFrame,
    adjustment_event_df: pd.DataFrame,
    adv_window: int,
) -> pd.DataFrame:
    columns = _periodic_event_ledger_columns()
    if stock_df.empty:
        return _empty_df(columns)

    price_by_code = {
        str(code): frame.sort_values("date", kind="stable").reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    statements_by_code = {
        str(code): frame.sort_values("disclosed_date", kind="stable").reset_index(drop=True)
        for code, frame in statement_df.groupby("code", sort=False)
    }
    adjustment_events_by_code = {
        str(code): [
            ShareAdjustmentEvent(
                date=str(row["date"]),
                adjustment_factor=float(row["adjustment_factor"]),
            )
            for row in frame.to_dict(orient="records")
        ]
        for code, frame in adjustment_event_df.groupby("code", sort=False)
    }
    feature_cache: dict[tuple[str, str], dict[str, Any]] = {}
    records: list[dict[str, Any]] = []

    for stock in stock_df.to_dict(orient="records"):
        code = str(stock["code"])
        price_frame = price_by_code.get(code)
        if price_frame is None or price_frame.empty:
            continue
        price_dates = price_frame["date"].astype(str).to_numpy()
        year = str(stock["year"])
        entry_date = str(stock["entry_date"])
        exit_date = str(stock["exit_date"])
        record: dict[str, Any] = {
            "event_id": f"{code}:{year}",
            "year": year,
            "code": code,
            "company_name": str(stock.get("company_name") or ""),
            "market": str(stock.get("market") or ""),
            "market_code": str(stock.get("market_code") or ""),
            "sector_33_name": str(stock.get("sector_33_name") or ""),
            "scale_category": str(stock.get("scale_category") or ""),
            "listed_date": str(stock.get("listed_date") or ""),
            "status": None,
            "entry_date": entry_date,
            "exit_date": exit_date,
            "entry_open": None,
            "entry_close": None,
            "entry_previous_close": None,
            "exit_close": None,
            "holding_trading_days": None,
            "holding_calendar_days": None,
            "event_return": None,
            "event_return_pct": None,
            "cagr_pct": None,
            "max_drawdown_pct": None,
            "max_runup_pct": None,
            "annualized_volatility_pct": None,
            "sharpe_ratio": None,
            "sortino_ratio": None,
            "calmar_ratio": None,
        }
        entry_positions = np.where(price_dates == entry_date)[0]
        exit_positions = np.where(price_dates == exit_date)[0]
        if len(entry_positions) == 0:
            record["status"] = "missing_entry_session"
            records.append(record)
            continue
        if len(exit_positions) == 0:
            record["status"] = "missing_exit_session"
            records.append(record)
            continue
        entry_idx = int(entry_positions[0])
        exit_idx = int(exit_positions[0])
        if entry_idx > exit_idx:
            record["status"] = "empty_holding_window"
            records.append(record)
            continue
        entry_open = _to_nullable_float(price_frame.iloc[entry_idx]["open"])
        entry_close = _to_nullable_float(price_frame.iloc[entry_idx]["close"])
        entry_previous_close = (
            _to_nullable_float(price_frame.iloc[entry_idx - 1]["close"])
            if entry_idx > 0
            else None
        )
        exit_close = _to_nullable_float(price_frame.iloc[exit_idx]["close"])
        if entry_open is None or entry_open <= 0:
            record["status"] = "invalid_entry_open"
            records.append(record)
            continue
        if exit_close is None or exit_close <= 0:
            record["status"] = "invalid_exit_close"
            records.append(record)
            continue

        cache_key = (code, entry_date)
        feature_values = feature_cache.get(cache_key)
        if feature_values is None:
            statement_frame = statements_by_code.get(
                code,
                _annual_empty_result_df(list(statement_df.columns)),
            )
            statement_frame_for_entry = statement_frame.copy()
            statement_frame_for_entry.attrs["as_of_date"] = entry_date
            baseline = _resolve_baseline_share_snapshot(
                statement_frame_for_entry,
                as_of_date=entry_date,
                adjustment_events=adjustment_events_by_code.get(code, []),
            )
            latest_fy = _latest_fy_statement(
                statement_frame_for_entry,
                as_of_date=entry_date,
            )
            feature_values = _build_value_feature_values(
                statement_frame=statement_frame_for_entry,
                latest_fy=latest_fy,
                baseline=baseline,
                as_of_date=entry_date,
                entry_open=entry_open,
                entry_previous_close=entry_previous_close,
                price_frame=price_frame,
                entry_idx=entry_idx,
                adv_window=adv_window,
            )
            feature_cache[cache_key] = feature_values

        holding_calendar_days = (pd.Timestamp(exit_date) - pd.Timestamp(entry_date)).days
        record.update(
            {
                **feature_values,
                **_fast_period_return_metrics(
                    entry_open=entry_open,
                    exit_close=exit_close,
                    entry_date=entry_date,
                    exit_date=exit_date,
                ),
                "status": "realized",
                "entry_open": entry_open,
                "entry_close": entry_close,
                "entry_previous_close": entry_previous_close,
                "exit_close": exit_close,
                "holding_trading_days": int(exit_idx - entry_idx + 1),
                "holding_calendar_days": int(holding_calendar_days),
            }
        )
        records.append(record)

    if not records:
        return _empty_df(columns)
    result = pd.DataFrame(records)
    for column in columns:
        if column not in result.columns:
            result[column] = None
    return result[columns].sort_values(["year", "market", "code"], kind="stable").reset_index(drop=True)


def _apply_periodic_score_columns(scored_panel_df: pd.DataFrame) -> pd.DataFrame:
    if scored_panel_df.empty:
        return scored_panel_df.copy()
    result = build_value_composite_score_frame(
        scored_panel_df,
        group_columns=("year", "market"),
        required_positive_columns=(),
        score_column="standard_pbr_tilt_score",
        weights=STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
    )
    result = build_value_composite_score_frame(
        result,
        group_columns=("year", "market"),
        required_positive_columns=(),
        score_column="prime_size_tilt_score",
        weights=PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
    )
    result = build_value_composite_score_frame(
        result,
        group_columns=("year", "market"),
        required_positive_columns=(),
        score_column="equal_weight_score",
        weights=EQUAL_VALUE_COMPOSITE_WEIGHTS,
    )
    return result.reset_index(drop=True)


def _select_top_events_for_count(
    group_df: pd.DataFrame,
    *,
    score_method: ScoreMethodSpec,
    score_values: pd.Series,
    selection_count: int,
    liquidity_scenario: Any,
) -> pd.DataFrame:
    eligible = group_df.copy()
    eligible["composite_score"] = score_values.loc[group_df.index]
    mask = pd.Series(True, index=eligible.index)
    if liquidity_scenario.min_adv60_mil_jpy is not None:
        mask &= (
            pd.to_numeric(eligible["avg_trading_value_60d_mil_jpy"], errors="coerce")
            >= liquidity_scenario.min_adv60_mil_jpy
        )
    if liquidity_scenario.min_market_cap_bil_jpy is not None:
        mask &= (
            pd.to_numeric(eligible["market_cap_bil_jpy"], errors="coerce")
            >= liquidity_scenario.min_market_cap_bil_jpy
        )
    eligible = eligible[mask].copy()
    eligible = eligible[pd.to_numeric(eligible["composite_score"], errors="coerce").notna()].copy()
    if eligible.empty:
        return _empty_df([])
    ranked = eligible.sort_values(["composite_score", "code"], ascending=[False, True], kind="stable").copy()
    ranked["selection_rank"] = np.arange(len(ranked), dtype=int) + 1
    selected = ranked.head(min(selection_count, len(ranked))).copy()
    selected["eligible_count"] = int(len(eligible))
    selected["selection_count"] = int(selection_count)
    selected["score_method"] = score_method.name
    selected["score_method_label"] = score_method.label
    selected["liquidity_scenario"] = liquidity_scenario.name
    selected["liquidity_scenario_label"] = liquidity_scenario.label
    return selected


def _build_selected_event_df(
    scored_panel_df: pd.DataFrame,
    walkforward_weight_df: pd.DataFrame,
    *,
    selection_counts: Sequence[int],
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "score_method",
        "score_method_label",
        "liquidity_scenario",
        "liquidity_scenario_label",
        "selection_count",
        "eligible_count",
        "selection_rank",
        "composite_score",
        "event_id",
        "year",
        "rebalance_period",
        "rebalance_months",
        "code",
        "company_name",
        "market",
        "market_code",
        "sector_33_name",
        "entry_date",
        "exit_date",
        "entry_open",
        "exit_close",
        "event_return_pct",
        "event_return_winsor_pct",
        "low_pbr_score",
        "small_market_cap_score",
        "low_forward_per_score",
        "pbr",
        "market_cap_bil_jpy",
        "forward_per",
        "avg_trading_value_60d_mil_jpy",
    ]
    if scored_panel_df.empty:
        return _empty_df(columns)
    frames: list[pd.DataFrame] = []
    for market_scope in _MARKET_SCOPE_ORDER:
        scope_df = _frame_for_market_scope(scored_panel_df, market_scope)
        if scope_df.empty:
            continue
        for period, period_df in scope_df.groupby("year", sort=True):
            period_value = str(period)
            for method in PERIODIC_SCORE_METHODS:
                if method.name == "walkforward_regression_weight":
                    score_values = _walkforward_score_for_scope_year(
                        period_df,
                        walkforward_weight_df,
                        market_scope=market_scope,
                        year=period_value,
                    )
                elif method.score_column is not None:
                    score_values = pd.to_numeric(period_df[method.score_column], errors="coerce")
                else:
                    continue
                for liquidity_scenario in LIQUIDITY_SCENARIOS:
                    for count in selection_counts:
                        selected = _select_top_events_for_count(
                            period_df,
                            score_method=method,
                            score_values=score_values,
                            selection_count=int(count),
                            liquidity_scenario=liquidity_scenario,
                        )
                        if selected.empty:
                            continue
                        selected["market_scope"] = market_scope
                        selected["rebalance_period"] = period_value
                        frames.append(selected)
    if not frames:
        return _empty_df(columns)
    result = pd.concat(frames, ignore_index=True)
    for column in columns:
        if column not in result.columns:
            result[column] = None
    result = result[columns]
    return _market_scope_sort(
        result,
        [
            "score_method",
            "liquidity_scenario",
            "rebalance_months",
            "selection_count",
            "year",
            "selection_rank",
        ],
    )


def _period_selection_stats(selection: pd.DataFrame) -> dict[str, Any]:
    period_returns = (
        selection.groupby("year", sort=True)["event_return_winsor_pct"]
        .mean()
        .dropna()
        .astype(float)
    )
    period_std = float(period_returns.std(ddof=1)) if len(period_returns) > 1 else None
    period_mean = float(period_returns.mean()) if len(period_returns) else None
    period_t = (
        period_mean / (period_std / math.sqrt(len(period_returns)))
        if period_mean is not None
        and period_std is not None
        and not math.isclose(period_std, 0.0, abs_tol=1e-12)
        else None
    )
    return {
        "period_count": int(len(period_returns)),
        "period_mean_return_pct": period_mean,
        "period_return_std_pct": period_std,
        "period_t_stat": period_t,
        "positive_period_rate_pct": float((period_returns > 0.0).mean() * 100.0)
        if len(period_returns)
        else None,
        "min_period_return_pct": float(period_returns.min()) if len(period_returns) else None,
        "max_period_return_pct": float(period_returns.max()) if len(period_returns) else None,
    }


def _build_selection_summary_df(selected_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "score_method",
        "score_method_label",
        "liquidity_scenario",
        "liquidity_scenario_label",
        "rebalance_months",
        "selection_count",
        "event_count",
        "period_count",
        "mean_return_pct",
        "median_return_pct",
        "win_rate_pct",
        "period_mean_return_pct",
        "period_return_std_pct",
        "period_t_stat",
        "positive_period_rate_pct",
        "min_period_return_pct",
        "max_period_return_pct",
        "mean_composite_score",
        "mean_adv60_mil_jpy",
        "mean_market_cap_bil_jpy",
    ]
    if selected_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    group_columns = [
        "market_scope",
        "score_method",
        "liquidity_scenario",
        "rebalance_months",
        "selection_count",
    ]
    for keys, group in selected_event_df.groupby(group_columns, observed=True, sort=False):
        market_scope, score_method, liquidity_scenario, rebalance_months, selection_count = keys
        returns = pd.to_numeric(group["event_return_winsor_pct"], errors="coerce").dropna()
        stats = _period_selection_stats(group)
        records.append(
            {
                "market_scope": str(market_scope),
                "score_method": str(score_method),
                "score_method_label": str(group["score_method_label"].iloc[0]),
                "liquidity_scenario": str(liquidity_scenario),
                "liquidity_scenario_label": str(group["liquidity_scenario_label"].iloc[0]),
                "rebalance_months": int(cast(int, rebalance_months)),
                "selection_count": int(cast(int, selection_count)),
                "event_count": int(len(group)),
                "mean_return_pct": float(returns.mean()) if not returns.empty else None,
                "median_return_pct": float(returns.median()) if not returns.empty else None,
                "win_rate_pct": float((returns > 0.0).mean() * 100.0)
                if not returns.empty
                else None,
                **stats,
                "mean_composite_score": _series_mean(group["composite_score"]),
                "mean_adv60_mil_jpy": _series_mean(group["avg_trading_value_60d_mil_jpy"]),
                "mean_market_cap_bil_jpy": _series_mean(group["market_cap_bil_jpy"]),
            }
        )
    return _market_scope_sort(
        pd.DataFrame(records),
        ["score_method", "liquidity_scenario", "rebalance_months", "selection_count"],
    )


def _build_portfolio_daily_df(
    selected_event_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    return build_event_portfolio_daily_df(
        selected_event_df,
        price_df,
        group_columns=_PERIODIC_PORTFOLIO_GROUP_COLUMNS,
    )


def _build_portfolio_summary_df(
    portfolio_daily_df: pd.DataFrame,
    selected_event_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "score_method",
        "score_method_label",
        "liquidity_scenario",
        "liquidity_scenario_label",
        "rebalance_months",
        "selection_count",
        "realized_event_count",
        "start_date",
        "end_date",
        "active_days",
        "avg_active_positions",
        "max_active_positions",
        "total_return_pct",
        "cagr_pct",
        "max_drawdown_pct",
        "annualized_volatility_pct",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
    ]
    if portfolio_daily_df.empty:
        return _empty_df(columns)
    event_counts = (
        selected_event_df.groupby(
            [
                "market_scope",
                "score_method",
                "liquidity_scenario",
                "rebalance_months",
                "selection_count",
            ],
            observed=True,
            sort=False,
        )
        .size()
        .to_dict()
    )
    label_lookup = {
        (
            str(row["market_scope"]),
            str(row["score_method"]),
            str(row["liquidity_scenario"]),
            int(cast(int, row["rebalance_months"])),
            int(cast(int, row["selection_count"])),
        ): (str(row["score_method_label"]), str(row["liquidity_scenario_label"]))
        for row in selected_event_df.to_dict(orient="records")
    }
    records: list[dict[str, Any]] = []
    for keys, group in portfolio_daily_df.groupby(
        ["market_scope", "score_method", "liquidity_scenario", "rebalance_months", "selection_count"],
        observed=True,
        sort=False,
    ):
        market_scope, score_method, liquidity_scenario, rebalance_months, selection_count = keys
        start_date = str(group["date"].iloc[0])
        end_date = str(group["date"].iloc[-1])
        total_return = float(group["portfolio_value"].iloc[-1] - 1.0)
        period_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
        cagr = None
        if period_days > 0 and total_return > -1.0:
            cagr_value = (1.0 + total_return) ** (365.25 / period_days) - 1.0
            cagr = float(cagr_value) if math.isfinite(cagr_value) else None
        drawdown = pd.to_numeric(group["drawdown_pct"], errors="coerce").min()
        max_drawdown_pct = float(drawdown) if pd.notna(drawdown) else None
        labels = label_lookup.get(
            (
                str(market_scope),
                str(score_method),
                str(liquidity_scenario),
                int(cast(int, rebalance_months)),
                int(cast(int, selection_count)),
            ),
            (str(score_method), str(liquidity_scenario)),
        )
        records.append(
            {
                "market_scope": str(market_scope),
                "score_method": str(score_method),
                "score_method_label": labels[0],
                "liquidity_scenario": str(liquidity_scenario),
                "liquidity_scenario_label": labels[1],
                "rebalance_months": int(cast(int, rebalance_months)),
                "selection_count": int(cast(int, selection_count)),
                "realized_event_count": int(event_counts.get(keys, 0)),
                "start_date": start_date,
                "end_date": end_date,
                "active_days": int(len(group)),
                "avg_active_positions": _series_mean(group["active_positions"]),
                "max_active_positions": int(pd.to_numeric(group["active_positions"]).max()),
                "total_return_pct": total_return * 100.0,
                "cagr_pct": cagr * 100.0 if cagr is not None else None,
                "max_drawdown_pct": max_drawdown_pct,
                **_daily_stats(group["mean_daily_return"]),
                "calmar_ratio": (
                    cagr / abs(max_drawdown_pct / 100.0)
                    if cagr is not None and max_drawdown_pct is not None and max_drawdown_pct < -1e-12
                    else None
                ),
            }
        )
    return _market_scope_sort(
        pd.DataFrame(records),
        ["score_method", "liquidity_scenario", "rebalance_months", "selection_count"],
    )


def run_annual_value_periodic_rebalance(
    db_path: str | Path,
    *,
    markets: Sequence[str] = DEFAULT_MARKETS,
    rebalance_months: Sequence[int] = DEFAULT_REBALANCE_MONTHS,
    selection_counts: Sequence[int] = DEFAULT_SELECTION_COUNTS,
    start_year: int | None = None,
    end_year: int | None = None,
    winsor_lower: float = DEFAULT_WINSOR_LOWER,
    winsor_upper: float = DEFAULT_WINSOR_UPPER,
    min_train_observations: int = DEFAULT_MIN_TRAIN_OBSERVATIONS,
    adv_window: int = DEFAULT_ADV_WINDOW,
    required_positive_columns: Sequence[str] = (),
    include_incomplete_last_period: bool = False,
) -> AnnualValuePeriodicRebalanceResult:
    if not (0.0 <= winsor_lower < winsor_upper <= 1.0):
        raise ValueError("winsor bounds must satisfy 0 <= lower < upper <= 1")
    if min_train_observations < 5:
        raise ValueError("min_train_observations must be >= 5")
    resolved_db_path = str(Path(db_path).expanduser())
    normalized_markets = _normalize_selected_markets(markets)
    normalized_months = _normalize_rebalance_months(rebalance_months)
    normalized_counts = _normalize_selection_counts(selection_counts)
    normalized_adv_window = _validate_adv_window(adv_window)
    normalized_positive_columns = _normalize_required_positive_columns(required_positive_columns)
    market_codes = _market_query_codes(normalized_markets)

    calendars: list[pd.DataFrame] = []
    with _open_analysis_connection(resolved_db_path) as ctx:
        conn = ctx.connection
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
        available_start_date, available_end_date = _fetch_date_range(
            conn,
            table_name="stock_data",
        )
        trading_dates = _query_trading_dates(conn, start_year=start_year, end_year=end_year)
        for months in normalized_months:
            calendars.append(
                _build_rebalance_calendar_df(
                    trading_dates,
                    rebalance_months=months,
                    include_incomplete_last_period=include_incomplete_last_period,
                )
            )
        rebalance_calendar_df = (
            pd.concat(calendars, ignore_index=True)
            if calendars
            else _empty_df([])
        )
        if rebalance_calendar_df.empty:
            empty = _empty_df([])
            return AnnualValuePeriodicRebalanceResult(
                db_path=resolved_db_path,
                source_mode=source_mode,
                source_detail=source_detail,
                available_start_date=available_start_date,
                available_end_date=available_end_date,
                analysis_start_date=None,
                analysis_end_date=None,
                selected_markets=normalized_markets,
                rebalance_months=normalized_months,
                selection_counts=normalized_counts,
                winsor_lower=winsor_lower,
                winsor_upper=winsor_upper,
                min_train_observations=min_train_observations,
                adv_window=normalized_adv_window,
                required_positive_columns=normalized_positive_columns,
                current_market_snapshot_only=False,
                score_policy="no rebalance periods available",
                rebalance_calendar_df=empty.copy(),
                event_ledger_df=empty.copy(),
                scored_panel_df=empty.copy(),
                walkforward_weight_df=empty.copy(),
                selected_event_df=empty.copy(),
                selection_summary_df=empty.copy(),
                portfolio_daily_df=empty.copy(),
                portfolio_summary_df=empty.copy(),
            )
        stock_frames: list[pd.DataFrame] = []
        pit_stock_master_flags: list[bool] = []
        for _, calendar_part in rebalance_calendar_df.groupby(
            "rebalance_months",
            sort=False,
        ):
            stock_part, uses_pit_stock_master_part = _query_entry_stock_master(
                conn,
                calendar_df=calendar_part.reset_index(drop=True),
                market_codes=market_codes,
            )
            if not stock_part.empty:
                stock_frames.append(stock_part)
            pit_stock_master_flags.append(uses_pit_stock_master_part)
        stock_df = (
            pd.concat(stock_frames, ignore_index=True)
            if stock_frames
            else _empty_df([])
        )
        uses_pit_stock_master = all(pit_stock_master_flags) if pit_stock_master_flags else False
        allowed_codes = set(stock_df["code"].astype(str))
        statement_df = _query_statement_rows(conn, codes=tuple(sorted(allowed_codes)))
        price_start_year = int(str(rebalance_calendar_df["entry_date"].min())[:4]) - 1
        price_df = _query_price_rows(
            conn,
            codes=tuple(sorted(allowed_codes)),
            start_date=f"{price_start_year:04d}-01-01",
            end_date=str(rebalance_calendar_df["exit_date"].max()),
        )
        adjustment_event_df = _query_adjustment_event_rows(
            conn,
            codes=tuple(sorted(allowed_codes)),
            end_date=str(rebalance_calendar_df["exit_date"].max()),
        )

    event_ledger_df = _build_periodic_event_ledger_fast(
        stock_df=stock_df,
        statement_df=statement_df,
        price_df=price_df,
        adjustment_event_df=adjustment_event_df,
        adv_window=normalized_adv_window,
    )
    period_metadata = rebalance_calendar_df[
        ["year", "calendar_year", "start_month", "rebalance_months"]
    ].drop_duplicates("year")
    if not event_ledger_df.empty:
        event_ledger_df = event_ledger_df.merge(period_metadata, on="year", how="left")
    scored_panel_df = _build_scored_panel_df(
        event_ledger_df,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        required_positive_columns=normalized_positive_columns,
    )
    if not scored_panel_df.empty and not event_ledger_df.empty:
        scored_panel_df = scored_panel_df.merge(
            event_ledger_df[["event_id", "calendar_year", "start_month", "rebalance_months"]]
            .drop_duplicates("event_id"),
            on="event_id",
            how="left",
        )
    scored_panel_df = _apply_periodic_score_columns(scored_panel_df)
    walkforward_weight_df = _build_walkforward_weight_df(
        scored_panel_df,
        min_train_observations=min_train_observations,
    )
    selected_event_df = _build_selected_event_df(
        scored_panel_df,
        walkforward_weight_df,
        selection_counts=normalized_counts,
    )
    selection_summary_df = _build_selection_summary_df(selected_event_df)
    portfolio_daily_df = _build_portfolio_daily_df(selected_event_df, price_df)
    portfolio_summary_df = _build_portfolio_summary_df(portfolio_daily_df, selected_event_df)
    realized_df = event_ledger_df[event_ledger_df["status"].astype(str) == "realized"].copy()
    return AnnualValuePeriodicRebalanceResult(
        db_path=resolved_db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=str(realized_df["entry_date"].min()) if not realized_df.empty else None,
        analysis_end_date=str(realized_df["exit_date"].max()) if not realized_df.empty else None,
        selected_markets=normalized_markets,
        rebalance_months=normalized_months,
        selection_counts=normalized_counts,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        min_train_observations=min_train_observations,
        adv_window=normalized_adv_window,
        required_positive_columns=normalized_positive_columns,
        current_market_snapshot_only=not uses_pit_stock_master,
        score_policy=(
            "full liquidation and replacement at each N-month period; entry is first "
            "period trading day open and exit is the prior period's last trading day "
            "close; scores are PIT as of each entry date"
        ),
        rebalance_calendar_df=rebalance_calendar_df,
        event_ledger_df=event_ledger_df,
        scored_panel_df=scored_panel_df,
        walkforward_weight_df=walkforward_weight_df,
        selected_event_df=selected_event_df,
        selection_summary_df=selection_summary_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
    )


def _fmt(value: object, digits: int = 2) -> str:
    if value is None:
        return "-"
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        return "-"
    return f"{number:.{digits}f}"


def _build_summary_markdown(result: AnnualValuePeriodicRebalanceResult) -> str:
    lines = [
        "# Annual Value Periodic Rebalance",
        "",
        "## Setup",
        "",
        f"- DB path: `{result.db_path}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Rebalance months: `{', '.join(str(value) for value in result.rebalance_months)}`",
        f"- Selection counts: `{', '.join(str(value) for value in result.selection_counts)}`",
        f"- Required positive columns: `{', '.join(result.required_positive_columns) or 'none'}`",
        f"- Score policy: {result.score_policy}.",
        "",
        "## Top Portfolio Rows",
        "",
    ]
    summary = result.portfolio_summary_df.copy()
    if summary.empty:
        lines.append("- No portfolio summary rows were produced.")
    else:
        focus = summary[
            (summary["market_scope"].astype(str).isin(["prime", "standard"]))
            & (summary["liquidity_scenario"].astype(str).isin(["none", "adv10m"]))
        ].copy()
        if focus.empty:
            focus = summary.copy()
        focus = focus.sort_values("sharpe_ratio", ascending=False, na_position="last").head(16)
        for row in focus.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['score_method']}` / "
                f"`{row['liquidity_scenario']}` / `{int(cast(int, row['rebalance_months']))}m` / "
                f"top `{int(cast(int, row['selection_count']))}`: "
                f"CAGR `{_fmt(row['cagr_pct'])}%`, "
                f"Sharpe `{_fmt(row['sharpe_ratio'])}`, "
                f"maxDD `{_fmt(row['max_drawdown_pct'])}%`, "
                f"events `{int(cast(int, row['realized_event_count']))}`"
            )
    return "\n".join(lines)



def write_annual_value_periodic_rebalance_bundle(
    result: AnnualValuePeriodicRebalanceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_VALUE_PERIODIC_REBALANCE_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_value_periodic_rebalance",
        params={
            "db_path": result.db_path,
            "markets": list(result.selected_markets),
            "rebalance_months": list(result.rebalance_months),
            "selection_counts": list(result.selection_counts),
            "winsor_lower": result.winsor_lower,
            "winsor_upper": result.winsor_upper,
            "min_train_observations": result.min_train_observations,
            "adv_window": result.adv_window,
            "required_positive_columns": list(result.required_positive_columns),
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_value_periodic_rebalance_bundle(
    bundle_path: str | Path,
) -> AnnualValuePeriodicRebalanceResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualValuePeriodicRebalanceResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_value_periodic_rebalance_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_VALUE_PERIODIC_REBALANCE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_value_periodic_rebalance_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_VALUE_PERIODIC_REBALANCE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
