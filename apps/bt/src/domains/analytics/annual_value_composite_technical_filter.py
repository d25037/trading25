"""Technical filter overlay research for annual value-composite selections."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    _open_analysis_connection,
    _query_price_rows,
    _table_exists,
)
from src.domains.analytics.annual_value_composite_selection import (
    get_annual_value_composite_selection_latest_bundle_path,
    load_annual_value_composite_selection_bundle,
    _annual_selection_stats,
    _build_portfolio_daily_df,
    _build_portfolio_summary_df,
    _empty_df,
    _market_scope_sort,
    _series_mean,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    load_research_bundle_info,
    resolve_required_bundle_path,
    write_dataclass_research_bundle,
)

ANNUAL_VALUE_COMPOSITE_TECHNICAL_FILTER_EXPERIMENT_ID = (
    "market-behavior/annual-value-composite-technical-filter"
)
DEFAULT_SMA_WINDOW = 250
DEFAULT_SLOPE_WINDOW = 20
DEFAULT_NEAR_SMA_THRESHOLD = 0.95
DEFAULT_WARMUP_CALENDAR_DAYS = 540
_PORTFOLIO_FOCUS_MARKET_SCOPES: tuple[str, ...] = ("standard",)
_PORTFOLIO_FOCUS_SCORE_METHODS: tuple[str, ...] = (
    "walkforward_regression_weight",
    "equal_weight",
)
_PORTFOLIO_FOCUS_LIQUIDITY_SCENARIOS: tuple[str, ...] = ("none",)
_PORTFOLIO_FOCUS_SELECTION_FRACTIONS: tuple[float, ...] = (0.10,)

_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "enriched_selected_event_df",
    "technical_filter_event_df",
    "technical_filter_summary_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)


@dataclass(frozen=True)
class TechnicalFilterSpec:
    name: str
    label: str
    description: str


@dataclass(frozen=True)
class AnnualValueCompositeTechnicalFilterResult:
    db_path: str
    source_mode: str
    source_detail: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    sma_window: int
    slope_window: int
    near_sma_threshold: float
    selected_event_count: int
    technical_feature_count: int
    technical_policy: str
    enriched_selected_event_df: pd.DataFrame
    technical_filter_event_df: pd.DataFrame
    technical_filter_summary_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


TECHNICAL_FILTERS: tuple[TechnicalFilterSpec, ...] = (
    TechnicalFilterSpec(
        "baseline",
        "Baseline selected value names",
        "No technical filter; preserves the upstream annual value-composite selection.",
    ),
    TechnicalFilterSpec(
        "stock_above_sma250",
        "Stock close > SMA250",
        "Entry previous-session stock close is above its 250-session SMA.",
    ),
    TechnicalFilterSpec(
        "topix_above_sma250",
        "TOPIX close > SMA250",
        "Entry previous-session TOPIX close is above its 250-session SMA.",
    ),
    TechnicalFilterSpec(
        "stock_and_topix_above_sma250",
        "Stock and TOPIX > SMA250",
        "Both stock and TOPIX trend filters pass at the entry previous session.",
    ),
    TechnicalFilterSpec(
        "stock_near_or_above_sma250",
        "Stock close >= 95% of SMA250",
        "Stock is above or close to the 250-session SMA, allowing early recoveries.",
    ),
    TechnicalFilterSpec(
        "stock_above_sma250_or_positive_slope",
        "Stock > SMA250 or SMA250 slope > 0",
        "Stock is above SMA250, or its SMA250 has a positive 20-session slope.",
    ),
    TechnicalFilterSpec(
        "stock_below_sma250",
        "Stock close < SMA250",
        "Entry previous-session stock close is below its 250-session SMA.",
    ),
    TechnicalFilterSpec(
        "stock_below_sma250_and_topix_above_sma250",
        "Stock < SMA250 and TOPIX > SMA250",
        "Stock is below its SMA250 while TOPIX remains above its SMA250.",
    ),
    TechnicalFilterSpec(
        "stock_and_topix_below_sma250",
        "Stock and TOPIX < SMA250",
        "Both stock and TOPIX are below their 250-session SMA at the entry previous session.",
    ),
)
_FILTER_LABELS = {spec.name: spec.label for spec in TECHNICAL_FILTERS}


def run_annual_value_composite_technical_filter(
    input_bundle_path: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    sma_window: int = DEFAULT_SMA_WINDOW,
    slope_window: int = DEFAULT_SLOPE_WINDOW,
    near_sma_threshold: float = DEFAULT_NEAR_SMA_THRESHOLD,
    focus_standard_top10_no_liquidity: bool = False,
) -> AnnualValueCompositeTechnicalFilterResult:
    if sma_window < 2:
        raise ValueError("sma_window must be >= 2")
    if slope_window < 1:
        raise ValueError("slope_window must be >= 1")
    if not 0.0 < near_sma_threshold <= 1.0:
        raise ValueError("near_sma_threshold must satisfy 0 < threshold <= 1")

    resolved_input = resolve_required_bundle_path(
        input_bundle_path,
        latest_bundle_resolver=lambda: get_annual_value_composite_selection_latest_bundle_path(
            output_root=output_root,
        ),
        missing_message=(
            "Annual value-composite selection bundle was not found. "
            "Run run_annual_value_composite_selection.py first."
        ),
    )
    input_info = load_research_bundle_info(resolved_input)
    value_result = load_annual_value_composite_selection_bundle(resolved_input)
    selected_event_df = value_result.selected_event_df.copy()
    if focus_standard_top10_no_liquidity:
        selected_event_df = _standard_top10_focus_event_df(selected_event_df)
    source_mode, source_detail, stock_price_df, topix_price_df = _load_technical_price_frames(
        value_result.db_path,
        selected_event_df,
        sma_window=sma_window,
    )
    enriched = _add_technical_features(
        selected_event_df,
        stock_price_df=stock_price_df,
        topix_price_df=topix_price_df,
        sma_window=sma_window,
        slope_window=slope_window,
        near_sma_threshold=near_sma_threshold,
    )
    technical_filter_event_df = _build_technical_filter_event_df(enriched)
    technical_filter_summary_df = _build_technical_filter_summary_df(
        technical_filter_event_df
    )
    portfolio_daily_df, portfolio_summary_df = _build_technical_portfolio_tables(
        technical_filter_event_df,
        stock_price_df=stock_price_df,
    )
    technical_feature_count = int(
        pd.to_numeric(enriched["stock_price_to_sma250"], errors="coerce").notna().sum()
    )
    return AnnualValueCompositeTechnicalFilterResult(
        db_path=value_result.db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=value_result.analysis_start_date,
        analysis_end_date=value_result.analysis_end_date,
        sma_window=sma_window,
        slope_window=slope_window,
        near_sma_threshold=near_sma_threshold,
        selected_event_count=int(len(selected_event_df)),
        technical_feature_count=technical_feature_count,
        technical_policy=(
            "technical features use the latest trading session strictly before entry_date; "
            f"stock and TOPIX SMA use {sma_window} sessions; slope uses {slope_window} sessions; "
            + (
                "input rows are limited to the standard/no-liquidity/top-10% focus rows "
                "for equal-weight and walk-forward value scores"
                if focus_standard_top10_no_liquidity
                else (
                    "portfolio curves are rebuilt for the standard/no-liquidity/top-10% "
                    "equal-weight and walk-forward value score focus rows"
                )
            )
        ),
        enriched_selected_event_df=enriched,
        technical_filter_event_df=technical_filter_event_df,
        technical_filter_summary_df=technical_filter_summary_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
    )


def _load_technical_price_frames(
    db_path: str,
    selected_event_df: pd.DataFrame,
    *,
    sma_window: int,
) -> tuple[str, str, pd.DataFrame, pd.DataFrame]:
    if selected_event_df.empty:
        return "unknown", "no selected events", _empty_df([]), _empty_df([])
    entry_dates = pd.to_datetime(selected_event_df["entry_date"], errors="coerce").dropna()
    exit_dates = pd.to_datetime(selected_event_df["exit_date"], errors="coerce").dropna()
    if entry_dates.empty or exit_dates.empty:
        return "unknown", "no valid event dates", _empty_df([]), _empty_df([])
    warmup_days = max(DEFAULT_WARMUP_CALENDAR_DAYS, int(math.ceil(sma_window * 2.2)))
    start_date = (entry_dates.min() - pd.Timedelta(days=warmup_days)).strftime("%Y-%m-%d")
    end_date = exit_dates.max().strftime("%Y-%m-%d")
    selected_codes = tuple(sorted(set(selected_event_df["code"].astype(str))))
    with _open_analysis_connection(db_path) as ctx:
        stock_price_df = _query_price_rows(
            ctx.connection,
            codes=selected_codes,
            start_date=start_date,
            end_date=end_date,
        )
        topix_price_df = _query_topix_rows(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
        )
        return str(ctx.source_mode), ctx.source_detail, stock_price_df, topix_price_df


def _query_topix_rows(
    conn: Any,
    *,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    columns = ["date", "close"]
    if not _table_exists(conn, "topix_data"):
        return _empty_df(columns)
    df = conn.execute(
        """
        SELECT
            date,
            close
        FROM topix_data
        WHERE date >= ? AND date <= ?
          AND close IS NOT NULL
        ORDER BY date
        """,
        [start_date, end_date],
    ).fetchdf()
    if df.empty:
        return _empty_df(columns)
    df["date"] = df["date"].astype(str)
    return df[columns]


def _add_technical_features(
    selected_event_df: pd.DataFrame,
    *,
    stock_price_df: pd.DataFrame,
    topix_price_df: pd.DataFrame,
    sma_window: int,
    slope_window: int,
    near_sma_threshold: float,
) -> pd.DataFrame:
    if selected_event_df.empty:
        return _ensure_technical_columns(selected_event_df.copy())
    stock_features = _build_stock_feature_lookup(
        selected_event_df,
        stock_price_df=stock_price_df,
        sma_window=sma_window,
        slope_window=slope_window,
    )
    topix_features = _build_index_feature_lookup(
        selected_event_df,
        topix_price_df=topix_price_df,
        sma_window=sma_window,
        slope_window=slope_window,
    )
    result = selected_event_df.copy().reset_index(drop=True)
    stock_feature_df = pd.DataFrame(stock_features)
    topix_feature_df = pd.DataFrame(topix_features)
    result = pd.concat([result, stock_feature_df, topix_feature_df], axis=1)
    result["stock_above_sma250"] = pd.to_numeric(
        result["stock_price_to_sma250"], errors="coerce"
    ) > 1.0
    result["stock_near_or_above_sma250"] = pd.to_numeric(
        result["stock_price_to_sma250"], errors="coerce"
    ) >= near_sma_threshold
    result["stock_sma250_slope_positive"] = pd.to_numeric(
        result["stock_sma250_slope_20d_pct"], errors="coerce"
    ) > 0.0
    result["stock_above_sma250_or_positive_slope"] = (
        result["stock_above_sma250"] | result["stock_sma250_slope_positive"]
    )
    result["topix_above_sma250"] = pd.to_numeric(
        result["topix_price_to_sma250"], errors="coerce"
    ) > 1.0
    result["stock_below_sma250"] = pd.to_numeric(
        result["stock_price_to_sma250"], errors="coerce"
    ) < 1.0
    result["topix_below_sma250"] = pd.to_numeric(
        result["topix_price_to_sma250"], errors="coerce"
    ) < 1.0
    result["stock_and_topix_above_sma250"] = (
        result["stock_above_sma250"] & result["topix_above_sma250"]
    )
    result["stock_below_sma250_and_topix_above_sma250"] = (
        result["stock_below_sma250"] & result["topix_above_sma250"]
    )
    result["stock_and_topix_below_sma250"] = (
        result["stock_below_sma250"] & result["topix_below_sma250"]
    )
    return _ensure_technical_columns(result)


def _build_stock_feature_lookup(
    selected_event_df: pd.DataFrame,
    *,
    stock_price_df: pd.DataFrame,
    sma_window: int,
    slope_window: int,
) -> list[dict[str, Any]]:
    feature_frames = {
        str(code): _build_sma_feature_frame(
            frame,
            sma_window=sma_window,
            slope_window=slope_window,
        )
        for code, frame in stock_price_df.groupby("code", sort=False)
    }
    rows: list[dict[str, Any]] = []
    for event in selected_event_df.to_dict(orient="records"):
        code = str(event.get("code", ""))
        entry_date = _coerce_timestamp(event.get("entry_date"))
        feature_df = feature_frames.get(code)
        row = _lookup_previous_feature_row(
            feature_df,
            entry_date=entry_date,
            prefix="stock",
        )
        rows.append(row)
    return rows


def _build_index_feature_lookup(
    selected_event_df: pd.DataFrame,
    *,
    topix_price_df: pd.DataFrame,
    sma_window: int,
    slope_window: int,
) -> list[dict[str, Any]]:
    feature_df = _build_sma_feature_frame(
        topix_price_df,
        sma_window=sma_window,
        slope_window=slope_window,
    )
    rows: list[dict[str, Any]] = []
    for event in selected_event_df.to_dict(orient="records"):
        entry_date = _coerce_timestamp(event.get("entry_date"))
        rows.append(
            _lookup_previous_feature_row(
                feature_df,
                entry_date=entry_date,
                prefix="topix",
            )
        )
    return rows


def _build_sma_feature_frame(
    frame: pd.DataFrame,
    *,
    sma_window: int,
    slope_window: int,
) -> pd.DataFrame:
    columns = ["date", "close", "sma250", "price_to_sma250", "sma250_slope_20d_pct"]
    if frame.empty:
        return _empty_df(columns)
    result = frame.copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.normalize()
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result = result.dropna(subset=["date"]).sort_values("date", kind="stable")
    result = result.drop_duplicates("date", keep="last").reset_index(drop=True)
    result["sma250"] = result["close"].rolling(sma_window, min_periods=sma_window).mean()
    result["price_to_sma250"] = result["close"] / result["sma250"]
    result["sma250_slope_20d_pct"] = (result["sma250"] / result["sma250"].shift(slope_window) - 1.0) * 100.0
    return result[columns]


def _lookup_previous_feature_row(
    feature_df: pd.DataFrame | None,
    *,
    entry_date: pd.Timestamp | None,
    prefix: str,
) -> dict[str, Any]:
    if feature_df is None or feature_df.empty or entry_date is None:
        return _missing_feature_row(prefix)
    dates = pd.DatetimeIndex(pd.to_datetime(feature_df["date"], errors="coerce"))
    position = dates.searchsorted(entry_date, side="left") - 1
    if position < 0:
        return _missing_feature_row(prefix)
    row = feature_df.iloc[int(position)]
    feature_date = cast(pd.Timestamp, row["date"])
    return {
        f"{prefix}_technical_feature_date": feature_date.strftime("%Y-%m-%d"),
        f"{prefix}_technical_feature_lag_days": float((entry_date - feature_date).days),
        f"{prefix}_close": _float_or_nan(row.get("close")),
        f"{prefix}_sma250": _float_or_nan(row.get("sma250")),
        f"{prefix}_price_to_sma250": _float_or_nan(row.get("price_to_sma250")),
        f"{prefix}_sma250_slope_20d_pct": _float_or_nan(row.get("sma250_slope_20d_pct")),
    }


def _missing_feature_row(prefix: str) -> dict[str, Any]:
    return {
        f"{prefix}_technical_feature_date": None,
        f"{prefix}_technical_feature_lag_days": np.nan,
        f"{prefix}_close": np.nan,
        f"{prefix}_sma250": np.nan,
        f"{prefix}_price_to_sma250": np.nan,
        f"{prefix}_sma250_slope_20d_pct": np.nan,
    }


def _coerce_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    ts = pd.to_datetime(str(value), errors="coerce")
    if pd.isna(ts):
        return None
    return cast(pd.Timestamp, ts)


def _ensure_technical_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for prefix in ("stock", "topix"):
        for column in (
            "technical_feature_date",
            "technical_feature_lag_days",
            "close",
            "sma250",
            "price_to_sma250",
            "sma250_slope_20d_pct",
        ):
            name = f"{prefix}_{column}"
            if name not in result.columns:
                result[name] = np.nan
    for column in (
        "stock_above_sma250",
        "stock_near_or_above_sma250",
        "stock_sma250_slope_positive",
        "stock_above_sma250_or_positive_slope",
        "topix_above_sma250",
        "stock_below_sma250",
        "topix_below_sma250",
        "stock_and_topix_above_sma250",
        "stock_below_sma250_and_topix_above_sma250",
        "stock_and_topix_below_sma250",
    ):
        if column not in result.columns:
            result[column] = False
    return result


def _build_technical_filter_event_df(enriched_selected_event_df: pd.DataFrame) -> pd.DataFrame:
    if enriched_selected_event_df.empty:
        return _empty_df([])
    frames: list[pd.DataFrame] = []
    for spec in TECHNICAL_FILTERS:
        if spec.name == "baseline":
            filtered = enriched_selected_event_df.copy()
        else:
            filtered = enriched_selected_event_df[
                enriched_selected_event_df[spec.name].fillna(False).astype(bool)
            ].copy()
        if filtered.empty:
            continue
        filtered["technical_filter"] = spec.name
        filtered["technical_filter_label"] = spec.label
        frames.append(filtered)
    if not frames:
        return _empty_df([])
    return pd.concat(frames, ignore_index=True)


def _build_technical_filter_summary_df(technical_filter_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "score_method",
        "score_method_label",
        "liquidity_scenario",
        "liquidity_scenario_label",
        "selection_fraction",
        "technical_filter",
        "technical_filter_label",
        "event_count",
        "baseline_event_count",
        "kept_event_pct",
        "technical_coverage_pct",
        "year_count",
        "median_annual_names",
        "mean_return_pct",
        "median_return_pct",
        "win_rate_pct",
        "p10_return_pct",
        "worst_return_pct",
        "annual_mean_return_pct",
        "annual_return_std_pct",
        "year_t_stat",
        "positive_year_rate_pct",
        "min_year_return_pct",
        "max_year_return_pct",
        "mean_composite_score",
        "mean_stock_price_to_sma250",
        "mean_topix_price_to_sma250",
    ]
    if technical_filter_event_df.empty:
        return _empty_df(columns)
    group_columns = ["market_scope", "score_method", "liquidity_scenario", "selection_fraction"]
    baseline_counts = (
        technical_filter_event_df[
            technical_filter_event_df["technical_filter"].astype(str) == "baseline"
        ]
        .groupby(group_columns, observed=True, sort=False)
        .size()
        .to_dict()
    )
    records: list[dict[str, Any]] = []
    for keys, group in technical_filter_event_df.groupby(
        [*group_columns, "technical_filter"],
        observed=True,
        sort=False,
    ):
        market_scope, score_method, scenario_name, selection_fraction, technical_filter = keys
        returns = pd.to_numeric(group["event_return_winsor_pct"], errors="coerce").dropna()
        annual_counts = group.groupby("year", sort=True).size()
        stats = _annual_selection_stats(group)
        base_count = int(baseline_counts.get((market_scope, score_method, scenario_name, selection_fraction), 0))
        feature_count = int(pd.to_numeric(group["stock_price_to_sma250"], errors="coerce").notna().sum())
        records.append(
            {
                "market_scope": str(market_scope),
                "score_method": str(score_method),
                "score_method_label": str(group["score_method_label"].iloc[0]),
                "liquidity_scenario": str(scenario_name),
                "liquidity_scenario_label": str(group["liquidity_scenario_label"].iloc[0]),
                "selection_fraction": float(cast(float, selection_fraction)),
                "technical_filter": str(technical_filter),
                "technical_filter_label": _FILTER_LABELS.get(str(technical_filter), str(technical_filter)),
                "event_count": int(len(group)),
                "baseline_event_count": base_count,
                "kept_event_pct": float(len(group) / base_count * 100.0) if base_count else None,
                "technical_coverage_pct": float(feature_count / len(group) * 100.0) if len(group) else None,
                "year_count": stats["year_count"],
                "median_annual_names": float(annual_counts.median()) if not annual_counts.empty else None,
                "mean_return_pct": float(returns.mean()) if not returns.empty else None,
                "median_return_pct": float(returns.median()) if not returns.empty else None,
                "win_rate_pct": float((returns > 0.0).mean() * 100.0) if not returns.empty else None,
                "p10_return_pct": float(returns.quantile(0.10)) if not returns.empty else None,
                "worst_return_pct": float(returns.min()) if not returns.empty else None,
                **stats,
                "mean_composite_score": _series_mean(group["composite_score"]),
                "mean_stock_price_to_sma250": _series_mean(group["stock_price_to_sma250"]),
                "mean_topix_price_to_sma250": _series_mean(group["topix_price_to_sma250"]),
            }
        )
    result = pd.DataFrame(records)
    return _market_scope_sort(
        result[columns],
        ["score_method", "liquidity_scenario", "selection_fraction", "technical_filter"],
    )


def _build_technical_portfolio_tables(
    technical_filter_event_df: pd.DataFrame,
    *,
    stock_price_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    if technical_filter_event_df.empty:
        return _empty_df([]), _empty_df([])
    portfolio_event_df = _portfolio_focus_event_df(technical_filter_event_df)
    for filter_name, filter_group in portfolio_event_df.groupby("technical_filter", sort=False):
        daily = _build_portfolio_daily_df(filter_group, stock_price_df)
        if daily.empty:
            continue
        daily["technical_filter"] = str(filter_name)
        daily["technical_filter_label"] = _FILTER_LABELS.get(str(filter_name), str(filter_name))
        summary = _build_portfolio_summary_df(daily.drop(columns=["technical_filter", "technical_filter_label"]), filter_group)
        summary["technical_filter"] = str(filter_name)
        summary["technical_filter_label"] = _FILTER_LABELS.get(str(filter_name), str(filter_name))
        daily_frames.append(daily)
        summary_frames.append(summary)
    daily_df = pd.concat(daily_frames, ignore_index=True) if daily_frames else _empty_df([])
    summary_df = pd.concat(summary_frames, ignore_index=True) if summary_frames else _empty_df([])
    return daily_df, summary_df


def _portfolio_focus_event_df(technical_filter_event_df: pd.DataFrame) -> pd.DataFrame:
    focus = _standard_top10_focus_event_df(technical_filter_event_df)
    return focus if not focus.empty else technical_filter_event_df


def _standard_top10_focus_event_df(frame: pd.DataFrame) -> pd.DataFrame:
    focus = frame[
        (frame["market_scope"].astype(str).isin(_PORTFOLIO_FOCUS_MARKET_SCOPES))
        & (frame["score_method"].astype(str).isin(_PORTFOLIO_FOCUS_SCORE_METHODS))
        & (
            frame["liquidity_scenario"].astype(str).isin(_PORTFOLIO_FOCUS_LIQUIDITY_SCENARIOS)
        )
        & (
            pd.to_numeric(frame["selection_fraction"], errors="coerce")
            .round(6)
            .isin([round(value, 6) for value in _PORTFOLIO_FOCUS_SELECTION_FRACTIONS])
        )
    ].copy()
    return focus


def _float_or_nan(value: Any) -> float:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return float("nan")
    return number if math.isfinite(number) else float("nan")


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


def _build_summary_markdown(result: AnnualValueCompositeTechnicalFilterResult) -> str:
    lines = [
        "# Annual Value Composite Technical Filter",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Input run id: `{result.input_run_id}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Selected event rows: `{result.selected_event_count}`",
        f"- Rows with stock SMA features: `{result.technical_feature_count}`",
        f"- Technical policy: {result.technical_policy}.",
        "",
        "## Focus Rows",
        "",
    ]
    summary = result.portfolio_summary_df.copy()
    if summary.empty:
        lines.append("- No portfolio summary rows were produced.")
        return "\n".join(lines)
    focus = summary[
        (summary["market_scope"].astype(str) == "standard")
        & (
            summary["score_method"]
            .astype(str)
            .isin(["walkforward_regression_weight", "equal_weight"])
        )
        & (summary["liquidity_scenario"].astype(str) == "none")
        & (summary["selection_fraction"].astype(float).isin([0.10]))
    ].copy()
    if focus.empty:
        focus = summary.head(16)
    focus = focus.sort_values("sharpe_ratio", ascending=False, na_position="last").head(16)
    for row in focus.to_dict(orient="records"):
        lines.append(
            "- "
            f"`{row['market_scope']}` / `{row['score_method']}` / "
            f"`{row['technical_filter']}` / top `{float(row['selection_fraction']) * 100:.0f}%`: "
            f"CAGR `{_fmt(row['cagr_pct'])}%`, "
            f"Sharpe `{_fmt(row['sharpe_ratio'])}`, "
            f"maxDD `{_fmt(row['max_drawdown_pct'])}%`, "
            f"events `{int(cast(int, row['realized_event_count']))}`"
        )
    return "\n".join(lines)


def write_annual_value_composite_technical_filter_bundle(
    result: AnnualValueCompositeTechnicalFilterResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_VALUE_COMPOSITE_TECHNICAL_FILTER_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_value_composite_technical_filter",
        params={
            "input_bundle_path": result.input_bundle_path,
            "sma_window": result.sma_window,
            "slope_window": result.slope_window,
            "near_sma_threshold": result.near_sma_threshold,
            "focus_standard_top10_no_liquidity": (
                "input rows are limited to the standard/no-liquidity/top-10% focus rows"
                in result.technical_policy
            ),
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_value_composite_technical_filter_bundle(
    bundle_path: str | Path,
) -> AnnualValueCompositeTechnicalFilterResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualValueCompositeTechnicalFilterResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_value_composite_technical_filter_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_VALUE_COMPOSITE_TECHNICAL_FILTER_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_value_composite_technical_filter_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_VALUE_COMPOSITE_TECHNICAL_FILTER_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


__all__: Sequence[str] = (
    "ANNUAL_VALUE_COMPOSITE_TECHNICAL_FILTER_EXPERIMENT_ID",
    "AnnualValueCompositeTechnicalFilterResult",
    "TECHNICAL_FILTERS",
    "get_annual_value_composite_technical_filter_bundle_path_for_run_id",
    "get_annual_value_composite_technical_filter_latest_bundle_path",
    "load_annual_value_composite_technical_filter_bundle",
    "run_annual_value_composite_technical_filter",
    "write_annual_value_composite_technical_filter_bundle",
)
