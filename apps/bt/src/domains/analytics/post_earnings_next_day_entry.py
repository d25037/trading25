"""Post-earnings next-session entry research."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

from src.domains.analytics.earnings_holdthrough_expectancy import (
    _add_price_features,
    _append_liquidity_features,
    _assert_required_tables,
    _base_event_record,
    _bucket_pre_return,
    _classify_overheat_state,
    _coverage_rate_pct,
    _event_strength_rate_pct,
    _expand_market_scope,
    _fill_missing_feature_values,
    _float_or_nan,
    _infer_pre_windows,
    _load_adjustment_events_by_code,
    _offset_calendar_date,
    _query_price_rows,
    _query_statement_rows,
    _query_topix_rows,
    _return_pct,
    _return_summary,
    _sort_summary_df,
    _str_or_none,
    _summary_columns,
    enrich_event_features_with_prime_liquidity_residuals,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.jpx_daily_price_limits import (
    JPX_DAILY_PRICE_LIMITS_DEFINITION_NOTE,
    JPX_DAILY_PRICE_LIMITS_REFERENCE_LABEL,
    resolve_standard_daily_limit_width,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

POST_EARNINGS_NEXT_DAY_ENTRY_EXPERIMENT_ID = (
    "market-behavior/post-earnings-next-day-entry"
)
DEFAULT_PRE_WINDOWS: tuple[int, ...] = (20, 60)
DEFAULT_HORIZONS: tuple[int, ...] = (1, 5, 20)
DEFAULT_LIQUIDITY_WINDOW = 60
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
_GAP_EXTREME_THRESHOLD_PCT = 10.0
_PRICE_EPSILON = 1e-6
_LIMIT_TOLERANCE = 0.995


@dataclass(frozen=True)
class PostEarningsNextDayEntryResult:
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
    jpx_reference_label: str
    jpx_definition_note: str
    event_feature_df: pd.DataFrame
    execution_diagnostics_df: pd.DataFrame
    post_entry_expectancy_df: pd.DataFrame
    attempted_entry_outcome_df: pd.DataFrame
    limit_no_fill_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame


def run_post_earnings_next_day_entry_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    pre_windows: Iterable[int] = DEFAULT_PRE_WINDOWS,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    liquidity_window: int = DEFAULT_LIQUIDITY_WINDOW,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
) -> PostEarningsNextDayEntryResult:
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
        snapshot_prefix="post-earnings-next-day-entry-",
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
    execution_diagnostics_df = _build_execution_diagnostics_df(scoped_event_df)
    post_entry_expectancy_df = _build_post_entry_expectancy_df(
        scoped_event_df,
        horizons=resolved_horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    attempted_entry_outcome_df = _build_attempted_entry_outcome_df(
        scoped_event_df,
        horizons=resolved_horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    limit_no_fill_df = _build_limit_no_fill_df(scoped_event_df)
    coverage_diagnostics_df = _build_coverage_diagnostics_df(scoped_event_df)

    return PostEarningsNextDayEntryResult(
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
        jpx_reference_label=JPX_DAILY_PRICE_LIMITS_REFERENCE_LABEL,
        jpx_definition_note=JPX_DAILY_PRICE_LIMITS_DEFINITION_NOTE,
        event_feature_df=event_feature_df,
        execution_diagnostics_df=execution_diagnostics_df,
        post_entry_expectancy_df=post_entry_expectancy_df,
        attempted_entry_outcome_df=attempted_entry_outcome_df,
        limit_no_fill_df=limit_no_fill_df,
        coverage_diagnostics_df=coverage_diagnostics_df,
    )


def write_post_earnings_next_day_entry_bundle(
    result: PostEarningsNextDayEntryResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=POST_EARNINGS_NEXT_DAY_ENTRY_EXPERIMENT_ID,
        module=__name__,
        function="run_post_earnings_next_day_entry_research",
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
            "jpx_reference_label": result.jpx_reference_label,
            "event_count": int(len(result.event_feature_df)),
            "code_count": int(result.event_feature_df["code"].nunique())
            if "code" in result.event_feature_df
            else 0,
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "execution_diagnostics_df": result.execution_diagnostics_df,
            "post_entry_expectancy_df": result.post_entry_expectancy_df,
            "attempted_entry_outcome_df": result.attempted_entry_outcome_df,
            "limit_no_fill_df": result.limit_no_fill_df,
            "event_feature_df": result.event_feature_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: PostEarningsNextDayEntryResult) -> str:
    diagnostics = _top_rows_for_markdown(result.coverage_diagnostics_df, limit=16)
    execution = _top_rows_for_markdown(
        result.execution_diagnostics_df,
        sort_columns=["market_scope", "is_fy", "event_strength", "execution_label"],
        limit=30,
    )
    expectancy = _top_rows_for_markdown(
        result.post_entry_expectancy_df,
        sort_columns=["market_scope", "horizon", "is_fy", "event_strength"],
        limit=30,
    )
    attempted = _top_rows_for_markdown(
        result.attempted_entry_outcome_df,
        sort_columns=["market_scope", "horizon", "is_fy", "event_strength"],
        limit=30,
    )
    no_fill = _top_rows_for_markdown(
        result.limit_no_fill_df,
        sort_columns=["market_scope", "execution_label", "event_strength"],
        limit=30,
    )
    return "\n".join(
        [
            "# Post-Earnings Next-Day Entry",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Pre windows: `{list(result.pre_windows)}`",
            f"- Forward horizons: `{list(result.horizons)}`",
            f"- Liquidity window: `{result.liquidity_window}`",
            f"- Stop-limit reference: `{result.jpx_reference_label}`",
            "",
            "## Coverage Diagnostics",
            "",
            diagnostics,
            "",
            "## Execution Diagnostics",
            "",
            execution,
            "",
            "## Post-Entry Expectancy",
            "",
            expectancy,
            "",
            "## Attempted Entry Outcome",
            "",
            attempted,
            "",
            "## Limit No-Fill",
            "",
            no_fill,
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


def _build_event_feature_df(
    statement_df: pd.DataFrame,
    price_df: pd.DataFrame,
    topix_df: pd.DataFrame,
    adjustment_events_by_code: dict[str, list[Any]],
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
            record = _base_event_record(row)
            record["status"] = "missing_price_history"
            _fill_missing_feature_values(record, pre_windows, horizons)
            _fill_missing_entry_values(record, horizons)
            records.append(record)
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
    for column in ("is_fy", "has_next_guidance", "entry_executable"):
        if column in event_df.columns:
            event_df[column] = event_df[column].astype(object)
    return event_df[columns].sort_values(["disclosed_date", "code"], kind="stable").reset_index(
        drop=True
    )


def _build_single_event_record(
    row: Any,
    code_prices: pd.DataFrame,
    topix_panel: pd.DataFrame,
    adjustment_events: Sequence[Any],
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
        _fill_missing_entry_values(record, horizons)
        return record

    pre_row = code_prices.iloc[pre_idx]
    entry_row = code_prices.iloc[entry_idx]
    record["status"] = "attempted"
    record["pre_event_date"] = str(pre_row["date"])
    record["entry_date"] = str(entry_row["date"])
    record["pre_event_close"] = _float_or_nan(pre_row["close"])
    record["entry_reference_close"] = record["pre_event_close"]
    record["entry_open"] = _float_or_nan(entry_row["open"])
    record["entry_high"] = _float_or_nan(entry_row["high"])
    record["entry_low"] = _float_or_nan(entry_row["low"])
    record["entry_close"] = _float_or_nan(entry_row["close"])
    record["entry_gap_pct"] = _return_pct(record["entry_open"], record["entry_reference_close"])
    record["execution_label"] = _classify_entry_execution(
        record["entry_reference_close"],
        record["entry_open"],
        record["entry_high"],
        record["entry_low"],
        record["entry_close"],
    )
    record["entry_executable"] = record["execution_label"] in {
        "executable_open",
        "gap_extreme_executable",
    }
    record["entry_price"] = record["entry_open"] if record["entry_executable"] else np.nan

    for window in pre_windows:
        price_lag_idx = pre_idx - window
        if price_lag_idx >= 0:
            lag_row = code_prices.iloc[price_lag_idx]
            price_lag_close = _float_or_nan(lag_row["close"])
            pre_return_pct = _return_pct(record["pre_event_close"], price_lag_close)
            topix_return_pct = _topix_close_return_pct(
                topix_panel,
                start_date=str(lag_row["date"]),
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

    record["overheat_state"] = _classify_overheat_state(
        record.get("pre_return_20d_pct")
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
        exit_date = str(code_prices.iloc[exit_idx]["date"])
        forward_return_pct = _return_pct(exit_close, record["entry_price"])
        topix_return_pct = _topix_close_return_pct(
            topix_panel,
            start_date=record["entry_date"],
            end_date=exit_date,
        )
        record[f"forward_return_{horizon}d_pct"] = forward_return_pct
        record[f"forward_topix_return_{horizon}d_pct"] = topix_return_pct
        record[f"forward_excess_return_{horizon}d_pct"] = (
            forward_return_pct - topix_return_pct
            if math.isfinite(forward_return_pct) and math.isfinite(topix_return_pct)
            else np.nan
        )
    return record


def _classify_entry_execution(
    pre_close: float,
    entry_open: float,
    entry_high: float,
    entry_low: float,
    entry_close: float,
) -> str:
    values = [pre_close, entry_open, entry_high, entry_low, entry_close]
    if any(not math.isfinite(_float_or_nan(value)) for value in values):
        return "missing_entry_session"
    limit_width = resolve_standard_daily_limit_width(pre_close)
    one_price = (
        abs(entry_open - entry_high) <= _PRICE_EPSILON
        and abs(entry_high - entry_low) <= _PRICE_EPSILON
        and abs(entry_low - entry_close) <= _PRICE_EPSILON
    )
    if one_price and limit_width is not None:
        if entry_close >= pre_close + limit_width * _LIMIT_TOLERANCE:
            return "limit_up_no_fill"
        if entry_close <= pre_close - limit_width * _LIMIT_TOLERANCE:
            return "limit_down_no_fill"
    gap_pct = abs(_return_pct(entry_open, pre_close))
    if math.isfinite(gap_pct) and gap_pct >= _GAP_EXTREME_THRESHOLD_PCT:
        return "gap_extreme_executable"
    return "executable_open"


def _build_execution_diagnostics_df(scoped_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        group_columns = [
            "market_scope",
            "is_fy",
            "event_strength",
            "overheat_state",
            "execution_label",
        ]
        for keys, frame in scoped_df.groupby(group_columns, sort=False, dropna=False):
            key_values = dict(zip(group_columns, keys, strict=True))
            rows.append(
                {
                    **key_values,
                    "event_count": int(len(frame)),
                    "code_count": int(frame["code"].nunique()),
                    "entry_executable_count": int((frame["entry_executable"] == True).sum()),  # noqa: E712
                    "limit_up_no_fill_count": int(
                        (frame["execution_label"] == "limit_up_no_fill").sum()
                    ),
                    "limit_down_no_fill_count": int(
                        (frame["execution_label"] == "limit_down_no_fill").sum()
                    ),
                }
            )
    columns = [
        "market_scope",
        "is_fy",
        "event_strength",
        "overheat_state",
        "execution_label",
        "event_count",
        "code_count",
        "entry_executable_count",
        "limit_up_no_fill_count",
        "limit_down_no_fill_count",
    ]
    return _sort_summary_df(pd.DataFrame(rows), columns=columns)


def _build_post_entry_expectancy_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    executable = scoped_df[scoped_df["entry_executable"] == True].copy() if not scoped_df.empty else scoped_df  # noqa: E712
    return _build_entry_grouped_return_df(
        executable,
        horizons=horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        execution_scope="executable",
        include_no_fill_rates=False,
    )


def _build_attempted_entry_outcome_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return _build_entry_grouped_return_df(
        scoped_df,
        horizons=horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        execution_scope="attempted",
        include_no_fill_rates=True,
    )


def _build_entry_grouped_return_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
    execution_scope: str,
    include_no_fill_rates: bool,
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
            "event_strength",
            *window_columns,
            "overheat_state",
            "adv60_to_free_float_bucket",
        ]
        for horizon in horizons:
            return_col = f"forward_excess_return_{horizon}d_pct"
            for keys, frame in scoped_df.groupby(group_columns, sort=False, dropna=False):
                key_values = dict(zip(group_columns, keys, strict=True))
                row = {
                    **key_values,
                    "execution_scope": execution_scope,
                    "horizon": horizon,
                    **_return_summary(frame, return_col, severe_loss_threshold_pct),
                    "positive_event_rate_pct": _event_strength_rate_pct(frame, "positive"),
                    "negative_event_rate_pct": _event_strength_rate_pct(frame, "negative"),
                }
                if include_no_fill_rates:
                    row.update(_no_fill_rates(frame))
                rows.append(row)
    columns = [
        "market_scope",
        "is_fy",
        "event_strength",
        *[f"pre_return_{window}d_bucket" for window in _infer_pre_windows(scoped_df)],
        "overheat_state",
        "adv60_to_free_float_bucket",
        "execution_scope",
        "horizon",
        *_summary_columns(),
        "positive_event_rate_pct",
        "negative_event_rate_pct",
    ]
    if include_no_fill_rates:
        columns.extend(
            [
                "entry_executable_rate_pct",
                "limit_up_no_fill_rate_pct",
                "limit_down_no_fill_rate_pct",
                "gap_extreme_executable_rate_pct",
            ]
        )
    return _sort_summary_df(pd.DataFrame(rows), columns=columns)


def _build_limit_no_fill_df(scoped_df: pd.DataFrame) -> pd.DataFrame:
    if scoped_df.empty:
        return pd.DataFrame(columns=_event_feature_columns(DEFAULT_PRE_WINDOWS, DEFAULT_HORIZONS))
    frame = scoped_df[
        scoped_df["execution_label"].isin(["limit_up_no_fill", "limit_down_no_fill"])
    ].copy()
    return frame.reset_index(drop=True)


def _build_coverage_diagnostics_df(scoped_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        for market_scope, frame in scoped_df.groupby("market_scope", sort=False):
            rows.append(
                {
                    "market_scope": market_scope,
                    "event_count": int(len(frame)),
                    "attempted_count": int((frame["status"] == "attempted").sum()),
                    "entry_executable_count": int((frame["entry_executable"] == True).sum()),  # noqa: E712
                    "code_count": int(frame["code"].nunique()),
                    "fy_event_count": int((frame["is_fy"] == True).sum()),  # noqa: E712
                    "positive_event_count": int((frame["event_strength"] == "positive").sum()),
                    "negative_event_count": int((frame["event_strength"] == "negative").sum()),
                    "limit_up_no_fill_count": int(
                        (frame["execution_label"] == "limit_up_no_fill").sum()
                    ),
                    "limit_down_no_fill_count": int(
                        (frame["execution_label"] == "limit_down_no_fill").sum()
                    ),
                    "med_adv60_coverage_pct": _coverage_rate_pct(frame["med_adv60_mil_jpy"]),
                }
            )
    columns = [
        "market_scope",
        "event_count",
        "attempted_count",
        "entry_executable_count",
        "code_count",
        "fy_event_count",
        "positive_event_count",
        "negative_event_count",
        "limit_up_no_fill_count",
        "limit_down_no_fill_count",
        "med_adv60_coverage_pct",
    ]
    return _sort_summary_df(pd.DataFrame(rows), columns=columns)


def _no_fill_rates(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {
            "entry_executable_rate_pct": np.nan,
            "limit_up_no_fill_rate_pct": np.nan,
            "limit_down_no_fill_rate_pct": np.nan,
            "gap_extreme_executable_rate_pct": np.nan,
        }
    return {
        "entry_executable_rate_pct": float((frame["entry_executable"] == True).mean() * 100.0),  # noqa: E712
        "limit_up_no_fill_rate_pct": float(
            (frame["execution_label"] == "limit_up_no_fill").mean() * 100.0
        ),
        "limit_down_no_fill_rate_pct": float(
            (frame["execution_label"] == "limit_down_no_fill").mean() * 100.0
        ),
        "gap_extreme_executable_rate_pct": float(
            (frame["execution_label"] == "gap_extreme_executable").mean() * 100.0
        ),
    }


def _topix_close_return_pct(topix_panel: pd.DataFrame, *, start_date: str, end_date: str) -> float:
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


def _fill_missing_entry_values(record: dict[str, Any], horizons: Sequence[int]) -> None:
    record["entry_reference_close"] = np.nan
    record["entry_open"] = np.nan
    record["entry_high"] = np.nan
    record["entry_low"] = np.nan
    record["entry_close"] = np.nan
    record["entry_gap_pct"] = np.nan
    record["entry_price"] = np.nan
    record["entry_executable"] = False
    record["execution_label"] = "missing_entry_session"
    for horizon in horizons:
        record[f"forward_return_{horizon}d_pct"] = np.nan
        record[f"forward_topix_return_{horizon}d_pct"] = np.nan
        record[f"forward_excess_return_{horizon}d_pct"] = np.nan


def _event_feature_columns(pre_windows: Sequence[int], horizons: Sequence[int]) -> list[str]:
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
        "entry_reference_close",
        "entry_open",
        "entry_high",
        "entry_low",
        "entry_close",
        "entry_gap_pct",
        "entry_price",
        "entry_executable",
        "execution_label",
        "signed_pre_move",
        "med_adv60_mil_jpy",
        "med_adv60_source_sessions",
        "free_float_market_cap_mil_jpy",
        "adv60_to_free_float_pct",
        "adv60_to_free_float_bucket",
        "liquidity_residual_z",
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
