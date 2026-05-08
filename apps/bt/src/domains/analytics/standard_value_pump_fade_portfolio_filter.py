"""Daily portfolio lens for Standard value pump/fade filters."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from src.domains.analytics.annual_value_breakout_periodic_rebalance import (
    _BREAKOUT_GROUP_COLUMNS,
)
from src.domains.analytics.annual_value_composite_selection import (
    _daily_stats,
    _series_mean,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_dataclass_research_bundle,
)
from src.domains.analytics.standard_value_pump_fade_decomposition import (
    run_standard_value_pump_fade_decomposition_from_frames,
)

STANDARD_VALUE_PUMP_FADE_PORTFOLIO_FILTER_EXPERIMENT_ID = (
    "market-behavior/standard-value-pump-fade-portfolio-filter"
)
DEFAULT_FILTER_POLICIES: tuple[str, ...] = (
    "base",
    "exclude_risk_ge3",
    "exclude_large_month_high_fade",
    "exclude_deep_drawdown_after_large_month",
    "exclude_large_month_high_fade_or_deep_drawdown",
)
_FILTER_GROUP_COLUMNS: tuple[str, ...] = (
    *_BREAKOUT_GROUP_COLUMNS,
    "pump_fade_policy",
    "refill_mode",
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "diagnostic_event_df",
    "filtered_selected_event_df",
    "filter_coverage_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)


@dataclass(frozen=True)
class StandardValuePumpFadePortfolioFilterResult:
    db_path: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    base_selection_count: int
    refill_pool_selection_count: int
    filter_policies: tuple[str, ...]
    diagnostic_event_df: pd.DataFrame
    filtered_selected_event_df: pd.DataFrame
    filter_coverage_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


def run_standard_value_pump_fade_portfolio_filter_from_frames(
    *,
    db_path: str,
    selected_event_df: pd.DataFrame,
    price_history_df: pd.DataFrame,
    base_selection_count: int = 10,
    refill_pool_selection_count: int = 100,
    filter_policies: tuple[str, ...] = DEFAULT_FILTER_POLICIES,
) -> StandardValuePumpFadePortfolioFilterResult:
    """Build daily portfolio curves after pump/fade hard filters."""

    if selected_event_df.empty:
        empty = _empty_df([])
        return StandardValuePumpFadePortfolioFilterResult(
            db_path=str(db_path),
            analysis_start_date=None,
            analysis_end_date=None,
            base_selection_count=int(base_selection_count),
            refill_pool_selection_count=int(refill_pool_selection_count),
            filter_policies=tuple(filter_policies),
            diagnostic_event_df=empty.copy(),
            filtered_selected_event_df=empty.copy(),
            filter_coverage_df=empty.copy(),
            portfolio_daily_df=empty.copy(),
            portfolio_summary_df=empty.copy(),
        )

    diagnostic_event_df = _build_diagnostic_event_df(
        selected_event_df,
        price_history_df,
        db_path=str(db_path),
        refill_pool_selection_count=int(refill_pool_selection_count),
    )
    annotated_df = _attach_diagnostics(selected_event_df, diagnostic_event_df)
    filtered_selected_event_df, filter_coverage_df = _build_filtered_selected_events(
        annotated_df,
        base_selection_count=int(base_selection_count),
        refill_pool_selection_count=int(refill_pool_selection_count),
        filter_policies=tuple(filter_policies),
    )
    portfolio_daily_df = _build_filter_portfolio_daily_df(
        filtered_selected_event_df,
        price_history_df,
    )
    portfolio_summary_df = _build_filter_portfolio_summary_df(
        portfolio_daily_df,
        filtered_selected_event_df,
    )
    return StandardValuePumpFadePortfolioFilterResult(
        db_path=str(db_path),
        analysis_start_date=_min_str(portfolio_daily_df.get("date")),
        analysis_end_date=_max_str(portfolio_daily_df.get("date")),
        base_selection_count=int(base_selection_count),
        refill_pool_selection_count=int(refill_pool_selection_count),
        filter_policies=tuple(filter_policies),
        diagnostic_event_df=diagnostic_event_df,
        filtered_selected_event_df=filtered_selected_event_df,
        filter_coverage_df=filter_coverage_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
    )


def write_standard_value_pump_fade_portfolio_filter_bundle(
    result: StandardValuePumpFadePortfolioFilterResult,
    *,
    output_root: str | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=STANDARD_VALUE_PUMP_FADE_PORTFOLIO_FILTER_EXPERIMENT_ID,
        module=__name__,
        function="run_standard_value_pump_fade_portfolio_filter_from_frames",
        params={
            "base_selection_count": result.base_selection_count,
            "refill_pool_selection_count": result.refill_pool_selection_count,
            "filter_policies": list(result.filter_policies),
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def _build_diagnostic_event_df(
    selected_event_df: pd.DataFrame,
    price_history_df: pd.DataFrame,
    *,
    db_path: str,
    refill_pool_selection_count: int,
) -> pd.DataFrame:
    pool = selected_event_df[
        pd.to_numeric(selected_event_df["selection_count"], errors="coerce")
        == int(refill_pool_selection_count)
    ].copy()
    if pool.empty:
        pool = selected_event_df.copy()
    snapshot_df = pd.DataFrame(
        {
            "snapshot_date": pool["signal_date"].astype(str),
            "rank": pd.to_numeric(pool["selection_rank"], errors="coerce"),
            "code": pool["code"].astype(str),
            "company_name": pool["company_name"].astype(str),
            "market_code": pool["market_code"].astype(str),
            "score": pd.to_numeric(pool["composite_score"], errors="coerce"),
            "score_before_boost": pd.to_numeric(
                pool["value_composite_score"],
                errors="coerce",
            ),
            "breakout_boost": pd.to_numeric(pool["composite_score"], errors="coerce")
            - pd.to_numeric(pool["value_composite_score"], errors="coerce"),
            "pbr": pd.to_numeric(pool["pbr"], errors="coerce"),
            "forward_per": pd.to_numeric(pool["forward_per"], errors="coerce"),
            "market_cap_bil_jpy": pd.to_numeric(
                pool["market_cap_bil_jpy"],
                errors="coerce",
            ),
            "avg_trading_value_60d_mil_jpy": pd.to_numeric(
                pool["avg_trading_value_60d_mil_jpy"],
                errors="coerce",
            ),
        }
    )
    snapshot_df = snapshot_df.dropna(subset=["snapshot_date", "rank", "code"])
    return run_standard_value_pump_fade_decomposition_from_frames(
        db_path=db_path,
        ranking_snapshot_df=snapshot_df,
        price_history_df=price_history_df,
        top_ranks=(int(refill_pool_selection_count),),
    ).candidate_event_df


def _attach_diagnostics(
    selected_event_df: pd.DataFrame,
    diagnostic_event_df: pd.DataFrame,
) -> pd.DataFrame:
    if selected_event_df.empty or diagnostic_event_df.empty:
        return selected_event_df.copy()
    diagnostic_columns = [
        "snapshot_date",
        "code",
        "volatility_60d_pct",
        "drawdown_from_2y_high_pct",
        "max_month",
        "max_month_body_pct",
        "max_month_range_pct",
        "max_month_high",
        "max_month_close",
        "current_vs_max_month_close_pct",
        "current_vs_max_month_high_pct",
        "large_month_candle",
        "faded_after_large_month",
        "faded_after_large_month_high",
        "deep_drawdown_after_large_month",
        "speculative_risk_score",
        "speculative_risk_bucket",
        "pattern_bucket",
    ]
    diagnostics = (
        diagnostic_event_df[diagnostic_columns]
        .sort_values(["snapshot_date", "code"], kind="stable")
        .drop_duplicates(["snapshot_date", "code"], keep="first")
    )
    annotated = selected_event_df.merge(
        diagnostics,
        left_on=["signal_date", "code"],
        right_on=["snapshot_date", "code"],
        how="left",
    ).drop(columns=["snapshot_date"], errors="ignore")
    return annotated.reset_index(drop=True)


def _build_filtered_selected_events(
    annotated_df: pd.DataFrame,
    *,
    base_selection_count: int,
    refill_pool_selection_count: int,
    filter_policies: tuple[str, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if annotated_df.empty:
        return _empty_df([]), _empty_df([])
    base_columns = list(annotated_df.columns)
    output_columns = [*base_columns, "pump_fade_policy", "refill_mode"]
    frames: list[pd.DataFrame] = []
    coverage_rows: list[dict[str, Any]] = []
    group_columns_without_count = [
        column for column in _BREAKOUT_GROUP_COLUMNS if column != "selection_count"
    ]
    for keys, group in annotated_df.groupby(
        group_columns_without_count,
        observed=True,
        sort=False,
    ):
        key_dict = dict(zip(group_columns_without_count, keys, strict=True))
        base = group[
            pd.to_numeric(group["selection_count"], errors="coerce")
            == int(base_selection_count)
        ].sort_values("selection_rank", kind="stable")
        pool = group[
            pd.to_numeric(group["selection_count"], errors="coerce")
            == int(refill_pool_selection_count)
        ].sort_values("selection_rank", kind="stable")
        if pool.empty:
            pool = base.copy()
        for policy in filter_policies:
            if policy == "base":
                selected = base.copy()
                mode = "base"
                frames.append(_tag_variant(selected, policy=policy, refill_mode=mode))
                coverage_rows.append(
                    _coverage_row(
                        key_dict,
                        policy=policy,
                        refill_mode=mode,
                        base=base,
                        selected=selected,
                    )
                )
                continue
            top_filtered = base[_policy_mask(base, policy)].copy()
            frames.append(_tag_variant(top_filtered, policy=policy, refill_mode="drop_only"))
            coverage_rows.append(
                _coverage_row(
                    key_dict,
                    policy=policy,
                    refill_mode="drop_only",
                    base=base,
                    selected=top_filtered,
                )
            )
            refill_filtered = _refill_to_top_n(
                pool,
                policy=policy,
                base_selection_count=base_selection_count,
            )
            frames.append(
                _tag_variant(refill_filtered, policy=policy, refill_mode="refill_to_top_n")
            )
            coverage_rows.append(
                _coverage_row(
                    key_dict,
                    policy=policy,
                    refill_mode="refill_to_top_n",
                    base=base,
                    selected=refill_filtered,
                )
            )
    if not frames:
        return _empty_df(output_columns), pd.DataFrame(coverage_rows)
    result = pd.concat(frames, ignore_index=True, sort=False)
    for column in output_columns:
        if column not in result.columns:
            result[column] = None
    return result[output_columns], pd.DataFrame(coverage_rows)


def _refill_to_top_n(
    pool: pd.DataFrame,
    *,
    policy: str,
    base_selection_count: int,
) -> pd.DataFrame:
    if pool.empty:
        return pool.copy()
    period_columns = [
        column
        for column in ("year", "rebalance_period", "entry_date", "exit_date")
        if column in pool.columns
    ]
    frames: list[pd.DataFrame] = []
    for _, period_df in pool.groupby(period_columns, observed=True, sort=False):
        selected = (
            period_df[_policy_mask(period_df, policy)]
            .sort_values("selection_rank", kind="stable")
            .head(int(base_selection_count))
            .copy()
        )
        if selected.empty:
            continue
        selected["selection_count"] = int(base_selection_count)
        selected["selection_rank"] = np.arange(len(selected), dtype=int) + 1
        frames.append(selected)
    if not frames:
        return pool.head(0).copy()
    return pd.concat(frames, ignore_index=True, sort=False)


def _policy_mask(frame: pd.DataFrame, policy: str) -> pd.Series:
    if policy == "exclude_risk_ge3":
        return pd.to_numeric(frame["speculative_risk_score"], errors="coerce").fillna(99) < 3
    if policy == "exclude_large_month_high_fade":
        return ~_bool_column(frame, "faded_after_large_month_high")
    if policy == "exclude_deep_drawdown_after_large_month":
        return ~_bool_column(frame, "deep_drawdown_after_large_month")
    if policy == "exclude_large_month_high_fade_or_deep_drawdown":
        return ~(
            _bool_column(frame, "faded_after_large_month_high")
            | _bool_column(frame, "deep_drawdown_after_large_month")
        )
    if policy == "exclude_month_fade_patterns":
        return ~(
            _bool_column(frame, "faded_after_large_month")
            | _bool_column(frame, "faded_after_large_month_high")
        )
    if policy == "exclude_deep_fade_after_large_month":
        return ~_bool_column(frame, "deep_drawdown_after_large_month")
    raise ValueError(f"unsupported pump/fade filter policy: {policy}")


def _bool_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].fillna(False).astype(bool)


def _tag_variant(frame: pd.DataFrame, *, policy: str, refill_mode: str) -> pd.DataFrame:
    result = frame.copy()
    result["pump_fade_policy"] = policy
    result["refill_mode"] = refill_mode
    return result


def _coverage_row(
    key_dict: dict[str, Any],
    *,
    policy: str,
    refill_mode: str,
    base: pd.DataFrame,
    selected: pd.DataFrame,
) -> dict[str, Any]:
    risk_source = (
        base["speculative_risk_score"]
        if "speculative_risk_score" in base.columns
        else pd.Series(dtype=float)
    )
    risk = pd.to_numeric(risk_source, errors="coerce")
    close_fade = _bool_column(base, "faded_after_large_month")
    high_fade = _bool_column(base, "faded_after_large_month_high")
    deep_drawdown = _bool_column(base, "deep_drawdown_after_large_month")
    return {
        **key_dict,
        "selection_count": int(base["selection_count"].iloc[0]) if not base.empty else None,
        "pump_fade_policy": policy,
        "refill_mode": refill_mode,
        "base_event_count": int(len(base)),
        "selected_event_count": int(len(selected)),
        "avg_selected_rank": _series_mean(selected.get("selection_rank", pd.Series(dtype=float))),
        "base_risk_ge3_count": int((risk >= 3).sum()) if not base.empty else 0,
        "base_month_close_fade_count": int(close_fade.sum()) if not base.empty else 0,
        "base_month_high_fade_count": int(high_fade.sum()) if not base.empty else 0,
        "base_deep_drawdown_after_large_month_count": int(deep_drawdown.sum()) if not base.empty else 0,
    }


def _build_filter_portfolio_daily_df(
    selected_event_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        *_FILTER_GROUP_COLUMNS,
        "date",
        "active_positions",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    if selected_event_df.empty or price_df.empty:
        return _empty_df(columns)
    price_by_code = {
        str(code): frame.sort_values("date", kind="stable").reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    aggregate: dict[tuple[Any, ...], list[float]] = defaultdict(lambda: [0.0, 0.0])
    for event in selected_event_df.to_dict(orient="records"):
        price_frame = price_by_code.get(str(event["code"]))
        if price_frame is None:
            continue
        path_df = price_frame[
            (price_frame["date"].astype(str) >= str(event["entry_date"]))
            & (price_frame["date"].astype(str) <= str(event["exit_date"]))
        ].copy()
        if path_df.empty:
            continue
        entry_open_value = _finite_float(event.get("entry_open"))
        if entry_open_value is None or entry_open_value <= 0:
            continue
        close_values = pd.to_numeric(path_df["close"], errors="coerce").astype(float).to_numpy()
        if not np.isfinite(close_values).all():
            continue
        previous_close = np.concatenate(([entry_open_value], close_values[:-1]))
        daily_returns = close_values / previous_close - 1.0
        group_key = tuple(event[column] for column in _FILTER_GROUP_COLUMNS)
        for date_value, daily_return in zip(path_df["date"].astype(str), daily_returns, strict=True):
            aggregate[(*group_key, str(date_value))][0] += float(daily_return)
            aggregate[(*group_key, str(date_value))][1] += 1.0
    if not aggregate:
        return _empty_df(columns)
    records = [
        {
            **dict(zip(_FILTER_GROUP_COLUMNS, key[:-1], strict=True)),
            "date": key[-1],
            "active_positions": int(values[1]),
            "mean_daily_return": float(values[0] / values[1]),
            "mean_daily_return_pct": float(values[0] / values[1] * 100.0),
        }
        for key, values in aggregate.items()
    ]
    daily_df = pd.DataFrame(records).sort_values(
        [*_FILTER_GROUP_COLUMNS, "date"],
        kind="stable",
    ).reset_index(drop=True)
    daily_df["portfolio_value"] = np.nan
    daily_df["drawdown_pct"] = np.nan
    for _, group in daily_df.groupby(list(_FILTER_GROUP_COLUMNS), observed=True, sort=False):
        idx = list(group.index)
        values = (1.0 + daily_df.loc[idx, "mean_daily_return"]).cumprod()
        peaks = values.cummax()
        daily_df.loc[idx, "portfolio_value"] = values.to_numpy()
        daily_df.loc[idx, "drawdown_pct"] = ((values / peaks - 1.0) * 100.0).to_numpy()
    return daily_df[columns]


def _build_filter_portfolio_summary_df(
    portfolio_daily_df: pd.DataFrame,
    selected_event_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        *_FILTER_GROUP_COLUMNS,
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
        selected_event_df.groupby(list(_FILTER_GROUP_COLUMNS), observed=True, sort=False)
        .size()
        .to_dict()
    )
    records: list[dict[str, Any]] = []
    for keys, group in portfolio_daily_df.groupby(list(_FILTER_GROUP_COLUMNS), observed=True, sort=False):
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
        records.append(
            {
                **dict(zip(_FILTER_GROUP_COLUMNS, keys, strict=True)),
                "realized_event_count": int(event_counts.get(tuple(keys), 0)),
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
    return pd.DataFrame(records).sort_values(
        [*_BREAKOUT_GROUP_COLUMNS, "pump_fade_policy", "refill_mode"],
        kind="stable",
    )


def _build_summary_markdown(result: StandardValuePumpFadePortfolioFilterResult) -> str:
    lines = [
        "# Standard Value Pump/Fade Portfolio Filter",
        "",
        f"- Analysis window: `{result.analysis_start_date}` -> `{result.analysis_end_date}`",
        f"- Base selection count: `{result.base_selection_count}`",
        f"- Refill pool selection count: `{result.refill_pool_selection_count}`",
        "",
        "## Portfolio Summary",
        "",
    ]
    summary = result.portfolio_summary_df.copy()
    if summary.empty:
        lines.append("- No portfolio rows.")
        return "\n".join(lines) + "\n"
    focus = summary.sort_values(["sharpe_ratio", "cagr_pct"], ascending=[False, False]).head(40)
    lines.extend(
        [
            "| Scope | Policy | Refill | Window | Lookback | Months | Top | CAGR | Sharpe | Sortino | MaxDD | Avg pos | Events |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in focus.to_dict(orient="records"):
        lines.append(
            "| "
            f"`{row.get('market_scope')}` | "
            f"`{row.get('pump_fade_policy')}` | "
            f"`{row.get('refill_mode')}` | "
            f"{_fmt_int(row.get('breakout_window'))} | "
            f"{_fmt_int(row.get('breakout_lookback_sessions'))} | "
            f"{_fmt_int(row.get('rebalance_months'))} | "
            f"{_fmt_int(row.get('selection_count'))} | "
            f"{_fmt(row.get('cagr_pct'))}% | "
            f"{_fmt(row.get('sharpe_ratio'))} | "
            f"{_fmt(row.get('sortino_ratio'))} | "
            f"{_fmt(row.get('max_drawdown_pct'))}% | "
            f"{_fmt(row.get('avg_active_positions'))} | "
            f"{_fmt_int(row.get('realized_event_count'))} |"
        )
    lines.append("")
    return "\n".join(lines)


def _empty_df(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _finite_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _min_str(series: Any) -> str | None:
    if series is None:
        return None
    values = pd.Series(series).dropna()
    return None if values.empty else str(values.min())


def _max_str(series: Any) -> str | None:
    if series is None:
        return None
    values = pd.Series(series).dropna()
    return None if values.empty else str(values.max())


def _fmt(value: object) -> str:
    number = _finite_float(value)
    return "-" if number is None else f"{number:.2f}"


def _fmt_int(value: object) -> str:
    number = _finite_float(value)
    return "-" if number is None else str(int(number))


__all__ = [
    "DEFAULT_FILTER_POLICIES",
    "STANDARD_VALUE_PUMP_FADE_PORTFOLIO_FILTER_EXPERIMENT_ID",
    "StandardValuePumpFadePortfolioFilterResult",
    "run_standard_value_pump_fade_portfolio_filter_from_frames",
    "write_standard_value_pump_fade_portfolio_filter_bundle",
]
