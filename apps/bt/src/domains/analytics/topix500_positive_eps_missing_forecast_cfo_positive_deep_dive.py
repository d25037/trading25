"""Deep-dive on TOPIX500 FY actual EPS>0 events with missing forecast and CFO>0."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.domains.analytics import fy_eps_sign_next_fy_return as base_study
from src.domains.analytics import (
    standard_missing_forecast_cfo_non_positive_deep_dive as shared_deep_dive,
)
from src.domains.analytics.readonly_duckdb_support import SourceMode
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.analytics.standard_negative_eps_right_tail_decomposition import (
    DEFAULT_ADV_WINDOW,
    _compute_entry_adv,
    _open_analysis_connection,
    _query_price_rows,
    _query_statement_rows,
    _to_nullable_float,
)

TOPIX500_POSITIVE_EPS_MISSING_FORECAST_CFO_POSITIVE_DEEP_DIVE_EXPERIMENT_ID = (
    "market-behavior/topix500-positive-eps-missing-forecast-cfo-positive-deep-dive"
)
DEFAULT_PRIOR_SESSIONS = shared_deep_dive.DEFAULT_PRIOR_SESSIONS
DEFAULT_HORIZONS: tuple[int, ...] = shared_deep_dive.DEFAULT_HORIZONS
DEFAULT_RECENT_YEAR_WINDOW = shared_deep_dive.DEFAULT_RECENT_YEAR_WINDOW
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "benchmark_cell_summary_df",
    "subgroup_event_df",
    "year_summary_df",
    "recent_year_count_df",
    "recent_year_count_stats_df",
    "era_summary_df",
    "top_exclusion_summary_df",
    "followup_forecast_summary_df",
    "forecast_resume_summary_df",
    "prior_return_bucket_summary_df",
    "market_cap_bucket_summary_df",
    "sector_summary_df",
    "horizon_summary_df",
    "feature_split_summary_df",
    "feature_effect_summary_df",
    "top_winner_profile_df",
)
_BENCHMARK_CELL_SPECS: tuple[tuple[str, str, str, str], ...] = (
    (
        "target",
        "Target: EPS > 0 / forecast missing / CFO > 0",
        "forecast_missing",
        "cfo_positive",
    ),
    (
        "forecast_positive_cfo_positive",
        "Baseline: EPS > 0 / forecast > 0 / CFO > 0",
        "forecast_positive",
        "cfo_positive",
    ),
    (
        "forecast_positive_cfo_non_positive",
        "Baseline: EPS > 0 / forecast > 0 / CFO <= 0",
        "forecast_positive",
        "cfo_non_positive",
    ),
    (
        "forecast_missing_cfo_non_positive",
        "Sibling: EPS > 0 / forecast missing / CFO <= 0",
        "forecast_missing",
        "cfo_non_positive",
    ),
)


@dataclass(frozen=True)
class Topix500PositiveEpsMissingForecastCfoPositiveDeepDiveResult:
    db_path: str
    selected_market: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    base_scope_name: str
    subgroup_name: str
    adv_window: int
    prior_sessions: int
    horizons: tuple[int, ...]
    recent_year_window: int
    signed_event_count: int
    realized_event_count: int
    benchmark_cell_summary_df: pd.DataFrame
    subgroup_event_df: pd.DataFrame
    year_summary_df: pd.DataFrame
    recent_year_count_df: pd.DataFrame
    recent_year_count_stats_df: pd.DataFrame
    era_summary_df: pd.DataFrame
    top_exclusion_summary_df: pd.DataFrame
    followup_forecast_summary_df: pd.DataFrame
    forecast_resume_summary_df: pd.DataFrame
    prior_return_bucket_summary_df: pd.DataFrame
    market_cap_bucket_summary_df: pd.DataFrame
    sector_summary_df: pd.DataFrame
    horizon_summary_df: pd.DataFrame
    feature_split_summary_df: pd.DataFrame
    feature_effect_summary_df: pd.DataFrame
    top_winner_profile_df: pd.DataFrame


def _build_benchmark_cell_summary_df(cross_summary_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "comparison_key",
        "comparison_label",
        "forecast_sign",
        "cfo_sign",
        "signed_event_count",
        "realized_event_count",
        "mean_return_pct",
        "median_return_pct",
        "win_rate_pct",
    ]
    if cross_summary_df.empty:
        return shared_deep_dive._empty_result_df(columns)

    scope_df = cross_summary_df[
        (cross_summary_df["market_scope"].astype(str) == "topix500")
        & (cross_summary_df["eps_sign"].astype(str) == "positive")
    ].copy()
    if scope_df.empty:
        return shared_deep_dive._empty_result_df(columns)

    records: list[dict[str, Any]] = []
    for comparison_key, comparison_label, forecast_sign, cfo_sign in _BENCHMARK_CELL_SPECS:
        matched = scope_df[
            (scope_df["forecast_sign"].astype(str) == forecast_sign)
            & (scope_df["cfo_sign"].astype(str) == cfo_sign)
        ]
        if len(matched) != 1:
            continue
        row = matched.iloc[0]
        records.append(
            {
                "comparison_key": comparison_key,
                "comparison_label": comparison_label,
                "forecast_sign": forecast_sign,
                "cfo_sign": cfo_sign,
                "signed_event_count": int(row["signed_event_count"]),
                "realized_event_count": int(row["realized_event_count"]),
                "mean_return_pct": _to_nullable_float(row["mean_return_pct"]),
                "median_return_pct": _to_nullable_float(row["median_return_pct"]),
                "win_rate_pct": _to_nullable_float(row["win_rate_pct"]),
            }
        )
    return pd.DataFrame(records, columns=columns)


def _attach_entry_adv(
    subgroup_event_df: pd.DataFrame,
    *,
    price_df: pd.DataFrame,
    adv_window: int,
) -> pd.DataFrame:
    if subgroup_event_df.empty:
        return subgroup_event_df.copy()

    price_by_code = {
        str(code): frame.sort_values("date", kind="stable").reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    records: list[dict[str, Any]] = []
    for row in subgroup_event_df.to_dict(orient="records"):
        enriched: dict[str, Any] = {str(key): value for key, value in dict(row).items()}
        enriched["entry_adv"] = None
        enriched["entry_adv_window_observations"] = 0
        entry_date = row.get("entry_date")
        code = str(row["code"])
        price_frame = price_by_code.get(code)
        if price_frame is not None and not price_frame.empty and entry_date is not None:
            matches = price_frame.index[price_frame["date"].astype(str) == str(entry_date)]
            if len(matches) > 0:
                entry_adv, observations = _compute_entry_adv(
                    price_frame,
                    entry_idx=int(matches[0]),
                    adv_window=adv_window,
                )
                enriched["entry_adv"] = entry_adv
                enriched["entry_adv_window_observations"] = observations
        records.append(enriched)
    return pd.DataFrame(records)


def _build_target_portfolio_daily_df(
    *,
    event_ledger_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "date",
        "active_positions",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    if realized_df.empty or price_df.empty:
        return shared_deep_dive._empty_result_df(columns)

    price_by_code: dict[str, pd.DataFrame] = {
        str(code): frame.reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    aggregate: dict[str, list[float]] = {}
    for event in realized_df.itertuples(index=False):
        code = str(event.code)
        price_frame = price_by_code.get(code)
        if price_frame is None or price_frame.empty:
            continue
        path_df = price_frame[
            (price_frame["date"] >= str(event.entry_date))
            & (price_frame["date"] <= str(event.exit_date))
        ][["date", "close"]].copy()
        if path_df.empty:
            continue
        close_values = pd.to_numeric(path_df["close"], errors="coerce").astype(float).to_numpy()
        if not np.isfinite(close_values).all():
            continue
        entry_open = _to_nullable_float(event.entry_open)
        if entry_open is None:
            continue
        previous_close = np.concatenate(([entry_open], close_values[:-1]))
        daily_returns = close_values / previous_close - 1.0
        for date_value, daily_return in zip(path_df["date"].astype(str), daily_returns, strict=True):
            bucket = aggregate.setdefault(str(date_value), [0.0, 0.0])
            bucket[0] += float(daily_return)
            bucket[1] += 1.0

    if not aggregate:
        return shared_deep_dive._empty_result_df(columns)

    records = [
        {
            "date": date_value,
            "active_positions": int(values[1]),
            "mean_daily_return": float(values[0] / values[1]),
        }
        for date_value, values in aggregate.items()
    ]
    portfolio_daily_df = pd.DataFrame(records, columns=["date", "active_positions", "mean_daily_return"])
    portfolio_daily_df = portfolio_daily_df.sort_values("date", kind="stable").reset_index(drop=True)
    portfolio_daily_df["mean_daily_return_pct"] = portfolio_daily_df["mean_daily_return"] * 100.0
    values = (1.0 + portfolio_daily_df["mean_daily_return"]).cumprod()
    peaks = values.cummax()
    portfolio_daily_df["portfolio_value"] = values
    portfolio_daily_df["drawdown_pct"] = (values / peaks - 1.0) * 100.0
    return portfolio_daily_df[columns]


def _build_target_portfolio_summary(
    portfolio_daily_df: pd.DataFrame,
) -> dict[str, float | None]:
    if portfolio_daily_df.empty:
        return {
            "portfolio_total_return_pct": None,
            "portfolio_cagr_pct": None,
            "portfolio_max_drawdown_pct": None,
        }
    start_date = str(portfolio_daily_df.iloc[0]["date"])
    end_date = str(portfolio_daily_df.iloc[-1]["date"])
    total_return = float(portfolio_daily_df.iloc[-1]["portfolio_value"] - 1.0)
    period_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
    cagr_pct: float | None = None
    if period_days > 0:
        cagr = (1.0 + total_return) ** (365.25 / period_days) - 1.0
        cagr_pct = float(cagr * 100.0) if math.isfinite(cagr) else None
    drawdown_min = pd.to_numeric(portfolio_daily_df["drawdown_pct"], errors="coerce").min()
    return {
        "portfolio_total_return_pct": total_return * 100.0,
        "portfolio_cagr_pct": cagr_pct,
        "portfolio_max_drawdown_pct": float(drawdown_min) if pd.notna(drawdown_min) else None,
    }


def _build_top_exclusion_summary_df(
    subgroup_event_df: pd.DataFrame,
    *,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "exclude_top_n",
        "remaining_event_count",
        "mean_return_pct",
        "median_return_pct",
        "win_rate_pct",
        "portfolio_total_return_pct",
        "portfolio_cagr_pct",
        "portfolio_max_drawdown_pct",
    ]
    realized_df = subgroup_event_df[subgroup_event_df["status"] == "realized"].copy()
    if realized_df.empty:
        return shared_deep_dive._empty_result_df(columns)

    ranked_df = realized_df.sort_values("event_return_pct", ascending=False, kind="stable").reset_index(drop=True)
    records: list[dict[str, Any]] = []
    for exclude_top_n in (0, 1, 3, 5, 10):
        trimmed_df = ranked_df.iloc[exclude_top_n:].copy()
        portfolio_summary = _build_target_portfolio_summary(
            _build_target_portfolio_daily_df(
                event_ledger_df=trimmed_df,
                price_df=price_df,
            )
        )
        records.append(
            {
                "exclude_top_n": exclude_top_n,
                "remaining_event_count": int(len(trimmed_df)),
                "mean_return_pct": shared_deep_dive._series_stat(trimmed_df["event_return_pct"], "mean"),
                "median_return_pct": shared_deep_dive._series_stat(trimmed_df["event_return_pct"], "median"),
                "win_rate_pct": shared_deep_dive._bool_ratio_pct(trimmed_df["event_return"] > 0),
                **portfolio_summary,
            }
        )
    return pd.DataFrame(records, columns=columns)


def _build_top_winner_profile_df(
    subgroup_event_df: pd.DataFrame,
    *,
    limit: int = 20,
) -> pd.DataFrame:
    columns = [
        "code",
        "company_name",
        "sector_33_name",
        "disclosed_date",
        "entry_date",
        "exit_date",
        "event_return_pct",
        "prior_return_pct",
        "prior_return_bucket",
        "entry_market_cap_bil_jpy",
        "entry_adv",
        "followup_forecast_state",
        "followup_latest_forecast_eps",
        "followup_first_available_forecast_days",
        "followup_first_positive_forecast_days",
    ]
    realized_df = subgroup_event_df[subgroup_event_df["status"] == "realized"].copy()
    if realized_df.empty:
        return shared_deep_dive._empty_result_df(columns)
    return (
        realized_df.sort_values("event_return_pct", ascending=False, kind="stable")
        .head(limit)
        .reset_index(drop=True)[columns]
    )


def _build_summary_markdown(
    result: Topix500PositiveEpsMissingForecastCfoPositiveDeepDiveResult,
) -> str:
    lines = [
        "# TOPIX500 EPS > 0 / Forecast Missing / CFO > 0 Deep Dive",
        "",
        "## Setup",
        "",
        f"- Selected scope: `{result.selected_market}`",
        f"- Base scope: `{result.base_scope_name}`",
        f"- Subgroup: `{result.subgroup_name}`",
        f"- Signed events: `{result.signed_event_count}`",
        f"- Realized events: `{result.realized_event_count}`",
        "- Scope proxy: latest `stocks.scale_category` snapshot",
        "- `topix500` is a current-universe retrospective proxy, not a historical committee reconstruction.",
        f"- Prior-return lookback: `{result.prior_sessions}` trading sessions",
        f"- Horizon returns: `{', '.join(str(value) + 'd' for value in result.horizons)}` plus `next_fy`",
        "",
        "## Baseline Comparison",
        "",
    ]
    if result.benchmark_cell_summary_df.empty:
        lines.append("- No benchmark cell summary was available.")
    else:
        for row in result.benchmark_cell_summary_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['comparison_label']}`: "
                f"signed `{int(row['signed_event_count'])}`, "
                f"realized `{int(row['realized_event_count'])}`, "
                f"mean `{shared_deep_dive._fmt_num(row['mean_return_pct'])}%`, "
                f"median `{shared_deep_dive._fmt_num(row['median_return_pct'])}%`, "
                f"win `{shared_deep_dive._fmt_num(row['win_rate_pct'])}%`"
            )

    lines.extend(["", "## Trailing 10 Full Years", ""])
    if result.recent_year_count_stats_df.empty or result.recent_year_count_df.empty:
        lines.append("- No trailing-year count summary was available.")
    else:
        stats = result.recent_year_count_stats_df.iloc[0]
        lines.append(
            "- "
            f"`{stats['window_start_year']}-{stats['window_end_year']}` average "
            f"`{shared_deep_dive._fmt_num(stats['average_signed_code_count'])}` names/year, "
            f"max `{int(stats['max_signed_code_count'])}`, "
            f"min `{int(stats['min_signed_code_count'])}`"
        )
        lines.append("- Year counts:")
        for row in result.recent_year_count_df.to_dict(orient="records"):
            lines.append(
                "  "
                + f"`{row['disclosed_year']}` names `{int(row['signed_code_count'])}` "
                + f"(events `{int(row['signed_event_count'])}`, realized `{int(row['realized_event_count'])}`)"
            )

    lines.extend(["", "## Vintage", ""])
    for label, df in (("Era", result.era_summary_df), ("Year", result.year_summary_df.head(10))):
        if df.empty:
            lines.append(f"- {label}: no data.")
            continue
        lines.append(f"- {label}:")
        for row in df.to_dict(orient="records"):
            lines.append(
                "  "
                + f"`{row[df.columns[0]]}` signed `{int(row['signed_event_count'])}` "
                + f"realized `{int(row['realized_event_count'])}` "
                + f"mean `{shared_deep_dive._fmt_num(row['mean_return_pct'])}%` "
                + f"median `{shared_deep_dive._fmt_num(row['median_return_pct'])}%`"
            )

    lines.extend(["", "## Forecast Follow-up", ""])
    if result.followup_forecast_summary_df.empty:
        lines.append("- No follow-up classification was available.")
    else:
        for row in result.followup_forecast_summary_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['followup_forecast_state']}`: "
                f"signed `{int(row['signed_event_count'])}`, "
                f"realized `{int(row['realized_event_count'])}`, "
                f"mean `{shared_deep_dive._fmt_num(row['mean_return_pct'])}%`, "
                f"median `{shared_deep_dive._fmt_num(row['median_return_pct'])}%`, "
                f"first available days `{shared_deep_dive._fmt_num(row['mean_followup_first_available_forecast_days'])}`"
            )
    if not result.forecast_resume_summary_df.empty:
        lines.append("")
        lines.append("- Collapsed:")
        for row in result.forecast_resume_summary_df.to_dict(orient="records"):
            lines.append(
                "  "
                + f"`{row['forecast_resume_group']}`: "
                + f"signed `{int(row['signed_event_count'])}`, "
                + f"realized `{int(row['realized_event_count'])}`, "
                + f"mean `{shared_deep_dive._fmt_num(row['mean_return_pct'])}%`, "
                + f"median `{shared_deep_dive._fmt_num(row['median_return_pct'])}%`"
            )

    lines.extend(["", "## Sectors", ""])
    if result.sector_summary_df.empty:
        lines.append("- No sector summary was available.")
    else:
        for row in result.sector_summary_df.head(10).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['sector_33_name']}`: "
                f"signed `{int(row['signed_event_count'])}`, "
                f"realized `{int(row['realized_event_count'])}`, "
                f"mean `{shared_deep_dive._fmt_num(row['mean_return_pct'])}%`, "
                f"median `{shared_deep_dive._fmt_num(row['median_return_pct'])}%`"
            )

    lines.extend(["", "## Drawdown And Size", ""])
    for label, df in (
        ("Prior 252d buckets", result.prior_return_bucket_summary_df),
        ("Entry market-cap buckets", result.market_cap_bucket_summary_df),
    ):
        if df.empty:
            lines.append(f"- {label}: no data.")
            continue
        lines.append(f"- {label}:")
        for row in df.to_dict(orient="records"):
            lines.append(
                "  "
                + f"`{row[df.columns[0]]}` signed `{int(row['signed_event_count'])}` "
                + f"realized `{int(row['realized_event_count'])}` "
                + f"mean `{shared_deep_dive._fmt_num(row['mean_return_pct'])}%` "
                + f"median `{shared_deep_dive._fmt_num(row['median_return_pct'])}%`"
            )

    lines.extend(["", "## Feature Splits", ""])
    if result.feature_effect_summary_df.empty:
        lines.append("- No feature-split summary was available.")
    else:
        for row in result.feature_effect_summary_df.head(7).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['feature_label']}` upper-half minus lower-half median "
                f"`{shared_deep_dive._fmt_num(row['median_return_spread_pct'])}%`, "
                f"mean spread `{shared_deep_dive._fmt_num(row['mean_return_spread_pct'])}%`, "
                f"split ref `{shared_deep_dive._fmt_num(row['split_reference_value'])}`"
            )

    lines.extend(["", "## Horizon Path", ""])
    if result.horizon_summary_df.empty:
        lines.append("- No horizon path summary was available.")
    else:
        for row in result.horizon_summary_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['horizon_label']}`: "
                f"available `{int(row['available_event_count'])}`, "
                f"mean `{shared_deep_dive._fmt_num(row['mean_return_pct'])}%`, "
                f"median `{shared_deep_dive._fmt_num(row['median_return_pct'])}%`, "
                f"win `{shared_deep_dive._fmt_num(row['win_rate_pct'])}%`"
            )

    lines.extend(["", "## Top-N Exclusion", ""])
    if result.top_exclusion_summary_df.empty:
        lines.append("- No realized events were available.")
    else:
        for row in result.top_exclusion_summary_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"exclude top `{int(row['exclude_top_n'])}`: "
                f"remaining `{int(row['remaining_event_count'])}`, "
                f"mean `{shared_deep_dive._fmt_num(row['mean_return_pct'])}%`, "
                f"median `{shared_deep_dive._fmt_num(row['median_return_pct'])}%`, "
                f"CAGR `{shared_deep_dive._fmt_num(row['portfolio_cagr_pct'])}%`, "
                f"max DD `{shared_deep_dive._fmt_num(row['portfolio_max_drawdown_pct'])}%`"
            )

    lines.extend(["", "## Top Winners", ""])
    if result.top_winner_profile_df.empty:
        lines.append("- No top-winner profiles were available.")
    else:
        for row in result.top_winner_profile_df.head(10).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['code']}` {row['company_name']}: "
                f"return `{shared_deep_dive._fmt_num(row['event_return_pct'])}%`, "
                f"prior `{shared_deep_dive._fmt_num(row['prior_return_pct'])}%`, "
                f"mcap `{shared_deep_dive._fmt_num(row['entry_market_cap_bil_jpy'])}b`, "
                f"follow-up `{row['followup_forecast_state']}`"
            )

    return "\n".join(lines)


def _build_published_summary(
    result: Topix500PositiveEpsMissingForecastCfoPositiveDeepDiveResult,
) -> dict[str, Any]:
    return {
        "selectedMarket": result.selected_market,
        "baseScopeName": result.base_scope_name,
        "subgroupName": result.subgroup_name,
        "advWindow": result.adv_window,
        "priorSessions": result.prior_sessions,
        "horizons": list(result.horizons),
        "recentYearWindow": result.recent_year_window,
        "signedEventCount": result.signed_event_count,
        "realizedEventCount": result.realized_event_count,
        "benchmarkCellSummary": result.benchmark_cell_summary_df.to_dict(orient="records"),
        "recentYearCountSummary": result.recent_year_count_stats_df.to_dict(orient="records"),
        "recentYearCountByYear": result.recent_year_count_df.to_dict(orient="records"),
        "followupForecastSummary": result.followup_forecast_summary_df.to_dict(orient="records"),
        "forecastResumeSummary": result.forecast_resume_summary_df.to_dict(orient="records"),
        "sectorSummary": result.sector_summary_df.to_dict(orient="records"),
        "featureEffectSummary": result.feature_effect_summary_df.to_dict(orient="records"),
        "horizonSummary": result.horizon_summary_df.to_dict(orient="records"),
        "topExclusionSummary": result.top_exclusion_summary_df.to_dict(orient="records"),
    }


def run_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive(
    db_path: str,
    *,
    adv_window: int = DEFAULT_ADV_WINDOW,
    prior_sessions: int = DEFAULT_PRIOR_SESSIONS,
    horizons: Sequence[int] | None = None,
    recent_year_window: int = DEFAULT_RECENT_YEAR_WINDOW,
) -> Topix500PositiveEpsMissingForecastCfoPositiveDeepDiveResult:
    if adv_window <= 0:
        raise ValueError("adv_window must be positive")
    if prior_sessions <= 0:
        raise ValueError("prior_sessions must be positive")
    if recent_year_window <= 0:
        raise ValueError("recent_year_window must be positive")
    resolved_horizons = shared_deep_dive._normalize_horizons(horizons)
    base_result = base_study.run_fy_eps_sign_next_fy_return(
        db_path,
        markets=("topix500",),
    )
    benchmark_cell_summary_df = _build_benchmark_cell_summary_df(base_result.cross_summary_df)
    subgroup_event_df = base_result.event_ledger_df[
        (base_result.event_ledger_df["market"].astype(str) == "topix500")
        & (base_result.event_ledger_df["eps_sign"].astype(str) == "positive")
        & (base_result.event_ledger_df["forecast_sign"].astype(str) == "forecast_missing")
        & (base_result.event_ledger_df["cfo_sign"].astype(str) == "cfo_positive")
    ].copy()

    if subgroup_event_df.empty:
        empty_df = shared_deep_dive._empty_result_df([])
        return Topix500PositiveEpsMissingForecastCfoPositiveDeepDiveResult(
            db_path=db_path,
            selected_market="topix500",
            source_mode=base_result.source_mode,
            source_detail=base_result.source_detail,
            available_start_date=base_result.available_start_date,
            available_end_date=base_result.available_end_date,
            analysis_start_date=None,
            analysis_end_date=None,
            base_scope_name="TOPIX500 / FY actual EPS > 0",
            subgroup_name="EPS > 0 / forecast missing / CFO > 0",
            adv_window=adv_window,
            prior_sessions=prior_sessions,
            horizons=resolved_horizons,
            recent_year_window=recent_year_window,
            signed_event_count=0,
            realized_event_count=0,
            benchmark_cell_summary_df=benchmark_cell_summary_df,
            subgroup_event_df=empty_df.copy(),
            year_summary_df=empty_df.copy(),
            recent_year_count_df=empty_df.copy(),
            recent_year_count_stats_df=empty_df.copy(),
            era_summary_df=empty_df.copy(),
            top_exclusion_summary_df=empty_df.copy(),
            followup_forecast_summary_df=empty_df.copy(),
            forecast_resume_summary_df=empty_df.copy(),
            prior_return_bucket_summary_df=empty_df.copy(),
            market_cap_bucket_summary_df=empty_df.copy(),
            sector_summary_df=empty_df.copy(),
            horizon_summary_df=empty_df.copy(),
            feature_split_summary_df=empty_df.copy(),
            feature_effect_summary_df=empty_df.copy(),
            top_winner_profile_df=empty_df.copy(),
        )

    market_codes = base_study._market_query_codes(("topix500",))
    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start_date = base_result.available_start_date
        available_end_date = base_result.available_end_date
        stock_df = base_study._filter_stock_scope(
            base_study._query_canonical_stocks(conn, market_codes=market_codes),
            selected_markets=("topix500",),
        )
        scoped_codes = set(stock_df["code"].astype(str))
        subgroup_event_df = subgroup_event_df[
            subgroup_event_df["code"].astype(str).isin(scoped_codes)
        ].copy()
        signed_event_count = int(len(subgroup_event_df))
        realized_event_count = int((subgroup_event_df["status"].astype(str) == "realized").sum())
        analysis_start_date = str(subgroup_event_df["disclosed_date"].min())
        exit_candidates = subgroup_event_df["exit_date"].dropna().astype(str)
        analysis_end_date = str(exit_candidates.max()) if not exit_candidates.empty else None
        min_price_date = (
            pd.Timestamp(analysis_start_date)
            - pd.Timedelta(days=max(prior_sessions * 3, max(resolved_horizons) * 3, adv_window * 3, 400))
        ).strftime("%Y-%m-%d")
        end_candidates = [
            str(value)
            for value in subgroup_event_df["next_fy_disclosed_date"].dropna().astype(str).tolist()
            + subgroup_event_df["exit_date"].dropna().astype(str).tolist()
        ]
        requested_price_end_date = max(end_candidates) if end_candidates else (
            analysis_end_date or available_end_date
        )
        price_df = _query_price_rows(
            conn,
            market_codes=market_codes,
            start_date=max(str(available_start_date or min_price_date), min_price_date),
            end_date=str(requested_price_end_date or available_end_date),
        )
        statement_df = _query_statement_rows(conn, market_codes=market_codes)

    price_df = price_df[price_df["code"].astype(str).isin(scoped_codes)].copy().reset_index(drop=True)
    statement_df = (
        statement_df[statement_df["code"].astype(str).isin(scoped_codes)]
        .copy()
        .reset_index(drop=True)
    )
    subgroup_event_df = _attach_entry_adv(
        subgroup_event_df,
        price_df=price_df,
        adv_window=adv_window,
    )
    enriched_subgroup_df = shared_deep_dive._enrich_subgroup_events(
        subgroup_event_df,
        price_df=price_df,
        statement_df=statement_df,
        prior_sessions=prior_sessions,
        horizons=resolved_horizons,
    )
    year_summary_df = shared_deep_dive._build_year_summary_df(enriched_subgroup_df)
    recent_year_count_df = shared_deep_dive._build_recent_year_count_df(
        enriched_subgroup_df,
        available_end_date=available_end_date,
        window_years=recent_year_window,
    )
    recent_year_count_stats_df = shared_deep_dive._build_recent_year_count_stats_df(
        recent_year_count_df
    )
    era_summary_df = shared_deep_dive._build_era_summary_df(enriched_subgroup_df)
    top_exclusion_summary_df = _build_top_exclusion_summary_df(
        enriched_subgroup_df,
        price_df=price_df,
    )
    followup_forecast_summary_df = shared_deep_dive._build_followup_forecast_summary_df(
        enriched_subgroup_df
    )
    forecast_resume_summary_df = shared_deep_dive._build_forecast_resume_summary_df(
        enriched_subgroup_df
    )
    prior_return_bucket_summary_df = shared_deep_dive._build_prior_return_bucket_summary_df(
        enriched_subgroup_df
    )
    market_cap_bucket_summary_df = shared_deep_dive._build_market_cap_bucket_summary_df(
        enriched_subgroup_df
    )
    sector_summary_df = shared_deep_dive._build_sector_summary_df(enriched_subgroup_df)
    horizon_summary_df = shared_deep_dive._build_horizon_summary_df(
        enriched_subgroup_df,
        horizons=resolved_horizons,
    )
    feature_split_summary_df = shared_deep_dive._build_feature_split_summary_df(
        enriched_subgroup_df
    )
    feature_effect_summary_df = shared_deep_dive._build_feature_effect_summary_df(
        feature_split_summary_df
    )
    top_winner_profile_df = _build_top_winner_profile_df(enriched_subgroup_df)

    return Topix500PositiveEpsMissingForecastCfoPositiveDeepDiveResult(
        db_path=db_path,
        selected_market="topix500",
        source_mode=base_result.source_mode,
        source_detail=base_result.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        base_scope_name="TOPIX500 / FY actual EPS > 0",
        subgroup_name="EPS > 0 / forecast missing / CFO > 0",
        adv_window=adv_window,
        prior_sessions=prior_sessions,
        horizons=resolved_horizons,
        recent_year_window=recent_year_window,
        signed_event_count=signed_event_count,
        realized_event_count=realized_event_count,
        benchmark_cell_summary_df=benchmark_cell_summary_df,
        subgroup_event_df=enriched_subgroup_df,
        year_summary_df=year_summary_df,
        recent_year_count_df=recent_year_count_df,
        recent_year_count_stats_df=recent_year_count_stats_df,
        era_summary_df=era_summary_df,
        top_exclusion_summary_df=top_exclusion_summary_df,
        followup_forecast_summary_df=followup_forecast_summary_df,
        forecast_resume_summary_df=forecast_resume_summary_df,
        prior_return_bucket_summary_df=prior_return_bucket_summary_df,
        market_cap_bucket_summary_df=market_cap_bucket_summary_df,
        sector_summary_df=sector_summary_df,
        horizon_summary_df=horizon_summary_df,
        feature_split_summary_df=feature_split_summary_df,
        feature_effect_summary_df=feature_effect_summary_df,
        top_winner_profile_df=top_winner_profile_df,
    )


def write_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle(
    result: Topix500PositiveEpsMissingForecastCfoPositiveDeepDiveResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX500_POSITIVE_EPS_MISSING_FORECAST_CFO_POSITIVE_DEEP_DIVE_EXPERIMENT_ID,
        module=__name__,
        function="run_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive",
        params={
            "adv_window": result.adv_window,
            "prior_sessions": result.prior_sessions,
            "horizons": list(result.horizons),
            "recent_year_window": result.recent_year_window,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle(
    bundle_path: str | Path,
) -> Topix500PositiveEpsMissingForecastCfoPositiveDeepDiveResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix500PositiveEpsMissingForecastCfoPositiveDeepDiveResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX500_POSITIVE_EPS_MISSING_FORECAST_CFO_POSITIVE_DEEP_DIVE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX500_POSITIVE_EPS_MISSING_FORECAST_CFO_POSITIVE_DEEP_DIVE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
