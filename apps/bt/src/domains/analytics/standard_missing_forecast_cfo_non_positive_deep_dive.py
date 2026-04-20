"""Deep-dive on market-scoped EPS<0 events with missing forecast and CFO<=0."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.domains.analytics.fundamental_ranking import FundamentalRankingCalculator
from src.domains.analytics.fy_eps_sign_next_fy_return import (
    _query_canonical_stocks as _query_scope_canonical_stocks,
)
from src.domains.analytics.readonly_duckdb_support import fetch_date_range as _fetch_date_range
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
    DEFAULT_MARKET,
    _build_portfolio_daily_df,
    _build_portfolio_summary_df,
    _build_statement_rows_by_code,
    _open_analysis_connection,
    _query_market_codes,
    _query_price_rows,
    _query_statement_rows,
    _to_nullable_float,
    run_standard_negative_eps_right_tail_decomposition,
)

STANDARD_MISSING_FORECAST_CFO_NON_POSITIVE_DEEP_DIVE_EXPERIMENT_ID = (
    "market-behavior/standard-missing-forecast-cfo-non-positive-deep-dive"
)
PRIME_MISSING_FORECAST_CFO_NON_POSITIVE_DEEP_DIVE_EXPERIMENT_ID = (
    "market-behavior/prime-missing-forecast-cfo-non-positive-deep-dive"
)
TOPIX500_MISSING_FORECAST_CFO_NON_POSITIVE_DEEP_DIVE_EXPERIMENT_ID = (
    "market-behavior/topix500-missing-forecast-cfo-non-positive-deep-dive"
)
PRIME_EX_TOPIX500_MISSING_FORECAST_CFO_NON_POSITIVE_DEEP_DIVE_EXPERIMENT_ID = (
    "market-behavior/prime-ex-topix500-missing-forecast-cfo-non-positive-deep-dive"
)
SUBGROUP_KEY = "forecast_missing__cfo_non_positive"
DEFAULT_PRIOR_SESSIONS = 252
DEFAULT_HORIZONS: tuple[int, ...] = (21, 63, 126, 252)
DEFAULT_RECENT_YEAR_WINDOW = 10
_EXPERIMENT_ID_BY_MARKET: dict[str, str] = {
    "standard": STANDARD_MISSING_FORECAST_CFO_NON_POSITIVE_DEEP_DIVE_EXPERIMENT_ID,
    "prime": PRIME_MISSING_FORECAST_CFO_NON_POSITIVE_DEEP_DIVE_EXPERIMENT_ID,
    "topix500": TOPIX500_MISSING_FORECAST_CFO_NON_POSITIVE_DEEP_DIVE_EXPERIMENT_ID,
    "primeExTopix500": PRIME_EX_TOPIX500_MISSING_FORECAST_CFO_NON_POSITIVE_DEEP_DIVE_EXPERIMENT_ID,
}
_SCOPE_ALIAS_TO_CANONICAL: dict[str, str] = {
    "standard": "standard",
    "prime": "prime",
    "topix500": "topix500",
    "primeextopix500": "primeExTopix500",
    "prime_ex_topix500": "primeExTopix500",
}
_SCOPE_LABEL_BY_MARKET: dict[str, str] = {
    "standard": "Standard",
    "prime": "Prime",
    "topix500": "TOPIX500",
    "primeExTopix500": "Prime ex TOPIX500",
}
_SCOPE_NAME_BY_MARKET: dict[str, str] = {
    "standard": "standard / FY actual EPS < 0",
    "prime": "prime / FY actual EPS < 0",
    "topix500": "TOPIX500 / FY actual EPS < 0",
    "primeExTopix500": "primeExTopix500 / FY actual EPS < 0",
}
_BASE_MARKET_BY_SCOPE: dict[str, str] = {
    "standard": "standard",
    "prime": "prime",
    "topix500": "prime",
    "primeExTopix500": "prime",
}
_TOPIX500_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
    "TOPIX Mid400",
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
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
_ERA_BUCKET_ORDER: tuple[str, ...] = (
    "2010-2014",
    "2015-2019",
    "2020-2022",
    "2023-2025",
    "2026+",
    "unknown",
)
_PRIOR_RETURN_BUCKET_ORDER: tuple[str, ...] = (
    "<=-80%",
    "-80% to -50%",
    "-50% to -20%",
    ">-20%",
    "missing",
)
_MARKET_CAP_BUCKET_ORDER: tuple[str, ...] = (
    "<10b",
    "10b-50b",
    "50b-200b",
    ">=200b",
    "missing",
)
_FOLLOWUP_STATE_ORDER: tuple[str, ...] = (
    "turned_positive_before_next_fy",
    "turned_non_positive_before_next_fy",
    "still_missing_until_next_fy",
    "no_next_fy",
)
_FORECAST_RESUME_ORDER: tuple[str, ...] = (
    "forecast_resumed_before_next_fy",
    "forecast_stayed_missing_until_next_fy",
    "no_next_fy",
)
_FEATURE_SPLIT_SPECS: tuple[tuple[str, str], ...] = (
    ("prior_return_pct", "Prior 252d return (%)"),
    ("entry_market_cap_bil_jpy", "Entry market cap (JPY bn)"),
    ("entry_adv", "Entry ADV (JPY)"),
    ("actual_eps", "Actual EPS (higher = less negative)"),
    ("profit_margin_pct", "Profit margin (%)"),
    ("cfo_margin_pct", "CFO margin (%)"),
    ("equity_ratio_pct", "Equity ratio (%)"),
)


@dataclass(frozen=True)
class StandardMissingForecastCfoNonPositiveDeepDiveResult:
    db_path: str
    selected_market: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    base_scope_name: str
    subgroup_key: str
    adv_window: int
    prior_sessions: int
    horizons: tuple[int, ...]
    recent_year_window: int
    signed_event_count: int
    realized_event_count: int
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


def _empty_result_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _normalize_market(market: str) -> str:
    normalized = _SCOPE_ALIAS_TO_CANONICAL.get(str(market).strip().lower())
    if normalized is None:
        supported = ", ".join(sorted(_EXPERIMENT_ID_BY_MARKET))
        raise ValueError(f"Unsupported market: {market!r}. Supported markets: {supported}")
    return normalized


def _experiment_id_for_market(market: str) -> str:
    return _EXPERIMENT_ID_BY_MARKET[_normalize_market(market)]


def _scope_label_for_market(market: str) -> str:
    return _SCOPE_LABEL_BY_MARKET[_normalize_market(market)]


def _scope_name_for_market(market: str) -> str:
    return _SCOPE_NAME_BY_MARKET[_normalize_market(market)]


def _base_market_for_scope(market: str) -> str:
    return _BASE_MARKET_BY_SCOPE[_normalize_market(market)]


def _uses_current_scale_category_proxy(market: str) -> bool:
    return _normalize_market(market) in {"topix500", "primeExTopix500"}


def _filter_stock_scope(
    stock_df: pd.DataFrame,
    *,
    selected_market: str,
) -> pd.DataFrame:
    if stock_df.empty:
        return stock_df.copy()
    normalized = _normalize_market(selected_market)
    if normalized == "topix500":
        filtered = stock_df[
            (stock_df["market"] == "prime")
            & (stock_df["scale_category"].isin(_TOPIX500_SCALE_CATEGORIES))
        ].copy()
        filtered["market"] = "topix500"
        return filtered.reset_index(drop=True)
    if normalized == "primeExTopix500":
        filtered = stock_df[
            (stock_df["market"] == "prime")
            & (~stock_df["scale_category"].isin(_TOPIX500_SCALE_CATEGORIES))
        ].copy()
        filtered["market"] = "primeExTopix500"
        return filtered.reset_index(drop=True)
    return stock_df[stock_df["market"] == normalized].copy().reset_index(drop=True)


def _normalize_horizons(horizons: Sequence[int] | None) -> tuple[int, ...]:
    candidate = DEFAULT_HORIZONS if horizons is None else tuple(int(value) for value in horizons)
    if not candidate:
        raise ValueError("horizons must not be empty")
    if any(value <= 0 for value in candidate):
        raise ValueError("horizons must be positive")
    return tuple(dict.fromkeys(candidate))


def _bucket_era(disclosed_year: str | None) -> str:
    if disclosed_year is None:
        return "unknown"
    try:
        year = int(str(disclosed_year)[:4])
    except ValueError:
        return "unknown"
    if year <= 2014:
        return "2010-2014"
    if year <= 2019:
        return "2015-2019"
    if year <= 2022:
        return "2020-2022"
    if year <= 2025:
        return "2023-2025"
    return "2026+"


def _bucket_prior_return(prior_return_pct: float | None) -> str:
    value = _to_nullable_float(prior_return_pct)
    if value is None:
        return "missing"
    if value <= -80.0:
        return "<=-80%"
    if value <= -50.0:
        return "-80% to -50%"
    if value <= -20.0:
        return "-50% to -20%"
    return ">-20%"


def _bucket_market_cap(entry_market_cap_bil_jpy: float | None) -> str:
    value = _to_nullable_float(entry_market_cap_bil_jpy)
    if value is None:
        return "missing"
    if value < 10.0:
        return "<10b"
    if value < 50.0:
        return "10b-50b"
    if value < 200.0:
        return "50b-200b"
    return ">=200b"


def _fmt_num(value: float | int | None, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    if isinstance(value, int):
        return f"{value}"
    return f"{value:.{digits}f}"


def _series_stat(series: pd.Series, fn: str) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    if fn == "mean":
        return float(numeric.mean())
    if fn == "median":
        return float(numeric.median())
    if fn == "q25":
        return float(numeric.quantile(0.25))
    if fn == "q75":
        return float(numeric.quantile(0.75))
    raise ValueError(f"Unsupported fn: {fn}")


def _bool_ratio_pct(mask: pd.Series) -> float | None:
    if mask.empty:
        return None
    return float(mask.mean() * 100.0)


def _ratio_pct(numerator: float | None, denominator: float | None) -> float | None:
    if (
        numerator is None
        or denominator is None
        or not math.isfinite(numerator)
        or not math.isfinite(denominator)
        or math.isclose(denominator, 0.0, abs_tol=1e-12)
    ):
        return None
    return (numerator / denominator) * 100.0


def _locate_date_index(price_frame: pd.DataFrame, date_value: str | None) -> int | None:
    if date_value is None or price_frame.empty:
        return None
    matches = price_frame.index[price_frame["date"].astype(str) == str(date_value)]
    if len(matches) == 0:
        return None
    return int(matches[0])


def _compute_prior_return_pct(
    price_frame: pd.DataFrame,
    *,
    entry_idx: int,
    prior_sessions: int,
) -> tuple[float | None, int]:
    previous_idx = entry_idx - 1
    start_idx = entry_idx - prior_sessions
    if previous_idx < 0 or start_idx < 0:
        return None, 0
    previous_close = _to_nullable_float(price_frame.iloc[previous_idx]["close"])
    start_close = _to_nullable_float(price_frame.iloc[start_idx]["close"])
    if (
        previous_close is None
        or start_close is None
        or not math.isfinite(previous_close)
        or not math.isfinite(start_close)
        or math.isclose(start_close, 0.0, abs_tol=1e-12)
    ):
        return None, 0
    return (previous_close / start_close - 1.0) * 100.0, prior_sessions


def _compute_horizon_return_pct(
    price_frame: pd.DataFrame,
    *,
    entry_idx: int,
    exit_idx: int,
    entry_open: float | None,
    horizon: int,
) -> tuple[float | None, int]:
    if entry_open is None or not math.isfinite(entry_open) or math.isclose(entry_open, 0.0, abs_tol=1e-12):
        return None, 0
    target_idx = entry_idx + horizon - 1
    if target_idx > exit_idx or target_idx >= len(price_frame):
        return None, max(0, exit_idx - entry_idx + 1)
    target_close = _to_nullable_float(price_frame.iloc[target_idx]["close"])
    if target_close is None or not math.isfinite(target_close):
        return None, horizon
    return (target_close / entry_open - 1.0) * 100.0, horizon


def _resolve_followup_forecast(
    calculator: FundamentalRankingCalculator,
    statement_rows_by_code: dict[str, list[Any]],
    *,
    code: str,
    disclosed_date: str,
    next_fy_disclosed_date: str | None,
    baseline_shares: float | None,
) -> dict[str, Any]:
    if next_fy_disclosed_date is None:
        return {
            "followup_forecast_state": "no_next_fy",
            "followup_latest_forecast_eps": None,
            "followup_latest_forecast_disclosed_date": None,
            "followup_first_available_forecast_disclosed_date": None,
            "followup_first_available_forecast_days": None,
            "followup_first_positive_forecast_disclosed_date": None,
            "followup_first_positive_forecast_days": None,
        }

    rows = statement_rows_by_code.get(code, [])
    candidate_dates = sorted(
        {
            row.disclosed_date
            for row in rows
            if row.disclosed_date > disclosed_date and row.disclosed_date < next_fy_disclosed_date
        }
    )
    latest_snapshot = None
    first_available_snapshot = None
    first_positive_snapshot = None
    for candidate_date in candidate_dates:
        snapshot = calculator.resolve_latest_forecast_snapshot(
            rows,
            baseline_shares,
            as_of_date=candidate_date,
        )
        if snapshot is None:
            continue
        latest_snapshot = snapshot
        if first_available_snapshot is None:
            first_available_snapshot = snapshot
        if first_positive_snapshot is None and snapshot.value > 0:
            first_positive_snapshot = snapshot

    if latest_snapshot is None:
        state = "still_missing_until_next_fy"
    elif latest_snapshot.value > 0:
        state = "turned_positive_before_next_fy"
    else:
        state = "turned_non_positive_before_next_fy"

    def _days_between(target_date: str | None) -> int | None:
        if target_date is None:
            return None
        return int((pd.Timestamp(target_date) - pd.Timestamp(disclosed_date)).days)

    return {
        "followup_forecast_state": state,
        "followup_latest_forecast_eps": (
            float(latest_snapshot.value) if latest_snapshot is not None else None
        ),
        "followup_latest_forecast_disclosed_date": (
            latest_snapshot.disclosed_date if latest_snapshot is not None else None
        ),
        "followup_first_available_forecast_disclosed_date": (
            first_available_snapshot.disclosed_date
            if first_available_snapshot is not None
            else None
        ),
        "followup_first_available_forecast_days": _days_between(
            first_available_snapshot.disclosed_date
            if first_available_snapshot is not None
            else None
        ),
        "followup_first_positive_forecast_disclosed_date": (
            first_positive_snapshot.disclosed_date
            if first_positive_snapshot is not None
            else None
        ),
        "followup_first_positive_forecast_days": _days_between(
            first_positive_snapshot.disclosed_date
            if first_positive_snapshot is not None
            else None
        ),
    }


def _collapse_followup_forecast_state(state: str | None) -> str:
    normalized = str(state or "")
    if normalized in {"turned_positive_before_next_fy", "turned_non_positive_before_next_fy"}:
        return "forecast_resumed_before_next_fy"
    if normalized == "still_missing_until_next_fy":
        return "forecast_stayed_missing_until_next_fy"
    return "no_next_fy"


def _enrich_subgroup_events(
    subgroup_event_df: pd.DataFrame,
    *,
    price_df: pd.DataFrame,
    statement_df: pd.DataFrame,
    prior_sessions: int,
    horizons: Sequence[int],
) -> pd.DataFrame:
    if subgroup_event_df.empty:
        return subgroup_event_df.copy()

    calculator = FundamentalRankingCalculator()
    statement_rows_by_code = _build_statement_rows_by_code(statement_df)
    statement_snapshot_by_key = {
        (str(row["code"]), str(row["disclosed_date"])): row
        for row in statement_df.to_dict(orient="records")
    }
    price_by_code = {
        str(code): frame.sort_values("date", kind="stable").reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    records: list[dict[str, Any]] = []
    for row in subgroup_event_df.to_dict(orient="records"):
        code = str(row["code"])
        price_frame = price_by_code.get(code)
        entry_date = row.get("entry_date")
        exit_date = row.get("exit_date")
        entry_open = _to_nullable_float(row.get("entry_open"))
        baseline_shares = _to_nullable_float(row.get("baseline_shares"))
        statement_snapshot = statement_snapshot_by_key.get((code, str(row["disclosed_date"])), {})
        enriched: dict[str, Any] = {str(key): value for key, value in row.items()}
        enriched["era_bucket"] = _bucket_era(str(row.get("disclosed_year")))
        enriched["prior_return_sessions"] = 0
        enriched["prior_return_pct"] = None
        enriched["prior_return_bucket"] = "missing"
        enriched["entry_market_cap_bil_jpy"] = None
        enriched["market_cap_bucket"] = "missing"
        raw_profit = _to_nullable_float(statement_snapshot.get("profit"))
        raw_sales = _to_nullable_float(statement_snapshot.get("sales"))
        raw_equity = _to_nullable_float(statement_snapshot.get("equity"))
        raw_total_assets = _to_nullable_float(statement_snapshot.get("total_assets"))
        enriched["profit"] = raw_profit
        enriched["sales"] = raw_sales
        enriched["equity"] = raw_equity
        enriched["total_assets"] = raw_total_assets
        enriched["profit_margin_pct"] = _ratio_pct(raw_profit, raw_sales)
        enriched["cfo_margin_pct"] = _ratio_pct(
            _to_nullable_float(row.get("operating_cash_flow")),
            raw_sales,
        )
        enriched["equity_ratio_pct"] = _ratio_pct(raw_equity, raw_total_assets)
        enriched["forecast_resume_group"] = "no_next_fy"
        for horizon in horizons:
            enriched[f"h{horizon}_return_pct"] = None
            enriched[f"h{horizon}_observed_sessions"] = 0

        followup_payload = _resolve_followup_forecast(
            calculator,
            statement_rows_by_code,
            code=code,
            disclosed_date=str(row["disclosed_date"]),
            next_fy_disclosed_date=(
                str(row["next_fy_disclosed_date"])
                if row.get("next_fy_disclosed_date") not in (None, "", "None")
                else None
            ),
            baseline_shares=baseline_shares,
        )
        enriched.update(followup_payload)
        enriched["forecast_resume_group"] = _collapse_followup_forecast_state(
            str(enriched.get("followup_forecast_state"))
        )

        if (
            baseline_shares is not None
            and entry_open is not None
            and math.isfinite(baseline_shares)
            and math.isfinite(entry_open)
        ):
            market_cap_bil = entry_open * baseline_shares / 1_000_000_000.0
            enriched["entry_market_cap_bil_jpy"] = market_cap_bil
            enriched["market_cap_bucket"] = _bucket_market_cap(market_cap_bil)

        if price_frame is not None and not price_frame.empty and entry_date is not None:
            entry_idx = _locate_date_index(price_frame, str(entry_date))
            exit_idx = _locate_date_index(price_frame, str(exit_date)) if exit_date is not None else None
            if entry_idx is not None:
                prior_return_pct, observed_sessions = _compute_prior_return_pct(
                    price_frame,
                    entry_idx=entry_idx,
                    prior_sessions=prior_sessions,
                )
                enriched["prior_return_sessions"] = observed_sessions
                enriched["prior_return_pct"] = prior_return_pct
                enriched["prior_return_bucket"] = _bucket_prior_return(prior_return_pct)

                if exit_idx is not None and exit_idx >= entry_idx:
                    for horizon in horizons:
                        horizon_return_pct, observed_horizon_sessions = _compute_horizon_return_pct(
                            price_frame,
                            entry_idx=entry_idx,
                            exit_idx=exit_idx,
                            entry_open=entry_open,
                            horizon=horizon,
                        )
                        enriched[f"h{horizon}_return_pct"] = horizon_return_pct
                        enriched[f"h{horizon}_observed_sessions"] = observed_horizon_sessions

        records.append(enriched)

    result_df = pd.DataFrame(records)
    if result_df.empty:
        return result_df
    return result_df.sort_values(["disclosed_date", "code"], kind="stable").reset_index(drop=True)


def _build_grouped_summary_df(
    subgroup_event_df: pd.DataFrame,
    *,
    group_col: str,
    category_order: Sequence[str] | None = None,
) -> pd.DataFrame:
    columns = [
        group_col,
        "signed_event_count",
        "realized_event_count",
        "mean_return_pct",
        "median_return_pct",
        "q25_return_pct",
        "q75_return_pct",
        "win_rate_pct",
        "mean_prior_return_pct",
        "median_prior_return_pct",
        "mean_entry_market_cap_bil_jpy",
        "mean_entry_adv",
        "mean_followup_first_available_forecast_days",
        "mean_followup_first_positive_forecast_days",
    ]
    if subgroup_event_df.empty:
        return _empty_result_df(columns)

    records: list[dict[str, Any]] = []
    for group_value, group_df in subgroup_event_df.groupby(group_col, observed=True, sort=False):
        realized_df = group_df[group_df["status"] == "realized"].copy()
        records.append(
            {
                group_col: str(group_value),
                "signed_event_count": int(len(group_df)),
                "realized_event_count": int(len(realized_df)),
                "mean_return_pct": _series_stat(realized_df["event_return_pct"], "mean"),
                "median_return_pct": _series_stat(realized_df["event_return_pct"], "median"),
                "q25_return_pct": _series_stat(realized_df["event_return_pct"], "q25"),
                "q75_return_pct": _series_stat(realized_df["event_return_pct"], "q75"),
                "win_rate_pct": _bool_ratio_pct(realized_df["event_return"] > 0),
                "mean_prior_return_pct": _series_stat(realized_df["prior_return_pct"], "mean"),
                "median_prior_return_pct": _series_stat(realized_df["prior_return_pct"], "median"),
                "mean_entry_market_cap_bil_jpy": _series_stat(
                    realized_df["entry_market_cap_bil_jpy"], "mean"
                ),
                "mean_entry_adv": _series_stat(realized_df["entry_adv"], "mean"),
                "mean_followup_first_available_forecast_days": _series_stat(
                    realized_df["followup_first_available_forecast_days"], "mean"
                ),
                "mean_followup_first_positive_forecast_days": _series_stat(
                    realized_df["followup_first_positive_forecast_days"], "mean"
                ),
            }
        )

    summary_df = pd.DataFrame(records)
    if summary_df.empty:
        return _empty_result_df(columns)
    if category_order is not None:
        summary_df[group_col] = pd.Categorical(
            summary_df[group_col],
            categories=[value for value in category_order if value in set(summary_df[group_col])],
            ordered=True,
        )
        return summary_df.sort_values(group_col, kind="stable").reset_index(drop=True)
    return summary_df.sort_values(
        ["realized_event_count", "mean_return_pct", group_col],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)


def _build_year_summary_df(subgroup_event_df: pd.DataFrame) -> pd.DataFrame:
    return _build_grouped_summary_df(subgroup_event_df, group_col="disclosed_year")


def _build_recent_year_count_df(
    subgroup_event_df: pd.DataFrame,
    *,
    available_end_date: str | None,
    window_years: int,
) -> pd.DataFrame:
    columns = [
        "disclosed_year",
        "signed_code_count",
        "signed_event_count",
        "realized_event_count",
    ]
    if subgroup_event_df.empty or available_end_date is None or window_years <= 0:
        return _empty_result_df(columns)

    try:
        window_end_year = pd.Timestamp(available_end_date).year - 1
    except ValueError:
        return _empty_result_df(columns)
    if window_end_year <= 0:
        return _empty_result_df(columns)
    window_start_year = window_end_year - window_years + 1

    year_frame = subgroup_event_df.copy()
    year_frame["disclosed_year"] = pd.to_numeric(year_frame["disclosed_year"], errors="coerce")
    year_frame = year_frame[year_frame["disclosed_year"].notna()].copy()
    year_frame["disclosed_year"] = year_frame["disclosed_year"].astype(int)

    records: list[dict[str, Any]] = []
    for year in range(window_start_year, window_end_year + 1):
        year_df = year_frame[year_frame["disclosed_year"] == year].copy()
        records.append(
            {
                "disclosed_year": str(year),
                "signed_code_count": int(year_df["code"].astype(str).nunique()),
                "signed_event_count": int(len(year_df)),
                "realized_event_count": int((year_df["status"].astype(str) == "realized").sum()),
            }
        )
    return pd.DataFrame(records, columns=columns)


def _build_recent_year_count_stats_df(recent_year_count_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "window_start_year",
        "window_end_year",
        "year_count",
        "average_signed_code_count",
        "max_signed_code_count",
        "min_signed_code_count",
        "total_signed_code_count",
        "average_signed_event_count",
    ]
    if recent_year_count_df.empty:
        return _empty_result_df(columns)

    code_counts = pd.to_numeric(recent_year_count_df["signed_code_count"], errors="coerce").fillna(0)
    event_counts = pd.to_numeric(
        recent_year_count_df["signed_event_count"],
        errors="coerce",
    ).fillna(0)
    return pd.DataFrame(
        [
            {
                "window_start_year": str(recent_year_count_df.iloc[0]["disclosed_year"]),
                "window_end_year": str(recent_year_count_df.iloc[-1]["disclosed_year"]),
                "year_count": int(len(recent_year_count_df)),
                "average_signed_code_count": float(code_counts.mean()),
                "max_signed_code_count": int(code_counts.max()),
                "min_signed_code_count": int(code_counts.min()),
                "total_signed_code_count": int(code_counts.sum()),
                "average_signed_event_count": float(event_counts.mean()),
            }
        ],
        columns=columns,
    )


def _build_era_summary_df(subgroup_event_df: pd.DataFrame) -> pd.DataFrame:
    return _build_grouped_summary_df(
        subgroup_event_df,
        group_col="era_bucket",
        category_order=_ERA_BUCKET_ORDER,
    )


def _build_followup_forecast_summary_df(subgroup_event_df: pd.DataFrame) -> pd.DataFrame:
    return _build_grouped_summary_df(
        subgroup_event_df,
        group_col="followup_forecast_state",
        category_order=_FOLLOWUP_STATE_ORDER,
    )


def _build_forecast_resume_summary_df(subgroup_event_df: pd.DataFrame) -> pd.DataFrame:
    return _build_grouped_summary_df(
        subgroup_event_df,
        group_col="forecast_resume_group",
        category_order=_FORECAST_RESUME_ORDER,
    )


def _build_prior_return_bucket_summary_df(subgroup_event_df: pd.DataFrame) -> pd.DataFrame:
    return _build_grouped_summary_df(
        subgroup_event_df,
        group_col="prior_return_bucket",
        category_order=_PRIOR_RETURN_BUCKET_ORDER,
    )


def _build_market_cap_bucket_summary_df(subgroup_event_df: pd.DataFrame) -> pd.DataFrame:
    return _build_grouped_summary_df(
        subgroup_event_df,
        group_col="market_cap_bucket",
        category_order=_MARKET_CAP_BUCKET_ORDER,
    )


def _build_sector_summary_df(subgroup_event_df: pd.DataFrame) -> pd.DataFrame:
    return _build_grouped_summary_df(subgroup_event_df, group_col="sector_33_name")


def _build_horizon_summary_df(
    subgroup_event_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    columns = [
        "horizon_label",
        "available_event_count",
        "mean_return_pct",
        "median_return_pct",
        "q25_return_pct",
        "q75_return_pct",
        "win_rate_pct",
    ]
    realized_df = subgroup_event_df[subgroup_event_df["status"] == "realized"].copy()
    if realized_df.empty:
        return _empty_result_df(columns)

    records: list[dict[str, Any]] = []
    for horizon in horizons:
        horizon_col = f"h{horizon}_return_pct"
        available_df = realized_df[pd.to_numeric(realized_df[horizon_col], errors="coerce").notna()].copy()
        records.append(
            {
                "horizon_label": f"{horizon}d",
                "available_event_count": int(len(available_df)),
                "mean_return_pct": _series_stat(available_df[horizon_col], "mean"),
                "median_return_pct": _series_stat(available_df[horizon_col], "median"),
                "q25_return_pct": _series_stat(available_df[horizon_col], "q25"),
                "q75_return_pct": _series_stat(available_df[horizon_col], "q75"),
                "win_rate_pct": _bool_ratio_pct(pd.to_numeric(available_df[horizon_col], errors="coerce") > 0),
            }
        )

    records.append(
        {
            "horizon_label": "next_fy",
            "available_event_count": int(len(realized_df)),
            "mean_return_pct": _series_stat(realized_df["event_return_pct"], "mean"),
            "median_return_pct": _series_stat(realized_df["event_return_pct"], "median"),
            "q25_return_pct": _series_stat(realized_df["event_return_pct"], "q25"),
            "q75_return_pct": _series_stat(realized_df["event_return_pct"], "q75"),
            "win_rate_pct": _bool_ratio_pct(realized_df["event_return"] > 0),
        }
    )
    return pd.DataFrame(records, columns=columns)


def _build_feature_split_summary_df(subgroup_event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "feature_key",
        "feature_label",
        "feature_bucket",
        "available_event_count",
        "feature_mean_value",
        "feature_median_value",
        "mean_return_pct",
        "median_return_pct",
        "win_rate_pct",
        "split_reference_value",
    ]
    realized_df = subgroup_event_df[subgroup_event_df["status"] == "realized"].copy()
    if realized_df.empty:
        return _empty_result_df(columns)

    records: list[dict[str, Any]] = []
    for feature_key, feature_label in _FEATURE_SPLIT_SPECS:
        if feature_key not in realized_df.columns:
            continue
        available_df = realized_df[
            pd.to_numeric(realized_df[feature_key], errors="coerce").notna()
        ].copy()
        if len(available_df) < 2:
            continue
        available_df[feature_key] = pd.to_numeric(available_df[feature_key], errors="coerce")
        ordered_df = available_df.sort_values(feature_key, kind="stable").reset_index(drop=True)
        split_idx = max(1, len(ordered_df) // 2)
        lower_df = ordered_df.iloc[:split_idx].copy()
        upper_df = ordered_df.iloc[split_idx:].copy()
        if upper_df.empty:
            continue
        split_reference_value = _to_nullable_float(upper_df.iloc[0][feature_key])
        for bucket_label, bucket_df in (("lower_half", lower_df), ("upper_half", upper_df)):
            records.append(
                {
                    "feature_key": feature_key,
                    "feature_label": feature_label,
                    "feature_bucket": bucket_label,
                    "available_event_count": int(len(bucket_df)),
                    "feature_mean_value": _series_stat(bucket_df[feature_key], "mean"),
                    "feature_median_value": _series_stat(bucket_df[feature_key], "median"),
                    "mean_return_pct": _series_stat(bucket_df["event_return_pct"], "mean"),
                    "median_return_pct": _series_stat(bucket_df["event_return_pct"], "median"),
                    "win_rate_pct": _bool_ratio_pct(bucket_df["event_return"] > 0),
                    "split_reference_value": split_reference_value,
                }
            )
    return pd.DataFrame(records, columns=columns)


def _build_feature_effect_summary_df(feature_split_summary_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "feature_key",
        "feature_label",
        "split_reference_value",
        "lower_half_count",
        "upper_half_count",
        "lower_half_mean_return_pct",
        "upper_half_mean_return_pct",
        "mean_return_spread_pct",
        "lower_half_median_return_pct",
        "upper_half_median_return_pct",
        "median_return_spread_pct",
        "lower_half_win_rate_pct",
        "upper_half_win_rate_pct",
    ]
    if feature_split_summary_df.empty:
        return _empty_result_df(columns)

    records: list[dict[str, Any]] = []
    for feature_key, feature_df in feature_split_summary_df.groupby("feature_key", observed=True, sort=False):
        bucket_map = {
            str(row["feature_bucket"]): row
            for row in feature_df.to_dict(orient="records")
        }
        lower_row = bucket_map.get("lower_half")
        upper_row = bucket_map.get("upper_half")
        if lower_row is None or upper_row is None:
            continue
        lower_mean = _to_nullable_float(lower_row["mean_return_pct"])
        upper_mean = _to_nullable_float(upper_row["mean_return_pct"])
        lower_median = _to_nullable_float(lower_row["median_return_pct"])
        upper_median = _to_nullable_float(upper_row["median_return_pct"])
        records.append(
            {
                "feature_key": str(feature_key),
                "feature_label": str(lower_row["feature_label"]),
                "split_reference_value": _to_nullable_float(lower_row["split_reference_value"]),
                "lower_half_count": int(lower_row["available_event_count"]),
                "upper_half_count": int(upper_row["available_event_count"]),
                "lower_half_mean_return_pct": lower_mean,
                "upper_half_mean_return_pct": upper_mean,
                "mean_return_spread_pct": (
                    upper_mean - lower_mean
                    if lower_mean is not None and upper_mean is not None
                    else None
                ),
                "lower_half_median_return_pct": lower_median,
                "upper_half_median_return_pct": upper_median,
                "median_return_spread_pct": (
                    upper_median - lower_median
                    if lower_median is not None and upper_median is not None
                    else None
                ),
                "lower_half_win_rate_pct": _to_nullable_float(lower_row["win_rate_pct"]),
                "upper_half_win_rate_pct": _to_nullable_float(upper_row["win_rate_pct"]),
            }
        )
    summary_df = pd.DataFrame(records, columns=columns)
    if summary_df.empty:
        return summary_df
    return summary_df.sort_values(
        ["median_return_spread_pct", "mean_return_spread_pct", "feature_key"],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)


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
        return _empty_result_df(columns)

    ranked_df = realized_df.sort_values("event_return_pct", ascending=False, kind="stable").reset_index(drop=True)
    records: list[dict[str, Any]] = []
    for exclude_top_n in (0, 1, 3, 5, 10):
        trimmed_df = ranked_df.iloc[exclude_top_n:].copy()
        portfolio_total_return_pct = None
        portfolio_cagr_pct = None
        portfolio_max_drawdown_pct = None
        if not trimmed_df.empty:
            portfolio_daily_df = _build_portfolio_daily_df(
                event_ledger_df=trimmed_df,
                price_df=price_df,
            )
            portfolio_summary_df = _build_portfolio_summary_df(
                portfolio_daily_df=portfolio_daily_df,
                event_ledger_df=trimmed_df,
            )
            subgroup_portfolio_row = portfolio_summary_df[
                (portfolio_summary_df["group_key"].astype(str) == SUBGROUP_KEY)
                & (portfolio_summary_df["liquidity_filter"].astype(str) == "all_liquidity")
            ]
            if len(subgroup_portfolio_row) == 1:
                portfolio_row = subgroup_portfolio_row.iloc[0]
                portfolio_total_return_pct = _to_nullable_float(portfolio_row["total_return_pct"])
                portfolio_cagr_pct = _to_nullable_float(portfolio_row["cagr_pct"])
                portfolio_max_drawdown_pct = _to_nullable_float(portfolio_row["max_drawdown_pct"])
        records.append(
            {
                "exclude_top_n": exclude_top_n,
                "remaining_event_count": int(len(trimmed_df)),
                "mean_return_pct": _series_stat(trimmed_df["event_return_pct"], "mean"),
                "median_return_pct": _series_stat(trimmed_df["event_return_pct"], "median"),
                "win_rate_pct": _bool_ratio_pct(trimmed_df["event_return"] > 0),
                "portfolio_total_return_pct": portfolio_total_return_pct,
                "portfolio_cagr_pct": portfolio_cagr_pct,
                "portfolio_max_drawdown_pct": portfolio_max_drawdown_pct,
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
        "liquidity_state",
        "entry_market_cap_bil_jpy",
        "entry_adv",
        "followup_forecast_state",
        "followup_latest_forecast_eps",
        "followup_first_available_forecast_days",
        "followup_first_positive_forecast_days",
    ]
    realized_df = subgroup_event_df[subgroup_event_df["status"] == "realized"].copy()
    if realized_df.empty:
        return _empty_result_df(columns)
    return (
        realized_df.sort_values("event_return_pct", ascending=False, kind="stable")
        .head(limit)
        .reset_index(drop=True)[columns]
    )


def _build_summary_markdown(
    result: StandardMissingForecastCfoNonPositiveDeepDiveResult,
) -> str:
    lines = [
        f"# {_scope_label_for_market(result.selected_market)} Missing Forecast / CFO<=0 Deep Dive",
        "",
        "## Setup",
        "",
        f"- Selected scope: `{result.selected_market}`",
        f"- Base scope: `{result.base_scope_name}`",
        f"- Subgroup: `{result.subgroup_key}`",
        f"- Signed events: `{result.signed_event_count}`",
        f"- Realized events: `{result.realized_event_count}`",
        f"- Prior-return lookback: `{result.prior_sessions}` trading sessions",
        f"- Horizon returns: `{', '.join(str(value) + 'd' for value in result.horizons)}` plus `next_fy`",
        "",
        "## Top-N Exclusion",
        "",
    ]
    if _uses_current_scale_category_proxy(result.selected_market):
        lines[9:9] = [
            "- Scope proxy: latest `stocks.scale_category` snapshot",
            "- `topix500` / `primeExTopix500` are current-universe retrospective proxies, not historical committee reconstructions.",
            "",
        ]
    if result.top_exclusion_summary_df.empty:
        lines.append("- No realized events were available.")
    else:
        for row in result.top_exclusion_summary_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"exclude top `{int(row['exclude_top_n'])}`: "
                f"remaining `{int(row['remaining_event_count'])}`, "
                f"mean `{_fmt_num(row['mean_return_pct'])}%`, "
                f"median `{_fmt_num(row['median_return_pct'])}%`, "
                f"CAGR `{_fmt_num(row['portfolio_cagr_pct'])}%`, "
                f"max DD `{_fmt_num(row['portfolio_max_drawdown_pct'])}%`"
            )

    lines.extend(["", "## Vintage", ""])
    for label, df in (("Era", result.era_summary_df), ("Year", result.year_summary_df.head(8))):
        if df.empty:
            lines.append(f"- {label}: no data.")
            continue
        lines.append(f"- {label}:")
        for row in df.to_dict(orient="records"):
            lines.append(
                "  "
                + f"`{row[df.columns[0]]}` signed `{int(row['signed_event_count'])}` "
                + f"realized `{int(row['realized_event_count'])}` "
                + f"mean `{_fmt_num(row['mean_return_pct'])}%` "
                + f"median `{_fmt_num(row['median_return_pct'])}%`"
            )

    lines.extend(["", "## Trailing 10 Full Years", ""])
    if result.recent_year_count_stats_df.empty or result.recent_year_count_df.empty:
        lines.append("- No trailing-year count summary was available.")
    else:
        stats = result.recent_year_count_stats_df.iloc[0]
        lines.append(
            "- "
            f"`{stats['window_start_year']}-{stats['window_end_year']}` average "
            f"`{_fmt_num(stats['average_signed_code_count'])}` names/year, "
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
                f"mean `{_fmt_num(row['mean_return_pct'])}%`, "
                f"median `{_fmt_num(row['median_return_pct'])}%`, "
                f"first available days `{_fmt_num(row['mean_followup_first_available_forecast_days'])}`"
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
                + f"mean `{_fmt_num(row['mean_return_pct'])}%`, "
                + f"median `{_fmt_num(row['median_return_pct'])}%`"
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
                + f"mean `{_fmt_num(row['mean_return_pct'])}%` "
                + f"median `{_fmt_num(row['median_return_pct'])}%`"
            )

    lines.extend(["", "## Feature Splits", ""])
    if result.feature_effect_summary_df.empty:
        lines.append("- No feature-split summary was available.")
    else:
        for row in result.feature_effect_summary_df.head(7).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['feature_label']}` upper-half minus lower-half median "
                f"`{_fmt_num(row['median_return_spread_pct'])}%`, "
                f"mean spread `{_fmt_num(row['mean_return_spread_pct'])}%`, "
                f"split ref `{_fmt_num(row['split_reference_value'])}`"
            )

    lines.extend(["", "## Sectors", ""])
    if result.sector_summary_df.empty:
        lines.append("- No sector summary was available.")
    else:
        for row in result.sector_summary_df.head(8).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['sector_33_name']}`: "
                f"signed `{int(row['signed_event_count'])}`, "
                f"realized `{int(row['realized_event_count'])}`, "
                f"mean `{_fmt_num(row['mean_return_pct'])}%`, "
                f"median `{_fmt_num(row['median_return_pct'])}%`"
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
                f"mean `{_fmt_num(row['mean_return_pct'])}%`, "
                f"median `{_fmt_num(row['median_return_pct'])}%`, "
                f"win `{_fmt_num(row['win_rate_pct'])}%`"
            )

    lines.extend(["", "## Top Winners", ""])
    if result.top_winner_profile_df.empty:
        lines.append("- No top-winner profiles were available.")
    else:
        for row in result.top_winner_profile_df.head(10).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['code']}` {row['company_name']}: "
                f"return `{_fmt_num(row['event_return_pct'])}%`, "
                f"prior `{_fmt_num(row['prior_return_pct'])}%`, "
                f"mcap `{_fmt_num(row['entry_market_cap_bil_jpy'])}b`, "
                f"follow-up `{row['followup_forecast_state']}`"
            )

    return "\n".join(lines)


def _build_published_summary(
    result: StandardMissingForecastCfoNonPositiveDeepDiveResult,
) -> dict[str, Any]:
    return {
        "selectedMarket": result.selected_market,
        "baseScopeName": result.base_scope_name,
        "subgroupKey": result.subgroup_key,
        "advWindow": result.adv_window,
        "priorSessions": result.prior_sessions,
        "horizons": list(result.horizons),
        "recentYearWindow": result.recent_year_window,
        "signedEventCount": result.signed_event_count,
        "realizedEventCount": result.realized_event_count,
        "recentYearCountSummary": result.recent_year_count_stats_df.to_dict(orient="records"),
        "recentYearCountByYear": result.recent_year_count_df.to_dict(orient="records"),
        "topExclusionSummary": result.top_exclusion_summary_df.to_dict(orient="records"),
        "eraSummary": result.era_summary_df.to_dict(orient="records"),
        "followupForecastSummary": result.followup_forecast_summary_df.to_dict(orient="records"),
        "forecastResumeSummary": result.forecast_resume_summary_df.to_dict(orient="records"),
        "priorReturnBucketSummary": result.prior_return_bucket_summary_df.to_dict(orient="records"),
        "marketCapBucketSummary": result.market_cap_bucket_summary_df.to_dict(orient="records"),
        "horizonSummary": result.horizon_summary_df.to_dict(orient="records"),
        "featureEffectSummary": result.feature_effect_summary_df.to_dict(orient="records"),
    }


def run_standard_missing_forecast_cfo_non_positive_deep_dive(
    db_path: str,
    *,
    market: str = DEFAULT_MARKET,
    adv_window: int = DEFAULT_ADV_WINDOW,
    prior_sessions: int = DEFAULT_PRIOR_SESSIONS,
    horizons: Sequence[int] | None = None,
    recent_year_window: int = DEFAULT_RECENT_YEAR_WINDOW,
) -> StandardMissingForecastCfoNonPositiveDeepDiveResult:
    if prior_sessions <= 0:
        raise ValueError("prior_sessions must be positive")
    if recent_year_window <= 0:
        raise ValueError("recent_year_window must be positive")
    selected_market = _normalize_market(market)
    resolved_horizons = _normalize_horizons(horizons)
    base_market = _base_market_for_scope(selected_market)
    selected_scope_name = _scope_name_for_market(selected_market)

    base_result = run_standard_negative_eps_right_tail_decomposition(
        db_path,
        market=base_market,
        adv_window=adv_window,
    )
    subgroup_event_df = base_result.event_ledger_df[
        base_result.event_ledger_df["group_key"].astype(str) == SUBGROUP_KEY
    ].copy()

    if subgroup_event_df.empty:
        empty_df = _empty_result_df([])
        return StandardMissingForecastCfoNonPositiveDeepDiveResult(
            db_path=db_path,
            selected_market=selected_market,
            source_mode=base_result.source_mode,
            source_detail=base_result.source_detail,
            available_start_date=base_result.available_start_date,
            available_end_date=base_result.available_end_date,
            analysis_start_date=None,
            analysis_end_date=None,
            base_scope_name=selected_scope_name,
            subgroup_key=SUBGROUP_KEY,
            adv_window=adv_window,
            prior_sessions=prior_sessions,
            horizons=resolved_horizons,
            recent_year_window=recent_year_window,
            signed_event_count=0,
            realized_event_count=0,
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

    market_codes = _query_market_codes(base_market)
    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start_date, available_end_date = _fetch_date_range(conn, table_name="stock_data")
        stock_df = _query_scope_canonical_stocks(conn, market_codes=market_codes)
        scoped_stock_df = _filter_stock_scope(stock_df, selected_market=selected_market)
        scoped_codes = set(scoped_stock_df["code"].astype(str))
        subgroup_event_df = subgroup_event_df[
            subgroup_event_df["code"].astype(str).isin(scoped_codes)
        ].copy()
        if selected_market in {"topix500", "primeExTopix500"} and not subgroup_event_df.empty:
            subgroup_event_df["market"] = selected_market
        signed_event_count = int(len(subgroup_event_df))
        realized_event_count = (
            int((subgroup_event_df["status"].astype(str) == "realized").sum())
            if not subgroup_event_df.empty
            else 0
        )
        if subgroup_event_df.empty:
            empty_df = _empty_result_df([])
            return StandardMissingForecastCfoNonPositiveDeepDiveResult(
                db_path=db_path,
                selected_market=selected_market,
                source_mode=base_result.source_mode,
                source_detail=base_result.source_detail,
                available_start_date=available_start_date,
                available_end_date=available_end_date,
                analysis_start_date=None,
                analysis_end_date=None,
                base_scope_name=selected_scope_name,
                subgroup_key=SUBGROUP_KEY,
                adv_window=adv_window,
                prior_sessions=prior_sessions,
                horizons=resolved_horizons,
                recent_year_window=recent_year_window,
                signed_event_count=0,
                realized_event_count=0,
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
        analysis_start_date = str(subgroup_event_df["disclosed_date"].min())
        exit_candidates = subgroup_event_df["exit_date"].dropna().astype(str)
        analysis_end_date = str(exit_candidates.max()) if not exit_candidates.empty else None
        min_price_date = (
            pd.Timestamp(analysis_start_date)
            - pd.Timedelta(days=max(prior_sessions * 3, max(resolved_horizons) * 3, 400))
        ).strftime("%Y-%m-%d")
        end_candidates = [
            str(value)
            for value in subgroup_event_df["next_fy_disclosed_date"].dropna().astype(str).tolist()
            + subgroup_event_df["exit_date"].dropna().astype(str).tolist()
        ]
        requested_price_end_date = max(end_candidates) if end_candidates else (
            analysis_end_date or base_result.available_end_date
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

    enriched_subgroup_df = _enrich_subgroup_events(
        subgroup_event_df,
        price_df=price_df,
        statement_df=statement_df,
        prior_sessions=prior_sessions,
        horizons=resolved_horizons,
    )
    year_summary_df = _build_year_summary_df(enriched_subgroup_df)
    recent_year_count_df = _build_recent_year_count_df(
        enriched_subgroup_df,
        available_end_date=available_end_date,
        window_years=recent_year_window,
    )
    recent_year_count_stats_df = _build_recent_year_count_stats_df(recent_year_count_df)
    era_summary_df = _build_era_summary_df(enriched_subgroup_df)
    top_exclusion_summary_df = _build_top_exclusion_summary_df(
        enriched_subgroup_df,
        price_df=price_df,
    )
    followup_forecast_summary_df = _build_followup_forecast_summary_df(enriched_subgroup_df)
    forecast_resume_summary_df = _build_forecast_resume_summary_df(enriched_subgroup_df)
    prior_return_bucket_summary_df = _build_prior_return_bucket_summary_df(enriched_subgroup_df)
    market_cap_bucket_summary_df = _build_market_cap_bucket_summary_df(enriched_subgroup_df)
    sector_summary_df = _build_sector_summary_df(enriched_subgroup_df)
    horizon_summary_df = _build_horizon_summary_df(
        enriched_subgroup_df,
        horizons=resolved_horizons,
    )
    feature_split_summary_df = _build_feature_split_summary_df(enriched_subgroup_df)
    feature_effect_summary_df = _build_feature_effect_summary_df(feature_split_summary_df)
    top_winner_profile_df = _build_top_winner_profile_df(enriched_subgroup_df)

    return StandardMissingForecastCfoNonPositiveDeepDiveResult(
        db_path=db_path,
        selected_market=selected_market,
        source_mode=base_result.source_mode,
        source_detail=base_result.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        base_scope_name=selected_scope_name,
        subgroup_key=SUBGROUP_KEY,
        adv_window=adv_window,
        prior_sessions=prior_sessions,
        horizons=resolved_horizons,
        recent_year_window=recent_year_window,
        signed_event_count=signed_event_count,
        realized_event_count=realized_event_count,
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


def write_standard_missing_forecast_cfo_non_positive_deep_dive_bundle(
    result: StandardMissingForecastCfoNonPositiveDeepDiveResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=_experiment_id_for_market(result.selected_market),
        module=__name__,
        function="run_standard_missing_forecast_cfo_non_positive_deep_dive",
        params={
            "market": result.selected_market,
            "adv_window": result.adv_window,
            "prior_sessions": result.prior_sessions,
            "horizons": list(result.horizons),
            "recent_year_window": result.recent_year_window,
            "subgroup_key": result.subgroup_key,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_standard_missing_forecast_cfo_non_positive_deep_dive_bundle(
    bundle_path: str | Path,
) -> StandardMissingForecastCfoNonPositiveDeepDiveResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=StandardMissingForecastCfoNonPositiveDeepDiveResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_standard_missing_forecast_cfo_non_positive_deep_dive_latest_bundle_path(
    *,
    market: str = DEFAULT_MARKET,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        _experiment_id_for_market(market),
        output_root=output_root,
    )


def get_standard_missing_forecast_cfo_non_positive_deep_dive_bundle_path_for_run_id(
    run_id: str,
    *,
    market: str = DEFAULT_MARKET,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        _experiment_id_for_market(market),
        run_id,
        output_root=output_root,
    )
