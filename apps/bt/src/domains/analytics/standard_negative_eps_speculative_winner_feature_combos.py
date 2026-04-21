"""Mine pre-entry feature combos for speculative winners in standard EPS<0 cohorts."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any

import pandas as pd

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
    _group_label,
    _open_analysis_connection,
    _query_market_codes,
    _query_price_rows,
    _query_statement_rows,
    _to_nullable_float,
    run_standard_negative_eps_right_tail_decomposition,
)

STANDARD_NEGATIVE_EPS_SPECULATIVE_WINNER_FEATURE_COMBOS_EXPERIMENT_ID = (
    "market-behavior/standard-negative-eps-speculative-winner-feature-combos"
)
DEFAULT_WINNER_QUANTILE = 0.9
DEFAULT_MIN_EVENT_COUNT = 15
DEFAULT_MIN_WINNER_COUNT = 3
DEFAULT_TOP_EXAMPLES_LIMIT = 20
DEFAULT_SPARSE_SECTOR_MIN_EVENT_COUNT = 15
DEFAULT_VOLATILITY_WINDOW = 20
DEFAULT_VOLUME_RATIO_WINDOW = 20
DEFAULT_VOLUME_RATIO_BASELINE_WINDOW = 252
DEFAULT_PRIOR_RETURN_WINDOWS: tuple[int, ...] = (20, 63, 252)
DEFAULT_SCOPE_NAME = "standard / FY actual EPS < 0"
TARGET_COHORT_KEYS: tuple[str, ...] = (
    "forecast_positive__cfo_positive",
    "forecast_missing__cfo_non_positive",
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "winner_threshold_df",
    "feature_bucket_def_df",
    "event_feature_df",
    "single_feature_summary_df",
    "pair_combo_summary_df",
    "triplet_combo_summary_df",
    "group_comparison_df",
    "top_examples_df",
)


@dataclass(frozen=True)
class FeatureSpec:
    identifier: str
    label: str
    bucket_column: str
    bucket_order: tuple[str, ...] | None = None


_FEATURE_SPECS: tuple[FeatureSpec, ...] = (
    FeatureSpec(
        identifier="entry_market_cap_bil_jpy",
        label="Entry market cap (JPY bn)",
        bucket_column="entry_market_cap_bucket",
        bucket_order=("<5b", "5b-20b", "20b-50b", "50b-200b", ">=200b", "missing"),
    ),
    FeatureSpec(
        identifier="entry_adv",
        label="Entry ADV (JPY)",
        bucket_column="entry_adv_bucket",
        bucket_order=("<5m", "5m-20m", "20m-100m", "100m-500m", ">=500m", "missing"),
    ),
    FeatureSpec(
        identifier="entry_open",
        label="Entry open (JPY)",
        bucket_column="entry_open_bucket",
        bucket_order=("<100", "100-300", "300-1000", ">=1000", "missing"),
    ),
    FeatureSpec(
        identifier="prior_252d_return_pct",
        label="Prior 252d return (%)",
        bucket_column="prior_252d_return_bucket",
        bucket_order=("-80% to -50%", "-50% to -20%", ">-20%", "missing"),
    ),
    FeatureSpec(
        identifier="prior_20d_return_pct",
        label="Prior 20d return (%)",
        bucket_column="prior_20d_return_bucket",
        bucket_order=("<=-30%", "-30% to 0%", ">0%", "missing"),
    ),
    FeatureSpec(
        identifier="prior_63d_return_pct",
        label="Prior 63d return (%)",
        bucket_column="prior_63d_return_bucket",
        bucket_order=("<=-50%", "-50% to -10%", ">-10%", "missing"),
    ),
    FeatureSpec(
        identifier="volume_ratio_20d",
        label="Volume ratio 20d",
        bucket_column="volume_ratio_20d_bucket",
        bucket_order=("<0.7", "0.7-1.5", ">1.5", "missing"),
    ),
    FeatureSpec(
        identifier="pre_entry_volatility_20d",
        label="Pre-entry volatility 20d (%)",
        bucket_column="pre_entry_volatility_20d_bucket",
        bucket_order=("low", "mid", "high", "missing"),
    ),
    FeatureSpec(
        identifier="equity_ratio_pct",
        label="Equity ratio (%)",
        bucket_column="equity_ratio_bucket",
        bucket_order=("<30%", "30-50%", ">=50%", "missing"),
    ),
    FeatureSpec(
        identifier="profit_margin_pct",
        label="Profit margin (%)",
        bucket_column="profit_margin_bucket",
        bucket_order=("<=0%", "0-5%", ">5%", "missing"),
    ),
    FeatureSpec(
        identifier="cfo_margin_pct",
        label="CFO margin (%)",
        bucket_column="cfo_margin_bucket",
        bucket_order=("<=0%", "0-10%", ">10%", "missing"),
    ),
    FeatureSpec(
        identifier="sector_33_name",
        label="Sector (33)",
        bucket_column="sector_bucket",
        bucket_order=None,
    ),
)
_FEATURE_SPEC_BY_KEY: dict[str, FeatureSpec] = {spec.identifier: spec for spec in _FEATURE_SPECS}
_FEATURE_ORDER: dict[str, int] = {spec.identifier: idx for idx, spec in enumerate(_FEATURE_SPECS)}
_EVENT_FEATURE_COLUMNS: tuple[str, ...] = (
    "event_id",
    "code",
    "company_name",
    "market",
    "market_code",
    "sector_33_name",
    "sector_bucket",
    "listed_date",
    "disclosed_date",
    "disclosed_year",
    "fy_cycle_key",
    "next_fy_disclosed_date",
    "entry_date",
    "exit_date",
    "holding_trading_days",
    "holding_calendar_days",
    "cohort_key",
    "cohort_label",
    "winner_cutoff_pct",
    "winner_quantile",
    "is_winner",
    "event_return_pct",
    "actual_eps",
    "forecast_eps",
    "operating_cash_flow",
    "baseline_shares",
    "entry_open",
    "entry_close",
    "exit_close",
    "entry_adv",
    "entry_adv_bucket",
    "entry_adv_window_observations",
    "entry_market_cap_bil_jpy",
    "entry_market_cap_bucket",
    "entry_open_bucket",
    "prior_20d_return_pct",
    "prior_20d_return_bucket",
    "prior_63d_return_pct",
    "prior_63d_return_bucket",
    "prior_252d_return_pct",
    "prior_252d_return_bucket",
    "volume_ratio_20d",
    "volume_ratio_20d_bucket",
    "pre_entry_volatility_20d",
    "pre_entry_volatility_20d_bucket",
    "profit",
    "sales",
    "equity",
    "total_assets",
    "profit_margin_pct",
    "profit_margin_bucket",
    "cfo_margin_pct",
    "cfo_margin_bucket",
    "equity_ratio_pct",
    "equity_ratio_bucket",
)
_SUMMARY_COLUMNS: tuple[str, ...] = (
    "cohort_key",
    "cohort_label",
    "feature_count",
    "feature_keys",
    "feature_labels",
    "combo_key",
    "combo_label",
    "feature_1_key",
    "feature_1_label",
    "feature_1_bucket_label",
    "feature_2_key",
    "feature_2_label",
    "feature_2_bucket_label",
    "feature_3_key",
    "feature_3_label",
    "feature_3_bucket_label",
    "event_count",
    "winner_count",
    "winner_hit_rate",
    "lift_vs_base_rate",
    "winner_capture_rate",
    "mean_return_pct",
    "median_return_pct",
    "cohort_base_winner_rate",
    "rank_within_cohort",
)


@dataclass(frozen=True)
class StandardNegativeEpsSpeculativeWinnerFeatureCombosResult:
    db_path: str
    selected_market: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    scope_name: str
    adv_window: int
    winner_quantile: float
    min_event_count: int
    min_winner_count: int
    top_examples_limit: int
    sparse_sector_min_event_count: int
    winner_threshold_df: pd.DataFrame
    feature_bucket_def_df: pd.DataFrame
    event_feature_df: pd.DataFrame
    single_feature_summary_df: pd.DataFrame
    pair_combo_summary_df: pd.DataFrame
    triplet_combo_summary_df: pd.DataFrame
    group_comparison_df: pd.DataFrame
    top_examples_df: pd.DataFrame


def _empty_result_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _normalize_market(market: str) -> str:
    normalized = str(market).strip().lower()
    if normalized != DEFAULT_MARKET:
        raise ValueError("This study currently supports only the standard market.")
    return normalized


def _series_stat(series: pd.Series, fn: str) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    if fn == "mean":
        return float(numeric.mean())
    if fn == "median":
        return float(numeric.median())
    raise ValueError(f"Unsupported fn: {fn}")


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


def _compute_volume_ratio_20d(
    price_frame: pd.DataFrame,
    *,
    entry_idx: int,
    short_window: int = DEFAULT_VOLUME_RATIO_WINDOW,
    baseline_window: int = DEFAULT_VOLUME_RATIO_BASELINE_WINDOW,
) -> tuple[float | None, int]:
    if entry_idx <= 0:
        return None, 0
    history_df = price_frame.iloc[:entry_idx].copy()
    if history_df.empty:
        return None, 0
    volume_series = pd.to_numeric(history_df["volume"], errors="coerce").dropna()
    if volume_series.empty:
        return None, 0
    short_series = volume_series.iloc[-min(len(volume_series), short_window) :]
    baseline_series = volume_series.iloc[-min(len(volume_series), baseline_window) :]
    if short_series.empty or baseline_series.empty:
        return None, 0
    baseline_mean = float(baseline_series.mean())
    if not math.isfinite(baseline_mean) or math.isclose(baseline_mean, 0.0, abs_tol=1e-12):
        return None, 0
    return float(short_series.mean() / baseline_mean), int(len(short_series))


def _compute_pre_entry_volatility_20d(
    price_frame: pd.DataFrame,
    *,
    entry_idx: int,
    window: int = DEFAULT_VOLATILITY_WINDOW,
) -> tuple[float | None, int]:
    if entry_idx <= 1:
        return None, 0
    history_df = price_frame.iloc[:entry_idx].copy()
    if len(history_df) < 2:
        return None, 0
    close_series = pd.to_numeric(history_df["close"], errors="coerce")
    returns = close_series.pct_change().replace([math.inf, -math.inf], pd.NA).dropna()
    if returns.empty:
        return None, 0
    returns = returns.iloc[-min(len(returns), window) :]
    if len(returns) < 2:
        return None, int(len(returns))
    daily_vol = float(returns.std(ddof=0))
    if not math.isfinite(daily_vol):
        return None, int(len(returns))
    return float(daily_vol * math.sqrt(252.0) * 100.0), int(len(returns))


def _bucket_entry_market_cap(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 5.0:
        return "<5b"
    if numeric < 20.0:
        return "5b-20b"
    if numeric < 50.0:
        return "20b-50b"
    if numeric < 200.0:
        return "50b-200b"
    return ">=200b"


def _bucket_entry_adv(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 5_000_000.0:
        return "<5m"
    if numeric < 20_000_000.0:
        return "5m-20m"
    if numeric < 100_000_000.0:
        return "20m-100m"
    if numeric < 500_000_000.0:
        return "100m-500m"
    return ">=500m"


def _bucket_entry_open(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 100.0:
        return "<100"
    if numeric < 300.0:
        return "100-300"
    if numeric < 1000.0:
        return "300-1000"
    return ">=1000"


def _bucket_prior_252d_return(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric <= -50.0:
        return "-80% to -50%"
    if numeric <= -20.0:
        return "-50% to -20%"
    return ">-20%"


def _bucket_prior_20d_return(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric <= -30.0:
        return "<=-30%"
    if numeric <= 0.0:
        return "-30% to 0%"
    return ">0%"


def _bucket_prior_63d_return(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric <= -50.0:
        return "<=-50%"
    if numeric <= -10.0:
        return "-50% to -10%"
    return ">-10%"


def _bucket_volume_ratio_20d(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 0.7:
        return "<0.7"
    if numeric <= 1.5:
        return "0.7-1.5"
    return ">1.5"


def _bucket_pre_entry_volatility_20d(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 40.0:
        return "low"
    if numeric < 80.0:
        return "mid"
    return "high"


def _bucket_equity_ratio(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric < 30.0:
        return "<30%"
    if numeric < 50.0:
        return "30-50%"
    return ">=50%"


def _bucket_profit_margin(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric <= 0.0:
        return "<=0%"
    if numeric <= 5.0:
        return "0-5%"
    return ">5%"


def _bucket_cfo_margin(value: float | None) -> str:
    numeric = _to_nullable_float(value)
    if numeric is None:
        return "missing"
    if numeric <= 0.0:
        return "<=0%"
    if numeric <= 10.0:
        return "0-10%"
    return ">10%"


def _bucket_sector_name(value: str | None, *, keep_named: bool) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return "missing"
    if keep_named:
        return normalized
    return "other"


def _build_feature_bucket_def_df(
    *,
    sparse_sector_min_event_count: int,
) -> pd.DataFrame:
    rows = [
        {
            "feature_key": "entry_market_cap_bil_jpy",
            "bucket_label": "<5b",
            "definition": "entry market cap < 5bn JPY",
        },
        {
            "feature_key": "entry_market_cap_bil_jpy",
            "bucket_label": "5b-20b",
            "definition": "5bn <= entry market cap < 20bn JPY",
        },
        {
            "feature_key": "entry_market_cap_bil_jpy",
            "bucket_label": "20b-50b",
            "definition": "20bn <= entry market cap < 50bn JPY",
        },
        {
            "feature_key": "entry_market_cap_bil_jpy",
            "bucket_label": "50b-200b",
            "definition": "50bn <= entry market cap < 200bn JPY",
        },
        {
            "feature_key": "entry_market_cap_bil_jpy",
            "bucket_label": ">=200b",
            "definition": "entry market cap >= 200bn JPY",
        },
        {
            "feature_key": "entry_adv",
            "bucket_label": "<5m",
            "definition": "entry ADV < 5m JPY",
        },
        {
            "feature_key": "entry_adv",
            "bucket_label": "5m-20m",
            "definition": "5m <= entry ADV < 20m JPY",
        },
        {
            "feature_key": "entry_adv",
            "bucket_label": "20m-100m",
            "definition": "20m <= entry ADV < 100m JPY",
        },
        {
            "feature_key": "entry_adv",
            "bucket_label": "100m-500m",
            "definition": "100m <= entry ADV < 500m JPY",
        },
        {
            "feature_key": "entry_adv",
            "bucket_label": ">=500m",
            "definition": "entry ADV >= 500m JPY",
        },
        {
            "feature_key": "entry_open",
            "bucket_label": "<100",
            "definition": "entry open < 100 JPY",
        },
        {
            "feature_key": "entry_open",
            "bucket_label": "100-300",
            "definition": "100 <= entry open < 300 JPY",
        },
        {
            "feature_key": "entry_open",
            "bucket_label": "300-1000",
            "definition": "300 <= entry open < 1000 JPY",
        },
        {
            "feature_key": "entry_open",
            "bucket_label": ">=1000",
            "definition": "entry open >= 1000 JPY",
        },
        {
            "feature_key": "prior_252d_return_pct",
            "bucket_label": "-80% to -50%",
            "definition": "prior 252-session return <= -50%",
        },
        {
            "feature_key": "prior_252d_return_pct",
            "bucket_label": "-50% to -20%",
            "definition": "-50% < prior 252-session return <= -20%",
        },
        {
            "feature_key": "prior_252d_return_pct",
            "bucket_label": ">-20%",
            "definition": "prior 252-session return > -20%",
        },
        {
            "feature_key": "prior_20d_return_pct",
            "bucket_label": "<=-30%",
            "definition": "prior 20-session return <= -30%",
        },
        {
            "feature_key": "prior_20d_return_pct",
            "bucket_label": "-30% to 0%",
            "definition": "-30% < prior 20-session return <= 0%",
        },
        {
            "feature_key": "prior_20d_return_pct",
            "bucket_label": ">0%",
            "definition": "prior 20-session return > 0%",
        },
        {
            "feature_key": "prior_63d_return_pct",
            "bucket_label": "<=-50%",
            "definition": "prior 63-session return <= -50%",
        },
        {
            "feature_key": "prior_63d_return_pct",
            "bucket_label": "-50% to -10%",
            "definition": "-50% < prior 63-session return <= -10%",
        },
        {
            "feature_key": "prior_63d_return_pct",
            "bucket_label": ">-10%",
            "definition": "prior 63-session return > -10%",
        },
        {
            "feature_key": "volume_ratio_20d",  # gitleaks:allow - feature identifier
            "bucket_label": "<0.7",
            "definition": "trailing 20-session mean volume / trailing 252-session mean volume < 0.7",
        },
        {
            "feature_key": "volume_ratio_20d",  # gitleaks:allow - feature identifier
            "bucket_label": "0.7-1.5",
            "definition": "0.7 <= trailing 20-session mean volume / trailing 252-session mean volume <= 1.5",
        },
        {
            "feature_key": "volume_ratio_20d",  # gitleaks:allow - feature identifier
            "bucket_label": ">1.5",
            "definition": "trailing 20-session mean volume / trailing 252-session mean volume > 1.5",
        },
        {
            "feature_key": "pre_entry_volatility_20d",
            "bucket_label": "low",
            "definition": "annualized trailing 20-session close volatility < 40%",
        },
        {
            "feature_key": "pre_entry_volatility_20d",
            "bucket_label": "mid",
            "definition": "40% <= annualized trailing 20-session close volatility < 80%",
        },
        {
            "feature_key": "pre_entry_volatility_20d",
            "bucket_label": "high",
            "definition": "annualized trailing 20-session close volatility >= 80%",
        },
        {
            "feature_key": "equity_ratio_pct",
            "bucket_label": "<30%",
            "definition": "equity / total assets < 30%",
        },
        {
            "feature_key": "equity_ratio_pct",
            "bucket_label": "30-50%",
            "definition": "30% <= equity / total assets < 50%",
        },
        {
            "feature_key": "equity_ratio_pct",
            "bucket_label": ">=50%",
            "definition": "equity / total assets >= 50%",
        },
        {
            "feature_key": "profit_margin_pct",
            "bucket_label": "<=0%",
            "definition": "profit / sales <= 0%",
        },
        {
            "feature_key": "profit_margin_pct",
            "bucket_label": "0-5%",
            "definition": "0% < profit / sales <= 5%",
        },
        {
            "feature_key": "profit_margin_pct",
            "bucket_label": ">5%",
            "definition": "profit / sales > 5%",
        },
        {
            "feature_key": "cfo_margin_pct",
            "bucket_label": "<=0%",
            "definition": "operating cash flow / sales <= 0%",
        },
        {
            "feature_key": "cfo_margin_pct",
            "bucket_label": "0-10%",
            "definition": "0% < operating cash flow / sales <= 10%",
        },
        {
            "feature_key": "cfo_margin_pct",
            "bucket_label": ">10%",
            "definition": "operating cash flow / sales > 10%",
        },
        {
            "feature_key": "sector_33_name",
            "bucket_label": "named sector",
            "definition": (
                "keep sector_33_name when the realized-event count across the study is "
                f">= {sparse_sector_min_event_count}"
            ),
        },
        {
            "feature_key": "sector_33_name",
            "bucket_label": "other",
            "definition": (
                "collapse sector_33_name into other when the realized-event count across the study is "
                f"< {sparse_sector_min_event_count}"
            ),
        },
    ]
    missing_features = (
        "entry_market_cap_bil_jpy",
        "entry_adv",
        "entry_open",
        "prior_252d_return_pct",
        "prior_20d_return_pct",
        "prior_63d_return_pct",
        "volume_ratio_20d",
        "pre_entry_volatility_20d",
        "equity_ratio_pct",
        "profit_margin_pct",
        "cfo_margin_pct",
        "sector_33_name",
    )
    for feature_key in missing_features:
        rows.append(
            {
                "feature_key": feature_key,
                "bucket_label": "missing",
                "definition": "value unavailable at FY disclosure or before entry",
            }
        )
    return pd.DataFrame(rows, columns=["feature_key", "bucket_label", "definition"])


def _build_winner_threshold_df(
    realized_event_df: pd.DataFrame,
    *,
    winner_quantile: float,
) -> pd.DataFrame:
    columns = [
        "cohort_key",
        "cohort_label",
        "winner_quantile",
        "winner_cutoff_pct",
        "realized_event_count",
        "winner_event_count",
        "winner_base_rate",
    ]
    if realized_event_df.empty:
        return _empty_result_df(columns)

    records: list[dict[str, Any]] = []
    for cohort_key in TARGET_COHORT_KEYS:
        cohort_df = realized_event_df[realized_event_df["group_key"].astype(str) == cohort_key].copy()
        if cohort_df.empty:
            continue
        returns = pd.to_numeric(cohort_df["event_return_pct"], errors="coerce").dropna()
        if returns.empty:
            continue
        winner_cutoff_pct = float(returns.quantile(winner_quantile))
        winner_event_count = int((pd.to_numeric(cohort_df["event_return_pct"], errors="coerce") >= winner_cutoff_pct).sum())
        realized_event_count = int(len(cohort_df))
        winner_base_rate = (winner_event_count / realized_event_count) * 100.0
        records.append(
            {
                "cohort_key": cohort_key,
                "cohort_label": _group_label(cohort_key),
                "winner_quantile": float(winner_quantile),
                "winner_cutoff_pct": winner_cutoff_pct,
                "realized_event_count": realized_event_count,
                "winner_event_count": winner_event_count,
                "winner_base_rate": winner_base_rate,
            }
        )
    return pd.DataFrame(records, columns=columns)


def _attach_winner_labels(
    realized_event_df: pd.DataFrame,
    *,
    threshold_df: pd.DataFrame,
) -> pd.DataFrame:
    if realized_event_df.empty or threshold_df.empty:
        return realized_event_df.copy()
    threshold_lookup = {
        str(row["cohort_key"]): row for row in threshold_df.to_dict(orient="records")
    }
    labeled = realized_event_df.copy()
    labeled["cohort_key"] = labeled["group_key"].astype(str)
    labeled["cohort_label"] = labeled["cohort_key"].map(_group_label)
    labeled["winner_cutoff_pct"] = labeled["cohort_key"].map(
        lambda key: _to_nullable_float(threshold_lookup.get(str(key), {}).get("winner_cutoff_pct"))
    )
    labeled["winner_quantile"] = labeled["cohort_key"].map(
        lambda key: _to_nullable_float(threshold_lookup.get(str(key), {}).get("winner_quantile"))
    )
    labeled["is_winner"] = (
        pd.to_numeric(labeled["event_return_pct"], errors="coerce")
        >= pd.to_numeric(labeled["winner_cutoff_pct"], errors="coerce")
    )
    return labeled


def _enrich_realized_events(
    event_df: pd.DataFrame,
    *,
    price_df: pd.DataFrame,
    statement_df: pd.DataFrame,
) -> pd.DataFrame:
    if event_df.empty:
        return _empty_result_df(_EVENT_FEATURE_COLUMNS)

    statement_snapshot_by_key = {
        (str(row["code"]), str(row["disclosed_date"])): row
        for row in statement_df.to_dict(orient="records")
    }
    price_by_code = {
        str(code): frame.sort_values("date", kind="stable").reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    records: list[dict[str, Any]] = []
    for row in event_df.to_dict(orient="records"):
        code = str(row["code"])
        disclosed_date = str(row["disclosed_date"])
        price_frame = price_by_code.get(code)
        statement_snapshot = statement_snapshot_by_key.get((code, disclosed_date), {})
        entry_open = _to_nullable_float(row.get("entry_open"))
        baseline_shares = _to_nullable_float(row.get("baseline_shares"))
        raw_profit = _to_nullable_float(statement_snapshot.get("profit"))
        raw_sales = _to_nullable_float(statement_snapshot.get("sales"))
        raw_equity = _to_nullable_float(statement_snapshot.get("equity"))
        raw_total_assets = _to_nullable_float(statement_snapshot.get("total_assets"))
        operating_cash_flow = _to_nullable_float(row.get("operating_cash_flow"))
        enriched = {str(key): value for key, value in row.items()}
        enriched["profit"] = raw_profit
        enriched["sales"] = raw_sales
        enriched["equity"] = raw_equity
        enriched["total_assets"] = raw_total_assets
        enriched["entry_market_cap_bil_jpy"] = None
        enriched["prior_20d_return_pct"] = None
        enriched["prior_63d_return_pct"] = None
        enriched["prior_252d_return_pct"] = None
        enriched["volume_ratio_20d"] = None
        enriched["pre_entry_volatility_20d"] = None
        enriched["profit_margin_pct"] = _ratio_pct(raw_profit, raw_sales)
        enriched["cfo_margin_pct"] = _ratio_pct(operating_cash_flow, raw_sales)
        enriched["equity_ratio_pct"] = _ratio_pct(raw_equity, raw_total_assets)
        enriched["entry_market_cap_bucket"] = "missing"
        enriched["entry_adv_bucket"] = _bucket_entry_adv(_to_nullable_float(row.get("entry_adv")))
        enriched["entry_open_bucket"] = _bucket_entry_open(entry_open)
        enriched["prior_20d_return_bucket"] = "missing"
        enriched["prior_63d_return_bucket"] = "missing"
        enriched["prior_252d_return_bucket"] = "missing"
        enriched["volume_ratio_20d_bucket"] = "missing"
        enriched["pre_entry_volatility_20d_bucket"] = "missing"
        enriched["profit_margin_bucket"] = _bucket_profit_margin(enriched["profit_margin_pct"])
        enriched["cfo_margin_bucket"] = _bucket_cfo_margin(enriched["cfo_margin_pct"])
        enriched["equity_ratio_bucket"] = _bucket_equity_ratio(enriched["equity_ratio_pct"])
        enriched["sector_bucket"] = "missing"

        if (
            baseline_shares is not None
            and entry_open is not None
            and math.isfinite(baseline_shares)
            and math.isfinite(entry_open)
        ):
            market_cap_bil = entry_open * baseline_shares / 1_000_000_000.0
            enriched["entry_market_cap_bil_jpy"] = market_cap_bil
            enriched["entry_market_cap_bucket"] = _bucket_entry_market_cap(market_cap_bil)

        if price_frame is not None and not price_frame.empty:
            entry_idx = _locate_date_index(price_frame, str(row.get("entry_date")))
            if entry_idx is not None:
                for window in DEFAULT_PRIOR_RETURN_WINDOWS:
                    prior_return_pct, _ = _compute_prior_return_pct(
                        price_frame,
                        entry_idx=entry_idx,
                        prior_sessions=window,
                    )
                    enriched[f"prior_{window}d_return_pct"] = prior_return_pct
                enriched["prior_20d_return_bucket"] = _bucket_prior_20d_return(
                    enriched["prior_20d_return_pct"]
                )
                enriched["prior_63d_return_bucket"] = _bucket_prior_63d_return(
                    enriched["prior_63d_return_pct"]
                )
                enriched["prior_252d_return_bucket"] = _bucket_prior_252d_return(
                    enriched["prior_252d_return_pct"]
                )
                volume_ratio_20d, _ = _compute_volume_ratio_20d(price_frame, entry_idx=entry_idx)
                enriched["volume_ratio_20d"] = volume_ratio_20d
                enriched["volume_ratio_20d_bucket"] = _bucket_volume_ratio_20d(volume_ratio_20d)
                volatility_20d, _ = _compute_pre_entry_volatility_20d(
                    price_frame,
                    entry_idx=entry_idx,
                )
                enriched["pre_entry_volatility_20d"] = volatility_20d
                enriched["pre_entry_volatility_20d_bucket"] = _bucket_pre_entry_volatility_20d(
                    volatility_20d
                )

        records.append(enriched)

    enriched_df = pd.DataFrame(records)
    if enriched_df.empty:
        return _empty_result_df(_EVENT_FEATURE_COLUMNS)

    for column in _EVENT_FEATURE_COLUMNS:
        if column not in enriched_df.columns:
            enriched_df[column] = None
    return (
        enriched_df[list(_EVENT_FEATURE_COLUMNS)]
        .sort_values(["cohort_key", "disclosed_date", "code"], kind="stable")
        .reset_index(drop=True)
    )


def _apply_sector_bucket_collapse(
    event_feature_df: pd.DataFrame,
    *,
    sparse_sector_min_event_count: int,
) -> pd.DataFrame:
    if event_feature_df.empty:
        return event_feature_df.copy()
    collapsed = event_feature_df.copy()
    sector_counts = (
        collapsed["sector_33_name"].fillna("").astype(str).str.strip().value_counts()
    )
    collapsed["sector_bucket"] = collapsed["sector_33_name"].map(
        lambda value: _bucket_sector_name(
            value,
            keep_named=int(sector_counts.get(str(value or "").strip(), 0))
            >= sparse_sector_min_event_count,
        )
    )
    return collapsed


def _cohort_stats_lookup(threshold_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    return {
        str(row["cohort_key"]): {
            "realized_event_count": int(row["realized_event_count"]),
            "winner_event_count": int(row["winner_event_count"]),
            "winner_base_rate": _to_nullable_float(row["winner_base_rate"]),
            "winner_cutoff_pct": _to_nullable_float(row["winner_cutoff_pct"]),
        }
        for row in threshold_df.to_dict(orient="records")
    }


def _combo_payload(
    feature_specs: Sequence[FeatureSpec],
    bucket_labels: Sequence[str],
) -> dict[str, Any]:
    ordered_pairs = sorted(
        zip(feature_specs, bucket_labels, strict=True),
        key=lambda pair: _FEATURE_ORDER[pair[0].identifier],
    )
    ordered_specs = [pair[0] for pair in ordered_pairs]
    ordered_buckets = [str(pair[1]) for pair in ordered_pairs]
    payload: dict[str, Any] = {
        "feature_count": len(ordered_specs),
        "feature_keys": "|".join(spec.identifier for spec in ordered_specs),
        "feature_labels": " + ".join(spec.label for spec in ordered_specs),
        "combo_key": " | ".join(
            f"{spec.identifier}={bucket}" for spec, bucket in zip(ordered_specs, ordered_buckets, strict=True)
        ),
        "combo_label": " / ".join(
            f"{spec.label}: {bucket}"
            for spec, bucket in zip(ordered_specs, ordered_buckets, strict=True)
        ),
        "feature_1_key": None,
        "feature_1_label": None,
        "feature_1_bucket_label": None,
        "feature_2_key": None,
        "feature_2_label": None,
        "feature_2_bucket_label": None,
        "feature_3_key": None,
        "feature_3_label": None,
        "feature_3_bucket_label": None,
    }
    for idx, (spec, bucket) in enumerate(zip(ordered_specs, ordered_buckets, strict=True), start=1):
        payload[f"feature_{idx}_key"] = spec.identifier
        payload[f"feature_{idx}_label"] = spec.label
        payload[f"feature_{idx}_bucket_label"] = bucket
    return payload


def _metric_payload(
    cell_df: pd.DataFrame,
    *,
    cohort_stats: dict[str, Any],
) -> dict[str, Any]:
    event_count = int(len(cell_df))
    winner_count = int(cell_df["is_winner"].fillna(False).astype(bool).sum())
    winner_hit_rate = (winner_count / event_count) * 100.0 if event_count else None
    base_rate = _to_nullable_float(cohort_stats.get("winner_base_rate"))
    winner_capture_rate = None
    total_winner_count = int(cohort_stats.get("winner_event_count", 0))
    if total_winner_count > 0:
        winner_capture_rate = (winner_count / total_winner_count) * 100.0
    lift_vs_base_rate = None
    if (
        base_rate is not None
        and math.isfinite(base_rate)
        and not math.isclose(base_rate, 0.0, abs_tol=1e-12)
        and winner_hit_rate is not None
    ):
        lift_vs_base_rate = winner_hit_rate / base_rate
    return {
        "event_count": event_count,
        "winner_count": winner_count,
        "winner_hit_rate": winner_hit_rate,
        "lift_vs_base_rate": lift_vs_base_rate,
        "winner_capture_rate": winner_capture_rate,
        "mean_return_pct": _series_stat(cell_df["event_return_pct"], "mean"),
        "median_return_pct": _series_stat(cell_df["event_return_pct"], "median"),
        "cohort_base_winner_rate": base_rate,
    }


def _sort_and_rank_summary(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df.copy()
    ranked = summary_df.sort_values(
        [
            "cohort_key",
            "lift_vs_base_rate",
            "winner_hit_rate",
            "event_count",
            "mean_return_pct",
            "combo_key",
        ],
        ascending=[True, False, False, False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    ranked["rank_within_cohort"] = (
        ranked.groupby("cohort_key", sort=False).cumcount() + 1
    )
    return ranked


def _build_single_feature_summary_df(
    event_feature_df: pd.DataFrame,
    *,
    threshold_df: pd.DataFrame,
) -> pd.DataFrame:
    if event_feature_df.empty or threshold_df.empty:
        return _empty_result_df(_SUMMARY_COLUMNS)
    cohort_stats_lookup = _cohort_stats_lookup(threshold_df)
    records: list[dict[str, Any]] = []
    for cohort_key in TARGET_COHORT_KEYS:
        cohort_df = event_feature_df[event_feature_df["cohort_key"].astype(str) == cohort_key].copy()
        if cohort_df.empty:
            continue
        for spec in _FEATURE_SPECS:
            grouped = cohort_df.groupby(spec.bucket_column, sort=False, dropna=False)
            for bucket_label, cell_df in grouped:
                record = {
                    "cohort_key": cohort_key,
                    "cohort_label": _group_label(cohort_key),
                }
                record.update(_combo_payload((spec,), (str(bucket_label),)))
                record.update(_metric_payload(cell_df, cohort_stats=cohort_stats_lookup[cohort_key]))
                records.append(record)
    summary_df = pd.DataFrame(records, columns=_SUMMARY_COLUMNS)
    return _sort_and_rank_summary(summary_df)


def _build_pair_combo_summary_df(
    event_feature_df: pd.DataFrame,
    *,
    threshold_df: pd.DataFrame,
    min_event_count: int,
    min_winner_count: int,
) -> pd.DataFrame:
    columns = list(_SUMMARY_COLUMNS) + ["eligible_for_triplet_expansion"]
    if event_feature_df.empty or threshold_df.empty:
        return _empty_result_df(columns)
    cohort_stats_lookup = _cohort_stats_lookup(threshold_df)
    records: list[dict[str, Any]] = []
    for cohort_key in TARGET_COHORT_KEYS:
        cohort_df = event_feature_df[event_feature_df["cohort_key"].astype(str) == cohort_key].copy()
        if cohort_df.empty:
            continue
        for spec_a, spec_b in combinations(_FEATURE_SPECS, 2):
            grouped = cohort_df.groupby(
                [spec_a.bucket_column, spec_b.bucket_column],
                sort=False,
                dropna=False,
            )
            for (bucket_a, bucket_b), cell_df in grouped:
                if len(cell_df) < min_event_count:
                    continue
                metric_payload = _metric_payload(cell_df, cohort_stats=cohort_stats_lookup[cohort_key])
                record = {
                    "cohort_key": cohort_key,
                    "cohort_label": _group_label(cohort_key),
                    "eligible_for_triplet_expansion": int(metric_payload["winner_count"]) >= min_winner_count,
                }
                record.update(_combo_payload((spec_a, spec_b), (str(bucket_a), str(bucket_b))))
                record.update(metric_payload)
                records.append(record)
    pair_df = pd.DataFrame(records, columns=columns)
    return _sort_and_rank_summary(pair_df)


def _build_triplet_combo_summary_df(
    event_feature_df: pd.DataFrame,
    *,
    threshold_df: pd.DataFrame,
    pair_combo_summary_df: pd.DataFrame,
    min_event_count: int,
) -> pd.DataFrame:
    columns = list(_SUMMARY_COLUMNS) + ["parent_pair_combo_keys"]
    if event_feature_df.empty or threshold_df.empty or pair_combo_summary_df.empty:
        return _empty_result_df(columns)
    cohort_stats_lookup = _cohort_stats_lookup(threshold_df)
    cohort_df_by_key = {
        cohort_key: event_feature_df[event_feature_df["cohort_key"].astype(str) == cohort_key].copy()
        for cohort_key in TARGET_COHORT_KEYS
    }
    triplet_records: dict[tuple[str, str], dict[str, Any]] = {}
    for pair_row in pair_combo_summary_df.to_dict(orient="records"):
        if not bool(pair_row.get("eligible_for_triplet_expansion")):
            continue
        cohort_key = str(pair_row["cohort_key"])
        cohort_df = cohort_df_by_key.get(cohort_key)
        if cohort_df is None or cohort_df.empty:
            continue
        used_keys = [
            str(pair_row["feature_1_key"]),
            str(pair_row["feature_2_key"]),
        ]
        pair_mask = pd.Series(True, index=cohort_df.index)
        for feature_idx, feature_key in enumerate(used_keys, start=1):
            spec = _FEATURE_SPEC_BY_KEY[feature_key]
            bucket_label = str(pair_row[f"feature_{feature_idx}_bucket_label"])
            pair_mask &= cohort_df[spec.bucket_column].astype(str) == bucket_label
        pair_df = cohort_df[pair_mask].copy()
        if pair_df.empty:
            continue
        for extra_spec in _FEATURE_SPECS:
            if extra_spec.identifier in used_keys:
                continue
            grouped = pair_df.groupby(extra_spec.bucket_column, sort=False, dropna=False)
            for extra_bucket, cell_df in grouped:
                if len(cell_df) < min_event_count:
                    continue
                feature_keys = [*used_keys, extra_spec.identifier]
                bucket_lookup = {
                    str(pair_row["feature_1_key"]): str(pair_row["feature_1_bucket_label"]),
                    str(pair_row["feature_2_key"]): str(pair_row["feature_2_bucket_label"]),
                    extra_spec.identifier: str(extra_bucket),
                }
                ordered_specs = [
                    _FEATURE_SPEC_BY_KEY[key]
                    for key in sorted(feature_keys, key=lambda value: _FEATURE_ORDER[value])
                ]
                ordered_buckets = [bucket_lookup[spec.identifier] for spec in ordered_specs]
                combo_payload = _combo_payload(ordered_specs, ordered_buckets)
                record_key = (cohort_key, str(combo_payload["combo_key"]))
                metric_payload = _metric_payload(cell_df, cohort_stats=cohort_stats_lookup[cohort_key])
                existing = triplet_records.get(record_key)
                if existing is None:
                    record = {
                        "cohort_key": cohort_key,
                        "cohort_label": _group_label(cohort_key),
                        "parent_pair_combo_keys": {str(pair_row["combo_key"])},
                    }
                    record.update(combo_payload)
                    record.update(metric_payload)
                    triplet_records[record_key] = record
                    continue
                existing["parent_pair_combo_keys"].add(str(pair_row["combo_key"]))
    flattened_records: list[dict[str, Any]] = []
    for record in triplet_records.values():
        parent_keys = record["parent_pair_combo_keys"]
        flattened = dict(record)
        flattened["parent_pair_combo_keys"] = " || ".join(sorted(parent_keys))
        flattened_records.append(flattened)
    triplet_df = pd.DataFrame(flattened_records, columns=columns)
    return _sort_and_rank_summary(triplet_df)


def _build_group_comparison_df(
    *,
    pair_combo_summary_df: pd.DataFrame,
    triplet_combo_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "combo_kind",
        "combo_key",
        "combo_label",
        "feature_keys",
        "feature_labels",
        "strength_class",
        "forecast_positive_event_count",
        "forecast_positive_winner_hit_rate",
        "forecast_positive_lift_vs_base_rate",
        "forecast_positive_mean_return_pct",
        "forecast_positive_median_return_pct",
        "forecast_missing_event_count",
        "forecast_missing_winner_hit_rate",
        "forecast_missing_lift_vs_base_rate",
        "forecast_missing_mean_return_pct",
        "forecast_missing_median_return_pct",
    ]

    def _comparison_rows(summary_df: pd.DataFrame, *, combo_kind: str) -> list[dict[str, Any]]:
        if summary_df.empty:
            return []
        pair_key = TARGET_COHORT_KEYS[0]
        missing_key = TARGET_COHORT_KEYS[1]
        pair_rows = summary_df[summary_df["cohort_key"].astype(str) == pair_key].copy()
        missing_rows = summary_df[summary_df["cohort_key"].astype(str) == missing_key].copy()
        pair_lookup = {str(row["combo_key"]): row for row in pair_rows.to_dict(orient="records")}
        missing_lookup = {
            str(row["combo_key"]): row for row in missing_rows.to_dict(orient="records")
        }
        combo_keys = sorted(set(pair_lookup) | set(missing_lookup))
        records: list[dict[str, Any]] = []
        for combo_key in combo_keys:
            pair_row = pair_lookup.get(combo_key)
            missing_row = missing_lookup.get(combo_key)
            ref_row = pair_row or missing_row
            assert ref_row is not None
            records.append(
                {
                    "combo_kind": combo_kind,
                    "combo_key": combo_key,
                    "combo_label": ref_row["combo_label"],
                    "feature_keys": ref_row["feature_keys"],
                    "feature_labels": ref_row["feature_labels"],
                    "strength_class": (
                        "shared"
                        if pair_row is not None and missing_row is not None
                        else "forecast_positive_only"
                        if pair_row is not None
                        else "forecast_missing_only"
                    ),
                    "forecast_positive_event_count": (
                        int(pair_row["event_count"]) if pair_row is not None else 0
                    ),
                    "forecast_positive_winner_hit_rate": (
                        _to_nullable_float(pair_row["winner_hit_rate"]) if pair_row is not None else None
                    ),
                    "forecast_positive_lift_vs_base_rate": (
                        _to_nullable_float(pair_row["lift_vs_base_rate"]) if pair_row is not None else None
                    ),
                    "forecast_positive_mean_return_pct": (
                        _to_nullable_float(pair_row["mean_return_pct"]) if pair_row is not None else None
                    ),
                    "forecast_positive_median_return_pct": (
                        _to_nullable_float(pair_row["median_return_pct"]) if pair_row is not None else None
                    ),
                    "forecast_missing_event_count": (
                        int(missing_row["event_count"]) if missing_row is not None else 0
                    ),
                    "forecast_missing_winner_hit_rate": (
                        _to_nullable_float(missing_row["winner_hit_rate"])
                        if missing_row is not None
                        else None
                    ),
                    "forecast_missing_lift_vs_base_rate": (
                        _to_nullable_float(missing_row["lift_vs_base_rate"])
                        if missing_row is not None
                        else None
                    ),
                    "forecast_missing_mean_return_pct": (
                        _to_nullable_float(missing_row["mean_return_pct"])
                        if missing_row is not None
                        else None
                    ),
                    "forecast_missing_median_return_pct": (
                        _to_nullable_float(missing_row["median_return_pct"])
                        if missing_row is not None
                        else None
                    ),
                }
            )
        return records

    comparison_records = [
        *_comparison_rows(pair_combo_summary_df, combo_kind="pair"),
        *_comparison_rows(triplet_combo_summary_df, combo_kind="triplet"),
    ]
    if not comparison_records:
        return _empty_result_df(columns)
    return (
        pd.DataFrame(comparison_records, columns=columns)
        .sort_values(
            [
                "combo_kind",
                "strength_class",
                "forecast_positive_lift_vs_base_rate",
                "forecast_missing_lift_vs_base_rate",
                "combo_key",
            ],
            ascending=[True, True, False, False, True],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def _matching_combo_rows(
    event_row: pd.Series,
    summary_df: pd.DataFrame,
) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df.copy()
    cohort_key = str(event_row["cohort_key"])
    cohort_rows = summary_df[summary_df["cohort_key"].astype(str) == cohort_key].copy()
    if cohort_rows.empty:
        return cohort_rows

    def _row_matches_combo(summary_row: pd.Series) -> bool:
        for idx in (1, 2, 3):
            feature_key = summary_row.get(f"feature_{idx}_key")
            if feature_key is None or pd.isna(feature_key) or str(feature_key) == "":
                continue
            feature_key_str = str(feature_key)
            spec = _FEATURE_SPEC_BY_KEY[feature_key_str]
            expected_bucket = str(summary_row.get(f"feature_{idx}_bucket_label"))
            actual_bucket = str(event_row.get(spec.bucket_column))
            if expected_bucket != actual_bucket:
                return False
        return True

    mask = cohort_rows.apply(_row_matches_combo, axis=1)
    return cohort_rows[mask].copy().reset_index(drop=True)


def _build_top_examples_df(
    event_feature_df: pd.DataFrame,
    *,
    pair_combo_summary_df: pd.DataFrame,
    triplet_combo_summary_df: pd.DataFrame,
    top_examples_limit: int,
) -> pd.DataFrame:
    columns = [
        "cohort_key",
        "cohort_label",
        "code",
        "company_name",
        "disclosed_date",
        "entry_date",
        "exit_date",
        "event_return_pct",
        "winner_cutoff_pct",
        "matched_pair_combo_count",
        "best_pair_combo_key",
        "best_pair_lift_vs_base_rate",
        "matched_triplet_combo_count",
        "best_triplet_combo_key",
        "best_triplet_lift_vs_base_rate",
        "entry_market_cap_bucket",
        "entry_adv_bucket",
        "entry_open_bucket",
        "prior_20d_return_bucket",
        "prior_63d_return_bucket",
        "prior_252d_return_bucket",
        "volume_ratio_20d_bucket",
        "pre_entry_volatility_20d_bucket",
        "profit_margin_bucket",
        "cfo_margin_bucket",
        "equity_ratio_bucket",
        "sector_bucket",
    ]
    winner_df = event_feature_df[event_feature_df["is_winner"].fillna(False).astype(bool)].copy()
    if winner_df.empty:
        return _empty_result_df(columns)
    per_cohort_limit = max(1, math.ceil(top_examples_limit / len(TARGET_COHORT_KEYS)))
    winner_df = (
        winner_df.sort_values(
            ["cohort_key", "event_return_pct", "disclosed_date", "code"],
            ascending=[True, False, True, True],
            kind="stable",
        )
        .groupby("cohort_key", sort=False)
        .head(per_cohort_limit)
        .reset_index(drop=True)
    )
    records: list[dict[str, Any]] = []
    for row in winner_df.to_dict(orient="records"):
        event_row = pd.Series(row)
        matched_pairs = _matching_combo_rows(event_row, pair_combo_summary_df)
        matched_triplets = _matching_combo_rows(event_row, triplet_combo_summary_df)
        if matched_pairs.empty and matched_triplets.empty:
            continue
        best_pair = matched_pairs.iloc[0] if not matched_pairs.empty else None
        best_triplet = matched_triplets.iloc[0] if not matched_triplets.empty else None
        records.append(
            {
                "cohort_key": row["cohort_key"],
                "cohort_label": row["cohort_label"],
                "code": row["code"],
                "company_name": row["company_name"],
                "disclosed_date": row["disclosed_date"],
                "entry_date": row["entry_date"],
                "exit_date": row["exit_date"],
                "event_return_pct": _to_nullable_float(row["event_return_pct"]),
                "winner_cutoff_pct": _to_nullable_float(row["winner_cutoff_pct"]),
                "matched_pair_combo_count": int(len(matched_pairs)),
                "best_pair_combo_key": best_pair["combo_key"] if best_pair is not None else None,
                "best_pair_lift_vs_base_rate": (
                    _to_nullable_float(best_pair["lift_vs_base_rate"])
                    if best_pair is not None
                    else None
                ),
                "matched_triplet_combo_count": int(len(matched_triplets)),
                "best_triplet_combo_key": (
                    best_triplet["combo_key"] if best_triplet is not None else None
                ),
                "best_triplet_lift_vs_base_rate": (
                    _to_nullable_float(best_triplet["lift_vs_base_rate"])
                    if best_triplet is not None
                    else None
                ),
                "entry_market_cap_bucket": row["entry_market_cap_bucket"],
                "entry_adv_bucket": row["entry_adv_bucket"],
                "entry_open_bucket": row["entry_open_bucket"],
                "prior_20d_return_bucket": row["prior_20d_return_bucket"],
                "prior_63d_return_bucket": row["prior_63d_return_bucket"],
                "prior_252d_return_bucket": row["prior_252d_return_bucket"],
                "volume_ratio_20d_bucket": row["volume_ratio_20d_bucket"],
                "pre_entry_volatility_20d_bucket": row["pre_entry_volatility_20d_bucket"],
                "profit_margin_bucket": row["profit_margin_bucket"],
                "cfo_margin_bucket": row["cfo_margin_bucket"],
                "equity_ratio_bucket": row["equity_ratio_bucket"],
                "sector_bucket": row["sector_bucket"],
            }
        )
    if not records:
        return _empty_result_df(columns)
    return pd.DataFrame(records, columns=columns)


def _fmt_num(value: float | int | None, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    if isinstance(value, int):
        return str(value)
    return f"{value:.{digits}f}"


def _top_summary_rows(summary_df: pd.DataFrame, *, cohort_key: str, limit: int = 5) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df.copy()
    return (
        summary_df[summary_df["cohort_key"].astype(str) == cohort_key]
        .sort_values("rank_within_cohort", kind="stable")
        .head(limit)
        .reset_index(drop=True)
    )


def _build_summary_markdown(
    result: StandardNegativeEpsSpeculativeWinnerFeatureCombosResult,
) -> str:
    lines = [
        "# Standard Negative EPS Speculative Winner Feature Combos",
        "",
        "## Setup",
        "",
        f"- Scope: `{result.scope_name}`",
        "- Base event source: standard negative-EPS right-tail decomposition realized events only",
        "- Target cohorts: `forecast_positive__cfo_positive` and `forecast_missing__cfo_non_positive`",
        "- Entry: next trading session open after FY disclosure",
        "- Exit: previous trading session close before the next FY disclosure",
        (
            f"- Winner label: top `{_fmt_num((1.0 - result.winner_quantile) * 100.0, 0)}`% "
            "by next-FY event return within each cohort"
        ),
        f"- Pair-cell retention: `event_count >= {result.min_event_count}`",
        (
            f"- Triplet expansion gate: pair cell with `event_count >= {result.min_event_count}` "
            f"and `winner_count >= {result.min_winner_count}`"
        ),
        "",
        "## Cohort Sizes And Cutoffs",
        "",
    ]
    if result.winner_threshold_df.empty:
        lines.append("- No realized events were available in the target cohorts.")
        return "\n".join(lines)
    for row in result.winner_threshold_df.to_dict(orient="records"):
        lines.append(
            "- "
            f"`{row['cohort_key']}`: realized `{int(row['realized_event_count'])}`, "
            f"winner cutoff `{_fmt_num(row['winner_cutoff_pct'])}%`, "
            f"winner base rate `{_fmt_num(row['winner_base_rate'])}%`"
        )

    lines.extend(["", "## Strongest Two-Feature Combinations", ""])
    if result.pair_combo_summary_df.empty:
        lines.append("- No pair cells cleared the minimum event-count threshold.")
    else:
        for cohort_key in TARGET_COHORT_KEYS:
            lines.append(f"- `{cohort_key}`:")
            cohort_pairs = _top_summary_rows(result.pair_combo_summary_df, cohort_key=cohort_key)
            if cohort_pairs.empty:
                lines.append("  - No qualifying pair cells.")
                continue
            for row in cohort_pairs.to_dict(orient="records"):
                lines.append(
                    "  - "
                    f"{row['combo_label']}: events `{int(row['event_count'])}`, "
                    f"winner hit `{_fmt_num(row['winner_hit_rate'])}%`, "
                    f"lift `{_fmt_num(row['lift_vs_base_rate'], 2)}`, "
                    f"mean `{_fmt_num(row['mean_return_pct'])}%`, "
                    f"median `{_fmt_num(row['median_return_pct'])}%`"
                )

    lines.extend(["", "## Strongest Three-Feature Extensions", ""])
    if result.triplet_combo_summary_df.empty:
        lines.append("- No triplet cells cleared the pair-gated expansion rules.")
    else:
        for cohort_key in TARGET_COHORT_KEYS:
            lines.append(f"- `{cohort_key}`:")
            cohort_triplets = _top_summary_rows(result.triplet_combo_summary_df, cohort_key=cohort_key)
            if cohort_triplets.empty:
                lines.append("  - No qualifying triplet cells.")
                continue
            for row in cohort_triplets.to_dict(orient="records"):
                lines.append(
                    "  - "
                    f"{row['combo_label']}: events `{int(row['event_count'])}`, "
                    f"winner hit `{_fmt_num(row['winner_hit_rate'])}%`, "
                    f"lift `{_fmt_num(row['lift_vs_base_rate'], 2)}`"
                )

    lines.extend(["", "## Shared Vs Unique Signatures", ""])
    if result.group_comparison_df.empty:
        lines.append("- No cross-cohort pair/triplet signatures were available.")
    else:
        shared_df = result.group_comparison_df[
            result.group_comparison_df["strength_class"].astype(str) == "shared"
        ].head(5)
        if shared_df.empty:
            lines.append("- No signatures cleared the thresholds in both cohorts.")
        else:
            lines.append("- Shared:")
            for row in shared_df.to_dict(orient="records"):
                lines.append(
                    "  - "
                    f"[{row['combo_kind']}] {row['combo_label']}: "
                    f"turnaround lift `{_fmt_num(row['forecast_positive_lift_vs_base_rate'], 2)}`, "
                    f"missing-forecast lift `{_fmt_num(row['forecast_missing_lift_vs_base_rate'], 2)}`"
                )
        for strength_class in ("forecast_positive_only", "forecast_missing_only"):
            subset = result.group_comparison_df[
                result.group_comparison_df["strength_class"].astype(str) == strength_class
            ].head(3)
            heading = (
                "turnaround narrative only"
                if strength_class == "forecast_positive_only"
                else "missing-forecast only"
            )
            if subset.empty:
                continue
            lines.append(f"- {heading}:")
            for row in subset.to_dict(orient="records"):
                lines.append(f"  - [{row['combo_kind']}] {row['combo_label']}")

    return "\n".join(lines)


def _build_published_summary(
    result: StandardNegativeEpsSpeculativeWinnerFeatureCombosResult,
) -> dict[str, Any]:
    return {
        "selectedMarket": result.selected_market,
        "scopeName": result.scope_name,
        "winnerQuantile": result.winner_quantile,
        "minEventCount": result.min_event_count,
        "minWinnerCount": result.min_winner_count,
        "winnerThresholds": result.winner_threshold_df.to_dict(orient="records"),
        "topPairsByCohort": {
            cohort_key: _top_summary_rows(
                result.pair_combo_summary_df,
                cohort_key=cohort_key,
                limit=10,
            ).to_dict(orient="records")
            for cohort_key in TARGET_COHORT_KEYS
        },
        "topTripletsByCohort": {
            cohort_key: _top_summary_rows(
                result.triplet_combo_summary_df,
                cohort_key=cohort_key,
                limit=10,
            ).to_dict(orient="records")
            for cohort_key in TARGET_COHORT_KEYS
        },
        "comparisonHighlights": result.group_comparison_df.head(20).to_dict(orient="records"),
    }


def run_standard_negative_eps_speculative_winner_feature_combos(
    db_path: str,
    *,
    market: str = DEFAULT_MARKET,
    adv_window: int = DEFAULT_ADV_WINDOW,
    winner_quantile: float = DEFAULT_WINNER_QUANTILE,
    min_event_count: int = DEFAULT_MIN_EVENT_COUNT,
    min_winner_count: int = DEFAULT_MIN_WINNER_COUNT,
    top_examples_limit: int = DEFAULT_TOP_EXAMPLES_LIMIT,
    sparse_sector_min_event_count: int = DEFAULT_SPARSE_SECTOR_MIN_EVENT_COUNT,
) -> StandardNegativeEpsSpeculativeWinnerFeatureCombosResult:
    selected_market = _normalize_market(market)
    if adv_window <= 0:
        raise ValueError("adv_window must be positive")
    if not 0.0 < winner_quantile < 1.0:
        raise ValueError("winner_quantile must be between 0 and 1")
    if min_event_count <= 0:
        raise ValueError("min_event_count must be positive")
    if min_winner_count <= 0:
        raise ValueError("min_winner_count must be positive")
    if top_examples_limit <= 0:
        raise ValueError("top_examples_limit must be positive")
    if sparse_sector_min_event_count <= 0:
        raise ValueError("sparse_sector_min_event_count must be positive")

    base_result = run_standard_negative_eps_right_tail_decomposition(
        db_path,
        market=selected_market,
        adv_window=adv_window,
    )
    feature_bucket_def_df = _build_feature_bucket_def_df(
        sparse_sector_min_event_count=sparse_sector_min_event_count
    )
    base_event_df = base_result.event_ledger_df.copy()
    if base_event_df.empty:
        empty_df = _empty_result_df([])
        return StandardNegativeEpsSpeculativeWinnerFeatureCombosResult(
            db_path=db_path,
            selected_market=selected_market,
            source_mode=base_result.source_mode,
            source_detail=base_result.source_detail,
            available_start_date=base_result.available_start_date,
            available_end_date=base_result.available_end_date,
            analysis_start_date=None,
            analysis_end_date=None,
            scope_name=DEFAULT_SCOPE_NAME,
            adv_window=adv_window,
            winner_quantile=winner_quantile,
            min_event_count=min_event_count,
            min_winner_count=min_winner_count,
            top_examples_limit=top_examples_limit,
            sparse_sector_min_event_count=sparse_sector_min_event_count,
            winner_threshold_df=empty_df.copy(),
            feature_bucket_def_df=feature_bucket_def_df,
            event_feature_df=empty_df.copy(),
            single_feature_summary_df=empty_df.copy(),
            pair_combo_summary_df=empty_df.copy(),
            triplet_combo_summary_df=empty_df.copy(),
            group_comparison_df=empty_df.copy(),
            top_examples_df=empty_df.copy(),
        )

    realized_event_df = base_event_df[
        (base_event_df["status"].astype(str) == "realized")
        & (base_event_df["group_key"].astype(str).isin(TARGET_COHORT_KEYS))
    ].copy()
    winner_threshold_df = _build_winner_threshold_df(
        realized_event_df,
        winner_quantile=winner_quantile,
    )
    if winner_threshold_df.empty:
        empty_df = _empty_result_df([])
        return StandardNegativeEpsSpeculativeWinnerFeatureCombosResult(
            db_path=db_path,
            selected_market=selected_market,
            source_mode=base_result.source_mode,
            source_detail=base_result.source_detail,
            available_start_date=base_result.available_start_date,
            available_end_date=base_result.available_end_date,
            analysis_start_date=None,
            analysis_end_date=None,
            scope_name=DEFAULT_SCOPE_NAME,
            adv_window=adv_window,
            winner_quantile=winner_quantile,
            min_event_count=min_event_count,
            min_winner_count=min_winner_count,
            top_examples_limit=top_examples_limit,
            sparse_sector_min_event_count=sparse_sector_min_event_count,
            winner_threshold_df=winner_threshold_df,
            feature_bucket_def_df=feature_bucket_def_df,
            event_feature_df=empty_df.copy(),
            single_feature_summary_df=empty_df.copy(),
            pair_combo_summary_df=empty_df.copy(),
            triplet_combo_summary_df=empty_df.copy(),
            group_comparison_df=empty_df.copy(),
            top_examples_df=empty_df.copy(),
        )
    labeled_event_df = _attach_winner_labels(realized_event_df, threshold_df=winner_threshold_df)
    analysis_start_date = str(labeled_event_df["disclosed_date"].min())
    analysis_end_date = str(labeled_event_df["exit_date"].max())
    lookback_days = max(DEFAULT_VOLUME_RATIO_BASELINE_WINDOW * 3, 900)
    price_start_date = (
        pd.Timestamp(analysis_start_date) - pd.Timedelta(days=lookback_days)
    ).strftime("%Y-%m-%d")
    price_end_date = str(labeled_event_df["exit_date"].max())
    market_codes = _query_market_codes(selected_market)
    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        price_df = _query_price_rows(
            conn,
            market_codes=market_codes,
            start_date=price_start_date,
            end_date=price_end_date,
        )
        statement_df = _query_statement_rows(conn, market_codes=market_codes)

    scoped_codes = set(labeled_event_df["code"].astype(str))
    price_df = price_df[price_df["code"].astype(str).isin(scoped_codes)].copy().reset_index(drop=True)
    statement_df = (
        statement_df[statement_df["code"].astype(str).isin(scoped_codes)]
        .copy()
        .reset_index(drop=True)
    )
    event_feature_df = _enrich_realized_events(
        labeled_event_df,
        price_df=price_df,
        statement_df=statement_df,
    )
    event_feature_df = _apply_sector_bucket_collapse(
        event_feature_df,
        sparse_sector_min_event_count=sparse_sector_min_event_count,
    )
    single_feature_summary_df = _build_single_feature_summary_df(
        event_feature_df,
        threshold_df=winner_threshold_df,
    )
    pair_combo_summary_df = _build_pair_combo_summary_df(
        event_feature_df,
        threshold_df=winner_threshold_df,
        min_event_count=min_event_count,
        min_winner_count=min_winner_count,
    )
    triplet_combo_summary_df = _build_triplet_combo_summary_df(
        event_feature_df,
        threshold_df=winner_threshold_df,
        pair_combo_summary_df=pair_combo_summary_df,
        min_event_count=min_event_count,
    )
    group_comparison_df = _build_group_comparison_df(
        pair_combo_summary_df=pair_combo_summary_df,
        triplet_combo_summary_df=triplet_combo_summary_df,
    )
    top_examples_df = _build_top_examples_df(
        event_feature_df,
        pair_combo_summary_df=pair_combo_summary_df,
        triplet_combo_summary_df=triplet_combo_summary_df,
        top_examples_limit=top_examples_limit,
    )

    return StandardNegativeEpsSpeculativeWinnerFeatureCombosResult(
        db_path=db_path,
        selected_market=selected_market,
        source_mode=base_result.source_mode,
        source_detail=base_result.source_detail,
        available_start_date=base_result.available_start_date,
        available_end_date=base_result.available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        scope_name=DEFAULT_SCOPE_NAME,
        adv_window=adv_window,
        winner_quantile=winner_quantile,
        min_event_count=min_event_count,
        min_winner_count=min_winner_count,
        top_examples_limit=top_examples_limit,
        sparse_sector_min_event_count=sparse_sector_min_event_count,
        winner_threshold_df=winner_threshold_df,
        feature_bucket_def_df=feature_bucket_def_df,
        event_feature_df=event_feature_df,
        single_feature_summary_df=single_feature_summary_df,
        pair_combo_summary_df=pair_combo_summary_df,
        triplet_combo_summary_df=triplet_combo_summary_df,
        group_comparison_df=group_comparison_df,
        top_examples_df=top_examples_df,
    )


def write_standard_negative_eps_speculative_winner_feature_combos_bundle(
    result: StandardNegativeEpsSpeculativeWinnerFeatureCombosResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=STANDARD_NEGATIVE_EPS_SPECULATIVE_WINNER_FEATURE_COMBOS_EXPERIMENT_ID,
        module=__name__,
        function="run_standard_negative_eps_speculative_winner_feature_combos",
        params={
            "market": result.selected_market,
            "adv_window": result.adv_window,
            "winner_quantile": result.winner_quantile,
            "min_event_count": result.min_event_count,
            "min_winner_count": result.min_winner_count,
            "top_examples_limit": result.top_examples_limit,
            "sparse_sector_min_event_count": result.sparse_sector_min_event_count,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_standard_negative_eps_speculative_winner_feature_combos_bundle(
    bundle_path: str | Path,
) -> StandardNegativeEpsSpeculativeWinnerFeatureCombosResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=StandardNegativeEpsSpeculativeWinnerFeatureCombosResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_standard_negative_eps_speculative_winner_feature_combos_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        STANDARD_NEGATIVE_EPS_SPECULATIVE_WINNER_FEATURE_COMBOS_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_standard_negative_eps_speculative_winner_feature_combos_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        STANDARD_NEGATIVE_EPS_SPECULATIVE_WINNER_FEATURE_COMBOS_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
