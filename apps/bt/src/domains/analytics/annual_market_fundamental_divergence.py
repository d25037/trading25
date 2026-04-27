"""Market-level fundamental divergence research for the annual panel."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    get_annual_first_open_last_close_fundamental_panel_latest_bundle_path,
)
from src.domains.analytics.annual_fundamental_confounder_analysis import (
    DEFAULT_WINSOR_LOWER,
    DEFAULT_WINSOR_UPPER,
    _winsorize,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    load_research_bundle_info,
    load_research_bundle_tables,
    resolve_required_bundle_path,
    write_dataclass_research_bundle,
)

ANNUAL_MARKET_FUNDAMENTAL_DIVERGENCE_EXPERIMENT_ID = (
    "market-behavior/annual-market-fundamental-divergence"
)
DEFAULT_MIN_OBSERVATIONS = 30
_MARKET_ORDER: tuple[str, ...] = ("prime", "standard", "growth")
_MARKET_PAIRS: tuple[tuple[str, str], ...] = (
    ("prime", "standard"),
    ("prime", "growth"),
    ("standard", "growth"),
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "prepared_panel_df",
    "market_feature_profile_df",
    "market_pair_divergence_df",
    "feature_divergence_rank_df",
    "market_return_decomposition_df",
)


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    label: str
    family: str
    raw_column: str | None = None
    model_feature: bool = True


@dataclass(frozen=True)
class ReturnModelSpec:
    name: str
    label: str
    numeric_columns: tuple[str, ...]
    fixed_effect_columns: tuple[str, ...]


@dataclass(frozen=True)
class AnnualMarketFundamentalDivergenceResult:
    db_path: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    winsor_lower: float
    winsor_upper: float
    min_observations: int
    input_realized_event_count: int
    analysis_event_count: int
    score_policy: str
    prepared_panel_df: pd.DataFrame
    market_feature_profile_df: pd.DataFrame
    market_pair_divergence_df: pd.DataFrame
    feature_divergence_rank_df: pd.DataFrame
    market_return_decomposition_df: pd.DataFrame


FEATURE_SPECS: tuple[FeatureSpec, ...] = (
    FeatureSpec("pbr", "PBR", "valuation"),
    FeatureSpec("per", "PER", "valuation"),
    FeatureSpec("forward_per", "Forward PER", "valuation"),
    FeatureSpec("market_cap_bil_jpy", "Market cap, bn JPY", "size_liquidity"),
    FeatureSpec("avg_trading_value_60d_mil_jpy", "ADV60, mn JPY", "size_liquidity"),
    FeatureSpec("roe_pct", "ROE, pct", "profitability"),
    FeatureSpec("roa_pct", "ROA, pct", "profitability"),
    FeatureSpec("equity_ratio_pct", "Equity ratio, pct", "quality"),
    FeatureSpec("cfo_margin_pct", "CFO margin, pct", "cash_quality"),
    FeatureSpec("fcf_margin_pct", "FCF margin, pct", "cash_quality"),
    FeatureSpec("cfo_to_net_profit_ratio", "CFO / net profit", "cash_quality"),
    FeatureSpec("cfo_yield_pct", "CFO yield, pct", "yield"),
    FeatureSpec("fcf_yield_pct", "FCF yield, pct", "yield"),
    FeatureSpec("dividend_yield_pct", "Dividend yield, pct", "yield"),
    FeatureSpec("forecast_dividend_yield_pct", "Forecast dividend yield, pct", "yield"),
    FeatureSpec("payout_ratio_pct", "Payout ratio, pct", "payout"),
    FeatureSpec("forecast_payout_ratio_pct", "Forecast payout ratio, pct", "payout"),
    FeatureSpec("forward_eps_to_actual_eps", "Forward EPS / actual EPS", "forecast"),
    FeatureSpec(
        "eps_non_positive_flag",
        "EPS <= 0, pct",
        "diagnostic",
        raw_column=None,
        model_feature=True,
    ),
    FeatureSpec(
        "forward_per_non_positive_flag",
        "Forward PER <= 0, pct",
        "diagnostic",
        raw_column=None,
        model_feature=True,
    ),
    FeatureSpec("forecast_missing_flag", "Forecast EPS missing, pct", "diagnostic"),
    FeatureSpec("cfo_non_positive_flag", "CFO yield <= 0, pct", "diagnostic"),
)
_FEATURE_BY_NAME = {spec.name: spec for spec in FEATURE_SPECS}
_RAW_FEATURE_COLUMNS = tuple(spec.raw_column or spec.name for spec in FEATURE_SPECS)

RETURN_MODEL_SPECS: tuple[ReturnModelSpec, ...] = (
    ReturnModelSpec("market_only", "Market + year", (), ("year", "market")),
    ReturnModelSpec("sector_adjusted", "Market + year + sector", (), ("year", "sector_33_name", "market")),
    ReturnModelSpec(
        "value_size_adjusted",
        "Market + year + sector + value/size",
        ("pbr_year_z", "forward_per_year_z", "market_cap_bil_jpy_year_z"),
        ("year", "sector_33_name", "market"),
    ),
    ReturnModelSpec(
        "full_fundamental_adjusted",
        "Market + year + sector + fundamentals",
        (
            "pbr_year_z",
            "forward_per_year_z",
            "market_cap_bil_jpy_year_z",
            "avg_trading_value_60d_mil_jpy_year_z",
            "roe_pct_year_z",
            "cfo_yield_pct_year_z",
            "forward_eps_to_actual_eps_year_z",
            "forward_per_non_positive_flag_year_z",
            "forecast_missing_flag_year_z",
        ),
        ("year", "sector_33_name", "market"),
    ),
)


def _empty_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _finite_numeric(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").astype(float)
    return numeric.where(np.isfinite(numeric))


def _is_finite_number(value: object) -> bool:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return False
    return math.isfinite(number)


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


def _market_sort(frame: pd.DataFrame, extra_columns: Sequence[str]) -> pd.DataFrame:
    if frame.empty or "market" not in frame.columns:
        return frame.reset_index(drop=True)
    result = frame.copy()
    result["market"] = pd.Categorical(
        result["market"].astype(str),
        categories=[market for market in _MARKET_ORDER if market in set(result["market"].astype(str))],
        ordered=True,
    )
    return result.sort_values(["market", *extra_columns], kind="stable").reset_index(drop=True)


def _year_z_score(values: pd.Series, years: pd.Series) -> pd.Series:
    numeric = _finite_numeric(values)
    z_scores = pd.Series(np.nan, index=values.index, dtype="float64")
    helper = pd.DataFrame({"value": numeric, "year": years.astype(str)}, index=values.index)
    for _, group in helper.groupby("year", sort=False):
        valid = group["value"].dropna()
        if valid.empty:
            continue
        std = float(valid.std(ddof=0))
        if not math.isfinite(std) or math.isclose(std, 0.0, abs_tol=1e-12):
            z_scores.loc[valid.index] = 0.0
            continue
        z_scores.loc[valid.index] = (valid - float(valid.mean())) / std
    return z_scores


def _year_percentile(values: pd.Series, years: pd.Series) -> pd.Series:
    numeric = _finite_numeric(values)
    percentiles = pd.Series(np.nan, index=values.index, dtype="float64")
    helper = pd.DataFrame({"value": numeric, "year": years.astype(str)}, index=values.index)
    for _, group in helper.groupby("year", sort=False):
        valid = group["value"].dropna()
        count = len(valid)
        if count == 0:
            continue
        if count == 1:
            percentiles.loc[valid.index] = 0.5
            continue
        percentiles.loc[valid.index] = (valid.rank(method="average") - 1.0) / float(count - 1)
    return percentiles


def _normal_p_value_from_t(t_stat: float | None) -> float | None:
    if t_stat is None or not math.isfinite(t_stat):
        return None
    return math.erfc(abs(t_stat) / math.sqrt(2.0))


def _prepare_panel_df(
    event_ledger_df: pd.DataFrame,
    *,
    winsor_lower: float,
    winsor_upper: float,
) -> pd.DataFrame:
    required_columns = {
        "status",
        "event_id",
        "year",
        "code",
        "market",
        "sector_33_name",
        "event_return_pct",
    }
    missing = sorted(required_columns - set(event_ledger_df.columns))
    if missing:
        raise ValueError(f"Input event_ledger_df is missing required columns: {missing}")
    realized = event_ledger_df[event_ledger_df["status"].astype(str) == "realized"].copy()
    if realized.empty:
        return _empty_df([])
    realized["year"] = realized["year"].astype(str)
    realized["market"] = realized["market"].astype(str).str.lower()
    realized["sector_33_name"] = realized["sector_33_name"].fillna("unknown").astype(str)
    realized["event_return_pct"] = _finite_numeric(realized["event_return_pct"])
    realized["event_return_winsor_pct"] = _winsorize(
        realized["event_return_pct"],
        winsor_lower,
        winsor_upper,
    )
    for column in _RAW_FEATURE_COLUMNS:
        if column not in realized.columns:
            realized[column] = np.nan
        realized[column] = _finite_numeric(realized[column])

    eps = _finite_numeric(realized["eps"]) if "eps" in realized.columns else pd.Series(np.nan, index=realized.index)
    forward_eps = (
        _finite_numeric(realized["forward_eps"])
        if "forward_eps" in realized.columns
        else pd.Series(np.nan, index=realized.index)
    )
    forward_per = _finite_numeric(realized["forward_per"])
    cfo_yield = _finite_numeric(realized["cfo_yield_pct"])
    realized["eps_non_positive_flag"] = np.where(eps.notna(), (eps <= 0.0).astype(float) * 100.0, np.nan)
    realized["forward_per_non_positive_flag"] = np.where(
        forward_per.notna(),
        (forward_per <= 0.0).astype(float) * 100.0,
        np.nan,
    )
    realized["forecast_missing_flag"] = np.where(forward_eps.isna(), 100.0, 0.0)
    realized["cfo_non_positive_flag"] = np.where(
        cfo_yield.notna(),
        (cfo_yield <= 0.0).astype(float) * 100.0,
        np.nan,
    )

    for spec in FEATURE_SPECS:
        realized[f"{spec.name}_year_z"] = _year_z_score(realized[spec.name], realized["year"])
        realized[f"{spec.name}_year_percentile"] = _year_percentile(realized[spec.name], realized["year"])

    keep_columns = [
        "event_id",
        "year",
        "code",
        "company_name",
        "market",
        "market_code",
        "sector_33_name",
        "event_return_pct",
        "event_return_winsor_pct",
        *[spec.name for spec in FEATURE_SPECS],
        *[f"{spec.name}_year_z" for spec in FEATURE_SPECS],
        *[f"{spec.name}_year_percentile" for spec in FEATURE_SPECS],
    ]
    for column in keep_columns:
        if column not in realized.columns:
            realized[column] = None
    return realized[keep_columns].reset_index(drop=True)


def _quantile_distance(left: pd.Series, right: pd.Series) -> float | None:
    left_values = _finite_numeric(left).dropna().to_numpy(dtype=float)
    right_values = _finite_numeric(right).dropna().to_numpy(dtype=float)
    if len(left_values) == 0 or len(right_values) == 0:
        return None
    quantiles = np.linspace(0.05, 0.95, 19)
    left_q = np.quantile(left_values, quantiles)
    right_q = np.quantile(right_values, quantiles)
    return float(np.mean(np.abs(left_q - right_q)))


def _ks_statistic(left: pd.Series, right: pd.Series) -> float | None:
    left_values = np.sort(_finite_numeric(left).dropna().to_numpy(dtype=float))
    right_values = np.sort(_finite_numeric(right).dropna().to_numpy(dtype=float))
    if len(left_values) == 0 or len(right_values) == 0:
        return None
    values = np.sort(np.unique(np.concatenate([left_values, right_values])))
    left_cdf = np.searchsorted(left_values, values, side="right") / len(left_values)
    right_cdf = np.searchsorted(right_values, values, side="right") / len(right_values)
    return float(np.max(np.abs(left_cdf - right_cdf)))


def _build_market_feature_profile_df(panel_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market",
        "feature_name",
        "feature_label",
        "feature_family",
        "event_count",
        "non_null_count",
        "coverage_pct",
        "mean_value",
        "median_value",
        "p25_value",
        "p75_value",
        "mean_year_z",
        "median_year_percentile",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for market, market_group in panel_df.groupby("market", sort=False):
        for spec in FEATURE_SPECS:
            values = _finite_numeric(market_group[spec.name]).dropna()
            z_values = _finite_numeric(market_group[f"{spec.name}_year_z"]).dropna()
            pct_values = _finite_numeric(market_group[f"{spec.name}_year_percentile"]).dropna()
            records.append(
                {
                    "market": str(market),
                    "feature_name": spec.name,
                    "feature_label": spec.label,
                    "feature_family": spec.family,
                    "event_count": int(len(market_group)),
                    "non_null_count": int(len(values)),
                    "coverage_pct": float(len(values) / len(market_group) * 100.0)
                    if len(market_group)
                    else None,
                    "mean_value": float(values.mean()) if not values.empty else None,
                    "median_value": float(values.median()) if not values.empty else None,
                    "p25_value": float(values.quantile(0.25)) if not values.empty else None,
                    "p75_value": float(values.quantile(0.75)) if not values.empty else None,
                    "mean_year_z": float(z_values.mean()) if not z_values.empty else None,
                    "median_year_percentile": float(pct_values.median()) if not pct_values.empty else None,
                }
            )
    return _market_sort(pd.DataFrame(records), ["feature_family", "feature_name"])


def _build_market_pair_divergence_df(panel_df: pd.DataFrame, *, min_observations: int) -> pd.DataFrame:
    columns = [
        "market_pair",
        "left_market",
        "right_market",
        "feature_name",
        "feature_label",
        "feature_family",
        "left_count",
        "right_count",
        "left_mean_value",
        "right_mean_value",
        "left_median_value",
        "right_median_value",
        "mean_value_diff",
        "median_value_diff",
        "standardized_mean_diff",
        "median_year_z_diff",
        "iqr_year_z_diff",
        "quantile_distance",
        "ks_statistic",
        "divergence_score",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for left_market, right_market in _MARKET_PAIRS:
        left_group = panel_df[panel_df["market"].astype(str) == left_market]
        right_group = panel_df[panel_df["market"].astype(str) == right_market]
        for spec in FEATURE_SPECS:
            left_values = _finite_numeric(left_group[spec.name]).dropna()
            right_values = _finite_numeric(right_group[spec.name]).dropna()
            left_z = _finite_numeric(left_group[f"{spec.name}_year_z"]).dropna()
            right_z = _finite_numeric(right_group[f"{spec.name}_year_z"]).dropna()
            if len(left_z) < min_observations or len(right_z) < min_observations:
                smd = None
                median_z_diff = None
                iqr_z_diff = None
                q_distance = None
                ks = None
                divergence_score = None
            else:
                smd = float(left_z.mean() - right_z.mean())
                median_z_diff = float(left_z.median() - right_z.median())
                iqr_z_diff = float(
                    (left_z.quantile(0.75) - left_z.quantile(0.25))
                    - (right_z.quantile(0.75) - right_z.quantile(0.25))
                )
                q_distance = _quantile_distance(left_z, right_z)
                ks = _ks_statistic(left_z, right_z)
                divergence_score = (
                    abs(smd)
                    + (q_distance if q_distance is not None else 0.0)
                    + (ks if ks is not None else 0.0)
                )
            records.append(
                {
                    "market_pair": f"{left_market}_vs_{right_market}",
                    "left_market": left_market,
                    "right_market": right_market,
                    "feature_name": spec.name,
                    "feature_label": spec.label,
                    "feature_family": spec.family,
                    "left_count": int(len(left_values)),
                    "right_count": int(len(right_values)),
                    "left_mean_value": float(left_values.mean()) if not left_values.empty else None,
                    "right_mean_value": float(right_values.mean()) if not right_values.empty else None,
                    "left_median_value": float(left_values.median()) if not left_values.empty else None,
                    "right_median_value": float(right_values.median()) if not right_values.empty else None,
                    "mean_value_diff": (
                        float(left_values.mean() - right_values.mean())
                        if not left_values.empty and not right_values.empty
                        else None
                    ),
                    "median_value_diff": (
                        float(left_values.median() - right_values.median())
                        if not left_values.empty and not right_values.empty
                        else None
                    ),
                    "standardized_mean_diff": smd,
                    "median_year_z_diff": median_z_diff,
                    "iqr_year_z_diff": iqr_z_diff,
                    "quantile_distance": q_distance,
                    "ks_statistic": ks,
                    "divergence_score": divergence_score,
                }
            )
    return pd.DataFrame(records).sort_values(
        ["market_pair", "divergence_score"],
        ascending=[True, False],
        kind="stable",
    ).reset_index(drop=True)


def _build_feature_divergence_rank_df(pair_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market",
        "feature_name",
        "feature_label",
        "feature_family",
        "comparison_count",
        "mean_abs_standardized_mean_diff",
        "max_abs_standardized_mean_diff",
        "mean_quantile_distance",
        "mean_ks_statistic",
        "divergence_score",
    ]
    if pair_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for market in _MARKET_ORDER:
        market_rows = pair_df[
            (pair_df["left_market"].astype(str) == market) | (pair_df["right_market"].astype(str) == market)
        ].copy()
        for spec in FEATURE_SPECS:
            rows = market_rows[market_rows["feature_name"].astype(str) == spec.name].copy()
            if rows.empty:
                continue
            signed_smd: list[float] = []
            for row in rows.to_dict(orient="records"):
                value = row.get("standardized_mean_diff")
                if not _is_finite_number(value):
                    continue
                number = float(cast(float, value))
                if row.get("right_market") == market:
                    number = -number
                signed_smd.append(number)
            abs_smd = pd.Series([abs(value) for value in signed_smd], dtype="float64")
            q_distance = _finite_numeric(rows["quantile_distance"]).dropna()
            ks = _finite_numeric(rows["ks_statistic"]).dropna()
            score = (
                (float(abs_smd.mean()) if not abs_smd.empty else 0.0)
                + (float(q_distance.mean()) if not q_distance.empty else 0.0)
                + (float(ks.mean()) if not ks.empty else 0.0)
            )
            records.append(
                {
                    "market": market,
                    "feature_name": spec.name,
                    "feature_label": spec.label,
                    "feature_family": spec.family,
                    "comparison_count": int(len(rows)),
                    "mean_abs_standardized_mean_diff": (
                        float(abs_smd.mean()) if not abs_smd.empty else None
                    ),
                    "max_abs_standardized_mean_diff": float(abs_smd.max()) if not abs_smd.empty else None,
                    "mean_quantile_distance": float(q_distance.mean()) if not q_distance.empty else None,
                    "mean_ks_statistic": float(ks.mean()) if not ks.empty else None,
                    "divergence_score": score,
                }
            )
    return pd.DataFrame(records).sort_values(
        ["market", "divergence_score"],
        ascending=[True, False],
        kind="stable",
    ).reset_index(drop=True)


def _standardize_columns(frame: pd.DataFrame, columns: Sequence[str]) -> tuple[pd.DataFrame, list[str]]:
    standardized = pd.DataFrame(index=frame.index)
    kept: list[str] = []
    for column in columns:
        if column not in frame.columns:
            continue
        values = _finite_numeric(frame[column])
        std = float(values.std(ddof=0))
        if not math.isfinite(std) or math.isclose(std, 0.0, abs_tol=1e-12):
            continue
        standardized[column] = (values - float(values.mean())) / std
        kept.append(column)
    return standardized, kept


def _ols_terms(
    frame: pd.DataFrame,
    *,
    y_column: str,
    numeric_columns: Sequence[str],
    fixed_effect_columns: Sequence[str],
    min_observations: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    columns = [y_column, *numeric_columns, *fixed_effect_columns]
    data = frame[[column for column in columns if column in frame.columns]].copy()
    if y_column not in data.columns:
        return _empty_df([]), {"nobs": 0, "r_squared": None}
    data[y_column] = _finite_numeric(data[y_column])
    for column in numeric_columns:
        if column in data.columns:
            data[column] = _finite_numeric(data[column])
    data = data.dropna(subset=[y_column, *[column for column in numeric_columns if column in data.columns]]).copy()
    if len(data) < min_observations:
        return _empty_df([]), {"nobs": int(len(data)), "r_squared": None}
    standardized, kept_numeric = _standardize_columns(data, numeric_columns)
    x_parts: list[pd.DataFrame] = []
    if kept_numeric:
        x_parts.append(standardized[kept_numeric].reset_index(drop=True))
    dummy_columns: list[str] = []
    for column in fixed_effect_columns:
        if column not in data.columns:
            continue
        dummy_source = data[column].fillna("unknown").astype(str)
        if column == "market":
            dummy_source = pd.Series(
                pd.Categorical(dummy_source, categories=list(_MARKET_ORDER), ordered=True),
                index=data.index,
            )
        dummies = pd.get_dummies(dummy_source, prefix=column, drop_first=True)
        if not dummies.empty:
            dummy_columns.extend(list(dummies.columns))
            x_parts.append(dummies.astype(float).reset_index(drop=True))
    if not x_parts:
        return _empty_df([]), {"nobs": int(len(data)), "r_squared": None}
    x_df = pd.concat(x_parts, axis=1)
    x = np.column_stack([np.ones(len(x_df)), x_df.to_numpy(dtype=float)])
    y = data[y_column].to_numpy(dtype=float)
    beta = np.linalg.pinv(x) @ y
    fitted = x @ beta
    residual = y - fitted
    ss_res = float(np.sum(residual**2))
    ss_tot = float(np.sum((y - float(np.mean(y))) ** 2))
    r_squared = 0.0 if math.isclose(ss_tot, 0.0, abs_tol=1e-12) else 1.0 - ss_res / ss_tot
    x_pinv = np.linalg.pinv(x.T @ x)
    meat = x.T @ ((residual[:, None] ** 2) * x)
    denom = max(1, len(y) - x.shape[1])
    covariance = x_pinv @ meat @ x_pinv * (len(y) / denom)
    standard_errors = np.sqrt(np.maximum(np.diag(covariance), 0.0))
    x_columns = ["intercept", *list(x_df.columns)]
    rows: list[dict[str, Any]] = []
    for index, column in enumerate(x_columns):
        if column == "intercept":
            continue
        if column in kept_numeric:
            term = column
            term_type = "numeric"
        elif column.startswith("market_"):
            term = column.removeprefix("market_")
            term_type = "market"
        elif column in dummy_columns:
            term = column
            term_type = "fixed_effect"
        else:
            continue
        coefficient = float(beta[index])
        standard_error = float(standard_errors[index]) if math.isfinite(float(standard_errors[index])) else None
        t_stat = (
            coefficient / standard_error
            if standard_error is not None and not math.isclose(standard_error, 0.0, abs_tol=1e-12)
            else None
        )
        rows.append(
            {
                "term": term,
                "term_type": term_type,
                "coefficient_pct": coefficient,
                "robust_se": standard_error,
                "t_stat": t_stat,
                "p_value_normal_approx": _normal_p_value_from_t(t_stat),
            }
        )
    return pd.DataFrame(rows), {
        "nobs": int(len(data)),
        "r_squared": float(r_squared),
        "feature_count": int(x.shape[1]),
    }


def _build_market_return_decomposition_df(
    panel_df: pd.DataFrame,
    *,
    min_observations: int,
) -> pd.DataFrame:
    columns = [
        "model_name",
        "model_label",
        "term",
        "term_type",
        "observation_count",
        "r_squared",
        "coefficient_pct",
        "robust_se",
        "t_stat",
        "p_value_normal_approx",
    ]
    if panel_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for model in RETURN_MODEL_SPECS:
        coef_df, summary = _ols_terms(
            panel_df,
            y_column="event_return_winsor_pct",
            numeric_columns=model.numeric_columns,
            fixed_effect_columns=model.fixed_effect_columns,
            min_observations=min_observations,
        )
        for row in coef_df.to_dict(orient="records"):
            if row["term_type"] not in {"market", "numeric"}:
                continue
            records.append(
                {
                    "model_name": model.name,
                    "model_label": model.label,
                    "term": row["term"],
                    "term_type": row["term_type"],
                    "observation_count": summary.get("nobs"),
                    "r_squared": summary.get("r_squared"),
                    "coefficient_pct": row["coefficient_pct"],
                    "robust_se": row["robust_se"],
                    "t_stat": row["t_stat"],
                    "p_value_normal_approx": row["p_value_normal_approx"],
                }
            )
    return pd.DataFrame(records).reset_index(drop=True) if records else _empty_df(columns)


def run_annual_market_fundamental_divergence(
    input_bundle_path: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    winsor_lower: float = DEFAULT_WINSOR_LOWER,
    winsor_upper: float = DEFAULT_WINSOR_UPPER,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
) -> AnnualMarketFundamentalDivergenceResult:
    if not (0.0 <= winsor_lower < winsor_upper <= 1.0):
        raise ValueError("winsor bounds must satisfy 0 <= lower < upper <= 1")
    if min_observations < 3:
        raise ValueError("min_observations must be >= 3")
    resolved_input = resolve_required_bundle_path(
        input_bundle_path,
        latest_bundle_resolver=lambda: get_annual_first_open_last_close_fundamental_panel_latest_bundle_path(
            output_root=output_root,
        ),
        missing_message=(
            "Annual first-open last-close fundamental panel bundle was not found. "
            "Run run_annual_first_open_last_close_fundamental_panel.py first."
        ),
    )
    input_info = load_research_bundle_info(resolved_input)
    tables = load_research_bundle_tables(resolved_input, table_names=("event_ledger_df",))
    event_ledger_df = tables["event_ledger_df"]
    realized_count = int((event_ledger_df["status"].astype(str) == "realized").sum())
    panel_df = _prepare_panel_df(
        event_ledger_df,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
    )
    pair_df = _build_market_pair_divergence_df(panel_df, min_observations=min_observations)
    return AnnualMarketFundamentalDivergenceResult(
        db_path=str(resolved_input),
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=input_info.analysis_start_date,
        analysis_end_date=input_info.analysis_end_date,
        winsor_lower=winsor_lower,
        winsor_upper=winsor_upper,
        min_observations=min_observations,
        input_realized_event_count=realized_count,
        analysis_event_count=int(len(panel_df)),
        score_policy=(
            "features are compared on raw values plus year-wide z-scores and percentile ranks; "
            "market buckets are not rescaled within market, so cross-market level divergence is visible"
        ),
        prepared_panel_df=panel_df,
        market_feature_profile_df=_build_market_feature_profile_df(panel_df),
        market_pair_divergence_df=pair_df,
        feature_divergence_rank_df=_build_feature_divergence_rank_df(pair_df),
        market_return_decomposition_df=_build_market_return_decomposition_df(
            panel_df,
            min_observations=min_observations,
        ),
    )


def _build_summary_markdown(result: AnnualMarketFundamentalDivergenceResult) -> str:
    lines = [
        "# Annual Market Fundamental Divergence",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Input run id: `{result.input_run_id}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Winsorized return bounds: `{result.winsor_lower}` / `{result.winsor_upper}`",
        f"- Minimum observations: `{result.min_observations}`",
        f"- Input realized events: `{result.input_realized_event_count}`",
        f"- Analysis events: `{result.analysis_event_count}`",
        f"- Score policy: {result.score_policy}.",
        "",
        "## Top Divergence Features",
        "",
    ]
    if result.feature_divergence_rank_df.empty:
        lines.append("- No divergence rows were produced.")
    else:
        for market in _MARKET_ORDER:
            rows = result.feature_divergence_rank_df[
                result.feature_divergence_rank_df["market"].astype(str) == market
            ].head(8)
            if rows.empty:
                continue
            lines.append(f"### {market}")
            for row in rows.to_dict(orient="records"):
                lines.append(
                    "- "
                    f"`{row['feature_name']}`: "
                    f"score `{_fmt(row['divergence_score'])}`, "
                    f"mean abs SMD `{_fmt(row['mean_abs_standardized_mean_diff'])}`, "
                    f"KS `{_fmt(row['mean_ks_statistic'])}`"
                )
            lines.append("")
    lines.extend(["## Market Return Decomposition", ""])
    market_rows = result.market_return_decomposition_df[
        result.market_return_decomposition_df["term_type"].astype(str) == "market"
    ].copy() if not result.market_return_decomposition_df.empty else pd.DataFrame()
    if market_rows.empty:
        lines.append("- No market dummy rows were produced.")
    else:
        for row in market_rows.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['model_name']}` / `{row['term']}`: "
                f"coef `{_fmt(row['coefficient_pct'])}pp`, "
                f"t `{_fmt(row['t_stat'])}`, "
                f"r2 `{_fmt(row['r_squared'])}`"
            )
    return "\n".join(lines)


def _build_published_summary(result: AnnualMarketFundamentalDivergenceResult) -> dict[str, Any]:
    return {
        "inputBundlePath": result.input_bundle_path,
        "inputRunId": result.input_run_id,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "winsorLower": result.winsor_lower,
        "winsorUpper": result.winsor_upper,
        "minObservations": result.min_observations,
        "inputRealizedEventCount": result.input_realized_event_count,
        "analysisEventCount": result.analysis_event_count,
        "featureDivergenceRank": result.feature_divergence_rank_df.to_dict(orient="records"),
        "marketReturnDecomposition": result.market_return_decomposition_df.to_dict(orient="records"),
    }


def write_annual_market_fundamental_divergence_bundle(
    result: AnnualMarketFundamentalDivergenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_MARKET_FUNDAMENTAL_DIVERGENCE_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_market_fundamental_divergence",
        params={
            "input_bundle_path": result.input_bundle_path,
            "winsor_lower": result.winsor_lower,
            "winsor_upper": result.winsor_upper,
            "min_observations": result.min_observations,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_market_fundamental_divergence_bundle(
    bundle_path: str | Path,
) -> AnnualMarketFundamentalDivergenceResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualMarketFundamentalDivergenceResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_market_fundamental_divergence_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_MARKET_FUNDAMENTAL_DIVERGENCE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_market_fundamental_divergence_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_MARKET_FUNDAMENTAL_DIVERGENCE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
