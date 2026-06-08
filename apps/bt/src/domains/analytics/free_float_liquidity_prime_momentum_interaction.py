"""Prime-only momentum interaction study for free-float liquidity regimes."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.free_float_liquidity_regime_decomposition import (
    get_free_float_liquidity_regime_decomposition_latest_bundle_path,
    load_free_float_liquidity_regime_decomposition_bundle,
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
from src.shared.utils.pandas_type_guards import (
    numeric_series_or_empty,
    required_int,
    required_str,
)

FREE_FLOAT_LIQUIDITY_PRIME_MOMENTUM_INTERACTION_EXPERIMENT_ID = (
    "market-behavior/free-float-liquidity-prime-momentum-interaction"
)
DEFAULT_HORIZONS: tuple[int, ...] = (20, 60)
DEFAULT_MIN_OBSERVATIONS = 100
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "prime_panel_df",
    "factor_regression_df",
    "interaction_bucket_df",
    "momentum_residual_summary_df",
)
_MODEL_SPECS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("momentum_only", ("recent_return_20d_pct", "recent_return_60d_pct")),
    ("liquidity_only", ("liquidity_residual_z",)),
    (
        "momentum_plus_liquidity",
        ("recent_return_20d_pct", "recent_return_60d_pct", "liquidity_residual_z"),
    ),
    (
        "momentum_liquidity_interaction",
        (
            "recent_return_20d_pct",
            "recent_return_60d_pct",
            "liquidity_residual_z",
            "momentum_liquidity_interaction_z",
        ),
    ),
)


@dataclass(frozen=True)
class FreeFloatLiquidityPrimeMomentumInteractionResult:
    db_path: str
    source_mode: str
    source_detail: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    min_observations: int
    feature_policy: str
    prime_panel_df: pd.DataFrame
    factor_regression_df: pd.DataFrame
    interaction_bucket_df: pd.DataFrame
    momentum_residual_summary_df: pd.DataFrame


def run_free_float_liquidity_prime_momentum_interaction(
    input_bundle_path: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
) -> FreeFloatLiquidityPrimeMomentumInteractionResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    _validate_params(horizons=resolved_horizons, min_observations=min_observations)
    resolved_input = resolve_required_bundle_path(
        input_bundle_path,
        latest_bundle_resolver=lambda: (
            get_free_float_liquidity_regime_decomposition_latest_bundle_path(
                output_root=output_root,
            )
        ),
        missing_message=(
            "Free-float liquidity regime decomposition bundle was not found. "
            "Run run_free_float_liquidity_regime_decomposition.py first."
        ),
    )
    input_info = load_research_bundle_info(resolved_input)
    regime_result = load_free_float_liquidity_regime_decomposition_bundle(
        resolved_input
    )
    prime_panel_df = _build_prime_panel_df(
        regime_result.enriched_observation_df,
        horizons=resolved_horizons,
    )
    factor_regression_df = _build_factor_regression_df(
        prime_panel_df,
        horizons=resolved_horizons,
        min_observations=min_observations,
    )
    interaction_bucket_df = _build_interaction_bucket_df(
        prime_panel_df,
        horizons=resolved_horizons,
    )
    momentum_residual_summary_df = _build_momentum_residual_summary_df(
        interaction_bucket_df,
    )
    return FreeFloatLiquidityPrimeMomentumInteractionResult(
        db_path=regime_result.db_path,
        source_mode=str(regime_result.source_mode),
        source_detail=regime_result.source_detail,
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=regime_result.analysis_start_date,
        analysis_end_date=regime_result.analysis_end_date,
        horizons=resolved_horizons,
        min_observations=int(min_observations),
        feature_policy=(
            "Prime-only diagnostic over Phase 2 observations. Recent returns and "
            "liquidity residuals are known at the observation date; regressions use "
            "forward excess returns as outcomes and standardize numeric features "
            "inside each adv_window x horizon model sample."
        ),
        prime_panel_df=prime_panel_df,
        factor_regression_df=factor_regression_df,
        interaction_bucket_df=interaction_bucket_df,
        momentum_residual_summary_df=momentum_residual_summary_df,
    )


def write_free_float_liquidity_prime_momentum_interaction_bundle(
    result: FreeFloatLiquidityPrimeMomentumInteractionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=FREE_FLOAT_LIQUIDITY_PRIME_MOMENTUM_INTERACTION_EXPERIMENT_ID,
        module=__name__,
        function="run_free_float_liquidity_prime_momentum_interaction",
        params={
            "input_bundle_path": result.input_bundle_path,
            "horizons": list(result.horizons),
            "min_observations": result.min_observations,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_free_float_liquidity_prime_momentum_interaction_bundle(
    bundle_path: str | Path,
) -> FreeFloatLiquidityPrimeMomentumInteractionResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=FreeFloatLiquidityPrimeMomentumInteractionResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_free_float_liquidity_prime_momentum_interaction_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FREE_FLOAT_LIQUIDITY_PRIME_MOMENTUM_INTERACTION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_free_float_liquidity_prime_momentum_interaction_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FREE_FLOAT_LIQUIDITY_PRIME_MOMENTUM_INTERACTION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def build_summary_markdown(
    result: FreeFloatLiquidityPrimeMomentumInteractionResult,
) -> str:
    regression = _top_rows_for_markdown(
        result.factor_regression_df,
        sort_columns=["adv_window", "horizon", "model_name", "factor_name"],
        ascending=[True, True, True, True],
        limit=80,
    )
    buckets = _top_rows_for_markdown(
        result.interaction_bucket_df,
        sort_columns=["adv_window", "horizon", "momentum_state", "liquidity_state"],
        ascending=[True, True, True, True],
        limit=60,
    )
    summary = _top_rows_for_markdown(
        result.momentum_residual_summary_df,
        sort_columns=["adv_window", "horizon", "comparison_name"],
        ascending=[True, True, True],
        limit=40,
    )
    return "\n".join(
        [
            "# Free-Float Liquidity Prime Momentum Interaction",
            "",
            f"- Input bundle: `{result.input_bundle_path}`",
            f"- Input run id: `{result.input_run_id}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Horizons: `{list(result.horizons)}`",
            f"- Min observations: `{result.min_observations}`",
            "",
            "## Factor Regressions",
            "",
            regression,
            "",
            "## Interaction Buckets",
            "",
            buckets,
            "",
            "## Momentum Residual Summary",
            "",
            summary,
            "",
        ]
    )


def _validate_params(*, horizons: Sequence[int], min_observations: int) -> None:
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if min_observations < 10:
        raise ValueError("min_observations must be at least 10")


def _build_prime_panel_df(
    enriched_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    required_columns = [
        "date",
        "code",
        "company_name",
        "sector_33_name",
        "adv_window",
        "liquidity_regime",
        "recent_return_20d_pct",
        "recent_return_60d_pct",
        "liquidity_residual_z",
        "liquidity_residual_change",
        "adv_mil_jpy",
        "free_float_market_cap_bil_jpy",
    ]
    forward_columns = [f"forward_excess_return_{horizon}d_pct" for horizon in horizons]
    columns = [
        *required_columns,
        *forward_columns,
        "momentum_state",
        "liquidity_state",
        "recent_return_20d_cross_z",
        "recent_return_60d_cross_z",
        "liquidity_residual_cross_z",
        "momentum_composite_cross_z",
        "momentum_liquidity_interaction_z",
    ]
    if enriched_df.empty:
        return pd.DataFrame(columns=columns)
    prime = enriched_df[enriched_df["market_scope"].astype(str) == "prime"].copy()
    if prime.empty:
        return pd.DataFrame(columns=columns)
    keep_columns = [
        column
        for column in [*required_columns, *forward_columns]
        if column in prime.columns
    ]
    panel = prime[keep_columns].copy()
    panel["momentum_state"] = np.where(
        (pd.to_numeric(panel["recent_return_20d_pct"], errors="coerce") > 0)
        & (pd.to_numeric(panel["recent_return_60d_pct"], errors="coerce") > 0),
        "positive_20d_60d",
        "mixed_or_negative",
    )
    residual = pd.to_numeric(panel["liquidity_residual_z"], errors="coerce")
    panel["liquidity_state"] = np.select(
        [residual >= 1.0, residual <= -1.0],
        ["high_residual", "low_residual"],
        default="neutral_residual",
    )
    panel = _add_cross_sectional_z_features(panel)
    return (
        panel.reindex(columns=columns)
        .sort_values(["adv_window", "date", "code"], kind="stable")
        .reset_index(drop=True)
    )


def _add_cross_sectional_z_features(panel: pd.DataFrame) -> pd.DataFrame:
    result = panel.copy()
    group_keys = ["date", "adv_window"]
    for source_column, target_column in (
        ("recent_return_20d_pct", "recent_return_20d_cross_z"),
        ("recent_return_60d_pct", "recent_return_60d_cross_z"),
        ("liquidity_residual_z", "liquidity_residual_cross_z"),
    ):
        result[target_column] = result.groupby(group_keys, group_keys=False)[
            source_column
        ].transform(_zscore_series)
    result["momentum_composite_cross_z"] = result[
        ["recent_return_20d_cross_z", "recent_return_60d_cross_z"]
    ].mean(axis=1)
    result["momentum_liquidity_interaction_z"] = (
        result["momentum_composite_cross_z"] * result["liquidity_residual_cross_z"]
    )
    return result


def _build_factor_regression_df(
    panel_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    min_observations: int,
) -> pd.DataFrame:
    columns = [
        "adv_window",
        "horizon",
        "model_name",
        "factor_name",
        "observation_count",
        "r_squared",
        "coefficient_pct_per_1sd",
        "standard_error",
        "t_stat",
    ]
    if panel_df.empty:
        return pd.DataFrame(columns=columns)
    records: list[dict[str, Any]] = []
    for adv_window, adv_frame in panel_df.groupby("adv_window", sort=True):
        for horizon in horizons:
            y_column = f"forward_excess_return_{horizon}d_pct"
            if y_column not in adv_frame.columns:
                continue
            for model_name, factor_columns in _MODEL_SPECS:
                rows = _ols_rows(
                    adv_frame,
                    y_column=y_column,
                    factor_columns=factor_columns,
                    min_observations=min_observations,
                )
                for row in rows:
                    records.append(
                        {
                            "adv_window": int(cast(Any, adv_window)),
                            "horizon": int(horizon),
                            "model_name": model_name,
                            **row,
                        }
                    )
    return pd.DataFrame.from_records(records, columns=columns)


def _ols_rows(
    frame: pd.DataFrame,
    *,
    y_column: str,
    factor_columns: Sequence[str],
    min_observations: int,
) -> list[dict[str, Any]]:
    model_frame = frame[[y_column, *factor_columns]].copy()
    for column in model_frame.columns:
        model_frame[column] = pd.to_numeric(model_frame[column], errors="coerce")
    model_frame = model_frame.replace([np.inf, -np.inf], np.nan).dropna()
    if len(model_frame) < min_observations:
        return []
    y = model_frame[y_column].to_numpy(dtype=float)
    x_columns: list[np.ndarray[Any, np.dtype[np.float64]]] = []
    for factor_column in factor_columns:
        values = model_frame[factor_column].to_numpy(dtype=float)
        std = float(np.nanstd(values, ddof=1))
        if not np.isfinite(std) or std <= 0:
            return []
        x_columns.append((values - float(np.nanmean(values))) / std)
    x = np.column_stack([np.ones(len(model_frame)), *x_columns])
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    fitted = x @ beta
    residual = y - fitted
    dof = len(y) - x.shape[1]
    if dof <= 0:
        return []
    sigma2 = float((residual @ residual) / dof)
    xtx_inv = np.linalg.pinv(x.T @ x)
    standard_errors = np.sqrt(np.diag(xtx_inv) * sigma2)
    r_squared = _r_squared(y, fitted)
    rows: list[dict[str, Any]] = []
    for index, factor_name in enumerate(factor_columns, start=1):
        coefficient = float(beta[index])
        standard_error = float(standard_errors[index])
        rows.append(
            {
                "factor_name": factor_name,
                "observation_count": int(len(model_frame)),
                "r_squared": _round_float(r_squared),
                "coefficient_pct_per_1sd": _round_float(coefficient),
                "standard_error": _round_float(standard_error),
                "t_stat": _round_float(
                    coefficient / standard_error if standard_error > 0 else np.nan
                ),
            }
        )
    return rows


def _build_interaction_bucket_df(
    panel_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    columns = [
        "adv_window",
        "horizon",
        "momentum_state",
        "liquidity_state",
        "observation_count",
        "code_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "win_rate_pct",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_liquidity_residual_z",
    ]
    if panel_df.empty:
        return pd.DataFrame(columns=columns)
    records: list[dict[str, Any]] = []
    for keys, group in panel_df.groupby(
        ["adv_window", "momentum_state", "liquidity_state"],
        sort=True,
    ):
        adv_window, momentum_state, liquidity_state = keys
        for horizon in horizons:
            y_column = f"forward_excess_return_{horizon}d_pct"
            if y_column not in group.columns:
                continue
            returns = pd.to_numeric(group[y_column], errors="coerce")
            records.append(
                {
                    "adv_window": required_int(adv_window, field="adv_window"),
                    "horizon": int(horizon),
                    "momentum_state": required_str(momentum_state, field="momentum_state"),
                    "liquidity_state": required_str(
                        liquidity_state,
                        field="liquidity_state",
                    ),
                    "observation_count": int(returns.notna().sum()),
                    "code_count": int(group["code"].nunique()),
                    "mean_forward_excess_return_pct": _mean(returns),
                    "median_forward_excess_return_pct": _median(returns),
                    "win_rate_pct": _win_rate(returns),
                    "median_recent_return_20d_pct": _median(
                        group["recent_return_20d_pct"]
                    ),
                    "median_recent_return_60d_pct": _median(
                        group["recent_return_60d_pct"]
                    ),
                    "median_liquidity_residual_z": _median(
                        group["liquidity_residual_z"]
                    ),
                }
            )
    return pd.DataFrame.from_records(records, columns=columns).sort_values(
        ["adv_window", "horizon", "momentum_state", "liquidity_state"],
        kind="stable",
    )


def _build_momentum_residual_summary_df(bucket_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "adv_window",
        "horizon",
        "comparison_name",
        "lhs_group",
        "rhs_group",
        "lhs_observation_count",
        "rhs_observation_count",
        "mean_excess_spread_pct",
        "median_excess_spread_pct",
        "win_rate_spread_pct",
    ]
    if bucket_df.empty:
        return pd.DataFrame(columns=columns)
    records: list[dict[str, Any]] = []
    comparisons = (
        (
            "positive_momentum_high_minus_neutral",
            ("positive_20d_60d", "high_residual"),
            ("positive_20d_60d", "neutral_residual"),
        ),
        (
            "high_residual_positive_minus_mixed",
            ("positive_20d_60d", "high_residual"),
            ("mixed_or_negative", "high_residual"),
        ),
        (
            "positive_momentum_high_minus_low",
            ("positive_20d_60d", "high_residual"),
            ("positive_20d_60d", "low_residual"),
        ),
    )
    for keys, group in bucket_df.groupby(["adv_window", "horizon"], sort=True):
        adv_window, horizon = keys
        indexed = {
            (str(row["momentum_state"]), str(row["liquidity_state"])): row
            for row in group.to_dict(orient="records")
        }
        for comparison_name, lhs_key, rhs_key in comparisons:
            lhs = indexed.get(lhs_key)
            rhs = indexed.get(rhs_key)
            if lhs is None or rhs is None:
                continue
            records.append(
                {
                    "adv_window": required_int(adv_window, field="adv_window"),
                    "horizon": required_int(horizon, field="horizon"),
                    "comparison_name": comparison_name,
                    "lhs_group": "__".join(lhs_key),
                    "rhs_group": "__".join(rhs_key),
                    "lhs_observation_count": int(lhs["observation_count"]),
                    "rhs_observation_count": int(rhs["observation_count"]),
                    "mean_excess_spread_pct": _round_float(
                        float(lhs["mean_forward_excess_return_pct"])
                        - float(rhs["mean_forward_excess_return_pct"])
                    ),
                    "median_excess_spread_pct": _round_float(
                        float(lhs["median_forward_excess_return_pct"])
                        - float(rhs["median_forward_excess_return_pct"])
                    ),
                    "win_rate_spread_pct": _round_float(
                        float(lhs["win_rate_pct"]) - float(rhs["win_rate_pct"])
                    ),
                }
            )
    return pd.DataFrame.from_records(records, columns=columns)


def _zscore_series(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    std = float(values.std(ddof=1))
    if not np.isfinite(std) or std <= 0:
        return pd.Series(np.nan, index=series.index, dtype="float64")
    return (values - float(values.mean())) / std


def _r_squared(
    y: np.ndarray[Any, np.dtype[np.float64]],
    fitted: np.ndarray[Any, np.dtype[np.float64]],
) -> float:
    centered = y - y.mean()
    total = float(centered @ centered)
    if total <= 0:
        return np.nan
    residual = y - fitted
    return float(1.0 - (residual @ residual) / total)


def _top_rows_for_markdown(
    df: pd.DataFrame,
    *,
    sort_columns: Sequence[str],
    ascending: Sequence[bool],
    limit: int,
) -> str:
    if df.empty:
        return "_No rows._"
    frame = df.copy()
    existing_sort = [column for column in sort_columns if column in frame.columns]
    if existing_sort:
        frame = frame.sort_values(
            existing_sort,
            ascending=list(ascending)[: len(existing_sort)],
            kind="stable",
        )
    return _frame_to_markdown(frame.head(limit))


def _frame_to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._"
    columns = [str(column) for column in df.columns]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in df.itertuples(index=False, name=None):
        values = [_format_markdown_cell(value) for value in row]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _format_markdown_cell(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float):
        return str(_round_float(value, 6))
    return str(value).replace("|", "\\|")


def _mean(values: Iterable[Any] | None) -> float:
    if values is None:
        return np.nan
    series = numeric_series_or_empty(values)
    return _round_float(float(series.mean())) if not series.empty else np.nan


def _median(values: Iterable[Any] | None) -> float:
    if values is None:
        return np.nan
    series = numeric_series_or_empty(values)
    return _round_float(float(series.median())) if not series.empty else np.nan


def _win_rate(values: Iterable[Any] | None) -> float:
    if values is None:
        return np.nan
    series = numeric_series_or_empty(values)
    return (
        _round_float(float((series > 0).mean() * 100.0)) if not series.empty else np.nan
    )


def _round_float(value: Any, digits: int = 4) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return np.nan
    if not np.isfinite(numeric):
        return np.nan
    return round(numeric, digits)
