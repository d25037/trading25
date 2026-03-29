# pyright: reportUnusedFunction=false

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from src.domains.analytics.hedge_1357_nt_ratio_topix_support import (
    RULE_ORDER,
    SPLIT_ORDER,
    TARGET_ORDER,
    RuleName,
    SplitName,
    TargetName,
    _BETA_LOOKBACK_DAYS,
    _BETA_MIN_PERIODS,
    _TARGET_COLUMN_MAP,
)


def _expected_shortfall(series: pd.Series, tail_probability: float = 0.05) -> float | None:
    valid = series.dropna().sort_values()
    if valid.empty:
        return None
    tail_count = max(1, int(np.ceil(len(valid) * tail_probability)))
    return float(valid.iloc[:tail_count].mean())


def _max_drawdown(series: pd.Series) -> float | None:
    valid = series.dropna()
    if valid.empty:
        return None
    cumulative = (1.0 + valid).cumprod()
    peaks = cumulative.cummax()
    drawdown = 1.0 - (cumulative / peaks)
    return float(drawdown.max())


def _split_filter(df: pd.DataFrame, split_name: SplitName) -> pd.DataFrame:
    if split_name == "overall":
        return df.copy()
    return df[df["split"] == split_name].copy()


def _build_beta_neutral_weights(daily_proxy_returns_df: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for _stock_group, group_df in daily_proxy_returns_df.groupby("stock_group", sort=False):
        ordered = group_df.sort_values("date").copy()
        long_returns = ordered["long_next_close_to_close_return"].shift(1)
        etf_returns = ordered["etf_next_close_to_close_return"].shift(1)
        covariance = long_returns.rolling(
            _BETA_LOOKBACK_DAYS,
            min_periods=_BETA_MIN_PERIODS,
        ).cov(etf_returns)
        variance = etf_returns.rolling(
            _BETA_LOOKBACK_DAYS,
            min_periods=_BETA_MIN_PERIODS,
        ).var()
        weights = (-covariance / variance).replace([np.inf, -np.inf], np.nan)
        ordered["beta_neutral_weight"] = weights.clip(lower=0.0, upper=1.0)
        ordered["beta_neutral_weight_effective"] = ordered["beta_neutral_weight"].fillna(0.0)
        frames.append(ordered)
    if not frames:
        return daily_proxy_returns_df
    return pd.concat(frames, ignore_index=True)


def _evaluate_hedge_config(
    df: pd.DataFrame,
    *,
    target_name: TargetName,
    rule_name: RuleName,
    weight_label: str,
    fixed_weight: float | None,
) -> dict[str, Any]:
    long_column, etf_column = _TARGET_COLUMN_MAP[target_name]
    required_columns = [long_column, etf_column, rule_name]
    if fixed_weight is None:
        required_columns.extend(
            ["beta_neutral_weight", "beta_neutral_weight_effective"]
        )
    valid = df[required_columns].dropna(subset=[long_column, etf_column, rule_name]).copy()
    signal = valid[rule_name].astype(bool)
    if fixed_weight is None:
        raw_weight = valid["beta_neutral_weight_effective"].astype(float)
        mean_weight = (
            float(valid.loc[signal, "beta_neutral_weight"].dropna().mean())
            if signal.any()
            else np.nan
        )
    else:
        raw_weight = pd.Series(float(fixed_weight), index=valid.index)
        mean_weight = float(fixed_weight) if signal.any() else np.nan
    applied_weight = raw_weight.where(signal, 0.0)
    unhedged = valid[long_column]
    hedged = unhedged + applied_weight * valid[etf_column]
    hedge_delta = hedged - unhedged
    stress_mask = unhedged < 0
    non_stress_mask = ~stress_mask
    es_unhedged = _expected_shortfall(unhedged)
    es_hedged = _expected_shortfall(hedged)
    mdd_unhedged = _max_drawdown(unhedged)
    mdd_hedged = _max_drawdown(hedged)
    stress_unhedged = unhedged[stress_mask]
    stress_hedged = hedged[stress_mask]
    non_stress_delta = hedge_delta[non_stress_mask]
    down_day_hit_rate = (
        float((stress_hedged > stress_unhedged).mean())
        if not stress_unhedged.empty
        else np.nan
    )
    return {
        "target_name": target_name,
        "rule_name": rule_name,
        "weight_label": weight_label,
        "sample_count": int(valid.shape[0]),
        "active_day_count": int(signal.sum()),
        "active_ratio": float(signal.mean()) if not valid.empty else np.nan,
        "mean_weight_when_active": mean_weight,
        "unhedged_mean_return": float(unhedged.mean()) if not unhedged.empty else np.nan,
        "hedged_mean_return": float(hedged.mean()) if not hedged.empty else np.nan,
        "hedge_pnl_mean": float(hedge_delta.mean()) if not hedge_delta.empty else np.nan,
        "stress_day_count": int(stress_mask.sum()),
        "unhedged_stress_mean_return": (
            float(stress_unhedged.mean()) if not stress_unhedged.empty else np.nan
        ),
        "hedged_stress_mean_return": (
            float(stress_hedged.mean()) if not stress_hedged.empty else np.nan
        ),
        "stress_mean_loss_improvement": (
            float(stress_hedged.mean() - stress_unhedged.mean())
            if not stress_unhedged.empty
            else np.nan
        ),
        "expected_shortfall_5_unhedged": (
            float(es_unhedged) if es_unhedged is not None else np.nan
        ),
        "expected_shortfall_5_hedged": (
            float(es_hedged) if es_hedged is not None else np.nan
        ),
        "expected_shortfall_improvement": (
            float(es_hedged - es_unhedged)
            if es_unhedged is not None and es_hedged is not None
            else np.nan
        ),
        "max_drawdown_unhedged": (
            float(mdd_unhedged) if mdd_unhedged is not None else np.nan
        ),
        "max_drawdown_hedged": float(mdd_hedged) if mdd_hedged is not None else np.nan,
        "max_drawdown_improvement": (
            float(mdd_unhedged - mdd_hedged)
            if mdd_unhedged is not None and mdd_hedged is not None
            else np.nan
        ),
        "down_day_hit_rate": down_day_hit_rate,
        "carry_cost_non_stress": (
            float(non_stress_delta.mean()) if not non_stress_delta.empty else np.nan
        ),
    }


def _build_hedge_metrics(
    daily_proxy_returns_df: pd.DataFrame,
    *,
    fixed_weights: tuple[float, ...],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    weight_specs: list[tuple[str, float | None]] = [
        (f"fixed_{weight:.2f}", weight) for weight in fixed_weights
    ]
    weight_specs.append(("beta_neutral_60d", None))
    for stock_group, group_df in daily_proxy_returns_df.groupby("stock_group", sort=False):
        for split_name in SPLIT_ORDER:
            split_df = _split_filter(group_df, split_name)
            for target_name in TARGET_ORDER:
                for rule_name in RULE_ORDER:
                    for weight_label, fixed_weight in weight_specs:
                        metrics = _evaluate_hedge_config(
                            split_df,
                            target_name=target_name,
                            rule_name=rule_name,
                            weight_label=weight_label,
                            fixed_weight=fixed_weight,
                        )
                        metrics.update(
                            {
                                "split": split_name,
                                "stock_group": stock_group,
                            }
                        )
                        rows.append(metrics)
    return pd.DataFrame(rows)


def _evaluate_etf_strategy(
    df: pd.DataFrame,
    *,
    target_name: TargetName,
    rule_name: RuleName,
) -> dict[str, Any]:
    etf_column = _TARGET_COLUMN_MAP[target_name][1]
    valid = df[[etf_column, rule_name]].dropna(subset=[etf_column, rule_name]).copy()
    signal = valid[rule_name].astype(bool)
    etf_returns = valid[etf_column].astype(float)
    strategy_returns = etf_returns.where(signal, 0.0)
    active_returns = etf_returns[signal]
    expected_shortfall = _expected_shortfall(strategy_returns)
    max_drawdown = _max_drawdown(strategy_returns)
    strategy_total_return = (
        float(np.prod(1.0 + strategy_returns.to_numpy(dtype=float)) - 1.0)
        if not strategy_returns.empty
        else np.nan
    )
    return {
        "target_name": target_name,
        "rule_name": rule_name,
        "sample_count": int(valid.shape[0]),
        "active_day_count": int(signal.sum()),
        "active_ratio": float(signal.mean()) if not valid.empty else np.nan,
        "mean_return_when_active": (
            float(active_returns.mean()) if not active_returns.empty else np.nan
        ),
        "strategy_mean_return": (
            float(strategy_returns.mean()) if not strategy_returns.empty else np.nan
        ),
        "strategy_total_return": strategy_total_return,
        "expected_shortfall_5": (
            float(expected_shortfall) if expected_shortfall is not None else np.nan
        ),
        "max_drawdown": float(max_drawdown) if max_drawdown is not None else np.nan,
        "positive_rate_when_active": (
            float((active_returns > 0).mean()) if not active_returns.empty else np.nan
        ),
    }


def _build_etf_strategy_metrics(daily_market_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split_name in SPLIT_ORDER:
        split_df = _split_filter(daily_market_df, split_name)
        for target_name in TARGET_ORDER:
            for rule_name in RULE_ORDER:
                metrics = _evaluate_etf_strategy(
                    split_df,
                    target_name=target_name,
                    rule_name=rule_name,
                )
                metrics["split"] = split_name
                rows.append(metrics)
    return pd.DataFrame(rows)


def _build_annual_rule_summary(
    daily_proxy_returns_df: pd.DataFrame,
    *,
    fixed_weights: tuple[float, ...],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    weight_specs: list[tuple[str, float | None]] = [
        (f"fixed_{weight:.2f}", weight) for weight in fixed_weights
    ]
    weight_specs.append(("beta_neutral_60d", None))
    for (stock_group, year), group_df in daily_proxy_returns_df.groupby(
        ["stock_group", "calendar_year"],
        sort=False,
    ):
        for target_name in TARGET_ORDER:
            for rule_name in RULE_ORDER:
                for weight_label, fixed_weight in weight_specs:
                    metrics = _evaluate_hedge_config(
                        group_df,
                        target_name=target_name,
                        rule_name=rule_name,
                        weight_label=weight_label,
                        fixed_weight=fixed_weight,
                    )
                    metrics.update(
                        {
                            "stock_group": stock_group,
                            "calendar_year": int(year),
                        }
                    )
                    rows.append(metrics)
    return pd.DataFrame(rows)


__all__ = [
    "_build_annual_rule_summary",
    "_build_beta_neutral_weights",
    "_build_etf_strategy_metrics",
    "_build_hedge_metrics",
    "_evaluate_etf_strategy",
    "_evaluate_hedge_config",
    "_expected_shortfall",
    "_max_drawdown",
]
