"""Prime value volatility-participation research."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.annual_prime_value_technical_risk_decomposition import (
    DEFAULT_MARKET_SCOPE,
    DEFAULT_SELECTION_FRACTIONS,
    _build_portfolio_daily_df,
    _build_portfolio_summary_df,
    _focus_prime_events,
)
from src.domains.analytics.annual_value_composite_selection import (
    get_annual_value_composite_selection_latest_bundle_path,
    load_annual_value_composite_selection_bundle,
    _empty_df,
    _series_mean,
)
from src.domains.analytics.annual_value_composite_technical_filter import (
    _load_technical_price_frames,
)
from src.domains.analytics.annual_value_technical_feature_importance import (
    DEFAULT_BUCKET_COUNT,
    DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
    DEFAULT_FOCUS_SCORE_METHODS,
    DEFAULT_WARMUP_SMA_WINDOW,
    _build_enriched_event_df,
    _normalize_score_methods,
    _yearly_bucket_rank,
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

ANNUAL_PRIME_VALUE_VOLUME_VOLATILITY_PARTICIPATION_EXPERIMENT_ID = (
    "market-behavior/annual-prime-value-volume-volatility-participation"
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "enriched_event_df",
    "volatility_participation_summary_df",
    "participation_split_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)

_VOLATILITY_FEATURE_LABELS: dict[str, str] = {
    "volatility_20d_pct": "Volatility 20d",
    "volatility_60d_pct": "Volatility 60d",
    "downside_volatility_60d_pct": "Downside volatility 60d",
}
_VOLATILITY_FEATURES: tuple[str, ...] = tuple(_VOLATILITY_FEATURE_LABELS)
_PARTICIPATION_FEATURE_LABELS: dict[str, str] = {
    "volume_ratio_20_60": "Volume ratio 20/60",
    "trading_value_ratio_20_60": "Trading value ratio 20/60",
}
_PARTICIPATION_FEATURES: tuple[str, ...] = tuple(_PARTICIPATION_FEATURE_LABELS)


@dataclass(frozen=True)
class AnnualPrimeValueVolumeVolatilityParticipationResult:
    db_path: str
    source_mode: str
    source_detail: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    market_scope: str
    selection_fractions: tuple[float, ...]
    liquidity_scenario: str
    score_methods: tuple[str, ...]
    bucket_count: int
    participation_bucket_count: int
    selected_event_count: int
    feature_policy: str
    enriched_event_df: pd.DataFrame
    volatility_participation_summary_df: pd.DataFrame
    participation_split_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


def run_annual_prime_value_volume_volatility_participation(
    input_bundle_path: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    selection_fractions: Sequence[float] = DEFAULT_SELECTION_FRACTIONS,
    market_scope: str = DEFAULT_MARKET_SCOPE,
    liquidity_scenario: str = DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
    score_methods: Sequence[str] = DEFAULT_FOCUS_SCORE_METHODS,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    participation_bucket_count: int = 3,
) -> AnnualPrimeValueVolumeVolatilityParticipationResult:
    if bucket_count < 2:
        raise ValueError("bucket_count must be >= 2")
    if participation_bucket_count < 2:
        raise ValueError("participation_bucket_count must be >= 2")
    normalized_score_methods = _normalize_score_methods(score_methods)
    normalized_fractions = tuple(float(value) for value in selection_fractions)
    if not normalized_fractions:
        raise ValueError("selection_fractions must not be empty")

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
    selected_event_df = _focus_prime_events(
        value_result.selected_event_df,
        market_scope=market_scope,
        selection_fractions=normalized_fractions,
        liquidity_scenario=liquidity_scenario,
        score_methods=normalized_score_methods,
    )
    source_mode, source_detail, stock_price_df, topix_price_df = _load_technical_price_frames(
        value_result.db_path,
        selected_event_df,
        sma_window=DEFAULT_WARMUP_SMA_WINDOW,
    )
    enriched = _build_enriched_event_df(
        selected_event_df,
        stock_price_df=stock_price_df,
        topix_price_df=topix_price_df,
    )
    volatility_participation_summary_df = _build_volatility_participation_summary_df(
        enriched,
        bucket_count=bucket_count,
    )
    participation_split_df = _build_participation_split_df(
        enriched,
        bucket_count=bucket_count,
        participation_bucket_count=participation_bucket_count,
    )
    portfolio_event_df = _build_participation_portfolio_event_df(
        enriched,
        bucket_count=bucket_count,
        participation_bucket_count=participation_bucket_count,
    )
    portfolio_daily_df = _build_portfolio_daily_df(portfolio_event_df, stock_price_df)
    portfolio_summary_df = _build_portfolio_summary_df(portfolio_daily_df, portfolio_event_df)

    return AnnualPrimeValueVolumeVolatilityParticipationResult(
        db_path=value_result.db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        analysis_start_date=value_result.analysis_start_date,
        analysis_end_date=value_result.analysis_end_date,
        market_scope=str(market_scope),
        selection_fractions=normalized_fractions,
        liquidity_scenario=str(liquidity_scenario),
        score_methods=normalized_score_methods,
        bucket_count=int(bucket_count),
        participation_bucket_count=int(participation_bucket_count),
        selected_event_count=int(len(selected_event_df)),
        feature_policy=(
            "technical and participation features use the latest trading session strictly before entry_date; "
            "participation features are raw 20d/60d volume and trading-value ratios; "
            "portfolio lens uses equal-weight daily close path from annual entry open to exit close; "
            "fixed_55_25_20 is intentionally excluded"
        ),
        enriched_event_df=enriched,
        volatility_participation_summary_df=volatility_participation_summary_df,
        participation_split_df=participation_split_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
    )


def _build_volatility_participation_summary_df(
    enriched_event_df: pd.DataFrame,
    *,
    bucket_count: int,
) -> pd.DataFrame:
    columns = [
        "selection_fraction",
        "score_method",
        "volatility_feature_key",
        "volatility_feature_label",
        "event_count",
        "high_vol_event_count",
        "correlation_volume_ratio_20_60",
        "correlation_trading_value_ratio_20_60",
        "low_vol_median_volume_ratio_20_60",
        "high_vol_median_volume_ratio_20_60",
        "low_vol_median_trading_value_ratio_20_60",
        "high_vol_median_trading_value_ratio_20_60",
        "low_vol_mean_return_pct",
        "high_vol_mean_return_pct",
        "low_vol_p10_return_pct",
        "high_vol_p10_return_pct",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for keys, group in enriched_event_df.groupby(["selection_fraction", "score_method"], sort=False):
        selection_fraction, score_method = keys
        for vol_feature in _VOLATILITY_FEATURES:
            valid = group[pd.to_numeric(group[vol_feature], errors="coerce").notna()].copy()
            if valid.empty:
                continue
            valid["volatility_bucket_rank"] = _yearly_bucket_rank(valid, vol_feature, bucket_count=bucket_count)
            low = valid[valid["volatility_bucket_rank"].astype(int) == 1]
            high = valid[valid["volatility_bucket_rank"].astype(int) == bucket_count]
            records.append(
                {
                    "selection_fraction": float(selection_fraction),
                    "score_method": str(score_method),
                    "volatility_feature_key": vol_feature,
                    "volatility_feature_label": _VOLATILITY_FEATURE_LABELS[vol_feature],
                    "event_count": int(len(valid)),
                    "high_vol_event_count": int(len(high)),
                    "correlation_volume_ratio_20_60": _corr(valid[vol_feature], valid["volume_ratio_20_60"]),
                    "correlation_trading_value_ratio_20_60": _corr(
                        valid[vol_feature],
                        valid["trading_value_ratio_20_60"],
                    ),
                    "low_vol_median_volume_ratio_20_60": _median(low["volume_ratio_20_60"]),
                    "high_vol_median_volume_ratio_20_60": _median(high["volume_ratio_20_60"]),
                    "low_vol_median_trading_value_ratio_20_60": _median(low["trading_value_ratio_20_60"]),
                    "high_vol_median_trading_value_ratio_20_60": _median(high["trading_value_ratio_20_60"]),
                    "low_vol_mean_return_pct": _mean_return(low),
                    "high_vol_mean_return_pct": _mean_return(high),
                    "low_vol_p10_return_pct": _quantile(low["event_return_winsor_pct"], 0.10),
                    "high_vol_p10_return_pct": _quantile(high["event_return_winsor_pct"], 0.10),
                }
            )
    return pd.DataFrame(records, columns=columns)


def _build_participation_split_df(
    enriched_event_df: pd.DataFrame,
    *,
    bucket_count: int,
    participation_bucket_count: int,
) -> pd.DataFrame:
    columns = [
        "selection_fraction",
        "score_method",
        "volatility_feature_key",
        "volatility_feature_label",
        "participation_feature_key",
        "participation_feature_label",
        "variant",
        "event_count",
        "year_count",
        "mean_return_pct",
        "median_return_pct",
        "p10_return_pct",
        "worst_return_pct",
        "win_rate_pct",
        "median_volatility_feature",
        "median_participation_feature",
        "median_downside_volatility_60d_pct",
        "median_adv60_mil_jpy",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for keys, group in enriched_event_df.groupby(["selection_fraction", "score_method"], sort=False):
        selection_fraction, score_method = keys
        for vol_feature in _VOLATILITY_FEATURES:
            valid = group[pd.to_numeric(group[vol_feature], errors="coerce").notna()].copy()
            if valid.empty:
                continue
            valid["volatility_bucket_rank"] = _yearly_bucket_rank(valid, vol_feature, bucket_count=bucket_count)
            high_vol = valid[valid["volatility_bucket_rank"].astype(int) == bucket_count].copy()
            if high_vol.empty:
                continue
            for participation_feature in _PARTICIPATION_FEATURES:
                part_valid = high_vol[
                    pd.to_numeric(high_vol[participation_feature], errors="coerce").notna()
                ].copy()
                if part_valid.empty:
                    continue
                part_valid["participation_bucket_rank"] = _yearly_bucket_rank(
                    part_valid,
                    participation_feature,
                    bucket_count=participation_bucket_count,
                )
                frames: tuple[tuple[str, pd.DataFrame], ...] = (
                    ("all_selected", valid),
                    ("high_vol", high_vol),
                    (
                        "high_vol_low_participation",
                        part_valid[part_valid["participation_bucket_rank"].astype(int) == 1],
                    ),
                    (
                        "high_vol_high_participation",
                        part_valid[
                            part_valid["participation_bucket_rank"].astype(int) == participation_bucket_count
                        ],
                    ),
                )
                for variant, frame in frames:
                    records.append(
                        _split_record(
                            selection_fraction=float(selection_fraction),
                            score_method=str(score_method),
                            vol_feature=vol_feature,
                            participation_feature=participation_feature,
                            variant=variant,
                            frame=frame,
                        )
                    )
    return pd.DataFrame(records, columns=columns)


def _build_participation_portfolio_event_df(
    enriched_event_df: pd.DataFrame,
    *,
    bucket_count: int,
    participation_bucket_count: int,
) -> pd.DataFrame:
    columns = [
        *list(enriched_event_df.columns),
        "portfolio_feature_key",
        "portfolio_feature_label",
        "portfolio_variant",
        "portfolio_bucket_rank",
    ]
    if enriched_event_df.empty:
        return _empty_df(columns)
    records: list[dict[str, Any]] = []
    for keys, group in enriched_event_df.groupby(["selection_fraction", "score_method"], sort=False):
        _selection_fraction, _score_method = keys
        for vol_feature in ("volatility_20d_pct", "volatility_60d_pct"):
            valid = group[pd.to_numeric(group[vol_feature], errors="coerce").notna()].copy()
            if valid.empty:
                continue
            valid["volatility_bucket_rank"] = _yearly_bucket_rank(valid, vol_feature, bucket_count=bucket_count)
            high_vol = valid[valid["volatility_bucket_rank"].astype(int) == bucket_count].copy()
            for participation_feature in _PARTICIPATION_FEATURES:
                part_valid = high_vol[
                    pd.to_numeric(high_vol[participation_feature], errors="coerce").notna()
                ].copy()
                if part_valid.empty:
                    continue
                part_valid["participation_bucket_rank"] = _yearly_bucket_rank(
                    part_valid,
                    participation_feature,
                    bucket_count=participation_bucket_count,
                )
                variants: tuple[tuple[str, pd.DataFrame, int], ...] = (
                    ("baseline", valid, 0),
                    ("high_vol", high_vol, bucket_count),
                    (
                        "high_vol_low_participation",
                        part_valid[part_valid["participation_bucket_rank"].astype(int) == 1],
                        1,
                    ),
                    (
                        "high_vol_high_participation",
                        part_valid[
                            part_valid["participation_bucket_rank"].astype(int) == participation_bucket_count
                        ],
                        participation_bucket_count,
                    ),
                )
                feature_key = f"{vol_feature}__{participation_feature}"
                feature_label = (
                    f"{_VOLATILITY_FEATURE_LABELS[vol_feature]} x "
                    f"{_PARTICIPATION_FEATURE_LABELS[participation_feature]}"
                )
                for variant, frame, bucket_rank in variants:
                    for event in frame.to_dict(orient="records"):
                        payload: dict[str, Any] = {str(key): value for key, value in event.items()}
                        payload["portfolio_feature_key"] = feature_key
                        payload["portfolio_feature_label"] = feature_label
                        payload["portfolio_variant"] = variant
                        payload["portfolio_bucket_rank"] = int(bucket_rank)
                        records.append(payload)
    return pd.DataFrame(records, columns=columns)


def _split_record(
    *,
    selection_fraction: float,
    score_method: str,
    vol_feature: str,
    participation_feature: str,
    variant: str,
    frame: pd.DataFrame,
) -> dict[str, Any]:
    returns = pd.to_numeric(frame["event_return_winsor_pct"], errors="coerce").dropna()
    return {
        "selection_fraction": selection_fraction,
        "score_method": score_method,
        "volatility_feature_key": vol_feature,
        "volatility_feature_label": _VOLATILITY_FEATURE_LABELS[vol_feature],
        "participation_feature_key": participation_feature,
        "participation_feature_label": _PARTICIPATION_FEATURE_LABELS[participation_feature],
        "variant": variant,
        "event_count": int(len(frame)),
        "year_count": int(frame["year"].nunique()) if "year" in frame.columns else 0,
        "mean_return_pct": _series_mean(returns),
        "median_return_pct": float(returns.median()) if not returns.empty else np.nan,
        "p10_return_pct": _quantile(returns, 0.10),
        "worst_return_pct": float(returns.min()) if not returns.empty else np.nan,
        "win_rate_pct": float((returns > 0).mean() * 100.0) if not returns.empty else np.nan,
        "median_volatility_feature": _median(frame[vol_feature]),
        "median_participation_feature": _median(frame[participation_feature]),
        "median_downside_volatility_60d_pct": _median(frame["downside_volatility_60d_pct"]),
        "median_adv60_mil_jpy": _median(frame["avg_trading_value_60d_mil_jpy"]),
    }


def _corr(left: pd.Series, right: pd.Series) -> float:
    clean = pd.DataFrame(
        {
            "left": pd.to_numeric(left, errors="coerce"),
            "right": pd.to_numeric(right, errors="coerce"),
        }
    ).dropna()
    if len(clean) < 3:
        return float("nan")
    return float(clean["left"].corr(clean["right"]))


def _mean_return(frame: pd.DataFrame) -> float:
    mean = _series_mean(pd.to_numeric(frame["event_return_winsor_pct"], errors="coerce").dropna())
    return float(mean) if mean is not None else float("nan")


def _median(series: pd.Series) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.median()) if not clean.empty else float("nan")


def _quantile(series: pd.Series, q: float) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.quantile(q)) if not clean.empty else float("nan")


def _fmt(value: object, digits: int = 2) -> str:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        return "-"
    return f"{number:.{digits}f}"


def _build_summary_markdown(result: AnnualPrimeValueVolumeVolatilityParticipationResult) -> str:
    lines = [
        "# Annual Prime Value Volume Volatility Participation",
        "",
        "## Setup",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Market scope: `{result.market_scope}`",
        f"- Selection fractions: `{', '.join(str(value) for value in result.selection_fractions)}`",
        f"- Score methods: `{', '.join(result.score_methods)}`",
        f"- Selected event rows: `{result.selected_event_count}`",
        f"- Feature policy: {result.feature_policy}.",
        "",
        "## Volatility Participation Summary",
        "",
    ]
    focus = result.volatility_participation_summary_df[
        result.volatility_participation_summary_df["volatility_feature_key"].astype(str).isin(
            ("volatility_20d_pct", "volatility_60d_pct"),
        )
    ].copy()
    if focus.empty:
        lines.append("- No volatility-participation summary rows were produced.")
    else:
        for row in focus.sort_values(["selection_fraction", "score_method", "volatility_feature_key"]).to_dict(
            orient="records",
        ):
            lines.append(
                "- "
                f"`{_fmt(row['selection_fraction'], 2)}` / `{row['score_method']}` / "
                f"`{row['volatility_feature_key']}`: "
                f"corr vol-volume `{_fmt(row['correlation_volume_ratio_20_60'])}`, "
                f"corr vol-trading value `{_fmt(row['correlation_trading_value_ratio_20_60'])}`, "
                f"high-vol median volume ratio `{_fmt(row['high_vol_median_volume_ratio_20_60'])}`, "
                f"high-vol mean return `{_fmt(row['high_vol_mean_return_pct'])}%`, "
                f"high-vol p10 `{_fmt(row['high_vol_p10_return_pct'])}%`"
            )
    lines.extend(["", "## Participation Splits", ""])
    split = result.participation_split_df[
        result.participation_split_df["variant"].astype(str).isin(
            ("high_vol_low_participation", "high_vol_high_participation"),
        )
    ].copy()
    if split.empty:
        lines.append("- No participation split rows were produced.")
    else:
        for row in split.sort_values(
            ["selection_fraction", "score_method", "volatility_feature_key", "participation_feature_key", "variant"],
        ).head(32).to_dict(orient="records"):
            lines.append(
                "- "
                f"`{_fmt(row['selection_fraction'], 2)}` / `{row['score_method']}` / "
                f"`{row['volatility_feature_key']}` x `{row['participation_feature_key']}` / "
                f"`{row['variant']}`: events `{row['event_count']}`, "
                f"mean `{_fmt(row['mean_return_pct'])}%`, p10 `{_fmt(row['p10_return_pct'])}%`, "
                f"median participation `{_fmt(row['median_participation_feature'])}`"
            )
    return "\n".join(lines)


def write_annual_prime_value_volume_volatility_participation_bundle(
    result: AnnualPrimeValueVolumeVolatilityParticipationResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_PRIME_VALUE_VOLUME_VOLATILITY_PARTICIPATION_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_prime_value_volume_volatility_participation",
        params={
            "input_bundle_path": result.input_bundle_path,
            "selection_fractions": list(result.selection_fractions),
            "market_scope": result.market_scope,
            "liquidity_scenario": result.liquidity_scenario,
            "score_methods": list(result.score_methods),
            "bucket_count": result.bucket_count,
            "participation_bucket_count": result.participation_bucket_count,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_prime_value_volume_volatility_participation_bundle(
    bundle_path: str | Path,
) -> AnnualPrimeValueVolumeVolatilityParticipationResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualPrimeValueVolumeVolatilityParticipationResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_prime_value_volume_volatility_participation_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_PRIME_VALUE_VOLUME_VOLATILITY_PARTICIPATION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_prime_value_volume_volatility_participation_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_PRIME_VALUE_VOLUME_VOLATILITY_PARTICIPATION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


__all__: Sequence[str] = (
    "ANNUAL_PRIME_VALUE_VOLUME_VOLATILITY_PARTICIPATION_EXPERIMENT_ID",
    "AnnualPrimeValueVolumeVolatilityParticipationResult",
    "get_annual_prime_value_volume_volatility_participation_bundle_path_for_run_id",
    "get_annual_prime_value_volume_volatility_participation_latest_bundle_path",
    "load_annual_prime_value_volume_volatility_participation_bundle",
    "run_annual_prime_value_volume_volatility_participation",
    "write_annual_prime_value_volume_volatility_participation_bundle",
)
