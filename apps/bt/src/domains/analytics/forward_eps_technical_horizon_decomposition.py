"""Technical horizon decomposition for the production forward EPS strategy."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

from src.domains.analytics.forward_eps_trade_archetype_decomposition import (
    DEFAULT_HOLDOUT_MONTHS,
    DEFAULT_QUANTILE_BUCKET_COUNT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    DEFAULT_STRATEGY_NAME,
    _assign_quantile_bucket,
    _build_trade_metrics,
    _resolve_previous_index_value,
    run_forward_eps_trade_archetype_decomposition,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.strategy.indicators import compute_risk_adjusted_return, compute_rsi
from src.infrastructure.data_access.loaders import load_stock_data
from src.infrastructure.data_access.mode import data_access_mode_context

FORWARD_EPS_TECHNICAL_HORIZON_DECOMPOSITION_EXPERIMENT_ID = (
    "strategy-audit/forward-eps-technical-horizon-decomposition"
)
DEFAULT_DATASET_NAME = "primeExTopix500"
DEFAULT_SIZE_HAIRCUT = 0.5
DEFAULT_THRESHOLD_QUANTILE = 0.80
DEFAULT_RISK_RATIO_TYPE: Literal["sharpe", "sortino"] = "sharpe"
_HORIZONS: tuple[int, ...] = (10, 20, 60)
_WARMUP_CALENDAR_DAYS = 420


@dataclass(frozen=True)
class _TechnicalFeatureSpec:
    name: str
    family: str
    label: str
    horizon_days: int


@dataclass(frozen=True)
class ForwardEpsTechnicalHorizonDecompositionResult:
    db_path: str
    strategy_name: str
    dataset_name: str
    holdout_months: int
    severe_loss_threshold_pct: float
    quantile_bucket_count: int
    threshold_quantile: float
    size_haircut: float
    risk_ratio_type: str
    analysis_start_date: str
    analysis_end_date: str
    dataset_summary_df: pd.DataFrame
    scenario_summary_df: pd.DataFrame
    market_scope_summary_df: pd.DataFrame
    threshold_summary_df: pd.DataFrame
    horizon_bucket_summary_df: pd.DataFrame
    horizon_contrast_summary_df: pd.DataFrame
    horizon_tail_profile_df: pd.DataFrame
    horizon_candidate_summary_df: pd.DataFrame
    enriched_trade_df: pd.DataFrame


_FEATURE_SPECS: tuple[_TechnicalFeatureSpec, ...] = tuple(
    spec
    for horizon in _HORIZONS
    for spec in (
        _TechnicalFeatureSpec(
            name=f"rsi_{horizon}",
            family="rsi",
            label=f"RSI {horizon}",
            horizon_days=horizon,
        ),
        _TechnicalFeatureSpec(
            name=f"runup_{horizon}d_pct",
            family="runup",
            label=f"Run-up {horizon}d",
            horizon_days=horizon,
        ),
        _TechnicalFeatureSpec(
            name=f"risk_adjusted_return_{horizon}d",
            family="risk_adjusted_return",
            label=f"Risk-adjusted return {horizon}d",
            horizon_days=horizon,
        ),
    )
)

_TRADE_METRIC_COLUMNS: tuple[str, ...] = (
    "trade_count",
    "avg_trade_return_pct",
    "median_trade_return_pct",
    "win_rate_pct",
    "severe_loss_rate_pct",
    "worst_trade_return_pct",
    "p10_trade_return_pct",
)
_GROUP_COLUMNS: tuple[str, ...] = ("window_label", "market_scope")
_THRESHOLD_COLUMNS: tuple[str, ...] = (
    "market_scope",
    "feature_family",
    "feature_name",
    "feature_label",
    "horizon_days",
    "threshold_quantile",
    "threshold_value",
    "calibration_trade_count",
)
_BUCKET_COLUMNS: tuple[str, ...] = (
    *_GROUP_COLUMNS,
    "feature_family",
    "feature_name",
    "feature_label",
    "horizon_days",
    "bucket_rank",
    "bucket_count",
    "bucket_label",
    "coverage_pct",
    "feature_min",
    "feature_median",
    "feature_max",
    *_TRADE_METRIC_COLUMNS,
)
_CONTRAST_COLUMNS: tuple[str, ...] = (
    *_GROUP_COLUMNS,
    "feature_family",
    "feature_name",
    "feature_label",
    "horizon_days",
    "low_bucket_label",
    "high_bucket_label",
    "low_trade_count",
    "high_trade_count",
    "low_avg_trade_return_pct",
    "high_avg_trade_return_pct",
    "delta_high_minus_low_avg_trade_return_pct",
    "low_severe_loss_rate_pct",
    "high_severe_loss_rate_pct",
    "delta_high_minus_low_severe_loss_rate_pct",
)
_TAIL_COLUMNS: tuple[str, ...] = (
    *_GROUP_COLUMNS,
    "tail_cohort",
    "right_tail_threshold_pct",
    *_TRADE_METRIC_COLUMNS,
    *(f"median_{spec.name}" for spec in _FEATURE_SPECS),
)
_CANDIDATE_COLUMNS: tuple[str, ...] = (
    *_GROUP_COLUMNS,
    "candidate_name",
    "action_type",
    "candidate_description",
    "calibration_market_scope",
    "threshold_quantile",
    "selected_trade_count",
    "selected_coverage_pct",
    "selected_avg_trade_return_pct",
    "selected_median_trade_return_pct",
    "selected_severe_loss_rate_pct",
    "selected_worst_trade_return_pct",
    "selected_p10_trade_return_pct",
    "selected_right_tail_count",
    "kept_trade_count",
    "kept_avg_trade_return_pct",
    "kept_median_trade_return_pct",
    "kept_severe_loss_rate_pct",
    "kept_worst_trade_return_pct",
    "kept_p10_trade_return_pct",
    "kept_right_tail_count",
    "right_tail_retention_pct",
    "haircut_size_multiplier",
    "haircut_avg_trade_return_pct",
    "haircut_median_trade_return_pct",
    "haircut_severe_loss_rate_pct",
    "haircut_worst_trade_return_pct",
)


def run_forward_eps_technical_horizon_decomposition(
    *,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
    dataset_name: str = DEFAULT_DATASET_NAME,
    holdout_months: int = DEFAULT_HOLDOUT_MONTHS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    quantile_bucket_count: int = DEFAULT_QUANTILE_BUCKET_COUNT,
    threshold_quantile: float = DEFAULT_THRESHOLD_QUANTILE,
    size_haircut: float = DEFAULT_SIZE_HAIRCUT,
    risk_ratio_type: Literal["sharpe", "sortino"] = DEFAULT_RISK_RATIO_TYPE,
) -> ForwardEpsTechnicalHorizonDecompositionResult:
    if holdout_months <= 0:
        raise ValueError("holdout_months must be greater than 0")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if quantile_bucket_count < 2:
        raise ValueError("quantile_bucket_count must be at least 2")
    if not 0.0 < threshold_quantile < 1.0:
        raise ValueError("threshold_quantile must satisfy 0.0 < q < 1.0")
    if not 0.0 <= size_haircut <= 1.0:
        raise ValueError("size_haircut must satisfy 0.0 <= size_haircut <= 1.0")

    base_result = run_forward_eps_trade_archetype_decomposition(
        strategy_name=strategy_name,
        dataset_name=dataset_name,
        holdout_months=holdout_months,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        quantile_bucket_count=quantile_bucket_count,
    )
    frame = _prepare_horizon_frame(
        base_result.enriched_trade_df,
        dataset_name=dataset_name,
        risk_ratio_type=risk_ratio_type,
    )
    threshold_summary_df = _build_threshold_summary_df(
        frame=frame,
        threshold_quantile=threshold_quantile,
    )
    horizon_bucket_summary_df = _build_horizon_bucket_summary_df(
        frame=frame,
        quantile_bucket_count=quantile_bucket_count,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    horizon_contrast_summary_df = _build_horizon_contrast_summary_df(
        horizon_bucket_summary_df
    )
    horizon_tail_profile_df = _build_horizon_tail_profile_df(
        frame=frame,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    horizon_candidate_summary_df = _build_horizon_candidate_summary_df(
        frame=frame,
        threshold_summary_df=threshold_summary_df,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        threshold_quantile=threshold_quantile,
        size_haircut=size_haircut,
    )

    return ForwardEpsTechnicalHorizonDecompositionResult(
        db_path="multi://forward-eps-technical-horizon-decomposition",
        strategy_name=base_result.strategy_name,
        dataset_name=base_result.dataset_name,
        holdout_months=base_result.holdout_months,
        severe_loss_threshold_pct=base_result.severe_loss_threshold_pct,
        quantile_bucket_count=base_result.quantile_bucket_count,
        threshold_quantile=threshold_quantile,
        size_haircut=size_haircut,
        risk_ratio_type=risk_ratio_type,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        dataset_summary_df=base_result.dataset_summary_df,
        scenario_summary_df=base_result.scenario_summary_df,
        market_scope_summary_df=base_result.market_scope_summary_df,
        threshold_summary_df=threshold_summary_df,
        horizon_bucket_summary_df=horizon_bucket_summary_df,
        horizon_contrast_summary_df=horizon_contrast_summary_df,
        horizon_tail_profile_df=horizon_tail_profile_df,
        horizon_candidate_summary_df=horizon_candidate_summary_df,
        enriched_trade_df=frame,
    )


def write_forward_eps_technical_horizon_decomposition_bundle(
    result: ForwardEpsTechnicalHorizonDecompositionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=FORWARD_EPS_TECHNICAL_HORIZON_DECOMPOSITION_EXPERIMENT_ID,
        module=__name__,
        function="run_forward_eps_technical_horizon_decomposition",
        params={
            "strategy_name": result.strategy_name,
            "dataset_name": result.dataset_name,
            "holdout_months": result.holdout_months,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "quantile_bucket_count": result.quantile_bucket_count,
            "threshold_quantile": result.threshold_quantile,
            "size_haircut": result.size_haircut,
            "risk_ratio_type": result.risk_ratio_type,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "db_path": result.db_path,
            "strategy_name": result.strategy_name,
            "dataset_name": result.dataset_name,
            "holdout_months": result.holdout_months,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "quantile_bucket_count": result.quantile_bucket_count,
            "threshold_quantile": result.threshold_quantile,
            "size_haircut": result.size_haircut,
            "risk_ratio_type": result.risk_ratio_type,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
        },
        result_tables={
            "dataset_summary_df": result.dataset_summary_df,
            "scenario_summary_df": result.scenario_summary_df,
            "market_scope_summary_df": result.market_scope_summary_df,
            "threshold_summary_df": result.threshold_summary_df,
            "horizon_bucket_summary_df": result.horizon_bucket_summary_df,
            "horizon_contrast_summary_df": result.horizon_contrast_summary_df,
            "horizon_tail_profile_df": result.horizon_tail_profile_df,
            "horizon_candidate_summary_df": result.horizon_candidate_summary_df,
            "enriched_trade_df": result.enriched_trade_df,
        },
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_forward_eps_technical_horizon_decomposition_bundle(
    bundle_path: str | Path,
) -> ForwardEpsTechnicalHorizonDecompositionResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return ForwardEpsTechnicalHorizonDecompositionResult(
        db_path=str(metadata["db_path"]),
        strategy_name=str(metadata["strategy_name"]),
        dataset_name=str(metadata["dataset_name"]),
        holdout_months=int(metadata["holdout_months"]),
        severe_loss_threshold_pct=float(metadata["severe_loss_threshold_pct"]),
        quantile_bucket_count=int(metadata["quantile_bucket_count"]),
        threshold_quantile=float(metadata["threshold_quantile"]),
        size_haircut=float(metadata["size_haircut"]),
        risk_ratio_type=str(metadata["risk_ratio_type"]),
        analysis_start_date=str(metadata["analysis_start_date"]),
        analysis_end_date=str(metadata["analysis_end_date"]),
        dataset_summary_df=tables["dataset_summary_df"],
        scenario_summary_df=tables["scenario_summary_df"],
        market_scope_summary_df=tables["market_scope_summary_df"],
        threshold_summary_df=tables["threshold_summary_df"],
        horizon_bucket_summary_df=tables["horizon_bucket_summary_df"],
        horizon_contrast_summary_df=tables["horizon_contrast_summary_df"],
        horizon_tail_profile_df=tables["horizon_tail_profile_df"],
        horizon_candidate_summary_df=tables["horizon_candidate_summary_df"],
        enriched_trade_df=tables["enriched_trade_df"],
    )


def get_forward_eps_technical_horizon_decomposition_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FORWARD_EPS_TECHNICAL_HORIZON_DECOMPOSITION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_forward_eps_technical_horizon_decomposition_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FORWARD_EPS_TECHNICAL_HORIZON_DECOMPOSITION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _prepare_horizon_frame(
    enriched_trade_df: pd.DataFrame,
    *,
    dataset_name: str,
    risk_ratio_type: Literal["sharpe", "sortino"],
) -> pd.DataFrame:
    frame = enriched_trade_df.copy()
    if frame.empty:
        return _ensure_feature_columns(frame)
    if "market_scope" not in frame.columns:
        frame["market_scope"] = "unknown"
    if "window_label" not in frame.columns:
        frame["window_label"] = "full"
    frame["entry_date"] = pd.to_datetime(frame["entry_date"], errors="coerce").dt.normalize()
    frame = _add_price_horizon_features(
        frame,
        dataset_name=dataset_name,
        risk_ratio_type=risk_ratio_type,
    )
    return _ensure_feature_columns(frame)


def _add_price_horizon_features(
    frame: pd.DataFrame,
    *,
    dataset_name: str,
    risk_ratio_type: Literal["sharpe", "sortino"],
) -> pd.DataFrame:
    entry_dates = pd.to_datetime(frame["entry_date"], errors="coerce").dropna()
    if entry_dates.empty:
        return frame
    start_date = (entry_dates.min() - pd.Timedelta(days=_WARMUP_CALENDAR_DAYS)).strftime(
        "%Y-%m-%d"
    )
    end_date = entry_dates.max().strftime("%Y-%m-%d")
    feature_cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    with data_access_mode_context("direct"):
        for trade in frame.itertuples(index=False):
            payload = trade._asdict()
            symbol = str(payload.get("symbol", ""))
            entry_date = pd.to_datetime(payload.get("entry_date"), errors="coerce")
            feature_df = feature_cache.get(symbol)
            if feature_df is None:
                feature_df = _build_symbol_horizon_feature_df(
                    dataset_name=dataset_name,
                    stock_code=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    risk_ratio_type=risk_ratio_type,
                )
                feature_cache[symbol] = feature_df
            feature_date = (
                _resolve_previous_index_value(
                    cast(pd.DatetimeIndex, feature_df.index),
                    cast(pd.Timestamp, entry_date),
                )
                if pd.notna(entry_date)
                else None
            )
            feature_row = _extract_feature_row(feature_df, feature_date)
            payload.update(feature_row)
            payload["technical_feature_date"] = _format_timestamp(feature_date)
            payload["technical_feature_lag_days"] = (
                float((entry_date - feature_date).days)
                if pd.notna(entry_date) and feature_date is not None
                else np.nan
            )
            rows.append(payload)
    return pd.DataFrame(rows)


def _build_symbol_horizon_feature_df(
    *,
    dataset_name: str,
    stock_code: str,
    start_date: str,
    end_date: str,
    risk_ratio_type: Literal["sharpe", "sortino"],
) -> pd.DataFrame:
    stock_df = load_stock_data(
        dataset_name,
        stock_code,
        start_date=start_date,
        end_date=end_date,
    )
    close = pd.to_numeric(stock_df["Close"], errors="coerce").astype(float)
    feature_df = pd.DataFrame(index=cast(pd.DatetimeIndex, stock_df.index))
    for horizon in _HORIZONS:
        feature_df[f"rsi_{horizon}"] = compute_rsi(close, horizon)
        feature_df[f"runup_{horizon}d_pct"] = (close / close.shift(horizon) - 1.0) * 100.0
        feature_df[f"risk_adjusted_return_{horizon}d"] = compute_risk_adjusted_return(
            close=close,
            lookback_period=horizon,
            ratio_type=risk_ratio_type,
        )
    return feature_df


def _extract_feature_row(frame: pd.DataFrame, ts: pd.Timestamp | None) -> dict[str, Any]:
    if ts is None or ts not in frame.index:
        return {spec.name: np.nan for spec in _FEATURE_SPECS}
    row = frame.loc[ts]
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    return {spec.name: row.get(spec.name, np.nan) for spec in _FEATURE_SPECS}


def _ensure_feature_columns(frame: pd.DataFrame) -> pd.DataFrame:
    for spec in _FEATURE_SPECS:
        if spec.name not in frame.columns:
            frame[spec.name] = np.nan
    return frame


def _build_threshold_summary_df(
    *,
    frame: pd.DataFrame,
    threshold_quantile: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for market_scope, calibration_frame in _iter_calibration_groups(frame):
        for spec in _FEATURE_SPECS:
            values = pd.to_numeric(calibration_frame[spec.name], errors="coerce").dropna()
            rows.append(
                {
                    "market_scope": market_scope,
                    "feature_family": spec.family,
                    "feature_name": spec.name,
                    "feature_label": spec.label,
                    "horizon_days": spec.horizon_days,
                    "threshold_quantile": threshold_quantile,
                    "threshold_value": _finite_or_nan(values.quantile(threshold_quantile))
                    if not values.empty
                    else np.nan,
                    "calibration_trade_count": int(values.shape[0]),
                }
            )
    return _sort_table(_table_with_columns(rows, _THRESHOLD_COLUMNS))


def _build_horizon_bucket_summary_df(
    *,
    frame: pd.DataFrame,
    quantile_bucket_count: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload, group_frame in _iter_window_market_groups(frame):
        total_trade_count = len(group_frame)
        for spec in _FEATURE_SPECS:
            valid = group_frame.dropna(subset=[spec.name]).copy()
            if valid.empty:
                continue
            bucket_count = min(quantile_bucket_count, len(valid))
            valid["bucket_rank"] = _assign_quantile_bucket(
                pd.to_numeric(valid[spec.name], errors="coerce"),
                bucket_count=bucket_count,
            )
            for bucket_rank, bucket_frame in valid.groupby("bucket_rank", dropna=False):
                bucket_rank_int = _coerce_int(bucket_rank)
                metrics = _build_trade_metrics(
                    bucket_frame,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
                values = pd.to_numeric(bucket_frame[spec.name], errors="coerce")
                rows.append(
                    {
                        **payload,
                        "feature_family": spec.family,
                        "feature_name": spec.name,
                        "feature_label": spec.label,
                        "horizon_days": spec.horizon_days,
                        "bucket_rank": bucket_rank_int,
                        "bucket_count": bucket_count,
                        "bucket_label": f"Q{bucket_rank_int}/{bucket_count}",
                        "coverage_pct": _coverage(metrics["trade_count"], total_trade_count),
                        "feature_min": _finite_or_nan(values.min()),
                        "feature_median": _finite_or_nan(values.median()),
                        "feature_max": _finite_or_nan(values.max()),
                        **metrics,
                    }
                )
    return _sort_table(_table_with_columns(rows, _BUCKET_COLUMNS))


def _build_horizon_contrast_summary_df(
    horizon_bucket_summary_df: pd.DataFrame,
) -> pd.DataFrame:
    if horizon_bucket_summary_df.empty:
        return _table_with_columns([], _CONTRAST_COLUMNS)
    rows: list[dict[str, Any]] = []
    group_columns = [
        "window_label",
        "market_scope",
        "feature_family",
        "feature_name",
        "feature_label",
        "horizon_days",
    ]
    for keys, group in horizon_bucket_summary_df.groupby(
        group_columns,
        dropna=False,
        sort=False,
    ):
        q1 = group[group["bucket_rank"] == group["bucket_rank"].min()]
        qn = group[group["bucket_rank"] == group["bucket_rank"].max()]
        if q1.empty or qn.empty:
            continue
        q1_row = q1.iloc[0]
        qn_row = qn.iloc[0]
        rows.append(
            {
                **dict(zip(group_columns, keys, strict=True)),
                "low_bucket_label": q1_row["bucket_label"],
                "high_bucket_label": qn_row["bucket_label"],
                "low_trade_count": int(q1_row["trade_count"]),
                "high_trade_count": int(qn_row["trade_count"]),
                "low_avg_trade_return_pct": float(q1_row["avg_trade_return_pct"]),
                "high_avg_trade_return_pct": float(qn_row["avg_trade_return_pct"]),
                "delta_high_minus_low_avg_trade_return_pct": float(
                    qn_row["avg_trade_return_pct"] - q1_row["avg_trade_return_pct"]
                ),
                "low_severe_loss_rate_pct": float(q1_row["severe_loss_rate_pct"]),
                "high_severe_loss_rate_pct": float(qn_row["severe_loss_rate_pct"]),
                "delta_high_minus_low_severe_loss_rate_pct": float(
                    qn_row["severe_loss_rate_pct"] - q1_row["severe_loss_rate_pct"]
                ),
            }
        )
    return _sort_table(_table_with_columns(rows, _CONTRAST_COLUMNS))


def _build_horizon_tail_profile_df(
    *,
    frame: pd.DataFrame,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload, group_frame in _iter_window_market_groups(frame):
        returns = pd.to_numeric(group_frame["trade_return_pct"], errors="coerce")
        right_tail_threshold = returns.quantile(0.90) if returns.notna().any() else np.nan
        cohorts: tuple[tuple[str, pd.Series], ...] = (
            ("all", pd.Series(True, index=group_frame.index, dtype=bool)),
            ("severe_loss", returns <= severe_loss_threshold_pct),
            ("right_tail_p90", returns >= right_tail_threshold),
            ("non_severe", returns > severe_loss_threshold_pct),
        )
        for cohort_name, mask in cohorts:
            cohort = group_frame[mask.fillna(False)].copy()
            metrics = _build_trade_metrics(
                cohort,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            rows.append(
                {
                    **payload,
                    "tail_cohort": cohort_name,
                    "right_tail_threshold_pct": _finite_or_nan(right_tail_threshold),
                    **metrics,
                    **_feature_medians(cohort),
                }
            )
    return _sort_table(_table_with_columns(rows, _TAIL_COLUMNS))


def _build_horizon_candidate_summary_df(
    *,
    frame: pd.DataFrame,
    threshold_summary_df: pd.DataFrame,
    severe_loss_threshold_pct: float,
    threshold_quantile: float,
    size_haircut: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload, group_frame in _iter_window_market_groups(frame):
        calibration_scope = (
            str(payload["market_scope"])
            if str(payload["market_scope"]) in _available_threshold_scopes(threshold_summary_df)
            else "all"
        )
        thresholds = _thresholds_for_scope(threshold_summary_df, calibration_scope)
        returns = pd.to_numeric(group_frame["trade_return_pct"], errors="coerce")
        right_tail_threshold = returns.quantile(0.90) if returns.notna().any() else np.nan
        right_tail_mask = returns >= right_tail_threshold
        for candidate_name, action_type, description, mask in _candidate_masks(
            group_frame,
            thresholds=thresholds,
        ):
            mask = mask.fillna(False).astype(bool)
            selected = group_frame[mask].copy()
            kept = group_frame[~mask].copy() if action_type in {"exclude", "haircut"} else selected
            selected_metrics = _build_trade_metrics(
                selected,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            kept_metrics = _build_trade_metrics(
                kept,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            selected_right_tail_count = int(right_tail_mask[mask].sum())
            kept_right_tail_count = int(right_tail_mask[~mask].sum())
            total_right_tail_count = int(right_tail_mask.sum())
            haircut_payload = (
                _haircut_metrics(
                    group_frame,
                    mask=mask,
                    size_haircut=size_haircut,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
                if action_type in {"exclude", "haircut"}
                else _empty_haircut_metrics()
            )
            rows.append(
                {
                    **payload,
                    "candidate_name": candidate_name,
                    "action_type": action_type,
                    "candidate_description": description,
                    "calibration_market_scope": calibration_scope,
                    "threshold_quantile": threshold_quantile,
                    "selected_trade_count": selected_metrics["trade_count"],
                    "selected_coverage_pct": _coverage(
                        selected_metrics["trade_count"],
                        len(group_frame),
                    ),
                    "selected_avg_trade_return_pct": selected_metrics["avg_trade_return_pct"],
                    "selected_median_trade_return_pct": selected_metrics[
                        "median_trade_return_pct"
                    ],
                    "selected_severe_loss_rate_pct": selected_metrics["severe_loss_rate_pct"],
                    "selected_worst_trade_return_pct": selected_metrics[
                        "worst_trade_return_pct"
                    ],
                    "selected_p10_trade_return_pct": selected_metrics["p10_trade_return_pct"],
                    "selected_right_tail_count": selected_right_tail_count,
                    "kept_trade_count": kept_metrics["trade_count"],
                    "kept_avg_trade_return_pct": kept_metrics["avg_trade_return_pct"],
                    "kept_median_trade_return_pct": kept_metrics["median_trade_return_pct"],
                    "kept_severe_loss_rate_pct": kept_metrics["severe_loss_rate_pct"],
                    "kept_worst_trade_return_pct": kept_metrics["worst_trade_return_pct"],
                    "kept_p10_trade_return_pct": kept_metrics["p10_trade_return_pct"],
                    "kept_right_tail_count": kept_right_tail_count,
                    "right_tail_retention_pct": _coverage(
                        kept_right_tail_count,
                        total_right_tail_count,
                    ),
                    "haircut_size_multiplier": size_haircut,
                    **haircut_payload,
                }
            )
    return _sort_table(_table_with_columns(rows, _CANDIDATE_COLUMNS))


def _candidate_masks(
    frame: pd.DataFrame,
    *,
    thresholds: dict[str, float],
) -> tuple[tuple[str, Literal["baseline", "exclude", "haircut"], str, pd.Series], ...]:
    baseline = pd.Series(True, index=frame.index, dtype=bool)
    same_horizon_candidates: list[
        tuple[str, Literal["baseline", "exclude", "haircut"], str, pd.Series]
    ] = []
    for horizon in _HORIZONS:
        same_horizon_candidates.append(
            (
                f"overheat_same_horizon_{horizon}d_q80_overlap_ge2",
                "haircut",
                f"At least two of RSI, run-up, and RAR are above train Q80 on {horizon}d.",
                _overlap_mask(
                    frame,
                    thresholds,
                    (
                        f"rsi_{horizon}",
                        f"runup_{horizon}d_pct",
                        f"risk_adjusted_return_{horizon}d",
                    ),
                    min_count=2,
                ),
            )
        )
    cross_horizon_runup_rar = _overlap_mask(
        frame,
        thresholds,
        tuple(
            feature
            for horizon in _HORIZONS
            for feature in (f"runup_{horizon}d_pct", f"risk_adjusted_return_{horizon}d")
        ),
        min_count=3,
    )
    all_technical = _overlap_mask(
        frame,
        thresholds,
        tuple(spec.name for spec in _FEATURE_SPECS),
        min_count=4,
    )
    short_climax = _overlap_mask(
        frame,
        thresholds,
        ("rsi_10", "runup_10d_pct", "risk_adjusted_return_10d"),
        min_count=2,
    )
    trend_maturity = _overlap_mask(
        frame,
        thresholds,
        ("rsi_60", "runup_60d_pct", "risk_adjusted_return_60d"),
        min_count=2,
    )
    legacy = _overlap_mask(
        frame,
        thresholds,
        ("runup_20d_pct", "runup_60d_pct", "risk_adjusted_return_60d"),
        min_count=2,
    )
    return (
        ("baseline_all", "baseline", "No action; keep all trades.", baseline),
        *same_horizon_candidates,
        (
            "overheat_runup_rar_cross_horizon_q80_overlap_ge3",
            "haircut",
            "At least three run-up/RAR horizons are above train Q80.",
            cross_horizon_runup_rar,
        ),
        (
            "overheat_all_technical_q80_overlap_ge4",
            "haircut",
            "At least four of the nine technical horizon features are above train Q80.",
            all_technical,
        ),
        (
            "short_climax_10d_q80_overlap_ge2",
            "haircut",
            "RSI10/run-up10/RAR10 short climax overlap.",
            short_climax,
        ),
        (
            "trend_maturity_60d_q80_overlap_ge2",
            "haircut",
            "RSI60/run-up60/RAR60 trend maturity overlap.",
            trend_maturity,
        ),
        (
            "legacy_20_60_runup_rar60_q80_overlap_ge2",
            "haircut",
            "Closest v3 equivalent of the prior 20d/60d run-up plus RAR overlap.",
            legacy,
        ),
    )


def _overlap_mask(
    frame: pd.DataFrame,
    thresholds: dict[str, float],
    feature_names: tuple[str, ...],
    *,
    min_count: int,
) -> pd.Series:
    count = pd.Series(0, index=frame.index, dtype=int)
    for feature_name in feature_names:
        threshold = thresholds.get(feature_name, np.nan)
        if pd.isna(threshold):
            continue
        count += (pd.to_numeric(frame[feature_name], errors="coerce") >= threshold).astype(int)
    return count >= min_count


def _haircut_metrics(
    frame: pd.DataFrame,
    *,
    mask: pd.Series,
    size_haircut: float,
    severe_loss_threshold_pct: float,
) -> dict[str, Any]:
    adjusted = frame.copy()
    returns = pd.to_numeric(adjusted["trade_return_pct"], errors="coerce")
    adjusted["trade_return_pct"] = returns.where(~mask, returns * size_haircut)
    metrics = _build_trade_metrics(
        adjusted,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    return {
        "haircut_avg_trade_return_pct": metrics["avg_trade_return_pct"],
        "haircut_median_trade_return_pct": metrics["median_trade_return_pct"],
        "haircut_severe_loss_rate_pct": metrics["severe_loss_rate_pct"],
        "haircut_worst_trade_return_pct": metrics["worst_trade_return_pct"],
    }


def _empty_haircut_metrics() -> dict[str, float]:
    return {
        "haircut_avg_trade_return_pct": np.nan,
        "haircut_median_trade_return_pct": np.nan,
        "haircut_severe_loss_rate_pct": np.nan,
        "haircut_worst_trade_return_pct": np.nan,
    }


def _iter_calibration_groups(
    frame: pd.DataFrame,
) -> list[tuple[str, pd.DataFrame]]:
    train_frame = frame[frame["window_label"] == "train_pre_holdout"].copy()
    if train_frame.empty:
        train_frame = frame.copy()
    groups: list[tuple[str, pd.DataFrame]] = [("all", train_frame)]
    for market_scope, market_frame in train_frame.groupby(
        "market_scope",
        dropna=False,
        sort=False,
    ):
        groups.append((str(market_scope), market_frame.copy()))
    return groups


def _iter_window_market_groups(
    frame: pd.DataFrame,
) -> list[tuple[dict[str, Any], pd.DataFrame]]:
    rows: list[tuple[dict[str, Any], pd.DataFrame]] = []
    if frame.empty:
        return rows
    for window_label, window_frame in frame.groupby("window_label", dropna=False, sort=False):
        rows.append(({"window_label": str(window_label), "market_scope": "all"}, window_frame.copy()))
        for market_scope, market_frame in window_frame.groupby(
            "market_scope",
            dropna=False,
            sort=False,
        ):
            rows.append(
                (
                    {"window_label": str(window_label), "market_scope": str(market_scope)},
                    market_frame.copy(),
                )
            )
    return rows


def _thresholds_for_scope(
    threshold_summary_df: pd.DataFrame,
    market_scope: str,
) -> dict[str, float]:
    rows = threshold_summary_df[threshold_summary_df["market_scope"] == market_scope]
    return {
        str(row["feature_name"]): float(row["threshold_value"])
        for row in rows.to_dict("records")
        if pd.notna(row.get("threshold_value"))
    }


def _available_threshold_scopes(threshold_summary_df: pd.DataFrame) -> set[str]:
    if threshold_summary_df.empty:
        return set()
    return set(threshold_summary_df["market_scope"].astype(str).unique())


def _feature_medians(frame: pd.DataFrame) -> dict[str, float]:
    return {
        f"median_{spec.name}": _finite_or_nan(
            pd.to_numeric(frame[spec.name], errors="coerce").median()
        )
        for spec in _FEATURE_SPECS
        if spec.name in frame.columns
    }


def _coverage(count: int, total: int) -> float:
    return float(count / total * 100.0) if total > 0 else np.nan


def _coerce_int(value: Any) -> int:
    if pd.isna(value):
        return 0
    return int(value)


def _finite_or_nan(value: Any) -> float:
    if pd.isna(value):
        return np.nan
    return float(value)


def _format_timestamp(value: pd.Timestamp | None) -> str | None:
    if value is None or pd.isna(value):
        return None
    return value.strftime("%Y-%m-%d")


def _sort_table(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    sort_columns = [
        column
        for column in (
            "window_label",
            "market_scope",
            "feature_family",
            "horizon_days",
            "feature_name",
            "bucket_rank",
            "tail_cohort",
            "candidate_name",
        )
        if column in frame.columns
    ]
    if not sort_columns:
        return frame.reset_index(drop=True)
    return frame.sort_values(sort_columns, kind="stable").reset_index(drop=True)


def _table_with_columns(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=list(columns))


def _build_summary_markdown(result: ForwardEpsTechnicalHorizonDecompositionResult) -> str:
    scenario = result.scenario_summary_df
    full = _first_row(scenario[scenario["window_label"] == "full"])
    holdout = _first_row(scenario[scenario["window_label"] == f"holdout_{result.holdout_months}m"])
    candidate = result.horizon_candidate_summary_df
    full_prime_legacy = _first_row(
        candidate[
            (candidate["window_label"] == "full")
            & (candidate["market_scope"] == "prime")
            & (candidate["candidate_name"] == "legacy_20_60_runup_rar60_q80_overlap_ge2")
        ]
    )
    holdout_prime_legacy = _first_row(
        candidate[
            (candidate["window_label"] == f"holdout_{result.holdout_months}m")
            & (candidate["market_scope"] == "prime")
            & (candidate["candidate_name"] == "legacy_20_60_runup_rar60_q80_overlap_ge2")
        ]
    )
    lines = [
        "# Forward EPS Technical Horizon Decomposition",
        "",
        "## Scope",
        "",
        f"- Strategy: `{result.strategy_name}`",
        f"- Dataset / universe preset: `{result.dataset_name}`",
        f"- Analysis period: `{result.analysis_start_date}` -> `{result.analysis_end_date}`",
        f"- Holdout: `{result.holdout_months}` months",
        f"- Technical horizons: `{', '.join(str(h) for h in _HORIZONS)}` trading days",
        f"- Threshold calibration: train-window Q{int(result.threshold_quantile * 100)}`",
        "",
        "## Key Reads",
        "",
        f"- Full-history trades: `{_fmt_int(full.get('trade_count'))}`, avg `{_fmt_pct(full.get('avg_trade_return_pct'))}`.",
        f"- Holdout trades: `{_fmt_int(holdout.get('trade_count'))}`, avg `{_fmt_pct(holdout.get('avg_trade_return_pct'))}`.",
        f"- Prime legacy 20/60/RAR overlap selected full-history severe loss `{_fmt_pct(full_prime_legacy.get('selected_severe_loss_rate_pct'))}`, kept severe loss `{_fmt_pct(full_prime_legacy.get('kept_severe_loss_rate_pct'))}`.",
        f"- Prime legacy 20/60/RAR overlap holdout kept severe loss `{_fmt_pct(holdout_prime_legacy.get('kept_severe_loss_rate_pct'))}`.",
        "",
        "## Artifact Tables",
        "",
        "- `threshold_summary_df`",
        "- `horizon_bucket_summary_df`",
        "- `horizon_contrast_summary_df`",
        "- `horizon_tail_profile_df`",
        "- `horizon_candidate_summary_df`",
        "- `enriched_trade_df`",
    ]
    return "\n".join(lines)


def _build_published_summary(
    result: ForwardEpsTechnicalHorizonDecompositionResult,
) -> dict[str, Any]:
    candidate = result.horizon_candidate_summary_df
    full_prime_legacy = _first_row(
        candidate[
            (candidate["window_label"] == "full")
            & (candidate["market_scope"] == "prime")
            & (candidate["candidate_name"] == "legacy_20_60_runup_rar60_q80_overlap_ge2")
        ]
    )
    holdout_prime_short = _first_row(
        candidate[
            (candidate["window_label"] == f"holdout_{result.holdout_months}m")
            & (candidate["market_scope"] == "prime")
            & (candidate["candidate_name"] == "short_climax_10d_q80_overlap_ge2")
        ]
    )
    standard_legacy = _first_row(
        candidate[
            (candidate["window_label"] == "full")
            & (candidate["market_scope"] == "standard")
            & (candidate["candidate_name"] == "legacy_20_60_runup_rar60_q80_overlap_ge2")
        ]
    )
    return {
        "strategyName": result.strategy_name,
        "datasetName": result.dataset_name,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "holdoutMonths": result.holdout_months,
        "tradeCount": int(result.enriched_trade_df.shape[0]),
        "readoutSections": [
            {
                "title": "Decision",
                "body": (
                    "forward_eps_driven の technical overheat は Prime 限定の size "
                    "haircut / risk cap 候補として扱う。hard exclude はまだ採用しない。"
                ),
            },
            {
                "title": "Main Findings",
                "body": (
                    "Prime full の 20/60/RAR overlap は selected severe "
                    f"{_fmt_pct(full_prime_legacy.get('selected_severe_loss_rate_pct'))}、"
                    f"kept severe {_fmt_pct(full_prime_legacy.get('kept_severe_loss_rate_pct'))}。"
                    "Holdout の 10d short-climax は selected severe "
                    f"{_fmt_pct(holdout_prime_short.get('selected_severe_loss_rate_pct'))}、"
                    f"kept severe {_fmt_pct(holdout_prime_short.get('kept_severe_loss_rate_pct'))}。"
                ),
            },
            {
                "title": "Production Implication",
                "body": (
                    "Standard full の 20/60/RAR overlap selected avg は "
                    f"{_fmt_pct(standard_legacy.get('selected_avg_trade_return_pct'))} "
                    "で右尾も含むため、Prime の pruning rule を Standard へ流用しない。"
                ),
            },
        ],
        "scenarioSummary": result.scenario_summary_df.to_dict("records"),
        "topCandidates": result.horizon_candidate_summary_df.head(40).to_dict("records"),
    }


def _first_row(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {}
    return dict(frame.iloc[0].to_dict())


def _fmt_int(value: Any) -> str:
    if pd.isna(value):
        return "n/a"
    return str(int(value))


def _fmt_pct(value: Any) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{float(value):+.2f}%"


__all__ = [
    "DEFAULT_DATASET_NAME",
    "DEFAULT_HOLDOUT_MONTHS",
    "DEFAULT_QUANTILE_BUCKET_COUNT",
    "DEFAULT_RISK_RATIO_TYPE",
    "DEFAULT_SEVERE_LOSS_THRESHOLD_PCT",
    "DEFAULT_SIZE_HAIRCUT",
    "DEFAULT_STRATEGY_NAME",
    "DEFAULT_THRESHOLD_QUANTILE",
    "FORWARD_EPS_TECHNICAL_HORIZON_DECOMPOSITION_EXPERIMENT_ID",
    "ForwardEpsTechnicalHorizonDecompositionResult",
    "get_forward_eps_technical_horizon_decomposition_bundle_path_for_run_id",
    "get_forward_eps_technical_horizon_decomposition_latest_bundle_path",
    "load_forward_eps_technical_horizon_decomposition_bundle",
    "run_forward_eps_technical_horizon_decomposition",
    "write_forward_eps_technical_horizon_decomposition_bundle",
]
