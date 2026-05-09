"""Component decomposition for forward_eps_driven realized trades."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence, cast

import numpy as np
import pandas as pd

from src.domains.analytics.forward_eps_trade_archetype_decomposition import (
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    _build_trade_metrics,
    get_forward_eps_trade_archetype_decomposition_latest_bundle_path,
    load_forward_eps_trade_archetype_decomposition_bundle,
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

FORWARD_EPS_COMPONENT_DECOMPOSITION_EXPERIMENT_ID = (
    "strategy-audit/forward-eps-component-decomposition"
)
DEFAULT_QUANTILE_BUCKET_COUNT = 5
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "component_trade_df",
    "component_bucket_summary_df",
    "component_overlap_summary_df",
    "component_regression_summary_df",
)
_COMPONENT_NAMES: tuple[str, ...] = (
    "value",
    "expectation",
    "attention",
    "price_momentum",
)
_GROUP_COLUMNS: tuple[str, ...] = ("window_label", "market_scope")
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth", "unknown")


@dataclass(frozen=True)
class _ComponentSpec:
    name: str
    label: str
    description: str
    feature_names: tuple[str, ...]
    favorable_low_features: tuple[str, ...] = ()


@dataclass(frozen=True)
class ForwardEpsComponentDecompositionResult:
    db_path: str
    input_bundle_path: str
    input_run_id: str | None
    input_git_commit: str | None
    strategy_name: str
    dataset_name: str
    analysis_start_date: str
    analysis_end_date: str
    quantile_bucket_count: int
    severe_loss_threshold_pct: float
    trade_count: int
    component_policy: str
    component_trade_df: pd.DataFrame
    component_bucket_summary_df: pd.DataFrame
    component_overlap_summary_df: pd.DataFrame
    component_regression_summary_df: pd.DataFrame


COMPONENT_SPECS: tuple[_ComponentSpec, ...] = (
    _ComponentSpec(
        name="value",
        label="Value",
        description="Low PBR, low forward PER, and small market cap exposure.",
        feature_names=("pbr", "forward_per", "market_cap_bil_jpy"),
        favorable_low_features=("pbr", "forward_per", "market_cap_bil_jpy"),
    ),
    _ComponentSpec(
        name="expectation",
        label="Earnings expectation",
        description="Forward EPS growth/margin and freshness of disclosure.",
        feature_names=(
            "forward_eps_growth_value",
            "forward_eps_growth_margin",
            "days_since_disclosed",
        ),
        favorable_low_features=("days_since_disclosed",),
    ),
    _ComponentSpec(
        name="attention",
        label="Volume attention",
        description="Volume ratio and volume-ratio margin. ADV is not treated as alpha.",
        feature_names=("volume_ratio_value", "volume_ratio_margin"),
    ),
    _ComponentSpec(
        name="price_momentum",
        label="Price momentum / overheat",
        description="Entry-time price trend and overheat features.",
        feature_names=(
            "risk_adjusted_return_value",
            "stock_return_20d_pct",
            "stock_return_60d_pct",
            "rsi10",
        ),
    ),
)


def run_forward_eps_component_decomposition(
    input_bundle_path: str | Path | None = None,
    *,
    output_root: str | Path | None = None,
    quantile_bucket_count: int = DEFAULT_QUANTILE_BUCKET_COUNT,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
) -> ForwardEpsComponentDecompositionResult:
    if quantile_bucket_count < 2:
        raise ValueError("quantile_bucket_count must be at least 2")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    resolved_input = resolve_required_bundle_path(
        input_bundle_path,
        latest_bundle_resolver=lambda: get_forward_eps_trade_archetype_decomposition_latest_bundle_path(
            output_root=output_root,
        ),
        missing_message=(
            "Forward EPS trade-archetype bundle was not found. "
            "Run run_forward_eps_trade_archetype_decomposition.py first."
        ),
    )
    input_info = load_research_bundle_info(resolved_input)
    base_result = load_forward_eps_trade_archetype_decomposition_bundle(resolved_input)
    enriched = _prepare_trade_frame(base_result.enriched_trade_df)
    if enriched.empty:
        raise RuntimeError("forward EPS component decomposition received no enriched trades")
    component_trade_df = _build_component_trade_df(enriched)
    component_bucket_summary_df = _build_component_bucket_summary_df(
        component_trade_df,
        quantile_bucket_count=quantile_bucket_count,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    component_overlap_summary_df = _build_component_overlap_summary_df(
        component_trade_df,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    component_regression_summary_df = _build_component_regression_summary_df(
        component_trade_df,
    )
    return ForwardEpsComponentDecompositionResult(
        db_path=base_result.db_path,
        input_bundle_path=str(resolved_input),
        input_run_id=input_info.run_id,
        input_git_commit=input_info.git_commit,
        strategy_name=base_result.strategy_name,
        dataset_name=base_result.dataset_name,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        quantile_bucket_count=int(quantile_bucket_count),
        severe_loss_threshold_pct=float(severe_loss_threshold_pct),
        trade_count=int(len(component_trade_df)),
        component_policy=(
            "realized-trade decomposition only; scores are favorable percentile averages "
            "inside each window x market_scope group. ADV60 is retained only as capacity "
            "context and is not included in the attention component."
        ),
        component_trade_df=component_trade_df,
        component_bucket_summary_df=component_bucket_summary_df,
        component_overlap_summary_df=component_overlap_summary_df,
        component_regression_summary_df=component_regression_summary_df,
    )


def _prepare_trade_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if result.empty:
        return result
    if "window_label" not in result.columns:
        result["window_label"] = "full"
    if "market_scope" not in result.columns:
        result["market_scope"] = "unknown"
    if "dataset_name" not in result.columns:
        result["dataset_name"] = "unknown"
    for spec in COMPONENT_SPECS:
        for feature_name in spec.feature_names:
            if feature_name not in result.columns:
                result[feature_name] = np.nan
    result["trade_return_pct"] = pd.to_numeric(
        result["trade_return_pct"],
        errors="coerce",
    )
    return result.dropna(subset=["trade_return_pct"]).reset_index(drop=True)


def _build_component_trade_df(frame: pd.DataFrame) -> pd.DataFrame:
    groups: list[pd.DataFrame] = []
    for _, group in frame.groupby(list(_GROUP_COLUMNS), dropna=False, sort=False):
        scored = group.copy()
        for spec in COMPONENT_SPECS:
            scored[f"{spec.name}_component_score"] = _component_score(scored, spec)
        scored["value_attention_score"] = scored[
            ["value_component_score", "attention_component_score"]
        ].mean(axis=1)
        scored["value_expectation_attention_score"] = scored[
            [
                "value_component_score",
                "expectation_component_score",
                "attention_component_score",
            ]
        ].mean(axis=1)
        groups.append(scored)
    return _sort_component_table(pd.concat(groups, ignore_index=True))


def _component_score(frame: pd.DataFrame, spec: _ComponentSpec) -> pd.Series:
    parts: list[pd.Series] = []
    for feature_name in spec.feature_names:
        values = pd.to_numeric(frame[feature_name], errors="coerce")
        ranked = values.rank(method="average", pct=True)
        if feature_name in spec.favorable_low_features:
            ranked = 1.0 - ranked
        parts.append(ranked)
    if not parts:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.concat(parts, axis=1).mean(axis=1)


def _build_component_bucket_summary_df(
    component_trade_df: pd.DataFrame,
    *,
    quantile_bucket_count: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group_payload, group_frame in _iter_summary_groups(component_trade_df):
        total_count = len(group_frame)
        for spec in COMPONENT_SPECS:
            score_column = f"{spec.name}_component_score"
            valid = group_frame.dropna(subset=[score_column]).copy()
            if valid.empty:
                continue
            bucket_count = min(quantile_bucket_count, len(valid))
            valid["bucket_rank"] = _assign_quantile_bucket(
                valid[score_column],
                bucket_count=bucket_count,
            )
            for bucket_rank, bucket_frame in valid.groupby("bucket_rank", dropna=False):
                metrics = _build_trade_metrics(
                    bucket_frame,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
                bucket_rank_int = int(cast(Any, bucket_rank))
                rows.append(
                    {
                        **group_payload,
                        "component_name": spec.name,
                        "component_label": spec.label,
                        "bucket_rank": bucket_rank_int,
                        "bucket_count": bucket_count,
                        "bucket_label": f"Q{bucket_rank_int}/{bucket_count}",
                        "coverage_pct": metrics["trade_count"] / total_count * 100.0
                        if total_count
                        else np.nan,
                        "score_min": float(bucket_frame[score_column].min()),
                        "score_median": float(bucket_frame[score_column].median()),
                        "score_max": float(bucket_frame[score_column].max()),
                        **metrics,
                    }
                )
    return _sort_component_table(pd.DataFrame(rows))


def _build_component_overlap_summary_df(
    component_trade_df: pd.DataFrame,
    *,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group_payload, group_frame in _iter_summary_groups(component_trade_df):
        total_count = len(group_frame)
        thresholds = {
            component: pd.to_numeric(
                group_frame[f"{component}_component_score"],
                errors="coerce",
            ).quantile(0.80)
            for component in _COMPONENT_NAMES
        }
        masks = {
            component: pd.to_numeric(
                group_frame[f"{component}_component_score"],
                errors="coerce",
            )
            >= thresholds[component]
            for component in _COMPONENT_NAMES
        }
        candidates: tuple[tuple[str, str, pd.Series], ...] = (
            ("baseline_all", "All realized forward_eps_driven trades.", pd.Series(True, index=group_frame.index)),
            ("value_q80", "Top value component quintile.", masks["value"]),
            ("expectation_q80", "Top earnings expectation component quintile.", masks["expectation"]),
            ("attention_q80", "Top volume-attention component quintile.", masks["attention"]),
            ("price_momentum_q80", "Top price momentum / overheat component quintile.", masks["price_momentum"]),
            ("value_attention_q80", "Top value and volume-attention overlap.", masks["value"] & masks["attention"]),
            (
                "value_attention_expectation_q80",
                "Top value, volume-attention, and earnings-expectation overlap.",
                masks["value"] & masks["attention"] & masks["expectation"],
            ),
            (
                "value_attention_without_price_overheat",
                "Top value and attention while not in the top price-momentum/overheat quintile.",
                masks["value"] & masks["attention"] & ~masks["price_momentum"],
            ),
            (
                "attention_without_value",
                "Top attention without top value exposure.",
                masks["attention"] & ~masks["value"],
            ),
            (
                "value_without_attention",
                "Top value without top volume-attention exposure.",
                masks["value"] & ~masks["attention"],
            ),
        )
        baseline_metrics = _build_trade_metrics(
            group_frame,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        for candidate_name, description, mask in candidates:
            selected = group_frame[mask.fillna(False).astype(bool)].copy()
            metrics = _build_trade_metrics(
                selected,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
            rows.append(
                {
                    **group_payload,
                    "candidate_name": candidate_name,
                    "candidate_description": description,
                    "trade_count": metrics["trade_count"],
                    "coverage_pct": metrics["trade_count"] / total_count * 100.0
                    if total_count
                    else np.nan,
                    **metrics,
                    "baseline_avg_trade_return_pct": baseline_metrics[
                        "avg_trade_return_pct"
                    ],
                    "baseline_severe_loss_rate_pct": baseline_metrics[
                        "severe_loss_rate_pct"
                    ],
                    "delta_avg_trade_return_pct": metrics["avg_trade_return_pct"]
                    - baseline_metrics["avg_trade_return_pct"],
                    "delta_severe_loss_rate_pct": metrics["severe_loss_rate_pct"]
                    - baseline_metrics["severe_loss_rate_pct"],
                }
            )
    return _sort_component_table(pd.DataFrame(rows))


def _build_component_regression_summary_df(component_trade_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    component_columns = [f"{component}_component_score" for component in _COMPONENT_NAMES]
    for group_payload, group_frame in _iter_summary_groups(component_trade_df):
        for component, score_column in zip(_COMPONENT_NAMES, component_columns, strict=True):
            rows.extend(
                _regression_rows(
                    group_payload,
                    group_frame,
                    model_name="univariate",
                    component_names=(component,),
                    score_columns=(score_column,),
                )
            )
        rows.extend(
            _regression_rows(
                group_payload,
                group_frame,
                model_name="multivariate",
                component_names=_COMPONENT_NAMES,
                score_columns=tuple(component_columns),
            )
        )
    return _sort_component_table(pd.DataFrame(rows))


def _regression_rows(
    group_payload: dict[str, Any],
    group_frame: pd.DataFrame,
    *,
    model_name: str,
    component_names: tuple[str, ...],
    score_columns: tuple[str, ...],
) -> list[dict[str, Any]]:
    model_frame = group_frame[[*score_columns, "trade_return_pct"]].dropna().copy()
    if len(model_frame) <= len(score_columns) + 2:
        return []
    y = pd.to_numeric(model_frame["trade_return_pct"], errors="coerce").to_numpy(dtype=float)
    x_columns: list[np.ndarray[Any, np.dtype[np.float64]]] = []
    for score_column in score_columns:
        values = pd.to_numeric(model_frame[score_column], errors="coerce").to_numpy(dtype=float)
        std = float(np.nanstd(values, ddof=1))
        if not math.isfinite(std) or std <= 0:
            return []
        x_columns.append((values - float(np.nanmean(values))) / std)
    x = np.column_stack([np.ones(len(model_frame)), *x_columns])
    beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    residual = y - x @ beta
    dof = len(y) - x.shape[1]
    if dof <= 0:
        return []
    sigma2 = float((residual @ residual) / dof)
    xtx_inv = np.linalg.pinv(x.T @ x)
    standard_errors = np.sqrt(np.diag(xtx_inv) * sigma2)
    rows: list[dict[str, Any]] = []
    for index, component_name in enumerate(component_names, start=1):
        se = float(standard_errors[index])
        coef = float(beta[index])
        rows.append(
            {
                **group_payload,
                "model_name": model_name,
                "component_name": component_name,
                "coefficient_pct_per_1sd": coef,
                "t_stat": coef / se if se > 0 else np.nan,
                "observation_count": int(len(model_frame)),
                "r_squared": _r_squared(y, x @ beta),
            }
        )
    return rows


def _r_squared(y: np.ndarray[Any, np.dtype[np.float64]], fitted: np.ndarray[Any, np.dtype[np.float64]]) -> float:
    total = float(((y - y.mean()) @ (y - y.mean())))
    if total <= 0:
        return np.nan
    residual = y - fitted
    return float(1.0 - (residual @ residual) / total)


def _iter_summary_groups(frame: pd.DataFrame) -> list[tuple[dict[str, Any], pd.DataFrame]]:
    rows: list[tuple[dict[str, Any], pd.DataFrame]] = []
    for window_label, window_frame in frame.groupby("window_label", dropna=False, sort=False):
        rows.append(
            (
                {"window_label": str(window_label), "market_scope": "all"},
                window_frame.copy(),
            )
        )
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


def _assign_quantile_bucket(series: pd.Series, *, bucket_count: int) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    ranked_pct = values.rank(method="first", pct=True)
    bucket = ranked_pct.map(
        lambda value: math.ceil(float(value) * bucket_count)
        if pd.notna(value)
        else np.nan
    )
    return bucket.clip(1, bucket_count).astype(int)


def _sort_component_table(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    if "market_scope" in result.columns:
        result["_market_order"] = result["market_scope"].map(
            {scope: index for index, scope in enumerate(_MARKET_SCOPE_ORDER)}
        )
    sort_columns = [
        column
        for column in (
            "window_label",
            "_market_order",
            "market_scope",
            "component_name",
            "bucket_rank",
            "candidate_name",
            "model_name",
        )
        if column in result.columns
    ]
    if sort_columns:
        result = result.sort_values(sort_columns, kind="stable").reset_index(drop=True)
    return result.drop(columns=[column for column in result.columns if column.startswith("_")])


def _fmt(value: object, digits: int = 2) -> str:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(number):
        return "-"
    return f"{number:.{digits}f}"


def _fmt_int(value: object) -> str:
    try:
        return str(int(cast(Any, value)))
    except (TypeError, ValueError):
        return "-"


def _build_summary_markdown(result: ForwardEpsComponentDecompositionResult) -> str:
    lines = [
        "# Forward EPS Component Decomposition",
        "",
        "## Parameters",
        "",
        f"- Input bundle: `{result.input_bundle_path}`",
        f"- Strategy: `{result.strategy_name}`",
        f"- Dataset: `{result.dataset_name}`",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Trades: `{result.trade_count}`",
        f"- Component policy: {result.component_policy}.",
        "",
        "## Top Component Overlaps",
        "",
    ]
    overlap = result.component_overlap_summary_df.copy()
    if overlap.empty:
        lines.append("_No overlap rows._")
        return "\n".join(lines) + "\n"
    rows = overlap[
        (overlap["window_label"].astype(str) == "full")
        & (overlap["market_scope"].astype(str) == "all")
    ].sort_values("avg_trade_return_pct", ascending=False, na_position="last")
    lines.extend(
        [
            "| Candidate | Trades | Coverage | Avg | Severe | Delta avg | Delta severe |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in rows.head(12).itertuples(index=False):
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{row.candidate_name}`",
                    _fmt_int(row.trade_count),
                    f"{_fmt(row.coverage_pct)}%",
                    f"{_fmt(row.avg_trade_return_pct)}%",
                    f"{_fmt(row.severe_loss_rate_pct)}%",
                    f"{_fmt(row.delta_avg_trade_return_pct)}pt",
                    f"{_fmt(row.delta_severe_loss_rate_pct)}pt",
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def write_forward_eps_component_decomposition_bundle(
    result: ForwardEpsComponentDecompositionResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=FORWARD_EPS_COMPONENT_DECOMPOSITION_EXPERIMENT_ID,
        module=__name__,
        function="run_forward_eps_component_decomposition",
        params={
            "input_bundle_path": result.input_bundle_path,
            "quantile_bucket_count": result.quantile_bucket_count,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_forward_eps_component_decomposition_bundle(
    bundle_path: str | Path,
) -> ForwardEpsComponentDecompositionResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=ForwardEpsComponentDecompositionResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_forward_eps_component_decomposition_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FORWARD_EPS_COMPONENT_DECOMPOSITION_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_forward_eps_component_decomposition_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FORWARD_EPS_COMPONENT_DECOMPOSITION_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


__all__: Sequence[str] = (
    "COMPONENT_SPECS",
    "FORWARD_EPS_COMPONENT_DECOMPOSITION_EXPERIMENT_ID",
    "ForwardEpsComponentDecompositionResult",
    "get_forward_eps_component_decomposition_bundle_path_for_run_id",
    "get_forward_eps_component_decomposition_latest_bundle_path",
    "load_forward_eps_component_decomposition_bundle",
    "run_forward_eps_component_decomposition",
    "write_forward_eps_component_decomposition_bundle",
)
