"""Pre-earnings runup threshold response research."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

from src.domains.analytics.earnings_holdthrough_expectancy import (
    _expand_market_scope,
    _float_or_nan,
    _sort_summary_df,
    _str_or_none,
    _top_rows_for_markdown,
    run_earnings_holdthrough_expectancy_research,
)
from src.domains.analytics.post_earnings_next_day_entry import (
    DEFAULT_HORIZONS,
    DEFAULT_LIQUIDITY_WINDOW,
    DEFAULT_PRE_WINDOWS,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    run_post_earnings_next_day_entry_research,
)
from src.domains.analytics.pre_earnings_eps120_proxy import (
    run_pre_earnings_eps120_proxy_research,
)
from src.domains.analytics.readonly_duckdb_support import SourceMode
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

PRE_EARNINGS_RUNUP_THRESHOLD_RESPONSE_EXPERIMENT_ID = (
    "market-behavior/pre-earnings-runup-threshold-response"
)
DEFAULT_20D_RUNUP_THRESHOLDS: tuple[float, ...] = (0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0)
DEFAULT_60D_RUNUP_THRESHOLDS: tuple[float, ...] = (
    0.0,
    5.0,
    10.0,
    15.0,
    20.0,
    30.0,
    40.0,
    50.0,
)
DEFAULT_MIN_EVENTS = 50
_PERCENTILE_BUCKET_ORDER: tuple[str, ...] = (
    "top_10pct",
    "top_20pct",
    "middle_60pct",
    "bottom_20pct",
    "bottom_10pct",
    "missing",
)
_LIQUIDITY_SCOPE_ORDER: tuple[str, ...] = (
    "all_liquidity",
    "rerating_participation",
    "distribution_stress",
    "stale_liquidity",
    "neutral",
    "missing",
)


@dataclass(frozen=True)
class PreEarningsRunupThresholdResponseResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    pre_windows: tuple[int, ...]
    horizons: tuple[int, ...]
    liquidity_window: int
    severe_loss_threshold_pct: float
    thresholds_20d: tuple[float, ...]
    thresholds_60d: tuple[float, ...]
    min_events: int
    event_feature_df: pd.DataFrame
    threshold_response_df: pd.DataFrame
    joint_runup_response_df: pd.DataFrame
    percentile_response_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame


def run_pre_earnings_runup_threshold_response_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    pre_windows: Iterable[int] = DEFAULT_PRE_WINDOWS,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    liquidity_window: int = DEFAULT_LIQUIDITY_WINDOW,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    thresholds_20d: Sequence[float] = DEFAULT_20D_RUNUP_THRESHOLDS,
    thresholds_60d: Sequence[float] = DEFAULT_60D_RUNUP_THRESHOLDS,
    min_events: int = DEFAULT_MIN_EVENTS,
) -> PreEarningsRunupThresholdResponseResult:
    resolved_pre_windows = tuple(sorted({int(window) for window in pre_windows}))
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_thresholds_20d = _normalize_thresholds(thresholds_20d, name="thresholds_20d")
    resolved_thresholds_60d = _normalize_thresholds(thresholds_60d, name="thresholds_60d")
    _validate_params(
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        liquidity_window=liquidity_window,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        min_events=min_events,
    )

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    holdthrough = run_earnings_holdthrough_expectancy_research(
        db_path_obj,
        start_date=start_date,
        end_date=end_date,
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        liquidity_window=liquidity_window,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    post_entry = run_post_earnings_next_day_entry_research(
        db_path_obj,
        start_date=start_date,
        end_date=end_date,
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        liquidity_window=liquidity_window,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
    )
    eps120 = run_pre_earnings_eps120_proxy_research(
        db_path_obj,
        start_date=start_date,
        end_date=end_date,
        min_events=1,
    )

    event_feature_df = _merge_event_panels(
        holdthrough.event_feature_df,
        post_entry.event_feature_df,
        eps120.event_feature_df,
        horizons=resolved_horizons,
    )
    scoped_df = _expand_liquidity_scope(_expand_market_scope(event_feature_df))
    threshold_response_df = _build_threshold_response_df(
        scoped_df,
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        thresholds_by_window={
            20: resolved_thresholds_20d,
            60: resolved_thresholds_60d,
        },
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        min_events=min_events,
    )
    joint_runup_response_df = _build_joint_runup_response_df(
        scoped_df,
        horizons=resolved_horizons,
        thresholds_20d=resolved_thresholds_20d,
        thresholds_60d=resolved_thresholds_60d,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        min_events=min_events,
    )
    percentile_response_df = _build_percentile_response_df(
        scoped_df,
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        min_events=min_events,
    )
    coverage_diagnostics_df = _build_coverage_diagnostics_df(scoped_df)

    return PreEarningsRunupThresholdResponseResult(
        db_path=str(db_path_obj),
        source_mode=post_entry.source_mode,
        source_detail=post_entry.source_detail,
        market_source=post_entry.market_source,
        analysis_start_date=_str_or_none(event_feature_df["disclosed_date"].min())
        if "disclosed_date" in event_feature_df
        else None,
        analysis_end_date=_str_or_none(event_feature_df["disclosed_date"].max())
        if "disclosed_date" in event_feature_df
        else None,
        pre_windows=resolved_pre_windows,
        horizons=resolved_horizons,
        liquidity_window=liquidity_window,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        thresholds_20d=resolved_thresholds_20d,
        thresholds_60d=resolved_thresholds_60d,
        min_events=min_events,
        event_feature_df=event_feature_df,
        threshold_response_df=threshold_response_df,
        joint_runup_response_df=joint_runup_response_df,
        percentile_response_df=percentile_response_df,
        coverage_diagnostics_df=coverage_diagnostics_df,
    )


def write_pre_earnings_runup_threshold_response_bundle(
    result: PreEarningsRunupThresholdResponseResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=PRE_EARNINGS_RUNUP_THRESHOLD_RESPONSE_EXPERIMENT_ID,
        module=__name__,
        function="run_pre_earnings_runup_threshold_response_research",
        params={
            "pre_windows": list(result.pre_windows),
            "horizons": list(result.horizons),
            "liquidity_window": result.liquidity_window,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "thresholds_20d": list(result.thresholds_20d),
            "thresholds_60d": list(result.thresholds_60d),
            "min_events": result.min_events,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "event_count": int(len(result.event_feature_df)),
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "threshold_response_df": result.threshold_response_df,
            "joint_runup_response_df": result.joint_runup_response_df,
            "percentile_response_df": result.percentile_response_df,
            "event_feature_df": result.event_feature_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: PreEarningsRunupThresholdResponseResult) -> str:
    coverage = _top_rows_for_markdown(result.coverage_diagnostics_df, limit=24)
    thresholds = _top_rows_for_markdown(
        result.threshold_response_df,
        sort_columns=[
            "market_scope",
            "is_fy",
            "liquidity_scope",
            "pre_window",
            "direction",
            "threshold_pct",
            "horizon",
        ],
        limit=80,
    )
    joint = _top_rows_for_markdown(
        result.joint_runup_response_df,
        sort_columns=[
            "market_scope",
            "is_fy",
            "liquidity_scope",
            "threshold_20d_pct",
            "threshold_60d_pct",
            "horizon",
        ],
        limit=80,
    )
    percentile = _top_rows_for_markdown(
        result.percentile_response_df,
        sort_columns=[
            "market_scope",
            "is_fy",
            "liquidity_scope",
            "pre_window",
            "percentile_bucket_order",
            "horizon",
        ],
        limit=80,
    )
    return "\n".join(
        [
            "# Pre-Earnings Runup Threshold Response",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Pre windows: `{list(result.pre_windows)}`",
            f"- Forward horizons: `{list(result.horizons)}`",
            f"- 20d thresholds: `{list(result.thresholds_20d)}`",
            f"- 60d thresholds: `{list(result.thresholds_60d)}`",
            f"- Min events: `{result.min_events}`",
            "",
            "## Coverage Diagnostics",
            "",
            coverage,
            "",
            "## Threshold Response",
            "",
            thresholds,
            "",
            "## Joint Runup Response",
            "",
            joint,
            "",
            "## Percentile Response",
            "",
            percentile,
            "",
        ]
    )


def _validate_params(
    *,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    liquidity_window: int,
    severe_loss_threshold_pct: float,
    min_events: int,
) -> None:
    if not pre_windows or any(window <= 0 for window in pre_windows):
        raise ValueError("pre_windows must be positive")
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if liquidity_window <= 0:
        raise ValueError("liquidity_window must be positive")
    if severe_loss_threshold_pct >= 0.0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if min_events <= 0:
        raise ValueError("min_events must be positive")


def _normalize_thresholds(values: Sequence[float], *, name: str) -> tuple[float, ...]:
    normalized = tuple(sorted({float(value) for value in values}))
    if not normalized or any(not math.isfinite(value) or value < 0.0 for value in normalized):
        raise ValueError(f"{name} must contain finite non-negative values")
    return normalized


def _merge_event_panels(
    holdthrough_df: pd.DataFrame,
    post_entry_df: pd.DataFrame,
    eps120_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    keys = ["code", "disclosed_date"]
    base = post_entry_df.copy()
    hold_columns = [
        *keys,
        *[
            column
            for horizon in horizons
            for column in (
                f"forward_return_{horizon}d_pct",
                f"forward_excess_return_{horizon}d_pct",
            )
            if column in holdthrough_df.columns
        ],
    ]
    hold = holdthrough_df.loc[:, hold_columns].rename(
        columns={
            column: column.replace("forward_", "holdthrough_")
            for column in hold_columns
            if column not in keys
        }
    )
    base = base.merge(hold, on=keys, how="left")

    eps_columns = [
        column
        for column in (
            "eps120_target_eligible",
            "eps120_positive_target",
            "next_forecast_to_actual_eps_ratio",
            "forward_per",
            "forward_per_bucket",
        )
        if column in eps120_df.columns
    ]
    eps = eps120_df.loc[:, [*keys, *eps_columns]]
    base = base.merge(eps, on=keys, how="left")
    if "overheat_state" not in base.columns and "overheat_state" in eps120_df.columns:
        eps_overheat = eps120_df.loc[:, [*keys, "overheat_state"]]
        base = base.merge(eps_overheat, on=keys, how="left")
    return base.sort_values(["disclosed_date", "code"], kind="stable").reset_index(drop=True)


def _expand_liquidity_scope(scoped_df: pd.DataFrame) -> pd.DataFrame:
    if scoped_df.empty:
        result = scoped_df.copy()
        result["liquidity_scope"] = pd.Series(dtype="object")
        return result
    all_scope = scoped_df.copy()
    all_scope["liquidity_scope"] = "all_liquidity"
    actual = scoped_df.copy()
    actual["liquidity_scope"] = actual["liquidity_regime"].fillna("missing").astype(str)
    return pd.concat([all_scope, actual], ignore_index=True)


def _ensure_overheat_state(frame: pd.DataFrame) -> pd.DataFrame:
    if "overheat_state" in frame.columns:
        return frame
    result = frame.copy()
    result["overheat_state"] = "missing"
    return result


def _build_threshold_response_df(
    scoped_df: pd.DataFrame,
    *,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    thresholds_by_window: dict[int, Sequence[float]],
    severe_loss_threshold_pct: float,
    min_events: int,
) -> pd.DataFrame:
    scoped_df = _ensure_overheat_state(scoped_df)
    rows: list[dict[str, Any]] = []
    for window in pre_windows:
        return_col = f"pre_return_{window}d_pct"
        if return_col not in scoped_df.columns:
            continue
        thresholds = thresholds_by_window.get(window, thresholds_by_window.get(20, ()))
        for direction in ("ge", "le"):
            for threshold in thresholds:
                mask = _threshold_mask(scoped_df[return_col], direction=direction, threshold=threshold)
                selected = scoped_df[mask].copy()
                if selected.empty:
                    continue
                rows.extend(
                    _summarize_condition_frames(
                        selected,
                        horizons=horizons,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                        min_events=min_events,
                        condition_fields={
                            "condition_family": "single_threshold",
                            "pre_window": window,
                            "direction": direction,
                            "threshold_pct": float(threshold),
                            "condition_label": _condition_label(window, direction, threshold),
                        },
                    )
                )
    return _sort_summary_df(pd.DataFrame(rows), columns=_threshold_response_columns())


def _build_joint_runup_response_df(
    scoped_df: pd.DataFrame,
    *,
    horizons: Sequence[int],
    thresholds_20d: Sequence[float],
    thresholds_60d: Sequence[float],
    severe_loss_threshold_pct: float,
    min_events: int,
) -> pd.DataFrame:
    scoped_df = _ensure_overheat_state(scoped_df)
    columns = _joint_response_columns()
    if "pre_return_20d_pct" not in scoped_df.columns or "pre_return_60d_pct" not in scoped_df.columns:
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, Any]] = []
    for threshold_20d in thresholds_20d:
        for threshold_60d in thresholds_60d:
            selected = scoped_df[
                _threshold_mask(scoped_df["pre_return_20d_pct"], direction="ge", threshold=threshold_20d)
                & _threshold_mask(
                    scoped_df["pre_return_60d_pct"],
                    direction="ge",
                    threshold=threshold_60d,
                )
            ].copy()
            if selected.empty:
                continue
            rows.extend(
                _summarize_condition_frames(
                    selected,
                    horizons=horizons,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                    min_events=min_events,
                    condition_fields={
                        "condition_family": "joint_runup",
                        "threshold_20d_pct": float(threshold_20d),
                        "threshold_60d_pct": float(threshold_60d),
                        "condition_label": (
                            f"pre_return_20d_ge_{_threshold_token(threshold_20d)}"
                            f"__pre_return_60d_ge_{_threshold_token(threshold_60d)}"
                        ),
                    },
                )
            )
    return _sort_summary_df(pd.DataFrame(rows), columns=columns)


def _build_percentile_response_df(
    scoped_df: pd.DataFrame,
    *,
    pre_windows: Sequence[int],
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
    min_events: int,
) -> pd.DataFrame:
    scoped_df = _ensure_overheat_state(scoped_df)
    rows: list[dict[str, Any]] = []
    frame = scoped_df.copy()
    frame["event_year"] = frame["pre_event_date"].astype(str).str.slice(0, 4)
    frame = frame[frame["event_year"].str.match(r"^\d{4}$", na=False)].copy()
    if frame.empty:
        return pd.DataFrame(columns=_percentile_response_columns())
    for window in pre_windows:
        return_col = f"pre_return_{window}d_pct"
        if return_col not in frame.columns:
            continue
        ranked = frame.copy()
        ranked["_pre_return_rank_pct"] = ranked.groupby(
            ["market_scope", "is_fy", "event_year"],
            dropna=False,
        )[return_col].rank(pct=True)
        ranked["percentile_bucket"] = ranked["_pre_return_rank_pct"].map(_percentile_bucket)
        for bucket in _PERCENTILE_BUCKET_ORDER:
            selected = ranked[ranked["percentile_bucket"].astype(str).eq(bucket)].copy()
            if selected.empty:
                continue
            rows.extend(
                _summarize_condition_frames(
                    selected,
                    horizons=horizons,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                    min_events=min_events,
                    condition_fields={
                        "condition_family": "annual_percentile_bucket",
                        "pre_window": window,
                        "percentile_bucket": bucket,
                        "percentile_bucket_order": _PERCENTILE_BUCKET_ORDER.index(bucket),
                    },
                )
            )
    return _sort_summary_df(pd.DataFrame(rows), columns=_percentile_response_columns())


def _summarize_condition_frames(
    selected: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
    min_events: int,
    condition_fields: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    selected = _ensure_overheat_state(selected)
    group_columns = ["market_scope", "is_fy", "liquidity_scope", "overheat_state"]
    for keys, group in selected.groupby(group_columns, sort=False, dropna=False):
        if len(group) < min_events:
            continue
        key_values = dict(zip(group_columns, keys, strict=True))
        base_summary = {
            **condition_fields,
            **key_values,
            "event_count": int(len(group)),
            "code_count": int(group["code"].nunique()),
            **_eps120_summary(group),
            **_execution_summary(group),
        }
        for horizon in horizons:
            hold_col = f"holdthrough_excess_return_{horizon}d_pct"
            post_col = f"forward_excess_return_{horizon}d_pct"
            rows.append(
                {
                    **base_summary,
                    "horizon": int(horizon),
                    **_prefixed_return_summary(
                        group,
                        hold_col,
                        prefix="holdthrough",
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    ),
                    **_prefixed_return_summary(
                        group,
                        post_col,
                        prefix="post_entry",
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    ),
                }
            )
    return rows


def _threshold_mask(values: pd.Series, *, direction: str, threshold: float) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan)
    if direction == "ge":
        return numeric >= threshold
    if direction == "le":
        return numeric <= -threshold
    raise ValueError(f"unsupported direction: {direction}")


def _condition_label(window: int, direction: str, threshold: float) -> str:
    operator = "ge" if direction == "ge" else "le_negative"
    return f"pre_return_{window}d_{operator}_{_threshold_token(threshold)}"


def _threshold_token(value: float) -> str:
    return f"{float(value):.1f}".rstrip("0").rstrip(".").replace(".", "_")


def _eps120_summary(frame: pd.DataFrame) -> dict[str, float | int]:
    if "eps120_positive_target" not in frame.columns:
        return {
            "eps120_eligible_count": 0,
            "eps120_target_count": 0,
            "eps120_target_rate_pct": np.nan,
            "eps120_eligible_target_rate_pct": np.nan,
        }
    eligible = frame["eps120_target_eligible"] == True  # noqa: E712
    target = frame["eps120_positive_target"] == True  # noqa: E712
    return {
        "eps120_eligible_count": int(eligible.sum()),
        "eps120_target_count": int(target.sum()),
        "eps120_target_rate_pct": _bool_rate_pct(frame["eps120_positive_target"]),
        "eps120_eligible_target_rate_pct": _bool_rate_pct(frame.loc[eligible, "eps120_positive_target"]),
    }


def _execution_summary(frame: pd.DataFrame) -> dict[str, float | int]:
    labels = frame["execution_label"].astype(str) if "execution_label" in frame.columns else pd.Series(dtype=str)
    executable = frame["entry_executable"] == True if "entry_executable" in frame.columns else pd.Series(dtype=bool)  # noqa: E712
    return {
        "entry_executable_count": int(executable.sum()),
        "entry_executable_rate_pct": _bool_rate_pct(frame["entry_executable"])
        if "entry_executable" in frame.columns
        else np.nan,
        "limit_up_no_fill_count": int((labels == "limit_up_no_fill").sum()),
        "limit_up_no_fill_rate_pct": float((labels == "limit_up_no_fill").mean() * 100.0)
        if len(labels)
        else np.nan,
        "limit_down_no_fill_count": int((labels == "limit_down_no_fill").sum()),
        "limit_down_no_fill_rate_pct": float((labels == "limit_down_no_fill").mean() * 100.0)
        if len(labels)
        else np.nan,
    }


def _prefixed_return_summary(
    frame: pd.DataFrame,
    column: str,
    *,
    prefix: str,
    severe_loss_threshold_pct: float,
) -> dict[str, float | int]:
    if column not in frame.columns:
        valid = pd.Series(dtype=float)
    else:
        valid = (
            pd.to_numeric(frame[column], errors="coerce")
            .replace([np.inf, -np.inf], np.nan)
            .dropna()
        )
    return {
        f"{prefix}_valid_return_count": int(len(valid)),
        f"{prefix}_mean_excess_return_pct": float(valid.mean()) if not valid.empty else np.nan,
        f"{prefix}_median_excess_return_pct": float(valid.median()) if not valid.empty else np.nan,
        f"{prefix}_win_rate_pct": float((valid > 0).mean() * 100.0)
        if not valid.empty
        else np.nan,
        f"{prefix}_severe_loss_rate_pct": (
            float((valid <= severe_loss_threshold_pct).mean() * 100.0)
            if not valid.empty
            else np.nan
        ),
    }


def _bool_rate_pct(values: pd.Series) -> float:
    if values.empty:
        return np.nan
    return float((values == True).mean() * 100.0)  # noqa: E712


def _percentile_bucket(rank_pct: object) -> str:
    value = _float_or_nan(rank_pct)
    if not math.isfinite(value):
        return "missing"
    if value >= 0.9:
        return "top_10pct"
    if value >= 0.8:
        return "top_20pct"
    if value <= 0.1:
        return "bottom_10pct"
    if value <= 0.2:
        return "bottom_20pct"
    return "middle_60pct"


def _build_coverage_diagnostics_df(scoped_df: pd.DataFrame) -> pd.DataFrame:
    scoped_df = _ensure_overheat_state(scoped_df)
    rows: list[dict[str, Any]] = []
    if not scoped_df.empty:
        for keys, frame in scoped_df.groupby(
            ["market_scope", "is_fy", "liquidity_scope", "overheat_state"],
            sort=False,
            dropna=False,
        ):
            market_scope, is_fy, liquidity_scope, overheat_state = keys
            rows.append(
                {
                    "market_scope": market_scope,
                    "is_fy": is_fy,
                    "liquidity_scope": liquidity_scope,
                    "overheat_state": overheat_state,
                    "event_count": int(len(frame)),
                    "code_count": int(frame["code"].nunique()),
                    "pre_return_20d_coverage_pct": _numeric_coverage_pct(
                        frame.get("pre_return_20d_pct", pd.Series(dtype=float))
                    ),
                    "pre_return_60d_coverage_pct": _numeric_coverage_pct(
                        frame.get("pre_return_60d_pct", pd.Series(dtype=float))
                    ),
                    "eps120_eligible_count": int(
                        (frame.get("eps120_target_eligible", pd.Series(dtype=bool)) == True).sum()  # noqa: E712
                    ),
                }
            )
    return _sort_summary_df(
        pd.DataFrame(rows),
        columns=[
            "market_scope",
            "is_fy",
            "liquidity_scope",
            "overheat_state",
            "event_count",
            "code_count",
            "pre_return_20d_coverage_pct",
            "pre_return_60d_coverage_pct",
            "eps120_eligible_count",
        ],
    )


def _numeric_coverage_pct(values: pd.Series) -> float:
    if values.empty:
        return np.nan
    numeric = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan)
    return float(numeric.notna().mean() * 100.0)


def _base_response_columns() -> list[str]:
    return [
        "condition_family",
        "market_scope",
        "is_fy",
        "liquidity_scope",
        "overheat_state",
        "event_count",
        "code_count",
        "eps120_eligible_count",
        "eps120_target_count",
        "eps120_target_rate_pct",
        "eps120_eligible_target_rate_pct",
        "entry_executable_count",
        "entry_executable_rate_pct",
        "limit_up_no_fill_count",
        "limit_up_no_fill_rate_pct",
        "limit_down_no_fill_count",
        "limit_down_no_fill_rate_pct",
        "horizon",
        "holdthrough_valid_return_count",
        "holdthrough_mean_excess_return_pct",
        "holdthrough_median_excess_return_pct",
        "holdthrough_win_rate_pct",
        "holdthrough_severe_loss_rate_pct",
        "post_entry_valid_return_count",
        "post_entry_mean_excess_return_pct",
        "post_entry_median_excess_return_pct",
        "post_entry_win_rate_pct",
        "post_entry_severe_loss_rate_pct",
    ]


def _threshold_response_columns() -> list[str]:
    return [
        "condition_family",
        "pre_window",
        "direction",
        "threshold_pct",
        "condition_label",
        *_base_response_columns()[1:],
    ]


def _joint_response_columns() -> list[str]:
    return [
        "condition_family",
        "threshold_20d_pct",
        "threshold_60d_pct",
        "condition_label",
        *_base_response_columns()[1:],
    ]


def _percentile_response_columns() -> list[str]:
    return [
        "condition_family",
        "pre_window",
        "percentile_bucket",
        "percentile_bucket_order",
        *_base_response_columns()[1:],
    ]
