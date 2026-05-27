"""Report payload builders for TOPIX100 SMA-ratio LightGBM research."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from src.domains.analytics.topix_sma_ratio_rank_future_close_support import (
    HORIZON_ORDER,
)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _format_return(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4%}"


def _format_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1%}"


def _select_walkforward_comparison_row(
    result: Any,
    *,
    selected_horizon_key: str,
    model_name: str,
) -> pd.Series | None:
    scoped_df = result.walkforward.comparison_summary_df[
        (result.walkforward.comparison_summary_df["selected_horizon_key"] == selected_horizon_key)
        & (result.walkforward.comparison_summary_df["model_name"] == model_name)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_walkforward_gate_row(
    result: Any,
    *,
    selected_horizon_key: str,
) -> pd.Series | None:
    scoped_df = result.walkforward.exploratory_gate_df[
        result.walkforward.exploratory_gate_df["selected_horizon_key"]
        == selected_horizon_key
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_walkforward_top_feature(
    result: Any,
    *,
    selected_horizon_key: str,
) -> pd.Series | None:
    scoped_df = result.walkforward.feature_importance_df[
        result.walkforward.feature_importance_df["selected_horizon_key"]
        == selected_horizon_key
    ].copy()
    if scoped_df.empty:
        return None
    scoped_df = scoped_df.sort_values(
        ["importance_rank", "feature_name"],
        kind="stable",
    )
    return scoped_df.iloc[0]


def _highlight_tone_for_spread(value: float | None) -> str:
    if value is None:
        return "neutral"
    if value > 0:
        return "success"
    if value < 0:
        return "danger"
    return "neutral"


def build_research_bundle_summary_markdown(
    result: Any,
    *,
    base_result: Any,
) -> str:
    lines = [
        "# TOPIX100 SMA Ratio LightGBM",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{base_result.source_mode}`",
        f"- Available range: `{base_result.available_start_date} -> {base_result.available_end_date}`",
        f"- Analysis range: `{base_result.analysis_start_date} -> {base_result.analysis_end_date}`",
        f"- Walk-forward train/test/step: `{result.walkforward.train_window} / {result.walkforward.test_window} / {result.walkforward.step}`",
        f"- Overall gate status: `{result.walkforward.overall_gate_status}`",
        f"- Diagnostic fixed split: `{'included' if result.diagnostic is not None else 'skipped'}`",
        f"- Feature columns: `{', '.join(result.feature_columns)}`",
        "",
        "## Current Read",
        "",
        "The lookup baseline is rebuilt from train-only rows inside each split, then LightGBM ranks the following out-of-sample block with the same six SMA-ratio features.",
    ]

    for selected_horizon_key in HORIZON_ORDER:
        baseline_row = _select_walkforward_comparison_row(
            result,
            selected_horizon_key=selected_horizon_key,
            model_name="baseline",
        )
        lightgbm_row = _select_walkforward_comparison_row(
            result,
            selected_horizon_key=selected_horizon_key,
            model_name="lightgbm",
        )
        gate_row = _select_walkforward_gate_row(
            result,
            selected_horizon_key=selected_horizon_key,
        )
        top_feature = _select_walkforward_top_feature(
            result,
            selected_horizon_key=selected_horizon_key,
        )
        if baseline_row is not None and lightgbm_row is not None:
            lines.append(
                f"- `{selected_horizon_key}` overall Q1-Q10 spread: baseline `{_format_return(_as_float(baseline_row['q1_minus_q10_mean']))}`, LightGBM `{_format_return(_as_float(lightgbm_row['q1_minus_q10_mean']))}`."
            )
        if gate_row is not None and bool(gate_row["is_gate_horizon"]):
            lines.append(
                f"  Gate `{gate_row['gate_status']}` with median split spread `{_format_return(_as_float(gate_row['median_split_q1_minus_q10_mean']))}` and positive split share `{_format_percent(_as_float(gate_row['positive_split_share']))}`."
            )
        if top_feature is not None:
            lines.append(
                f"  Top feature: `{top_feature['feature_name']}` (mean gain `{float(top_feature['mean_importance_gain']):.4f}`)."
            )

    if result.diagnostic_error_message:
        lines.extend(
            [
                "",
                "## Diagnostic Note",
                "",
                f"- Fixed-split diagnostic was skipped: `{result.diagnostic_error_message}`",
            ]
        )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `walkforward_comparison_summary_df`: baseline / LightGBM overall OOS comparison by horizon",
            "- `walkforward_exploratory_gate_df`: gate status for `t_plus_5` / `t_plus_10`",
            "- `walkforward_feature_importance_df`: average LightGBM feature importance across valid splits",
            "- `walkforward_split_spread_df`: per-split baseline / LightGBM OOS spread diagnostics",
            "- `diagnostic_comparison_summary_df`: fixed-split reference comparison (if included)",
        ]
    )
    return "\n".join(lines)


def build_published_summary_payload(result: Any) -> dict[str, Any]:
    t_plus_5_row = _select_walkforward_comparison_row(
        result,
        selected_horizon_key="t_plus_5",
        model_name="lightgbm",
    )
    t_plus_10_row = _select_walkforward_comparison_row(
        result,
        selected_horizon_key="t_plus_10",
        model_name="lightgbm",
    )
    t_plus_10_feature = _select_walkforward_top_feature(
        result,
        selected_horizon_key="t_plus_10",
    )

    if result.walkforward.overall_gate_status == "passed":
        headline = (
            "Walk-forward OOS gate passed for both t_plus_5 and t_plus_10, so the "
            "SMA-ratio feature family remains useful after replacing the hand-crafted "
            "composite with a leak-free LightGBM ranker."
        )
    elif result.walkforward.overall_gate_status == "insufficient_coverage":
        headline = (
            "The walk-forward coverage was not long enough to judge the gate on every "
            "target horizon, so this bundle is informative but not yet conclusive."
        )
    else:
        headline = (
            "This bundle tests whether the TOPIX100 SMA-ratio family still survives "
            "a leak-free walk-forward LightGBM ranking setup."
        )

    result_bullets = [
        "Each split rebuilds the baseline composite from train-only rows, then trains LightGBM on the same train block and ranks only the following out-of-sample dates.",
        "The six inputs stay fixed: three price SMA ratios and three volume SMA ratios.",
    ]
    if t_plus_5_row is not None:
        result_bullets.append(
            f"At `t_plus_5`, LightGBM delivered overall Q1-Q10 spread `{_format_return(_as_float(t_plus_5_row['q1_minus_q10_mean']))}` with median split spread `{_format_return(_as_float(t_plus_5_row['median_split_q1_minus_q10_mean']))}`."
        )
    if t_plus_10_row is not None:
        result_bullets.append(
            f"At `t_plus_10`, LightGBM delivered overall Q1-Q10 spread `{_format_return(_as_float(t_plus_10_row['q1_minus_q10_mean']))}` with positive split share `{_format_percent(_as_float(t_plus_10_row['positive_split_share']))}`."
        )
    if t_plus_10_feature is not None:
        result_bullets.append(
            f"The highest average gain at `t_plus_10` was `{t_plus_10_feature['feature_name']}`."
        )

    highlights = [
        {
            "label": "Gate status",
            "value": result.walkforward.overall_gate_status,
            "tone": (
                "success"
                if result.walkforward.overall_gate_status == "passed"
                else "warning"
                if result.walkforward.overall_gate_status == "insufficient_coverage"
                else "danger"
            ),
            "detail": "walk-forward OOS",
        },
        {
            "label": "Train/Test",
            "value": f"{result.walkforward.train_window}/{result.walkforward.test_window}",
            "tone": "neutral",
            "detail": f"step {result.walkforward.step}",
        },
    ]
    for selected_horizon_key, row in (
        ("t_plus_5", t_plus_5_row),
        ("t_plus_10", t_plus_10_row),
    ):
        if row is None:
            continue
        highlights.append(
            {
                "label": selected_horizon_key,
                "value": _format_return(_as_float(row["q1_minus_q10_mean"])),
                "tone": _highlight_tone_for_spread(
                    _as_float(row["q1_minus_q10_mean"])
                ),
                "detail": "overall spread",
            }
        )

    return {
        "title": "TOPIX100 SMA Ratio LightGBM",
        "tags": ["TOPIX100", "sma-ratio", "lightgbm", "walk-forward"],
        "purpose": (
            "Check whether the existing TOPIX100 SMA-ratio feature family still "
            "produces usable cross-sectional ranking once the hand-crafted composite "
            "is replaced by a leak-free LightGBM ranker."
        ),
        "method": [
            "Build the same TOPIX100 SMA-ratio event panel used by the baseline study.",
            "Inside each walk-forward split, rebuild the baseline composite from train-only rows and fit LightGBM on the same train block.",
            "Evaluate the out-of-sample Q1/Q10 spread on t_plus_1, t_plus_5, and t_plus_10, and gate the study on t_plus_5 and t_plus_10.",
        ],
        "resultHeadline": headline,
        "resultBullets": result_bullets,
        "considerations": [
            "This remains a research bundle and does not include fees, slippage, or capacity constraints.",
            "The notebook linked from the canonical note stays viewer-only for the baseline bundle; LightGBM reproduction runs from the dedicated runner.",
        ],
        "selectedParameters": [
            {
                "label": "Feature columns",
                "value": ", ".join(result.feature_columns),
            },
            {
                "label": "Train/Test/Step",
                "value": (
                    f"{result.walkforward.train_window}/"
                    f"{result.walkforward.test_window}/"
                    f"{result.walkforward.step}"
                ),
            },
            {
                "label": "Diagnostic fixed split",
                "value": "included" if result.diagnostic is not None else "skipped",
            },
            {
                "label": "Gate horizons",
                "value": "t_plus_5, t_plus_10",
            },
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "walkforward_comparison_summary_df",
                "label": "Overall OOS comparison",
                "description": "Baseline and LightGBM Q1/Q10 spread by horizon on the combined walk-forward out-of-sample rows.",
            },
            {
                "name": "walkforward_exploratory_gate_df",
                "label": "Gate status",
                "description": "Overall spread, median split spread, and positive split share for the gate horizons.",
            },
            {
                "name": "walkforward_feature_importance_df",
                "label": "Average feature importance",
                "description": "Mean LightGBM gain importance across valid walk-forward splits.",
            },
            {
                "name": "walkforward_split_spread_df",
                "label": "Per-split spread",
                "description": "Split-level baseline versus LightGBM OOS spread diagnostics.",
            },
            {
                "name": "diagnostic_comparison_summary_df",
                "label": "Fixed-split diagnostic",
                "description": "Discovery / validation comparison used as a secondary reference when included.",
            },
        ],
    }
