"""Report builders for earnings hold-through expectancy research."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

import numpy as np
import pandas as pd

_MARKET_SCOPE_ORDER: tuple[str, ...] = (
    "all",
    "prime",
    "standard",
    "growth",
    "unknown",
)


def build_summary_markdown(result: Any) -> str:
    diagnostics = _top_rows_for_markdown(result.coverage_diagnostics_df, limit=16)
    precondition = _top_rows_for_markdown(
        result.precondition_outcome_df,
        sort_columns=[
            "market_scope",
            "horizon",
            "is_fy",
            "pre_return_60d_bucket",
            "adv60_to_free_float_bucket",
        ],
        limit=30,
    )
    bucket = _top_rows_for_markdown(
        result.bucket_expectancy_df,
        sort_columns=[
            "market_scope",
            "horizon",
            "is_fy",
            "has_next_guidance",
            "event_strength",
            "pre_return_60d_bucket",
        ],
        limit=30,
    )
    liquidity = _top_rows_for_markdown(
        result.liquidity_interaction_df,
        sort_columns=["market_scope", "horizon", "liquidity_regime", "event_strength"],
        limit=30,
    )
    signed = _top_rows_for_markdown(
        result.signed_premove_df,
        sort_columns=["market_scope", "horizon", "event_strength", "signed_pre_move"],
        limit=24,
    )
    holdthrough = _top_rows_for_markdown(
        result.holdthrough_return_df,
        sort_columns=["market_scope", "horizon", "is_fy", "has_next_guidance"],
        limit=24,
    )
    return "\n".join(
        [
            "# Earnings Hold-Through Expectancy",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Pre windows: `{list(result.pre_windows)}`",
            f"- Forward horizons: `{list(result.horizons)}`",
            f"- Liquidity window: `{result.liquidity_window}`",
            "",
            "## Coverage Diagnostics",
            "",
            diagnostics,
            "",
            "## Precondition Outcome",
            "",
            precondition,
            "",
            "## Bucket Expectancy",
            "",
            bucket,
            "",
            "## Liquidity Interaction",
            "",
            liquidity,
            "",
            "## Signed Pre-Move",
            "",
            signed,
            "",
            "## Hold-Through Returns",
            "",
            holdthrough,
            "",
        ]
    )


def _top_rows_for_markdown(
    frame: pd.DataFrame,
    *,
    sort_columns: Sequence[str] | None = None,
    limit: int,
) -> str:
    if frame.empty:
        return "_No rows._"
    display = frame.copy()
    if sort_columns:
        existing = [column for column in sort_columns if column in display.columns]
        if existing:
            display = display.sort_values(existing, kind="stable")
    display = display.head(limit).copy()
    for column in display.columns:
        if pd.api.types.is_float_dtype(display[column]):
            display[column] = display[column].map(
                lambda value: round(float(value), 4) if pd.notna(value) else value
            )
    return _frame_to_markdown(display)


def _frame_to_markdown(frame: pd.DataFrame) -> str:
    headers = [str(column) for column in frame.columns]
    rows = [
        [_format_markdown_cell(value) for value in row]
        for row in frame.itertuples(index=False, name=None)
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def _format_markdown_cell(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(cast(Any, value)):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value).replace("|", "\\|")


def sort_summary_df(pd_frame: pd.DataFrame, *, columns: Sequence[str]) -> pd.DataFrame:
    if pd_frame.empty:
        return pd.DataFrame(columns=list(columns))
    for column in columns:
        if column not in pd_frame.columns:
            pd_frame[column] = np.nan
    frame = pd_frame[list(columns)].copy()
    sort_columns = [
        column for column in ("market_scope", "horizon") if column in frame.columns
    ]
    if sort_columns:
        frame["_market_order"] = frame["market_scope"].map(
            {scope: idx for idx, scope in enumerate(_MARKET_SCOPE_ORDER)}
        ).fillna(len(_MARKET_SCOPE_ORDER))
        sort_by = [
            "_market_order",
            *[column for column in sort_columns if column != "market_scope"],
        ]
        frame = frame.sort_values(sort_by, kind="stable").drop(columns=["_market_order"])
    return frame.reset_index(drop=True)
