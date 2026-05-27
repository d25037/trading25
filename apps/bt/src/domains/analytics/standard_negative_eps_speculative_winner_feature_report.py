"""Report builders for standard negative-EPS speculative winner research."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd


TARGET_COHORT_KEYS: tuple[str, ...] = (
    "forecast_positive__cfo_positive",
    "forecast_missing__cfo_non_positive",
)


def fmt_num(value: float | int | None, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    if isinstance(value, int):
        return str(value)
    return f"{value:.{digits}f}"


def top_summary_rows(
    summary_df: pd.DataFrame,
    *,
    cohort_key: str,
    limit: int = 5,
) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df.copy()
    return (
        summary_df[summary_df["cohort_key"].astype(str) == cohort_key]
        .sort_values("rank_within_cohort", kind="stable")
        .head(limit)
        .reset_index(drop=True)
    )


def build_summary_markdown(result: Any) -> str:
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
            f"- Winner label: top `{fmt_num((1.0 - result.winner_quantile) * 100.0, 0)}`% "
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
            f"winner cutoff `{fmt_num(row['winner_cutoff_pct'])}%`, "
            f"winner base rate `{fmt_num(row['winner_base_rate'])}%`"
        )

    lines.extend(["", "## Strongest Two-Feature Combinations", ""])
    if result.pair_combo_summary_df.empty:
        lines.append("- No pair cells cleared the minimum event-count threshold.")
    else:
        for cohort_key in TARGET_COHORT_KEYS:
            lines.append(f"- `{cohort_key}`:")
            cohort_pairs = top_summary_rows(
                result.pair_combo_summary_df,
                cohort_key=cohort_key,
            )
            if cohort_pairs.empty:
                lines.append("  - No qualifying pair cells.")
                continue
            for row in cohort_pairs.to_dict(orient="records"):
                lines.append(
                    "  - "
                    f"{row['combo_label']}: events `{int(row['event_count'])}`, "
                    f"winner hit `{fmt_num(row['winner_hit_rate'])}%`, "
                    f"lift `{fmt_num(row['lift_vs_base_rate'], 2)}`, "
                    f"mean `{fmt_num(row['mean_return_pct'])}%`, "
                    f"median `{fmt_num(row['median_return_pct'])}%`"
                )

    lines.extend(["", "## Strongest Three-Feature Extensions", ""])
    if result.triplet_combo_summary_df.empty:
        lines.append("- No triplet cells cleared the pair-gated expansion rules.")
    else:
        for cohort_key in TARGET_COHORT_KEYS:
            lines.append(f"- `{cohort_key}`:")
            cohort_triplets = top_summary_rows(
                result.triplet_combo_summary_df,
                cohort_key=cohort_key,
            )
            if cohort_triplets.empty:
                lines.append("  - No qualifying triplet cells.")
                continue
            for row in cohort_triplets.to_dict(orient="records"):
                lines.append(
                    "  - "
                    f"{row['combo_label']}: events `{int(row['event_count'])}`, "
                    f"winner hit `{fmt_num(row['winner_hit_rate'])}%`, "
                    f"lift `{fmt_num(row['lift_vs_base_rate'], 2)}`"
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
                    f"turnaround lift `{fmt_num(row['forecast_positive_lift_vs_base_rate'], 2)}`, "
                    f"missing-forecast lift `{fmt_num(row['forecast_missing_lift_vs_base_rate'], 2)}`"
                )
        for strength_class in ("forecast_positive_only", "forecast_missing_only"):
            subset = result.group_comparison_df[
                result.group_comparison_df["strength_class"].astype(str)
                == strength_class
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


def build_published_summary(result: Any) -> dict[str, Any]:
    return {
        "selectedMarket": result.selected_market,
        "scopeName": result.scope_name,
        "winnerQuantile": result.winner_quantile,
        "minEventCount": result.min_event_count,
        "minWinnerCount": result.min_winner_count,
        "winnerThresholds": result.winner_threshold_df.to_dict(orient="records"),
        "topPairsByCohort": {
            cohort_key: top_summary_rows(
                result.pair_combo_summary_df,
                cohort_key=cohort_key,
                limit=10,
            ).to_dict(orient="records")
            for cohort_key in TARGET_COHORT_KEYS
        },
        "topTripletsByCohort": {
            cohort_key: top_summary_rows(
                result.triplet_combo_summary_df,
                cohort_key=cohort_key,
                limit=10,
            ).to_dict(orient="records")
            for cohort_key in TARGET_COHORT_KEYS
        },
        "comparisonHighlights": result.group_comparison_df.head(20).to_dict(
            orient="records"
        ),
    }
