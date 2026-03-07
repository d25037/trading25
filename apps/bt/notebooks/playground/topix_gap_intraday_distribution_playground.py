# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "pandas>=2.0.0",
#     "matplotlib>=3.0.0",
# ]
# ///

"""
TOPIX Gap / Intraday Distribution Playground (Marimo)

UI playground for DuckDB-backed analytics.
Computation logic must stay in src/domains, this notebook provides UI only.
"""

from __future__ import annotations

import marimo

app = marimo.App(width="full", app_title="TOPIX Gap / Intraday Distribution Playground")


@app.cell
def imports():
    import marimo as mo
    import sys
    from pathlib import Path
    import matplotlib.pyplot as plt
    import pandas as pd

    return mo, sys, Path, plt, pd


@app.cell
def bootstrap_project_root(sys, Path):
    project_root = Path.cwd()
    if project_root.name == "playground":
        project_root = project_root.parent.parent
    elif project_root.name == "notebooks":
        project_root = project_root.parent

    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from src.shared.config.settings import get_settings
    from src.domains.analytics.topix_gap_intraday_distribution import (
        STOCK_GROUP_ORDER,
        get_topix_available_date_range,
        run_topix_gap_intraday_distribution,
    )

    settings = get_settings()
    default_db_path = settings.market_db_path
    return (
        STOCK_GROUP_ORDER,
        default_db_path,
        get_topix_available_date_range,
        run_topix_gap_intraday_distribution,
    )


@app.cell
def initial_range(default_db_path, get_topix_available_date_range):
    try:
        available_start_date, available_end_date = get_topix_available_date_range(default_db_path)
    except Exception:
        available_start_date, available_end_date = None, None
    return available_start_date, available_end_date


@app.cell
def controls(mo, STOCK_GROUP_ORDER, default_db_path, initial_range):
    available_start_date, available_end_date = initial_range
    db_path = mo.ui.text(value=default_db_path, label="DuckDB Path")
    start_date = mo.ui.text(
        value=available_start_date or "",
        label="Start Date (YYYY-MM-DD)",
    )
    end_date = mo.ui.text(
        value=available_end_date or "",
        label="End Date (YYYY-MM-DD)",
    )
    selected_groups = mo.ui.text(
        value=", ".join(STOCK_GROUP_ORDER),
        label="Groups (comma separated)",
    )
    gap_threshold_1 = mo.ui.number(
        value=1.0,
        start=0.1,
        stop=10.0,
        step=0.1,
        label="Gap Threshold 1 (%)",
    )
    gap_threshold_2 = mo.ui.number(
        value=2.0,
        start=0.2,
        stop=20.0,
        step=0.1,
        label="Gap Threshold 2 (%)",
    )
    sample_size = mo.ui.number(
        value=1500,
        start=0,
        step=100,
        label="Sample Size Per Group/Bucket",
    )
    clip_lower = mo.ui.number(
        value=1.0,
        start=0.0,
        stop=49.0,
        step=0.5,
        label="Clip Lower Percentile",
    )
    clip_upper = mo.ui.number(
        value=99.0,
        start=51.0,
        stop=100.0,
        step=0.5,
        label="Clip Upper Percentile",
    )

    mo.vstack(
        [
            db_path,
            mo.hstack([start_date, end_date]),
            selected_groups,
            mo.hstack([gap_threshold_1, gap_threshold_2]),
            mo.hstack([sample_size, clip_lower, clip_upper]),
        ]
    )
    return (
        clip_lower,
        clip_upper,
        db_path,
        end_date,
        gap_threshold_1,
        gap_threshold_2,
        sample_size,
        selected_groups,
        start_date,
    )


@app.cell
def parsed_inputs(
    STOCK_GROUP_ORDER,
    clip_lower,
    clip_upper,
    db_path,
    end_date,
    gap_threshold_1,
    gap_threshold_2,
    sample_size,
    selected_groups,
    start_date,
):
    requested_groups = [
        value.strip()
        for value in selected_groups.value.split(",")
        if value.strip()
    ]
    if not requested_groups:
        requested_groups = list(STOCK_GROUP_ORDER)

    selected_start = start_date.value.strip() or None
    selected_end = end_date.value.strip() or None
    selected_db_path = db_path.value.strip()
    selected_sample_size = int(sample_size.value)
    selected_clip = (float(clip_lower.value), float(clip_upper.value))
    thresholds = (
        float(gap_threshold_1.value) / 100.0,
        float(gap_threshold_2.value) / 100.0,
    )
    return (
        requested_groups,
        selected_clip,
        selected_db_path,
        selected_end,
        selected_sample_size,
        selected_start,
        thresholds,
    )


@app.cell
def analysis_result(
    mo,
    parsed_inputs,
    run_topix_gap_intraday_distribution,
):
    (
        requested_groups,
        selected_clip,
        selected_db_path,
        selected_end,
        selected_sample_size,
        selected_start,
        thresholds,
    ) = parsed_inputs
    try:
        result = run_topix_gap_intraday_distribution(
            selected_db_path,
            start_date=selected_start,
            end_date=selected_end,
            gap_threshold_1=thresholds[0],
            gap_threshold_2=thresholds[1],
            selected_groups=requested_groups,
            sample_size=selected_sample_size,
            clip_percentiles=selected_clip,
        )
        error_message = None
    except Exception as exc:
        result = None
        error_message = str(exc)
    return error_message, result


@app.cell
def render_error(mo, error_message):
    if not error_message:
        return
    mo.md(f"## Input Error\n\n`{error_message}`")


@app.cell
def render_summary(mo, error_message, parsed_inputs, result):
    if error_message or result is None:
        return

    requested_groups, selected_clip, _, _, selected_sample_size, _, thresholds = parsed_inputs
    mo.md(
        f"""
## TOPIX Gap / Intraday Distribution Playground

- Source mode: **{result.source_mode}**
- Source detail: **{result.source_detail}**
- Available range: **{result.available_start_date} → {result.available_end_date}**
- Analysis range: **{result.analysis_start_date} → {result.analysis_end_date}**
- Selected groups: **{", ".join(requested_groups)}**
- Thresholds: **{thresholds[0] * 100:.1f}% / {thresholds[1] * 100:.1f}%**
- Sample size per group/bucket: **{selected_sample_size}**
- Plot clip: **{selected_clip[0]:.1f}% → {selected_clip[1]:.1f}%**
- Excluded TOPIX days without previous close: **{result.excluded_topix_days_without_prev_close}**
"""
    )


@app.cell
def render_day_counts_chart(plt, error_message, result):
    if error_message or result is None:
        return

    fig, ax = plt.subplots(figsize=(9, 4))
    day_counts = result.day_counts_df
    ax.bar(day_counts["gap_bucket_label"], day_counts["day_count"], color=["#8fb996", "#f4a259", "#bc4b51"])
    ax.set_title("TOPIX Gap Day Counts")
    ax.set_ylabel("Days")
    ax.grid(axis="y", alpha=0.2)
    plt.xticks(rotation=15, ha="right")
    fig.tight_layout()
    fig


@app.cell
def render_direction_chart(plt, error_message, result):
    if error_message or result is None:
        return

    summary_df = result.summary_df.copy()
    summary_df["label"] = summary_df["stock_group"] + "\n" + summary_df["gap_bucket_label"]

    fig, ax = plt.subplots(figsize=(14, 7))
    x = range(len(summary_df))
    up = summary_df["up_ratio"]
    down = summary_df["down_ratio"]
    flat = summary_df["flat_ratio"]

    ax.bar(x, up, label="Up", color="#2a9d8f")
    ax.bar(x, down, bottom=up, label="Down", color="#e76f51")
    ax.bar(x, flat, bottom=up + down, label="Flat", color="#7a7a7a")
    ax.set_ylim(0.0, 1.0)
    ax.set_ylabel("Ratio")
    ax.set_title("Up / Down / Flat Ratios by Group and Gap Bucket")
    ax.set_xticks(list(x))
    ax.set_xticklabels(summary_df["label"], rotation=35, ha="right")
    ax.legend()
    ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    fig


@app.cell
def render_distribution_chart(plt, error_message, result):
    if error_message or result is None:
        return

    plot_df = result.clipped_samples_df
    bucket_order = list(result.day_counts_df["gap_bucket_key"])
    bucket_labels = {
        row["gap_bucket_key"]: row["gap_bucket_label"]
        for _, row in result.day_counts_df.iterrows()
    }

    fig, axes = plt.subplots(len(bucket_order), 1, figsize=(12, 4 * len(bucket_order)), sharex=False)
    if len(bucket_order) == 1:
        axes = [axes]

    for ax, bucket_key in zip(axes, bucket_order, strict=True):
        bucket_df = plot_df[plot_df["gap_bucket_key"] == bucket_key]
        group_order = [
            group
            for group in result.selected_groups
            if not bucket_df[bucket_df["stock_group"] == group].empty
        ]
        boxplot_data = [
            bucket_df.loc[bucket_df["stock_group"] == group, "intraday_diff"].tolist()
            for group in group_order
        ]

        if any(boxplot_data):
            ax.boxplot(boxplot_data, labels=group_order, patch_artist=True)
        else:
            ax.text(0.5, 0.5, "No sampled rows in this bucket", ha="center", va="center")
            ax.set_xticks([])

        ax.set_title(bucket_labels[bucket_key])
        ax.set_ylabel("close - open")
        ax.grid(axis="y", alpha=0.2)

    fig.suptitle("Sampled Distribution of close - open (Clipped for Plotting)", y=1.02)
    fig.tight_layout()
    fig


@app.cell
def render_tables(mo, error_message, result):
    if error_message or result is None:
        return

    summary_columns = [
        "stock_group",
        "gap_bucket_label",
        "sample_count",
        "up_count",
        "down_count",
        "flat_count",
        "up_ratio",
        "down_ratio",
        "flat_ratio",
        "mean_intraday_diff",
        "median_intraday_diff",
        "p05_intraday_diff",
        "p25_intraday_diff",
        "p50_intraday_diff",
        "p75_intraday_diff",
        "p95_intraday_diff",
    ]
    sample_columns = [
        "stock_group",
        "gap_bucket_label",
        "date",
        "code",
        "intraday_diff",
        "direction",
        "sample_rank",
    ]
    mo.vstack(
        [
            mo.md("### Bucket Day Counts"),
            mo.Html(result.day_counts_df.to_html(index=False)),
            mo.md("### Exact Summary"),
            mo.Html(result.summary_df[summary_columns].to_html(index=False, float_format=lambda x: f"{x:.4f}")),
            mo.md("### Sampled Rows (first 100)"),
            mo.Html(result.samples_df[sample_columns].head(100).to_html(index=False, float_format=lambda x: f"{x:.4f}")),
        ]
    )


if __name__ == "__main__":
    app.run()
