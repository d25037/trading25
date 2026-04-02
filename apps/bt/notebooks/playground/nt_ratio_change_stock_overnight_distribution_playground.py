# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "pandas>=2.0.0",
#     "matplotlib>=3.0.0",
# ]
# ///

import marimo

__generated_with = "0.21.1"
app = marimo.App(
    width="full",
    app_title="NT Ratio Change / Stock Overnight Distribution Playground",
)


@app.cell
def _():
    import marimo as mo
    import matplotlib.pyplot as plt
    import pandas as pd
    import sys
    from pathlib import Path

    return Path, mo, pd, plt, sys


@app.cell
def _(Path, sys):
    from src.shared.research_notebook_viewer import (
        build_bundle_viewer_controls,
        ensure_bt_project_root_on_path,
        get_latest_bundle_defaults,
        load_bundle_selection,
        resolve_selected_bundle_path,
    )

    _project_root = ensure_bt_project_root_on_path(Path.cwd(), sys.path)
    from src.domains.analytics.research_bundle import load_research_bundle_info
    from src.domains.analytics.nt_ratio_change_stock_overnight_distribution import (
        NT_RATIO_CHANGE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
        STOCK_GROUP_ORDER,
        get_nt_ratio_change_stock_overnight_distribution_bundle_path_for_run_id,
        get_nt_ratio_change_stock_overnight_distribution_latest_bundle_path,
        load_nt_ratio_change_stock_overnight_distribution_research_bundle,
    )

    return (
        NT_RATIO_CHANGE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
        STOCK_GROUP_ORDER,
        build_bundle_viewer_controls,
        get_nt_ratio_change_stock_overnight_distribution_bundle_path_for_run_id,
        get_latest_bundle_defaults,
        get_nt_ratio_change_stock_overnight_distribution_latest_bundle_path,
        load_research_bundle_info,
        load_bundle_selection,
        load_nt_ratio_change_stock_overnight_distribution_research_bundle,
        _project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_nt_ratio_change_stock_overnight_distribution_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_nt_ratio_change_stock_overnight_distribution_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_nt_ratio_change_stock_overnight_distribution.py",
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_nt_ratio_change_stock_overnight_distribution_bundle_path_for_run_id,
    run_id,
    resolve_selected_bundle_path,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_nt_ratio_change_stock_overnight_distribution_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_nt_ratio_change_stock_overnight_distribution_research_bundle,
    load_research_bundle_info,
    load_bundle_selection,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_nt_ratio_change_stock_overnight_distribution_research_bundle,
        )
        error_message = None
    except Exception as exc:
        bundle_info = None
        result = None
        error_message = str(exc)
    return bundle_info, error_message, result


@app.cell
def _(error_message, mo):
    _error_view = mo.md("")
    if error_message:
        _error_view = mo.md(f"## Input Error\n\n`{error_message}`")
    _error_view
    return


@app.cell
def _(
    NT_RATIO_CHANGE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
    bundle_info,
    error_message,
    mo,
    result,
):
    _summary_view = mo.md("")
    if not error_message and result is not None:
        _stats = result.nt_ratio_stats
        _stats_lines = [
            "- NT ratio return stats: **no analyzable rows in range**",
        ]
        if _stats is not None:
            _stats_lines = [
                f"- NT ratio return sample count: **{_stats.sample_count}**",
                f"- Mean / Std: **{_stats.mean_return * 100:.4f}% / {_stats.std_return * 100:.4f}%**",
                f"- Bucket boundaries: **μ-{result.sigma_threshold_2:g}σ = {_stats.lower_threshold_2 * 100:.4f}%**, **μ-{result.sigma_threshold_1:g}σ = {_stats.lower_threshold_1 * 100:.4f}%**, **μ+{result.sigma_threshold_1:g}σ = {_stats.upper_threshold_1 * 100:.4f}%**, **μ+{result.sigma_threshold_2:g}σ = {_stats.upper_threshold_2 * 100:.4f}%**",
                f"- Min / Q25 / Median / Q75 / Max: **{_stats.min_return * 100:.4f}% / {_stats.q25_return * 100:.4f}% / {_stats.median_return * 100:.4f}% / {_stats.q75_return * 100:.4f}% / {_stats.max_return * 100:.4f}%**",
            ]

        _bundle_lines = []
        if bundle_info is not None:
            _bundle_lines = [
                f"- Experiment: **{NT_RATIO_CHANGE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID}**",
                f"- Bundle run: **{bundle_info.run_id}**",
            ]
        _summary_view = mo.md(
            "\n".join(
                [
                    "## NT Ratio Change / Stock Overnight Distribution Playground",
                    "",
                    f"- Source mode: **{result.source_mode}**",
                    f"- Source detail: **{result.source_detail}**",
                    f"- Available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Selected groups: **{', '.join(result.selected_groups)}**",
                    f"- Sigma thresholds: **{result.sigma_threshold_1:g}σ / {result.sigma_threshold_2:g}σ**",
                    f"- Sample size per group/bucket: **{result.sample_size}**",
                    f"- Plot clip: **{result.clip_percentiles[0]:.1f}% -> {result.clip_percentiles[1]:.1f}%**",
                    f"- Excluded NT ratio days without previous ratio: **{result.excluded_nt_ratio_days_without_prev_ratio}**",
                    f"- Excluded NT ratio days without next session: **{result.excluded_nt_ratio_days_without_next_session}**",
                    "- Event definition: **nt_ratio = N225_UNDERPX close / TOPIX close**",
                    "- Event definition: **nt_ratio_return = (nt_ratio - prev_nt_ratio) / prev_nt_ratio**",
                    "- Trade definition: **stock overnight_return = (next_open - event_close) / event_close**",
                    "",
                    *_bundle_lines,
                    *([""] if _bundle_lines else []),
                    *_stats_lines,
                ]
            )
        )
    _summary_view
    return


@app.cell
def _(error_message, mo, pd, result):
    _stats_table = mo.md("")
    if not error_message and result is not None and result.nt_ratio_stats is not None:
        _stats = result.nt_ratio_stats
        _stats_df = pd.DataFrame(
            [
                {
                    "sample_count": _stats.sample_count,
                    "mean_return": _stats.mean_return,
                    "std_return": _stats.std_return,
                    "lower_threshold_2": _stats.lower_threshold_2,
                    "lower_threshold_1": _stats.lower_threshold_1,
                    "upper_threshold_1": _stats.upper_threshold_1,
                    "upper_threshold_2": _stats.upper_threshold_2,
                    "min_return": _stats.min_return,
                    "q25_return": _stats.q25_return,
                    "median_return": _stats.median_return,
                    "q75_return": _stats.q75_return,
                    "max_return": _stats.max_return,
                }
            ]
        )
        _stats_table = mo.vstack(
            [
                mo.md("### NT Ratio Return Stats"),
                mo.Html(
                    _stats_df.to_html(
                        index=False,
                        float_format=lambda value: f"{value:.6f}",
                    )
                ),
            ]
        )
    _stats_table
    return


@app.cell
def _(error_message, mo, plt, result):
    _day_counts_chart = mo.md("")
    if not error_message and result is not None:
        _fig, _ax = plt.subplots(figsize=(9, 4))
        _day_counts = result.day_counts_df
        _bar_colors = {
            "return_le_mean_minus_2sd": "#bc4b51",
            "return_mean_minus_2sd_to_minus_1sd": "#e07a5f",
            "return_mean_minus_1sd_to_plus_1sd": "#9aa5b1",
            "return_mean_plus_1sd_to_plus_2sd": "#81b29a",
            "return_ge_mean_plus_2sd": "#2a9d8f",
        }
        _ax.bar(
            _day_counts["nt_ratio_bucket_label"],
            _day_counts["day_count"],
            color=[
                _bar_colors.get(bucket_key, "#7a7a7a")
                for bucket_key in _day_counts["nt_ratio_bucket_key"]
            ],
        )
        _ax.set_title("NT Ratio Return Event Day Counts")
        _ax.set_ylabel("Days")
        _ax.grid(axis="y", alpha=0.2)
        plt.xticks(rotation=15, ha="right")
        _fig.tight_layout()
        _day_counts_chart = _fig
    _day_counts_chart
    return


@app.cell
def _(error_message, mo, plt, result):
    _direction_chart = mo.md("")
    if not error_message and result is not None:
        _summary_df = result.summary_df.copy()
        _summary_df["label"] = (
            _summary_df["stock_group"] + "\n" + _summary_df["nt_ratio_bucket_label"]
        )

        _fig, _ax = plt.subplots(figsize=(14, 7))
        _x = range(len(_summary_df))
        _up = _summary_df["up_ratio"]
        _down = _summary_df["down_ratio"]
        _flat = _summary_df["flat_ratio"]

        _ax.bar(_x, _up, label="Up", color="#2a9d8f")
        _ax.bar(_x, _down, bottom=_up, label="Down", color="#e76f51")
        _ax.bar(_x, _flat, bottom=_up + _down, label="Flat", color="#7a7a7a")
        _ax.set_ylim(0.0, 1.0)
        _ax.set_ylabel("Ratio")
        _ax.set_title("Stock Overnight Up / Down / Flat Ratios by Group and NT Ratio Bucket")
        _ax.set_xticks(list(_x))
        _ax.set_xticklabels(_summary_df["label"], rotation=35, ha="right")
        _ax.legend()
        _ax.grid(axis="y", alpha=0.2)
        _fig.tight_layout()
        _direction_chart = _fig
    _direction_chart
    return


@app.cell
def _(error_message, mo, plt, result):
    _expected_return_chart = mo.md("")
    if not error_message and result is not None:
        _summary_df = result.summary_df[result.summary_df["sample_count"] > 0].copy()
        if _summary_df.empty:
            _expected_return_chart = mo.md("No sampled rows for expected-return chart.")
        else:
            _summary_df["label"] = (
                _summary_df["stock_group"] + "\n" + _summary_df["nt_ratio_bucket_label"]
            )
            _summary_df["mean_overnight_return_pct"] = (
                _summary_df["mean_overnight_return"] * 100.0
            )

            _fig, _ax = plt.subplots(figsize=(14, 5))
            _x = range(len(_summary_df))
            _values = _summary_df["mean_overnight_return_pct"].fillna(0.0)
            _colors = [
                "#7a7a7a"
                if raw_value != raw_value
                else ("#2a9d8f" if raw_value >= 0 else "#e76f51")
                for raw_value in _summary_df["mean_overnight_return"]
            ]
            _ax.bar(_x, _values, color=_colors)
            _ax.axhline(0.0, color="#444444", linewidth=1.0, alpha=0.7)
            _ax.set_ylabel("Mean overnight return (%)")
            _ax.set_title("Expected Stock Overnight Return by Group and NT Ratio Bucket")
            _ax.set_xticks(list(_x))
            _ax.set_xticklabels(_summary_df["label"], rotation=35, ha="right")
            _ax.grid(axis="y", alpha=0.2)
            _fig.tight_layout()
            _expected_return_chart = _fig
    _expected_return_chart
    return


@app.cell
def _(error_message, mo, plt, result):
    _distribution_chart = mo.md("")
    if not error_message and result is not None:
        _plot_df = result.clipped_samples_df
        _bucket_order = list(result.day_counts_df["nt_ratio_bucket_key"])
        _bucket_labels = {
            row["nt_ratio_bucket_key"]: row["nt_ratio_bucket_label"]
            for _, row in result.day_counts_df.iterrows()
        }

        _fig, _axes = plt.subplots(
            len(_bucket_order),
            1,
            figsize=(12, 4 * len(_bucket_order)),
            sharex=False,
        )
        if len(_bucket_order) == 1:
            _axes = [_axes]

        for _ax, _bucket_key in zip(_axes, _bucket_order, strict=True):
            _bucket_df = _plot_df[_plot_df["nt_ratio_bucket_key"] == _bucket_key]
            _group_order = [
                group
                for group in result.selected_groups
                if not _bucket_df[_bucket_df["stock_group"] == group].empty
            ]
            _boxplot_data = [
                _bucket_df.loc[
                    _bucket_df["stock_group"] == group, "overnight_diff"
                ].tolist()
                for group in _group_order
            ]

            if any(_boxplot_data):
                _ax.boxplot(_boxplot_data, labels=_group_order, patch_artist=True)
            else:
                _ax.text(
                    0.5,
                    0.5,
                    "No sampled rows in this bucket",
                    ha="center",
                    va="center",
                )
                _ax.set_xticks([])

            _ax.set_title(_bucket_labels[_bucket_key])
            _ax.set_ylabel("next_open - close")
            _ax.grid(axis="y", alpha=0.2)

        _fig.suptitle(
            "Sampled Distribution of Stock Overnight Diff (Clipped for Plotting)",
            y=1.02,
        )
        _fig.tight_layout()
        _distribution_chart = _fig
    _distribution_chart
    return


@app.cell
def _(error_message, mo, result):
    _table_view = mo.md("")
    if not error_message and result is not None:
        _summary_columns = [
            "stock_group",
            "nt_ratio_bucket_label",
            "sample_count",
            "up_count",
            "down_count",
            "flat_count",
            "up_ratio",
            "down_ratio",
            "flat_ratio",
            "mean_nt_ratio_return",
            "mean_overnight_return",
            "mean_overnight_diff",
            "median_overnight_diff",
            "p05_overnight_diff",
            "p25_overnight_diff",
            "p50_overnight_diff",
            "p75_overnight_diff",
            "p95_overnight_diff",
        ]
        _sample_columns = [
            "stock_group",
            "nt_ratio_bucket_label",
            "date",
            "next_date",
            "code",
            "nt_ratio_return",
            "overnight_diff",
            "overnight_return",
            "direction",
            "sample_rank",
        ]
        _daily_columns = [
            "stock_group",
            "date",
            "next_date",
            "nt_ratio_bucket_label",
            "nt_ratio_return",
            "day_mean_overnight_return",
            "day_up_ratio",
            "day_down_ratio",
            "constituent_count",
        ]
        _table_view = mo.vstack(
            [
                mo.md("### Bucket Day Counts"),
                mo.Html(result.day_counts_df.to_html(index=False)),
                mo.md(
                    "### Exact Summary\n\n"
                    "`mean_overnight_return = average((next_open-event_close)/event_close)`"
                ),
                mo.Html(
                    result.summary_df[_summary_columns].to_html(
                        index=False,
                        float_format=lambda value: f"{value:.4f}",
                    )
                ),
                mo.md("### Sampled Rows (first 100)"),
                mo.Html(
                    result.samples_df[_sample_columns].head(100).to_html(
                        index=False,
                        float_format=lambda value: f"{value:.4f}",
                    )
                ),
                mo.md("### Daily Group Returns (latest 120)"),
                mo.Html(
                    result.daily_group_returns_df[_daily_columns].tail(120).to_html(
                        index=False,
                        float_format=lambda value: f"{value:.4f}",
                    )
                ),
            ]
        )
    _table_view
    return


if __name__ == "__main__":
    app.run()
