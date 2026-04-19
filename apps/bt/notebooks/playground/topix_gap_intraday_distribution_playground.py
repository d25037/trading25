# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "pandas>=2.0.0",
#     "matplotlib>=3.0.0",
# ]
# ///

import marimo

__generated_with = "0.20.4"
app = marimo.App(
    width="full",
    app_title="TOPIX Gap / Intraday Distribution Playground",
)


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import matplotlib.pyplot as plt
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
    from src.domains.analytics.topix_gap_intraday_distribution import (
        TOPIX_GAP_INTRADAY_RESEARCH_EXPERIMENT_ID,
        get_topix_gap_intraday_distribution_bundle_path_for_run_id,
        get_topix_gap_intraday_distribution_latest_bundle_path,
        load_topix_gap_intraday_distribution_research_bundle,
    )

    return (
        TOPIX_GAP_INTRADAY_RESEARCH_EXPERIMENT_ID,
        build_bundle_viewer_controls,
        get_topix_gap_intraday_distribution_bundle_path_for_run_id,
        get_latest_bundle_defaults,
        get_topix_gap_intraday_distribution_latest_bundle_path,
        load_research_bundle_info,
        load_bundle_selection,
        load_topix_gap_intraday_distribution_research_bundle,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix_gap_intraday_distribution_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix_gap_intraday_distribution_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix_gap_intraday_distribution.py",
        docs_readme_path="apps/bt/docs/experiments/market-behavior/topix-gap-intraday-distribution/README.md",
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix_gap_intraday_distribution_bundle_path_for_run_id,
    run_id,
    resolve_selected_bundle_path,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix_gap_intraday_distribution_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_research_bundle_info,
    load_bundle_selection,
    load_topix_gap_intraday_distribution_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix_gap_intraday_distribution_research_bundle,
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
    TOPIX_GAP_INTRADAY_RESEARCH_EXPERIMENT_ID,
    bundle_info,
    error_message,
    mo,
    result,
):
    _summary_view = mo.md("")
    if not error_message and result is not None:
        _stats = result.gap_return_stats
        _stats_lines = [
            "- TOPIX gap return stats: **no analyzable rows in range**",
        ]
        if _stats is not None:
            _stats_lines = [
                f"- TOPIX gap return sample count: **{_stats.sample_count}**",
                f"- Mean / Std: **{_stats.mean_return * 100:.4f}% / {_stats.std_return * 100:.4f}%**",
                f"- Sigma thresholds: **{_stats.sigma_threshold_1:g}σ / {_stats.sigma_threshold_2:g}σ**",
                f"- Derived gap thresholds: **{_stats.threshold_1 * 100:.4f}% / {_stats.threshold_2 * 100:.4f}%**",
                f"- Min / Q25 / Median / Q75 / Max: **{_stats.min_return * 100:.4f}% / {_stats.q25_return * 100:.4f}% / {_stats.median_return * 100:.4f}% / {_stats.q75_return * 100:.4f}% / {_stats.max_return * 100:.4f}%**",
                f"- Rotation signal thresholds: **weak <= -{_stats.threshold_1 * 100:.4f}% / neutral between / strong >= {_stats.threshold_1 * 100:.4f}%**",
            ]
        _bundle_lines = []
        if bundle_info is not None:
            _bundle_lines = [
                f"- Experiment: **{TOPIX_GAP_INTRADAY_RESEARCH_EXPERIMENT_ID}**",
                f"- Bundle run: **{bundle_info.run_id}**",
            ]
        _summary_view = mo.md(
            "\n".join(
                [
                    "## TOPIX Gap / Intraday Distribution Playground",
                    "",
                    f"- Source mode: **{result.source_mode}**",
                    f"- Source detail: **{result.source_detail}**",
                    f"- Available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Selected groups: **{', '.join(result.selected_groups)}**",
                    f"- Sample size per group/bucket: **{result.sample_size}**",
                    f"- Plot clip: **{result.clip_percentiles[0]:.1f}% -> {result.clip_percentiles[1]:.1f}%**",
                    f"- Excluded TOPIX days without previous close: **{result.excluded_topix_days_without_prev_close}**",
                    "- Fixed rotation rule: **weak => TOPIX500 long / strong => PRIME ex TOPIX500 long / neutral => flat**",
                    "- Event definition: **topix_gap_return = (open - prev_close) / prev_close**",
                    "- Trade definition: **stock_intraday_return = (close - open) / open**",
                    "",
                    *_bundle_lines,
                    *_stats_lines,
                ]
            )
        )
    _summary_view
    return


@app.cell
def _(error_message, mo, pd, result):
    _stats_table = mo.md("")
    if not error_message and result is not None and result.gap_return_stats is not None:
        gap_return_stats = result.gap_return_stats
        _stats_df = pd.DataFrame(
            [
                {
                    "sample_count": gap_return_stats.sample_count,
                    "mean_return": gap_return_stats.mean_return,
                    "std_return": gap_return_stats.std_return,
                    "threshold_1": gap_return_stats.threshold_1,
                    "threshold_2": gap_return_stats.threshold_2,
                    "min_return": gap_return_stats.min_return,
                    "q25_return": gap_return_stats.q25_return,
                    "median_return": gap_return_stats.median_return,
                    "q75_return": gap_return_stats.q75_return,
                    "max_return": gap_return_stats.max_return,
                }
            ]
        )
        _stats_table = mo.vstack(
            [
                mo.md("### TOPIX Gap Return Stats"),
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
            "gap_le_negative_threshold_2": "#bc4b51",
            "gap_negative_threshold_2_to_1": "#e07a5f",
            "gap_negative_threshold_1_to_threshold_1": "#9aa5b1",
            "gap_threshold_1_to_2": "#81b29a",
            "gap_ge_threshold_2": "#2a9d8f",
        }
        _ax.bar(
            _day_counts["gap_bucket_label"],
            _day_counts["day_count"],
            color=[
                _bar_colors.get(bucket_key, "#7a7a7a")
                for bucket_key in _day_counts["gap_bucket_key"]
            ],
        )
        _ax.set_title("TOPIX Gap Day Counts")
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
            _summary_df["stock_group"] + "\n" + _summary_df["gap_bucket_label"]
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
        _ax.set_title("Up / Down / Flat Ratios by Group and Gap Bucket")
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
                _summary_df["stock_group"] + "\n" + _summary_df["gap_bucket_label"]
            )
            _summary_df["mean_intraday_return_pct"] = (
                _summary_df["mean_intraday_return"] * 100.0
            )

            _fig, _ax = plt.subplots(figsize=(14, 5))
            _x = range(len(_summary_df))
            _values = _summary_df["mean_intraday_return_pct"].fillna(0.0)
            _colors = [
                "#7a7a7a"
                if raw_value != raw_value
                else ("#2a9d8f" if raw_value >= 0 else "#e76f51")
                for raw_value in _summary_df["mean_intraday_return"]
            ]
            _ax.bar(_x, _values, color=_colors)
            _ax.axhline(0.0, color="#444444", linewidth=1.0, alpha=0.7)
            _ax.set_ylabel("Mean return (%)")
            _ax.set_title("Expected Intraday Return by Group and Gap Bucket")
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
        _bucket_order = list(result.day_counts_df["gap_bucket_key"])
        _bucket_labels = {
            row["gap_bucket_key"]: row["gap_bucket_label"]
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
            _bucket_df = _plot_df[_plot_df["gap_bucket_key"] == _bucket_key]
            _group_order = [
                group
                for group in result.selected_groups
                if not _bucket_df[_bucket_df["stock_group"] == group].empty
            ]
            _boxplot_data = [
                _bucket_df.loc[
                    _bucket_df["stock_group"] == group, "intraday_diff"
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
            _ax.set_ylabel("close - open")
            _ax.grid(axis="y", alpha=0.2)

        _fig.suptitle(
            "Sampled Distribution of close - open (Clipped for Plotting)",
            y=1.02,
        )
        _fig.tight_layout()
        _distribution_chart = _fig
    _distribution_chart
    return


@app.cell
def _(error_message, mo, result):
    _rotation_summary_view = mo.md("")
    if not error_message and result is not None:
        _overall_df = result.rotation_overall_summary_df
        if _overall_df.empty:
            _rotation_summary_view = mo.md(
                "## Simple Rotation Strategy\n\nNo analyzable TOPIX gap days in range."
            )
        else:
            _row = _overall_df.iloc[0]
            _rotation_summary_view = mo.md(
                f"""
    ## Simple Rotation Strategy

    - Rule: **weak => TOPIX500 long / strong => PRIME ex TOPIX500 long / neutral => flat**
    - Total days: **{int(_row["total_days"])}**
    - Trade days: **{int(_row["trade_days"])}** (weak: **{int(_row["weak_trade_days"])}**, strong: **{int(_row["strong_trade_days"])}**)
    - Flat days: **{int(_row["flat_days"])}**
    - Mean trade return: **{_row["mean_trade_return"] * 100:.3f}%**
    - Mean daily return: **{_row["mean_daily_return"] * 100:.3f}%**
    - Cumulative return: **{_row["cumulative_return"] * 100:.2f}%**
    - Max drawdown: **{_row["max_drawdown"] * 100:.2f}%**
    """
            )
    _rotation_summary_view
    return


@app.cell
def _(error_message, mo, pd, plt, result):
    _rotation_curve_chart = mo.md("")
    if not error_message and result is not None:
        _daily_df = result.rotation_daily_df.copy()
        if _daily_df.empty:
            _rotation_curve_chart = mo.md("No strategy daily rows to plot.")
        else:
            _dates = pd.to_datetime(_daily_df["date"])
            _signal_colors = _daily_df["signal_label"].map(
                {
                    "weak": "#e07a5f",
                    "strong": "#2a9d8f",
                    "neutral": "#9aa5b1",
                }
            )

            _fig, (_ax1, _ax2) = plt.subplots(
                2,
                1,
                figsize=(14, 8),
                sharex=True,
                gridspec_kw={"height_ratios": [2, 1]},
            )
            _ax1.plot(_dates, _daily_df["equity_curve"], color="#1d3557", linewidth=2.0)
            _ax1.set_title("Simple Rotation Strategy Equity Curve")
            _ax1.set_ylabel("Equity")
            _ax1.grid(axis="y", alpha=0.2)

            _ax2.bar(
                _dates,
                _daily_df["strategy_return"] * 100.0,
                color=_signal_colors,
                width=1.2,
            )
            _ax2.axhline(0.0, color="#444444", linewidth=1.0, alpha=0.7)
            _ax2.set_title("Daily Strategy Return")
            _ax2.set_ylabel("Return (%)")
            _ax2.grid(axis="y", alpha=0.2)

            _fig.tight_layout()
            _rotation_curve_chart = _fig
    _rotation_curve_chart
    return


@app.cell
def _(error_message, mo, result):
    _rotation_table_view = mo.md("")
    if not error_message and result is not None:
        if result.rotation_daily_df.empty:
            _rotation_table_view = mo.md("No strategy summary rows to display.")
        else:
            _overall_columns = [
                "strategy_name",
                "total_days",
                "trade_days",
                "flat_days",
                "weak_trade_days",
                "strong_trade_days",
                "missing_trade_days",
                "mean_trade_return",
                "median_trade_return",
                "mean_daily_return",
                "win_trade_ratio",
                "loss_trade_ratio",
                "cumulative_return",
                "final_equity",
                "max_drawdown",
            ]
            _signal_columns = [
                "signal_label",
                "selected_group",
                "position",
                "day_count",
                "mean_strategy_return",
                "median_strategy_return",
                "win_ratio",
                "loss_ratio",
                "cumulative_return",
            ]
            _daily_columns = [
                "date",
                "gap_bucket_label",
                "gap_return",
                "signal_label",
                "selected_group",
                "position",
                "selected_group_constituent_count",
                "selected_group_return",
                "strategy_return",
                "cumulative_return",
            ]
            _rotation_table_view = mo.vstack(
                [
                    mo.md("### Rotation Strategy Summary"),
                    mo.Html(
                        result.rotation_overall_summary_df[_overall_columns].to_html(
                            index=False,
                            float_format=lambda value: f"{value:.4f}",
                        )
                    ),
                    mo.md("### Rotation Strategy by Signal"),
                    mo.Html(
                        result.rotation_signal_summary_df[_signal_columns].to_html(
                            index=False,
                            float_format=lambda value: f"{value:.4f}",
                        )
                    ),
                    mo.md("### Rotation Strategy Daily Rows (latest 120)"),
                    mo.Html(
                        result.rotation_daily_df[_daily_columns].tail(120).to_html(
                            index=False,
                            float_format=lambda value: f"{value:.4f}",
                        )
                    ),
                ]
            )
    _rotation_table_view
    return


@app.cell
def _(error_message, mo, result):
    _table_view = mo.md("")
    if not error_message and result is not None:
        _summary_columns = [
            "stock_group",
            "gap_bucket_label",
            "sample_count",
            "up_count",
            "down_count",
            "flat_count",
            "up_ratio",
            "down_ratio",
            "flat_ratio",
            "mean_intraday_return",
            "mean_intraday_diff",
            "median_intraday_diff",
            "p05_intraday_diff",
            "p25_intraday_diff",
            "p50_intraday_diff",
            "p75_intraday_diff",
            "p95_intraday_diff",
        ]
        _sample_columns = [
            "stock_group",
            "gap_bucket_label",
            "date",
            "code",
            "intraday_diff",
            "intraday_return",
            "direction",
            "sample_rank",
        ]
        _table_view = mo.vstack(
            [
                mo.md("### Bucket Day Counts"),
                mo.Html(result.day_counts_df.to_html(index=False)),
                mo.md(
                    "### Exact Summary\n\n"
                    "`mean_intraday_return = average((close-open)/open)`"
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
            ]
        )
    _table_view
    return
if __name__ == "__main__":
    app.run()
