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
    app_title="TOPIX Extreme Close-to-Close Mode Playground",
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

    project_root = ensure_bt_project_root_on_path(Path.cwd(), sys.path)
    from src.domains.analytics.research_bundle import load_research_bundle_info
    from src.domains.analytics.topix_extreme_close_to_close_mode import (
        build_multi_timeframe_state_tables,
        get_topix_extreme_close_to_close_mode_bundle_path_for_run_id,
        get_topix_extreme_close_to_close_mode_latest_bundle_path,
        load_topix_extreme_close_to_close_mode_research_bundle,
    )

    return (
        build_multi_timeframe_state_tables,
        build_bundle_viewer_controls,
        get_latest_bundle_defaults,
        get_topix_extreme_close_to_close_mode_bundle_path_for_run_id,
        get_topix_extreme_close_to_close_mode_latest_bundle_path,
        load_bundle_selection,
        load_research_bundle_info,
        load_topix_extreme_close_to_close_mode_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix_extreme_close_to_close_mode_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix_extreme_close_to_close_mode_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix_extreme_close_to_close_mode.py",
        extra_note_lines=[
            "- `mode` is decided only by the sign of the largest absolute close-to-close move in the trailing `X` days.",
            "- `selected_window_days` is chosen from the discovery split, then inspected on validation.",
        ],
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix_extreme_close_to_close_mode_bundle_path_for_run_id,
    resolve_selected_bundle_path,
    run_id,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix_extreme_close_to_close_mode_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_bundle_selection,
    load_research_bundle_info,
    load_topix_extreme_close_to_close_mode_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix_extreme_close_to_close_mode_research_bundle,
        )
        error_message = None
    except Exception as exc:
        bundle_info = None
        result = None
        error_message = str(exc)
    return bundle_info, error_message, result


@app.cell
def _(bundle_info, error_message, mo, result):
    summary_view = mo.md("")
    if not error_message and result is not None:
        bundle_lines = []
        if bundle_info is not None:
            bundle_lines = [
                f"- Bundle run: **{bundle_info.run_id}**",
                f"- Bundle created at: **{bundle_info.created_at}**",
                f"- Bundle path: **{bundle_info.bundle_dir}**",
            ]
        summary_view = mo.md(
            "\n".join(
                [
                    "## TOPIX Extreme Close-to-Close Mode",
                    "",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Source mode: **{result.source_mode}**",
                    f"- Candidate windows: **{result.candidate_windows[0]}..{result.candidate_windows[-1]} ({len(result.candidate_windows)} values)**",
                    f"- Future horizons: **{', '.join(str(value) for value in result.future_horizons)}**",
                    f"- Validation ratio: **{result.validation_ratio:.2f}**",
                    f"- Selected overall X: **{result.selected_window_days}**",
                    f"- Selected short X: **{result.selected_short_window_days}**",
                    f"- Selected long X: **{result.selected_long_window_days}**",
                    f"- Selection metric: **{result.selection_metric}**",
                    "",
                    *bundle_lines,
                ]
            )
        )
    if error_message:
        summary_view = mo.md(f"## Error\n\n{error_message}")
    summary_view
    return


@app.cell
def _(error_message, mo, result):
    if error_message or result is None:
        score_split_view = mo.md("")
        detail_split_view = mo.md("")
        window_view = mo.md("")
        short_window_view = mo.md("")
        long_window_view = mo.md("")
    else:
        available_score_splits = result.window_score_df["sample_split"].drop_duplicates().tolist()
        available_detail_splits = result.mode_summary_df["sample_split"].drop_duplicates().tolist()
        default_detail_split = "validation" if "validation" in available_detail_splits else "discovery"
        window_options = {
            str(value): str(value)
            for value in sorted(result.mode_assignments_df["window_days"].unique())
        }
        score_split_view = mo.ui.dropdown(
            options={value: value for value in available_score_splits},
            value="discovery" if "discovery" in available_score_splits else available_score_splits[0],
            label="Score Split",
        )
        detail_split_view = mo.ui.dropdown(
            options={value: value for value in available_detail_splits},
            value=default_detail_split,
            label="Detail Split",
        )
        window_view = mo.ui.dropdown(
            options=window_options,
            value=str(result.selected_window_days),
            label="Window Days",
        )
        short_window_view = mo.ui.dropdown(
            options=window_options,
            value=str(result.selected_short_window_days),
            label="Short X",
        )
        long_window_view = mo.ui.dropdown(
            options=window_options,
            value=str(result.selected_long_window_days),
            label="Long X",
        )
    mo.vstack(
        [
            mo.hstack([score_split_view, detail_split_view, window_view]),
            mo.hstack([short_window_view, long_window_view]),
        ]
    )
    return detail_split_view, long_window_view, score_split_view, short_window_view, window_view


@app.cell
def _(error_message, mo, pd, result, score_split_view):
    score_table = mo.md("")
    if not error_message and result is not None:
        filtered_df = result.window_score_df[
            result.window_score_df["sample_split"] == score_split_view.value
        ].copy()
        filtered_df = filtered_df.sort_values(
            ["selection_score", "composite_score", "window_days"],
            ascending=[False, False, True],
            kind="stable",
        )
        display_df = filtered_df[
            [
                "selection_rank",
                "window_days",
                "bullish_count",
                "bearish_count",
                "bullish_segment_count",
                "bearish_segment_count",
                "balance_ratio",
                "directional_consistency",
                "mean_directional_accuracy",
                "mean_effect_size",
                "mean_return_separation",
                "composite_score",
            ]
        ].copy()
        score_table = mo.vstack(
            [
                mo.md("## Window Score Table"),
                mo.Html(display_df.head(20).round(4).to_html(index=False)),
            ]
        )
    score_table
    return


@app.cell
def _(detail_split_view, error_message, mo, pd, result, window_view):
    comparison_view = mo.md("")
    forward_view = mo.md("")
    recent_view = mo.md("")
    if not error_message and result is not None:
        _window_days = int(window_view.value)
        _split_name = detail_split_view.value
        _segment_df = result.segment_summary_df[
            (result.segment_summary_df["window_days"] == _window_days)
            & (result.segment_summary_df["sample_split"] == _split_name)
        ].copy()
        comparison_rows = []
        bullish_df = _segment_df[_segment_df["mode"] == "bullish"]
        bearish_df = _segment_df[_segment_df["mode"] == "bearish"]
        if not bullish_df.empty and not bearish_df.empty:
            bullish_row = bullish_df.iloc[0]
            bearish_row = bearish_df.iloc[0]
            comparison_rows.append(
                {
                    "bullish_days": int(bullish_row["total_segment_days"]),
                    "bearish_days": int(bearish_row["total_segment_days"]),
                    "bullish_segments": int(bullish_row["segment_count"]),
                    "bearish_segments": int(bearish_row["segment_count"]),
                    "bullish_mean_segment_days": float(bullish_row["mean_segment_day_count"]),
                    "bearish_mean_segment_days": float(bearish_row["mean_segment_day_count"]),
                    "bullish_mean_segment_return": float(bullish_row["mean_segment_return"]),
                    "bearish_mean_segment_return": float(bearish_row["mean_segment_return"]),
                    "mean_return_separation": float(bullish_row["mean_segment_return"])
                    - float(bearish_row["mean_segment_return"]),
                    "bullish_positive_segment_ratio": float(bullish_row["positive_segment_ratio"]),
                    "bearish_negative_segment_ratio": float(bearish_row["negative_segment_ratio"]),
                    "directional_accuracy": (
                        float(bullish_row["positive_segment_ratio"])
                        + float(bearish_row["negative_segment_ratio"])
                    )
                    / 2.0,
                }
            )
        comparison_df = pd.DataFrame(comparison_rows)
        comparison_view = mo.vstack(
            [
                mo.md("## Mode Segment Comparison"),
                mo.Html(comparison_df.round(4).to_html(index=False)),
            ]
        )

        forward_summary_df = result.mode_summary_df[
            (result.mode_summary_df["window_days"] == _window_days)
            & (result.mode_summary_df["sample_split"] == _split_name)
        ].copy()
        forward_rows = []
        for horizon_days in sorted(forward_summary_df["horizon_days"].unique()):
            horizon_df = forward_summary_df[
                forward_summary_df["horizon_days"] == horizon_days
            ]
            bullish_h_df = horizon_df[horizon_df["mode"] == "bullish"]
            bearish_h_df = horizon_df[horizon_df["mode"] == "bearish"]
            if bullish_h_df.empty or bearish_h_df.empty:
                continue
            bullish_h = bullish_h_df.iloc[0]
            bearish_h = bearish_h_df.iloc[0]
            forward_rows.append(
                {
                    "horizon_days": int(horizon_days),
                    "bullish_mean_future_return": float(bullish_h["mean_future_return"]),
                    "bearish_mean_future_return": float(bearish_h["mean_future_return"]),
                    "mean_return_separation": float(bullish_h["mean_future_return"])
                    - float(bearish_h["mean_future_return"]),
                    "bullish_hit_rate_positive": float(bullish_h["hit_rate_positive"]),
                    "bearish_hit_rate_negative": float(bearish_h["hit_rate_negative"]),
                    "directional_accuracy": (
                        float(bullish_h["hit_rate_positive"])
                        + float(bearish_h["hit_rate_negative"])
                    )
                    / 2.0,
                }
            )
        forward_df = pd.DataFrame(forward_rows)
        forward_view = mo.vstack(
            [
                mo.md("## Forward Return Snapshot"),
                mo.Html(forward_df.round(4).to_html(index=False)),
            ]
        )

        _recent_df = result.mode_assignments_df[
            result.mode_assignments_df["window_days"] == _window_days
        ].copy()
        _recent_df = _recent_df[
            [
                "date",
                "sample_split",
                "mode",
                "close",
                "close_return",
                "dominant_close_return",
                "dominant_event_date",
            ]
        ].tail(20)
        recent_view = mo.vstack(
            [
                mo.md("## Recent Mode Rows"),
                mo.Html(_recent_df.round(4).to_html(index=False)),
            ]
        )
    comparison_view
    forward_view
    recent_view
    return


@app.cell
def _(error_message, mo, pd, plt, result, window_view):
    chart_view = mo.md("")
    if not error_message and result is not None:
        _window_days = int(window_view.value)
        plot_df = result.mode_assignments_df[
            result.mode_assignments_df["window_days"] == _window_days
        ].copy()
        plot_df["date"] = pd.to_datetime(plot_df["date"])
        colors = plot_df["mode"].map(
            {
                "bullish": "#117a65",
                "bearish": "#c0392b",
            }
        )

        fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
        axes[0].plot(plot_df["date"], plot_df["close"], color="#1f2937", linewidth=1.2)
        axes[0].scatter(plot_df["date"], plot_df["close"], c=colors, s=16, alpha=0.75)
        axes[0].set_title(f"TOPIX Close with Mode Labels (X={_window_days})")
        axes[0].grid(alpha=0.2)

        axes[1].bar(
            plot_df["date"],
            plot_df["dominant_close_return"] * 100.0,
            color=colors,
            width=2.0,
            alpha=0.8,
        )
        axes[1].axhline(0.0, color="#666666", linewidth=1, alpha=0.6)
        axes[1].set_title("Dominant Trailing Close-to-Close Return (%)")
        axes[1].grid(alpha=0.2)
        fig.tight_layout()
        chart_view = fig
    chart_view
    return


@app.cell
def _(error_message, mo, pd, result):
    selected_view = mo.md("")
    if not error_message and result is not None:
        selected_df = result.selected_window_comparison_df.copy()
        selected_view = mo.vstack(
            [
                mo.md("## Selected Window Across Splits"),
                mo.Html(selected_df.round(4).to_html(index=False)),
            ]
        )
    selected_view
    return


@app.cell
def _(
    build_multi_timeframe_state_tables,
    detail_split_view,
    error_message,
    long_window_view,
    result,
    short_window_view,
):
    if error_message or result is None:
        multi_timeframe_payload = None
    else:
        _short_window_days = int(short_window_view.value)
        _long_window_days = int(long_window_view.value)
        multi_timeframe_payload = build_multi_timeframe_state_tables(
            result.mode_assignments_df,
            future_horizons=result.future_horizons,
            short_window_days=_short_window_days,
            long_window_days=_long_window_days,
        )
    return (multi_timeframe_payload,)


@app.cell
def _(detail_split_view, error_message, mo, pd, multi_timeframe_payload):
    matrix_view = mo.md("")
    segment_view = mo.md("")
    recent_state_view = mo.md("")
    if not error_message and multi_timeframe_payload is not None:
        state_daily_df, state_segment_df, state_summary_df, state_segment_summary_df = multi_timeframe_payload
        _split_name = detail_split_view.value

        summary_df = state_summary_df[state_summary_df["sample_split"] == _split_name].copy()
        if not summary_df.empty:
            pivot_df = (
                summary_df.pivot_table(
                    index=["state_label", "day_count"],
                    columns="horizon_days",
                    values="mean_future_return",
                )
                .reset_index()
                .sort_values("state_label", kind="stable")
            )
            pivot_df.columns = [
                str(column) if isinstance(column, str) else f"mean_future_return_{int(column)}d"
                for column in pivot_df.columns
            ]
            matrix_view = mo.vstack(
                [
                    mo.md("## 4-State Forward Return Matrix"),
                    mo.Html(pivot_df.round(4).to_html(index=False)),
                ]
            )

        _segment_df = state_segment_summary_df[
            state_segment_summary_df["sample_split"] == _split_name
        ].copy()
        if not _segment_df.empty:
            display_segment_df = _segment_df[
                [
                    "state_label",
                    "segment_count",
                    "total_segment_days",
                    "mean_segment_day_count",
                    "mean_segment_return",
                    "median_segment_return",
                    "positive_segment_ratio",
                    "negative_segment_ratio",
                ]
            ].sort_values("state_label", kind="stable")
            segment_view = mo.vstack(
                [
                    mo.md("## 4-State Segment Matrix"),
                    mo.Html(display_segment_df.round(4).to_html(index=False)),
                ]
            )

        _recent_df = state_daily_df[state_daily_df["sample_split"] == _split_name].copy()
        if not _recent_df.empty:
            _recent_df = _recent_df[
                [
                    "date",
                    "state_label",
                    "long_mode",
                    "short_mode",
                    "close_return",
                    "short_dominant_close_return",
                    "long_dominant_close_return",
                ]
            ].tail(24)
            recent_state_view = mo.vstack(
                [
                    mo.md("## Recent 4-State Rows"),
                    mo.Html(_recent_df.round(4).to_html(index=False)),
                ]
            )
    matrix_view
    segment_view
    recent_state_view
    return


if __name__ == "__main__":
    app.run()
