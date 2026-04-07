# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "pandas>=2.0.0",
# ]
# ///

import marimo

__generated_with = "0.21.1"
app = marimo.App(
    width="full",
    app_title="TOPIX Streak Extreme Mode Playground",
)


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import sys
    from pathlib import Path

    return Path, mo, pd, sys


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
    from src.domains.analytics.topix_streak_extreme_mode import (
        get_topix_streak_extreme_mode_bundle_path_for_run_id,
        get_topix_streak_extreme_mode_latest_bundle_path,
        load_topix_streak_extreme_mode_research_bundle,
    )

    return (
        build_bundle_viewer_controls,
        get_latest_bundle_defaults,
        get_topix_streak_extreme_mode_bundle_path_for_run_id,
        get_topix_streak_extreme_mode_latest_bundle_path,
        load_bundle_selection,
        load_research_bundle_info,
        load_topix_streak_extreme_mode_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix_streak_extreme_mode_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix_streak_extreme_mode_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix_streak_extreme_mode.py",
        extra_note_lines=[
            "- consecutive positive/negative daily close changes are merged into one streak candle",
            "- `X` is the number of trailing streak candles, not trading days",
            "- future horizons are still measured in calendar trading days from the current streak end",
        ],
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix_streak_extreme_mode_bundle_path_for_run_id,
    resolve_selected_bundle_path,
    run_id,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix_streak_extreme_mode_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_bundle_selection,
    load_research_bundle_info,
    load_topix_streak_extreme_mode_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix_streak_extreme_mode_research_bundle,
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
                    "## TOPIX Streak Extreme Mode",
                    "",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Source mode: **{result.source_mode}**",
                    f"- Candidate windows: **{result.candidate_windows[0]}..{result.candidate_windows[-1]} ({len(result.candidate_windows)} values)**",
                    f"- Future horizons: **{', '.join(str(value) for value in result.future_horizons)}**",
                    f"- Validation ratio: **{result.validation_ratio:.2f}**",
                    f"- Selected X (streak candles): **{result.selected_window_streaks}**",
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
        split_view = mo.md("")
        window_view = mo.md("")
    else:
        available_splits = result.window_score_df["sample_split"].drop_duplicates().tolist()
        window_options = {
            str(value): str(value)
            for value in sorted(result.mode_assignments_df["window_streaks"].unique())
        }
        split_view = mo.ui.dropdown(
            options={value: value for value in available_splits},
            value="discovery" if "discovery" in available_splits else available_splits[0],
            label="Split",
        )
        window_view = mo.ui.dropdown(
            options=window_options,
            value=str(result.selected_window_streaks),
            label="Window Streaks",
        )
    mo.hstack([split_view, window_view])
    return split_view, window_view


@app.cell
def _(error_message, mo, result, split_view):
    score_view = mo.md("")
    if not error_message and result is not None:
        score_df = result.window_score_df[
            result.window_score_df["sample_split"] == split_view.value
        ].copy()
        score_df = score_df.sort_values(
            ["selection_score", "composite_score", "window_streaks"],
            ascending=[False, False, True],
            kind="stable",
        )
        score_df = score_df[
            [
                "selection_rank",
                "window_streaks",
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
        ]
        score_view = mo.vstack(
            [
                mo.md("## Window Score Table"),
                mo.Html(score_df.head(20).round(4).to_html(index=False)),
            ]
        )
    score_view
    return


@app.cell
def _(error_message, mo, pd, result, split_view, window_view):
    comparison_view = mo.md("")
    forward_view = mo.md("")
    recent_mode_view = mo.md("")
    recent_streak_view = mo.md("")
    if not error_message and result is not None:
        _window = int(window_view.value)
        _split = split_view.value

        comparison_df = result.selected_window_comparison_df[
            (result.selected_window_comparison_df["window_streaks"] == _window)
            & (result.selected_window_comparison_df["sample_split"] == _split)
        ].copy()
        if comparison_df.empty:
            comparison_df = result.selected_window_comparison_df[
                result.selected_window_comparison_df["sample_split"] == _split
            ].copy()
        comparison_view = mo.vstack(
            [
                mo.md("## Mode Segment Comparison"),
                mo.Html(comparison_df.round(4).to_html(index=False)),
            ]
        )

        forward_df = result.mode_summary_df[
            (result.mode_summary_df["window_streaks"] == _window)
            & (result.mode_summary_df["sample_split"] == _split)
        ].copy()
        forward_rows = []
        for horizon_days in sorted(forward_df["horizon_days"].unique()):
            horizon_slice = forward_df[forward_df["horizon_days"] == horizon_days]
            bullish_df = horizon_slice[horizon_slice["mode"] == "bullish"]
            bearish_df = horizon_slice[horizon_slice["mode"] == "bearish"]
            if bullish_df.empty or bearish_df.empty:
                continue
            bullish_row = bullish_df.iloc[0]
            bearish_row = bearish_df.iloc[0]
            forward_rows.append(
                {
                    "horizon_days": int(horizon_days),
                    "bullish_mean_future_return": float(bullish_row["mean_future_return"]),
                    "bearish_mean_future_return": float(bearish_row["mean_future_return"]),
                    "mean_return_separation": float(bullish_row["mean_future_return"])
                    - float(bearish_row["mean_future_return"]),
                    "bullish_hit_rate_positive": float(bullish_row["hit_rate_positive"]),
                    "bearish_hit_rate_negative": float(bearish_row["hit_rate_negative"]),
                }
            )
        forward_view = mo.vstack(
            [
                mo.md("## Forward Return Snapshot"),
                mo.Html(pd.DataFrame(forward_rows).round(4).to_html(index=False)),
            ]
        )

        recent_mode_df = result.mode_assignments_df[
            result.mode_assignments_df["window_streaks"] == _window
        ][
            [
                "segment_id",
                "segment_start_date",
                "segment_end_date",
                "sample_split",
                "mode",
                "segment_return",
                "dominant_segment_return",
                "dominant_segment_start_date",
                "dominant_segment_end_date",
            ]
        ].tail(20)
        recent_mode_view = mo.vstack(
            [
                mo.md("## Recent Mode Rows"),
                mo.Html(recent_mode_df.round(4).to_html(index=False)),
            ]
        )

        recent_streak_df = result.streak_candle_df[
            [
                "segment_id",
                "start_date",
                "end_date",
                "mode",
                "segment_day_count",
                "synthetic_open",
                "synthetic_close",
                "segment_return",
            ]
        ].tail(20)
        recent_streak_view = mo.vstack(
            [
                mo.md("## Recent Streak Candles"),
                mo.Html(recent_streak_df.round(4).to_html(index=False)),
            ]
        )
    comparison_view
    forward_view
    recent_mode_view
    recent_streak_view
    return


if __name__ == "__main__":
    app.run()
