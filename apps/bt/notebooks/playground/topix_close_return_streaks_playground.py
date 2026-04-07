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
    app_title="TOPIX Close Return Streaks Playground",
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
    from src.domains.analytics.topix_close_return_streaks import (
        get_topix_close_return_streaks_bundle_path_for_run_id,
        get_topix_close_return_streaks_latest_bundle_path,
        load_topix_close_return_streaks_research_bundle,
    )

    return (
        build_bundle_viewer_controls,
        get_latest_bundle_defaults,
        get_topix_close_return_streaks_bundle_path_for_run_id,
        get_topix_close_return_streaks_latest_bundle_path,
        load_bundle_selection,
        load_research_bundle_info,
        load_topix_close_return_streaks_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix_close_return_streaks_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix_close_return_streaks_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix_close_return_streaks.py",
        extra_note_lines=[
            "- consecutive positive/negative close-to-close returns are treated as one segment",
            "- `streak_daily_df` is the in-progress view; `streak_segment_df` is the completed composite-candle view",
            "- `segment_end_summary_df` shows what happened after a streak finished",
        ],
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix_close_return_streaks_bundle_path_for_run_id,
    resolve_selected_bundle_path,
    run_id,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix_close_return_streaks_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_bundle_selection,
    load_research_bundle_info,
    load_topix_close_return_streaks_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix_close_return_streaks_research_bundle,
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
                    "## TOPIX Close Return Streaks",
                    "",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Source mode: **{result.source_mode}**",
                    f"- Future horizons: **{', '.join(str(value) for value in result.future_horizons)}**",
                    f"- Validation ratio: **{result.validation_ratio:.2f}**",
                    f"- Streak day bucket cap: **{result.max_streak_day_bucket}+**",
                    f"- Segment length bucket cap: **{result.max_segment_length_bucket}+**",
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
        horizon_view = mo.md("")
    else:
        available_splits = result.streak_state_summary_df["sample_split"].drop_duplicates().tolist()
        split_view = mo.ui.dropdown(
            options={value: value for value in available_splits},
            value="validation" if "validation" in available_splits else available_splits[0],
            label="Split",
        )
        horizon_view = mo.ui.dropdown(
            options={str(value): str(value) for value in result.future_horizons},
            value=str(result.future_horizons[1] if len(result.future_horizons) > 1 else result.future_horizons[0]),
            label="Horizon",
        )
    mo.hstack([split_view, horizon_view])
    return horizon_view, split_view


@app.cell
def _(error_message, horizon_view, mo, pd, result, split_view):
    streak_state_view = mo.md("")
    segment_view = mo.md("")
    segment_end_view = mo.md("")
    recent_daily_view = mo.md("")
    recent_segment_view = mo.md("")

    if not error_message and result is not None:
        _split = split_view.value
        _horizon = int(horizon_view.value)

        streak_state_df = result.streak_state_summary_df[
            (result.streak_state_summary_df["sample_split"] == _split)
            & (result.streak_state_summary_df["horizon_days"] == _horizon)
        ].copy()
        streak_state_df = streak_state_df.sort_values(
            ["mode", "streak_day_bucket"],
            ascending=[True, True],
            kind="stable",
        )
        streak_state_df = streak_state_df[
            [
                "mode",
                "streak_day_label",
                "sample_count",
                "mean_close_return",
                "mean_remaining_segment_days",
                "completion_rate",
                "continuation_rate_1d",
                "mean_future_return",
                "hit_rate_positive",
                "hit_rate_negative",
            ]
        ]
        streak_state_view = mo.vstack(
            [
                mo.md("## Streak Day Forward Summary"),
                mo.Html(streak_state_df.round(4).to_html(index=False)),
            ]
        )

        segment_df = result.segment_summary_df[
            result.segment_summary_df["sample_split"] == _split
        ].copy()
        segment_df = segment_df.sort_values(
            ["mode", "segment_length_bucket"],
            ascending=[True, True],
            kind="stable",
        )
        segment_df = segment_df[
            [
                "mode",
                "segment_length_label",
                "segment_count",
                "mean_segment_day_count",
                "mean_segment_return",
                "positive_segment_ratio",
                "negative_segment_ratio",
                "mean_synthetic_range",
            ]
        ]
        segment_view = mo.vstack(
            [
                mo.md("## Completed Segment Summary"),
                mo.Html(segment_df.round(4).to_html(index=False)),
            ]
        )

        segment_end_df = result.segment_end_summary_df[
            (result.segment_end_summary_df["sample_split"] == _split)
            & (result.segment_end_summary_df["horizon_days"] == _horizon)
        ].copy()
        segment_end_df = segment_end_df.sort_values(
            ["mode", "segment_length_bucket"],
            ascending=[True, True],
            kind="stable",
        )
        segment_end_df = segment_end_df[
            [
                "mode",
                "segment_length_label",
                "sample_count",
                "mean_segment_return",
                "mean_future_return",
                "hit_rate_positive",
                "hit_rate_negative",
            ]
        ]
        segment_end_view = mo.vstack(
            [
                mo.md("## After Segment End"),
                mo.Html(segment_end_df.round(4).to_html(index=False)),
            ]
        )

        recent_daily_df = result.streak_daily_df[
            [
                "date",
                "sample_split",
                "mode",
                "close",
                "close_return",
                "streak_day",
                "remaining_segment_days",
                "segment_start_date",
                "segment_end_date",
                "segment_day_count",
                "segment_return",
            ]
        ].tail(20)
        recent_daily_view = mo.vstack(
            [
                mo.md("## Recent Streak Rows"),
                mo.Html(recent_daily_df.round(4).to_html(index=False)),
            ]
        )

        recent_segment_df = result.streak_segment_df[
            [
                "segment_id",
                "mode",
                "start_date",
                "end_date",
                "segment_day_count",
                "synthetic_open",
                "synthetic_high",
                "synthetic_low",
                "synthetic_close",
                "segment_return",
                "is_complete",
                "segment_sample_split",
            ]
        ].tail(20)
        recent_segment_view = mo.vstack(
            [
                mo.md("## Recent Composite Candles"),
                mo.Html(recent_segment_df.round(4).to_html(index=False)),
            ]
        )

    streak_state_view
    segment_view
    segment_end_view
    recent_daily_view
    recent_segment_view
    return


if __name__ == "__main__":
    app.run()
