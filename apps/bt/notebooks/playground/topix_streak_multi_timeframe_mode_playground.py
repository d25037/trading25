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
    app_title="TOPIX Streak Multi-Timeframe Mode Playground",
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
    from src.domains.analytics.topix_streak_multi_timeframe_mode import (
        get_topix_streak_multi_timeframe_mode_bundle_path_for_run_id,
        get_topix_streak_multi_timeframe_mode_latest_bundle_path,
        load_topix_streak_multi_timeframe_mode_research_bundle,
    )

    return (
        build_bundle_viewer_controls,
        get_latest_bundle_defaults,
        get_topix_streak_multi_timeframe_mode_bundle_path_for_run_id,
        get_topix_streak_multi_timeframe_mode_latest_bundle_path,
        load_bundle_selection,
        load_research_bundle_info,
        load_topix_streak_multi_timeframe_mode_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(
    get_latest_bundle_defaults,
    get_topix_streak_multi_timeframe_mode_latest_bundle_path,
):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix_streak_multi_timeframe_mode_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix_streak_multi_timeframe_mode.py",
        extra_note_lines=[
            "- this notebook inspects a short/long pair scan over streak-candle mode states",
            "- `short X` and `long X` are both counted in streak candles, not trading days",
            "- pair selection is based on 4-state validation ordering stability, not on a single-window score",
        ],
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix_streak_multi_timeframe_mode_bundle_path_for_run_id,
    resolve_selected_bundle_path,
    run_id,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix_streak_multi_timeframe_mode_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_bundle_selection,
    load_research_bundle_info,
    load_topix_streak_multi_timeframe_mode_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix_streak_multi_timeframe_mode_research_bundle,
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
                    "## TOPIX Streak Multi-Timeframe Mode",
                    "",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Source mode: **{result.source_mode}**",
                    f"- Candidate windows: **{result.candidate_windows[0]}..{result.candidate_windows[-1]} ({len(result.candidate_windows)} values)**",
                    f"- Stability horizons: **{', '.join(str(value) for value in result.stability_horizons)}**",
                    f"- Base standalone X: **{result.selected_base_window_streaks}**",
                    f"- Selected short / long pair: **{result.selected_short_window_streaks} / {result.selected_long_window_streaks}**",
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
        pair_view = mo.md("")
    else:
        pair_options = {
            f"{int(row['short_window_streaks'])}/{int(row['long_window_streaks'])}": (
                f"{int(row['short_window_streaks'])}/{int(row['long_window_streaks'])}"
            )
            for row in result.pair_score_df.head(20).to_dict(orient="records")
        }
        pair_view = mo.ui.dropdown(
            options=pair_options,
            value=f"{result.selected_short_window_streaks}/{result.selected_long_window_streaks}",
            label="Pair",
        )
    pair_view
    return (pair_view,)


@app.cell
def _(error_message, mo, result):
    leaderboard_view = mo.md("")
    if not error_message and result is not None:
        leaderboard_df = result.pair_score_df[
            [
                "selection_rank",
                "short_window_streaks",
                "long_window_streaks",
                "selection_score",
                "ranking_consistency",
                "locked_best_worst_ratio",
                "mean_spread",
                "disagreement_ratio",
            ]
        ].copy()
        leaderboard_view = mo.vstack(
            [
                mo.md("## Pair Leaderboard"),
                mo.Html(leaderboard_df.head(20).round(4).to_html(index=False)),
            ]
        )
    leaderboard_view
    return


@app.cell
def _(error_message, mo, pair_view, result):
    forward_view = mo.md("")
    segment_view = mo.md("")
    rank_view = mo.md("")
    recent_view = mo.md("")
    if not error_message and result is not None:
        short_window, long_window = (
            int(value) for value in str(pair_view.value).split("/", maxsplit=1)
        )
        if (
            short_window != result.selected_short_window_streaks
            or long_window != result.selected_long_window_streaks
        ):
            forward_df = mo.md(
                "Only the selected pair is materialized in this bundle. Re-run the runner if you want a different pair published."
            )
            forward_view = forward_df
        else:
            forward_df = result.selected_pair_state_summary_df[
                result.selected_pair_state_summary_df["sample_split"] == "validation"
            ].copy()
            segment_df = result.selected_pair_state_segment_summary_df[
                result.selected_pair_state_segment_summary_df["sample_split"] == "validation"
            ].copy()
            recent_df = result.selected_pair_state_streak_df.tail(20).copy()
            forward_view = mo.vstack(
                [
                    mo.md("## Validation Forward Summary"),
                    mo.Html(
                        forward_df[
                            [
                                "horizon_days",
                                "state_label",
                                "state_candle_count",
                                "mean_future_return",
                                "hit_rate_positive",
                            ]
                        ]
                        .round(4)
                        .to_html(index=False)
                    ),
                ]
            )
            segment_view = mo.vstack(
                [
                    mo.md("## Validation Segment Summary"),
                    mo.Html(
                        segment_df[
                            [
                                "state_label",
                                "segment_count",
                                "mean_segment_return",
                                "mean_state_candle_count",
                                "mean_state_day_count",
                            ]
                        ]
                        .round(4)
                        .to_html(index=False)
                    ),
                ]
            )
            rank_view = mo.vstack(
                [
                    mo.md("## Stability Horizon Ordering"),
                    mo.Html(
                        result.selected_pair_horizon_rank_df[
                            [
                                "horizon_days",
                                "rank_position",
                                "state_label",
                                "mean_future_return",
                            ]
                        ]
                        .round(4)
                        .to_html(index=False)
                    ),
                ]
            )
            recent_view = mo.vstack(
                [
                    mo.md("## Recent Selected-Pair States"),
                    mo.Html(
                        recent_df[
                            [
                                "segment_end_date",
                                "state_label",
                                "short_mode",
                                "long_mode",
                                "segment_return",
                            ]
                        ]
                        .round(4)
                        .to_html(index=False)
                    ),
                ]
            )
    mo.vstack([forward_view, segment_view, rank_view, recent_view])
    return


if __name__ == "__main__":
    app.run()
