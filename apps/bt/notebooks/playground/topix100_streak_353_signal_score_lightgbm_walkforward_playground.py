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
    app_title="TOPIX100 Streak 3/53 Signal Score LightGBM Walk-Forward",
)


@app.cell
def _():
    import marimo as mo
    import sys
    from pathlib import Path

    return Path, mo, sys


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
    from src.domains.analytics.topix100_streak_353_signal_score_lightgbm import (
        format_topix100_streak_353_signal_score_lightgbm_notebook_error,
    )
    from src.domains.analytics.topix100_streak_353_signal_score_lightgbm_walkforward import (
        get_topix100_streak_353_signal_score_lightgbm_walkforward_bundle_path_for_run_id,
        get_topix100_streak_353_signal_score_lightgbm_walkforward_latest_bundle_path,
        load_topix100_streak_353_signal_score_lightgbm_walkforward_research_bundle,
    )

    return (
        build_bundle_viewer_controls,
        format_topix100_streak_353_signal_score_lightgbm_notebook_error,
        get_latest_bundle_defaults,
        get_topix100_streak_353_signal_score_lightgbm_walkforward_bundle_path_for_run_id,
        get_topix100_streak_353_signal_score_lightgbm_walkforward_latest_bundle_path,
        load_bundle_selection,
        load_research_bundle_info,
        load_topix100_streak_353_signal_score_lightgbm_walkforward_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix100_streak_353_signal_score_lightgbm_walkforward_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix100_streak_353_signal_score_lightgbm_walkforward_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix100_streak_353_signal_score_lightgbm_walkforward.py",
        extra_note_lines=[
            "- `walkforward_model_comparison_df` is the main overfitting check",
            "- `walkforward_split_comparison_df` shows whether LightGBM actually won split by split",
            "- `walkforward_feature_importance_df` shows average importance across all refits",
        ],
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix100_streak_353_signal_score_lightgbm_walkforward_bundle_path_for_run_id,
    resolve_selected_bundle_path,
    run_id,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix100_streak_353_signal_score_lightgbm_walkforward_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    format_topix100_streak_353_signal_score_lightgbm_notebook_error,
    load_bundle_selection,
    load_research_bundle_info,
    load_topix100_streak_353_signal_score_lightgbm_walkforward_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix100_streak_353_signal_score_lightgbm_walkforward_research_bundle,
        )
        error_message = None
    except Exception as exc:
        bundle_info = None
        result = None
        error_message = format_topix100_streak_353_signal_score_lightgbm_notebook_error(
            exc
        )
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
                    "## TOPIX100 Streak 3/53 Signal Score LightGBM Walk-Forward",
                    "",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Walk-forward windows: **{result.train_window}/{result.test_window}/{result.step}**",
                    f"- Split count: **{result.split_count}**",
                    f"- Top-k grid: **{', '.join(str(value) for value in result.top_k_values)}**",
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
        content = mo.md("")
    else:
        content = mo.vstack(
            [
                mo.md("## Overall Comparison"),
                mo.Html(result.walkforward_model_comparison_df.round(4).to_html(index=False)),
                mo.md("## Per-Split Comparison"),
                mo.Html(result.walkforward_split_comparison_df.round(4).to_html(index=False)),
                mo.md("## Overall Summary"),
                mo.Html(result.walkforward_model_summary_df.round(4).to_html(index=False)),
                mo.md("## Average Feature Importance"),
                mo.Html(result.walkforward_feature_importance_df.round(4).to_html(index=False)),
                mo.md("## Score Deciles"),
                mo.Html(result.walkforward_score_decile_df.round(4).to_html(index=False)),
            ]
        )
    content
    return


if __name__ == "__main__":
    app.run()
