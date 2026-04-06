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
    app_title="TOPIX100 Streak 3/53 Multivariate Priority",
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
    from src.domains.analytics.topix100_streak_353_multivariate_priority import (
        get_topix100_streak_353_multivariate_priority_bundle_path_for_run_id,
        get_topix100_streak_353_multivariate_priority_latest_bundle_path,
        load_topix100_streak_353_multivariate_priority_research_bundle,
    )

    return (
        build_bundle_viewer_controls,
        get_latest_bundle_defaults,
        get_topix100_streak_353_multivariate_priority_bundle_path_for_run_id,
        get_topix100_streak_353_multivariate_priority_latest_bundle_path,
        load_bundle_selection,
        load_research_bundle_info,
        load_topix100_streak_353_multivariate_priority_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix100_streak_353_multivariate_priority_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix100_streak_353_multivariate_priority_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix100_streak_353_multivariate_priority.py",
        extra_note_lines=[
            "- `subset_rule_scorecard_df` is the main reading table",
            "- `feature_priority_df` shows the Shapley-based priority ranking",
            "- `feature_leave_one_out_df` is the practical simplification table",
        ],
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix100_streak_353_multivariate_priority_bundle_path_for_run_id,
    resolve_selected_bundle_path,
    run_id,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix100_streak_353_multivariate_priority_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_bundle_selection,
    load_research_bundle_info,
    load_topix100_streak_353_multivariate_priority_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix100_streak_353_multivariate_priority_research_bundle,
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
                    "## TOPIX100 Streak 3/53 Multivariate Priority",
                    "",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Price / volume lens: **{result.price_feature} x {result.volume_feature}**",
                    f"- Fixed short / long pair: **{result.short_window_streaks} / {result.long_window_streaks}**",
                    f"- Joined horizon rows: **{result.joined_event_count}**",
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
                mo.md("## Full-Feature Setup"),
                mo.Html(result.full_feature_setup_df.round(4).to_html(index=False)),
                mo.md("## Q1 vs Q10 Comparison"),
                mo.Html(result.validation_extreme_bucket_comparison_df.round(4).to_html(index=False)),
                mo.md("## Feature Priority"),
                mo.Html(result.feature_priority_df.round(4).to_html(index=False)),
                mo.md("## Leave-One-Out"),
                mo.Html(result.feature_leave_one_out_df.round(4).to_html(index=False)),
                mo.md("## Subset Rule Scorecard"),
                mo.Html(result.subset_rule_scorecard_df.round(4).to_html(index=False)),
                mo.md("## Candidate Scorecard"),
                mo.Html(result.subset_candidate_scorecard_df.round(4).to_html(index=False)),
            ]
        )
    content
    return


if __name__ == "__main__":
    app.run()
