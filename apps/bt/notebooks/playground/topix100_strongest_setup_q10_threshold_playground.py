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
    app_title="TOPIX100 Strongest Setup vs Q10 Threshold",
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
    from src.domains.analytics.topix100_strongest_setup_q10_threshold import (
        get_topix100_strongest_setup_q10_threshold_bundle_path_for_run_id,
        get_topix100_strongest_setup_q10_threshold_latest_bundle_path,
        load_topix100_strongest_setup_q10_threshold_research_bundle,
    )

    return (
        build_bundle_viewer_controls,
        get_latest_bundle_defaults,
        get_topix100_strongest_setup_q10_threshold_bundle_path_for_run_id,
        get_topix100_strongest_setup_q10_threshold_latest_bundle_path,
        load_bundle_selection,
        load_research_bundle_info,
        load_topix100_strongest_setup_q10_threshold_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix100_strongest_setup_q10_threshold_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix100_strongest_setup_q10_threshold_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix100_strongest_setup_q10_threshold.py",
        extra_note_lines=[
            "- read `band_vs_q10_reference_scorecard_df` first; that is the actual decision surface",
            "- `strongest_setup_lower_tail_band_summary_df` is the cleaner execution table than the single-decile summary",
            "- this study answers where the strongest setup still beats the best non-strong Q10 alternative",
        ],
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix100_strongest_setup_q10_threshold_bundle_path_for_run_id,
    resolve_selected_bundle_path,
    run_id,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix100_strongest_setup_q10_threshold_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_bundle_selection,
    load_research_bundle_info,
    load_topix100_strongest_setup_q10_threshold_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix100_strongest_setup_q10_threshold_research_bundle,
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
                    "## TOPIX100 Strongest Setup vs Q10 Threshold",
                    "",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Strongest setup: **{result.strongest_volume_bucket_label} x {result.strongest_state_label}**",
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
        split_view = mo.md("")
    else:
        split_view = mo.ui.dropdown(
            options={"validation": "validation", "full": "full", "discovery": "discovery"},
            value="validation",
            label="Split",
        )
    split_view
    return (split_view,)


@app.cell
def _(error_message, mo, result, split_view):
    if error_message or result is None:
        content = mo.md("")
    else:
        split_name = split_view.value
        scorecard_df = result.band_vs_q10_reference_scorecard_df[
            result.band_vs_q10_reference_scorecard_df["sample_split"] == split_name
        ].copy()
        band_df = result.strongest_setup_lower_tail_band_summary_df[
            result.strongest_setup_lower_tail_band_summary_df["sample_split"] == split_name
        ].copy()
        reference_df = result.q10_non_strong_reference_df[
            result.q10_non_strong_reference_df["sample_split"] == split_name
        ].copy()
        decile_df = result.strongest_setup_decile_summary_df[
            result.strongest_setup_decile_summary_df["sample_split"] == split_name
        ].copy()

        content = mo.vstack(
            [
                mo.md("## Band vs Reference Scorecard"),
                mo.Html(scorecard_df.round(4).to_html(index=False)),
                mo.md("## Strongest Setup Lower-Tail Bands"),
                mo.Html(band_df.round(4).to_html(index=False)),
                mo.md("## Best Non-Strong Q10 Reference"),
                mo.Html(reference_df.round(4).to_html(index=False)),
                mo.md("## Strongest Setup by Single Decile"),
                mo.Html(decile_df.round(4).to_html(index=False)),
            ]
        )
    content
    return


if __name__ == "__main__":
    app.run()
