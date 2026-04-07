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
    app_title="TOPIX100 Streak 3/53 Transfer Playground",
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
    from src.domains.analytics.topix100_streak_353_transfer import (
        get_topix100_streak_353_transfer_bundle_path_for_run_id,
        get_topix100_streak_353_transfer_latest_bundle_path,
        load_topix100_streak_353_transfer_research_bundle,
    )

    return (
        build_bundle_viewer_controls,
        get_latest_bundle_defaults,
        get_topix100_streak_353_transfer_bundle_path_for_run_id,
        get_topix100_streak_353_transfer_latest_bundle_path,
        load_bundle_selection,
        load_research_bundle_info,
        load_topix100_streak_353_transfer_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(
    get_latest_bundle_defaults,
    get_topix100_streak_353_transfer_latest_bundle_path,
):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix100_streak_353_transfer_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix100_streak_353_transfer.py",
        extra_note_lines=[
            "- this notebook inspects the fixed TOPIX-learned streak pair on TOPIX100 constituents",
            "- `3 / 53` is not re-optimized here; every stock uses the same pair",
            "- prioritize `state_date_summary_df` and `state_stock_consistency_df` over the raw event table",
        ],
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix100_streak_353_transfer_bundle_path_for_run_id,
    resolve_selected_bundle_path,
    run_id,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix100_streak_353_transfer_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_bundle_selection,
    load_research_bundle_info,
    load_topix100_streak_353_transfer_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix100_streak_353_transfer_research_bundle,
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
                    "## TOPIX100 Streak 3/53 Transfer",
                    "",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Fixed short / long pair: **{result.short_window_streaks} / {result.long_window_streaks}**",
                    f"- Future horizons: **{', '.join(str(value) for value in result.future_horizons)}**",
                    f"- TOPIX100 constituents: **{result.topix100_constituent_count}**",
                    f"- Covered constituents: **{result.covered_constituent_count}**",
                    f"- Valid state events: **{result.valid_event_count}**",
                    f"- Valid dates: **{result.valid_date_count}**",
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
        horizon_view = mo.md("")
    else:
        horizon_view = mo.ui.dropdown(
            options={str(value): str(value) for value in result.future_horizons},
            value=str(result.future_horizons[0]),
            label="Horizon",
        )
    horizon_view
    return (horizon_view,)


@app.cell
def _(error_message, horizon_view, mo, result):
    date_view = mo.md("")
    stock_view = mo.md("")
    event_view = mo.md("")
    recent_view = mo.md("")
    if not error_message and result is not None:
        horizon = int(horizon_view.value)
        date_df = result.state_date_summary_df[
            (result.state_date_summary_df["sample_split"] == "validation")
            & (result.state_date_summary_df["horizon_days"] == horizon)
        ].copy()
        stock_df = result.state_stock_consistency_df[
            (result.state_stock_consistency_df["sample_split"] == "validation")
            & (result.state_stock_consistency_df["horizon_days"] == horizon)
        ].copy()
        event_df = result.state_event_summary_df[
            (result.state_event_summary_df["sample_split"] == "validation")
            & (result.state_event_summary_df["horizon_days"] == horizon)
        ].copy()
        recent_df = result.state_event_df.tail(24).copy()

        date_view = mo.vstack(
            [
                mo.md("## Validation Date-Balanced Summary"),
                mo.Html(date_df.round(4).to_html(index=False)),
            ]
        )
        stock_view = mo.vstack(
            [
                mo.md("## Validation Stock-Breadth Summary"),
                mo.Html(stock_df.round(4).to_html(index=False)),
            ]
        )
        event_view = mo.vstack(
            [
                mo.md("## Validation Raw Event Summary"),
                mo.Html(event_df.round(4).to_html(index=False)),
            ]
        )
        recent_view = mo.vstack(
            [
                mo.md("## Recent State Events"),
                mo.Html(
                    recent_df[
                        [
                            "segment_end_date",
                            "code",
                            "company_name",
                            "state_label",
                            "segment_day_count",
                            "segment_return",
                        ]
                    ]
                    .round(4)
                    .to_html(index=False)
                ),
            ]
        )
    mo.vstack([date_view, stock_view, event_view, recent_view])
    return


if __name__ == "__main__":
    app.run()
