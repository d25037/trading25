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
    app_title="TOPIX100 VI Change Regime Conditioning Playground",
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
    from src.domains.analytics.topix100_vi_change_regime_conditioning import (
        get_topix100_vi_change_regime_conditioning_bundle_path_for_run_id,
        get_topix100_vi_change_regime_conditioning_latest_bundle_path,
        load_topix100_vi_change_regime_conditioning_research_bundle,
    )

    return (
        build_bundle_viewer_controls,
        get_topix100_vi_change_regime_conditioning_bundle_path_for_run_id,
        get_latest_bundle_defaults,
        get_topix100_vi_change_regime_conditioning_latest_bundle_path,
        load_research_bundle_info,
        load_bundle_selection,
        load_topix100_vi_change_regime_conditioning_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix100_vi_change_regime_conditioning_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix100_vi_change_regime_conditioning_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix100_vi_change_regime_conditioning.py",
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix100_vi_change_regime_conditioning_bundle_path_for_run_id,
    run_id,
    resolve_selected_bundle_path,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix100_vi_change_regime_conditioning_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_research_bundle_info,
    load_bundle_selection,
    load_topix100_vi_change_regime_conditioning_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix100_vi_change_regime_conditioning_research_bundle,
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
        summary_view = mo.md(
            "\n".join(
                [
                    "## TOPIX100 VI Change Regime Conditioning",
                    "",
                    *(
                        [
                            f"- Bundle run id: **{bundle_info.run_id}**",
                            f"- Bundle created at: **{bundle_info.created_at}**",
                            f"- Bundle path: **{bundle_info.bundle_dir}**",
                        ]
                        if bundle_info is not None
                        else []
                    ),
                    f"- Source mode: **{result.source_mode}**",
                    f"- Source detail: **{result.source_detail}**",
                    f"- VI available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Latest TOPIX100 constituent count: **{result.universe_constituent_count}**",
                    f"- Valid dates with VI regime: **{result.valid_date_count}**",
                    f"- Sigma thresholds: **{result.sigma_threshold_1} / {result.sigma_threshold_2}**",
                    "",
                    "Primary split is fixed to **price vs 20SMA gap x volume_sma_20_80**.",
                    "Regimes are bucketed on **same-day Nikkei VI change**, then collapsed to **Weak / Neutral / Strong**.",
                ]
            )
        )
    summary_view
    return


@app.cell
def _(error_message, mo, pd, result):
    stats_view = mo.md("")
    if not error_message and result is not None:
        vi_stats_df = pd.DataFrame(
            [result.vi_change_stats.__dict__] if result.vi_change_stats else []
        )
        stats_view = mo.vstack(
            [
                mo.md("## Regime Stats"),
                mo.md("### Nikkei VI Change"),
                mo.Html(vi_stats_df.round(6).to_html(index=False)),
            ]
        )
    stats_view
    return


@app.cell
def _(error_message, mo, result):
    day_count_view = mo.md("")
    if not error_message and result is not None:
        day_count_view = mo.vstack(
            [
                mo.md("## Bucket Coverage"),
                mo.Html(result.regime_day_counts_df.round(6).to_html(index=False)),
                mo.md("## Collapsed Coverage"),
                mo.Html(result.regime_group_day_counts_df.round(6).to_html(index=False)),
            ]
        )
    day_count_view
    return


@app.cell
def _(error_message, mo, result):
    summary_tables_view = mo.md("")
    if not error_message and result is not None:
        summary_tables_view = mo.vstack(
            [
                mo.md("## Detailed Summary"),
                mo.Html(result.regime_summary_df.round(6).to_html(index=False)),
                mo.md("## Collapsed Summary"),
                mo.Html(result.regime_group_summary_df.round(6).to_html(index=False)),
            ]
        )
    summary_tables_view
    return


@app.cell
def _(error_message, mo, result):
    hypothesis_view = mo.md("")
    if not error_message and result is not None:
        hypothesis_view = mo.vstack(
            [
                mo.md("## Detailed Hypothesis Table"),
                mo.Html(result.regime_hypothesis_df.round(6).to_html(index=False)),
                mo.md("## Collapsed Hypothesis Table"),
                mo.Html(result.regime_group_hypothesis_df.round(6).to_html(index=False)),
            ]
        )
    hypothesis_view
    return


if __name__ == "__main__":
    app.run()
