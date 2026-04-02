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
app = marimo.App(width="full", app_title="Risk Adjusted Return Playground")


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
    from src.domains.analytics.risk_adjusted_return_research import (
        RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID,
        get_risk_adjusted_return_bundle_path_for_run_id,
        get_risk_adjusted_return_latest_bundle_path,
        load_risk_adjusted_return_research_bundle,
    )

    return (
        RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID,
        build_bundle_viewer_controls,
        ensure_bt_project_root_on_path,
        get_risk_adjusted_return_bundle_path_for_run_id,
        get_latest_bundle_defaults,
        get_risk_adjusted_return_latest_bundle_path,
        load_research_bundle_info,
        load_bundle_selection,
        load_risk_adjusted_return_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_risk_adjusted_return_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_risk_adjusted_return_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_risk_adjusted_return_research.py",
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_risk_adjusted_return_bundle_path_for_run_id,
    run_id,
    resolve_selected_bundle_path,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_risk_adjusted_return_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_research_bundle_info,
    load_bundle_selection,
    load_risk_adjusted_return_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_risk_adjusted_return_research_bundle,
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
    RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID,
    bundle_info,
    error_message,
    mo,
    result,
):
    _summary_view = mo.md("")
    if not error_message and result is not None:
        latest_text = (
            f"{result.latest_ratio:.4f}" if result.latest_ratio is not None else "N/A"
        )
        bundle_lines = []
        if bundle_info is not None:
            bundle_lines = [
                f"- Experiment: **{RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID}**",
                f"- Bundle run: **{bundle_info.run_id}**",
            ]
        _summary_view = mo.md(
            "\n".join(
                [
                    "## Risk Adjusted Return Playground",
                    "",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Seed: **{result.seed}**",
                    f"- Days: **{result.n_days}**",
                    f"- Lookback period: **{result.lookback_period}**",
                    f"- Ratio type: **{result.ratio_type}**",
                    f"- Latest valid ratio: **{latest_text}**",
                    "",
                    *bundle_lines,
                ]
            )
        )
    _summary_view
    return


@app.cell
def _(error_message, mo, pd, plt, result):
    _chart = mo.md("")
    if not error_message and result is not None:
        _series_df = result.series_df.copy()
        _series_df["date"] = pd.to_datetime(_series_df["date"])
        _fig, _axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
        _axes[0].plot(_series_df["date"], _series_df["close"], color="#1f77b4")
        _axes[0].set_title("Synthetic Close")
        _axes[0].grid(alpha=0.2)
        _axes[1].plot(
            _series_df["date"],
            _series_df["risk_adjusted_return"],
            color="#2ca02c",
        )
        _axes[1].set_title("Risk Adjusted Return")
        _axes[1].axhline(0.0, color="#666666", linewidth=1, alpha=0.6)
        _axes[1].grid(alpha=0.2)
        _fig.tight_layout()
        _chart = _fig
    _chart
    return


@app.cell
def _(error_message, mo, result):
    _table = mo.md("")
    if not error_message and result is not None:
        _table = mo.Html(result.series_df.tail(20).to_html(index=False))
    _table
    return


if __name__ == "__main__":
    app.run()
