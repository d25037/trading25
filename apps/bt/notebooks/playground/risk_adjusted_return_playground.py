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
    project_root = Path.cwd()
    if project_root.name == "playground":
        project_root = project_root.parent.parent
    elif project_root.name == "notebooks":
        project_root = project_root.parent

    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from src.domains.analytics.research_bundle import load_research_bundle_info
    from src.domains.analytics.risk_adjusted_return_research import (
        RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID,
        get_risk_adjusted_return_bundle_path_for_run_id,
        get_risk_adjusted_return_latest_bundle_path,
        load_risk_adjusted_return_research_bundle,
        run_risk_adjusted_return_research,
    )

    return (
        RISK_ADJUSTED_RETURN_RESEARCH_EXPERIMENT_ID,
        get_risk_adjusted_return_bundle_path_for_run_id,
        get_risk_adjusted_return_latest_bundle_path,
        load_research_bundle_info,
        load_risk_adjusted_return_research_bundle,
        run_risk_adjusted_return_research,
    )


@app.cell
def _(get_risk_adjusted_return_latest_bundle_path):
    try:
        latest_bundle_path = get_risk_adjusted_return_latest_bundle_path()
    except Exception:
        latest_bundle_path = None
    latest_run_id = latest_bundle_path.name if latest_bundle_path else ""
    latest_bundle_path_str = str(latest_bundle_path) if latest_bundle_path else ""
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(latest_bundle_path_str, latest_run_id, mo):
    mode = mo.ui.dropdown(
        options={"bundle": "Load Existing Bundle", "recompute": "Run Fresh Analysis"},
        value="bundle",
        label="Mode",
    )
    run_id = mo.ui.text(value=latest_run_id, label="Run ID")
    bundle_path = mo.ui.text(value=latest_bundle_path_str, label="Bundle Path (optional)")
    lookback = mo.ui.slider(20, 180, value=60, step=5, label="Lookback Period")
    ratio_type = mo.ui.dropdown(
        options=["sortino", "sharpe"],
        value="sortino",
        label="Ratio Type",
    )
    seed = mo.ui.number(value=42, start=0, stop=999999, step=1, label="Seed")
    n_days = mo.ui.slider(252, 1260, value=504, step=21, label="Days")
    recompute_controls = mo.vstack([lookback, ratio_type, seed, n_days])
    mo.vstack(
        [
            mo.md(
                "\n".join(
                    [
                        "### Research Runner",
                        "",
                        "- Default path is **viewer-first**: load an existing bundle by `Run ID` or `Bundle Path`.",
                        "- Fresh analysis only runs when `Mode = Run Fresh Analysis`.",
                        "- Canonical runner: `apps/bt/scripts/research/run_risk_adjusted_return_research.py`",
                    ]
                )
            ),
            mo.hstack([mode, run_id]),
            bundle_path,
            recompute_controls if mode.value == "recompute" else mo.md(""),
        ]
    )
    return bundle_path, lookback, mode, n_days, ratio_type, run_id, seed


@app.cell
def _(
    bundle_path,
    get_risk_adjusted_return_bundle_path_for_run_id,
    lookback,
    mode,
    n_days,
    ratio_type,
    run_id,
    seed,
):
    run_id_value = run_id.value.strip()
    bundle_path_value = bundle_path.value.strip()
    resolved_bundle_path = bundle_path_value
    if not resolved_bundle_path and run_id_value:
        resolved_bundle_path = str(
            get_risk_adjusted_return_bundle_path_for_run_id(run_id_value)
        )
    parsed_inputs = {
        "mode": mode.value,
        "selected_bundle_path": resolved_bundle_path or None,
        "lookback_period": int(lookback.value),
        "ratio_type": ratio_type.value,
        "seed": int(seed.value),
        "n_days": int(n_days.value),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_research_bundle_info,
    load_risk_adjusted_return_research_bundle,
    parsed_inputs,
    run_risk_adjusted_return_research,
):
    try:
        if parsed_inputs["mode"] == "bundle":
            selected_bundle_path = parsed_inputs["selected_bundle_path"]
            if not selected_bundle_path:
                raise ValueError(
                    "Set a bundle path or run id, or switch Mode to Run Fresh Analysis."
                )
            bundle_info = load_research_bundle_info(selected_bundle_path)
            result = load_risk_adjusted_return_research_bundle(selected_bundle_path)
        else:
            bundle_info = None
            result = run_risk_adjusted_return_research(
                lookback_period=parsed_inputs["lookback_period"],
                ratio_type=parsed_inputs["ratio_type"],
                seed=parsed_inputs["seed"],
                n_days=parsed_inputs["n_days"],
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
