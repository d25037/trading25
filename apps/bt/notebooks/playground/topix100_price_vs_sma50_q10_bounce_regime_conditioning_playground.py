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
    app_title="TOPIX100 SMA50 Q10 Bounce Regime Conditioning Playground",
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
    from src.domains.analytics.topix100_price_vs_sma_q10_bounce_regime_conditioning import (
        TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_REGIME_RESEARCH_EXPERIMENT_ID,
        get_topix100_price_vs_sma_q10_bounce_regime_conditioning_bundle_path_for_run_id,
        get_topix100_price_vs_sma_q10_bounce_regime_conditioning_latest_bundle_path,
        load_topix100_price_vs_sma_q10_bounce_regime_conditioning_research_bundle,
    )

    return (
        TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_REGIME_RESEARCH_EXPERIMENT_ID,
        build_bundle_viewer_controls,
        get_topix100_price_vs_sma_q10_bounce_regime_conditioning_bundle_path_for_run_id,
        get_latest_bundle_defaults,
        get_topix100_price_vs_sma_q10_bounce_regime_conditioning_latest_bundle_path,
        load_research_bundle_info,
        load_bundle_selection,
        load_topix100_price_vs_sma_q10_bounce_regime_conditioning_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix100_price_vs_sma_q10_bounce_regime_conditioning_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix100_price_vs_sma_q10_bounce_regime_conditioning_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix100_price_vs_sma_q10_bounce_regime_conditioning.py",
        docs_readme_path="apps/bt/docs/experiments/market-behavior/topix100-price-vs-sma-q10-bounce-regime-conditioning/README.md",
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix100_price_vs_sma_q10_bounce_regime_conditioning_bundle_path_for_run_id,
    run_id,
    resolve_selected_bundle_path,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix100_price_vs_sma_q10_bounce_regime_conditioning_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_research_bundle_info,
    load_bundle_selection,
    load_topix100_price_vs_sma_q10_bounce_regime_conditioning_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix100_price_vs_sma_q10_bounce_regime_conditioning_research_bundle,
        )
        error_message = None
    except Exception as exc:
        bundle_info = None
        result = None
        error_message = str(exc)
    return bundle_info, error_message, result


@app.cell
def _(
    TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_REGIME_RESEARCH_EXPERIMENT_ID,
    bundle_info,
    error_message,
    mo,
    result,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _view = mo.md(
            "\n".join(
                [
                    "## TOPIX100 SMA50 Q10 Bounce Regime Conditioning",
                    "",
                    f"- Experiment ID: **{TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_REGIME_RESEARCH_EXPERIMENT_ID}**",
                    *(
                        [
                            f"- Bundle run id: **{bundle_info.run_id}**",
                            f"- Bundle created at: **{bundle_info.created_at}**",
                            f"- Bundle path: **{bundle_info.bundle_dir}**",
                        ]
                        if bundle_info is not None
                        else []
                    ),
                    f"- Price feature: **{result.price_feature_label}**",
                    f"- Volume feature: **{result.volume_feature_label}**",
                    f"- Source mode: **{result.source_mode}**",
                    f"- Source detail: **{result.source_detail}**",
                    f"- Available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Effective analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Latest TOPIX100 constituent count: **{result.universe_constituent_count}**",
                    f"- Valid dates: **{result.valid_date_count}**",
                    "",
                    "This notebook conditions the `SMA50 Q10 Low + volume SMA 5/20 low` bounce slice on same-day `TOPIX close` and `NT ratio` regimes.",
                    "The main question is whether that thinner bounce setup strengthens in specific market states.",
                ]
            )
        )
    if error_message:
        _view = mo.md(f"## Error\n\n`{error_message}`")
    _view
    return


@app.cell
def _(error_message, mo, result):
    if error_message or result is None:
        regime_type_view = mo.md("")
        horizon_view = mo.md("")
        metric_view = mo.md("")
    else:
        regime_type_view = mo.ui.dropdown(
            options={
                "TOPIX Close": "topix_close",
                "NT Ratio": "nt_ratio",
            },
            value="topix_close",
            label="Regime Type",
        )
        horizon_view = mo.ui.dropdown(
            options={
                "t_plus_1": "t_plus_1",
                "t_plus_5": "t_plus_5",
                "t_plus_10": "t_plus_10",
            },
            value="t_plus_10",
            label="Horizon",
        )
        metric_view = mo.ui.dropdown(
            options={
                "future_return": "future_return",
                "future_close": "future_close",
            },
            value="future_return",
            label="Metric",
        )
    mo.hstack([regime_type_view, horizon_view, metric_view])
    return horizon_view, metric_view, regime_type_view


@app.cell
def _(error_message, mo, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _view = mo.vstack(
            [
                mo.md("### Regime Coverage"),
                mo.Html(result.regime_day_counts_df.round(6).to_html(index=False)),
                mo.md("### Regime Group Coverage"),
                mo.Html(result.regime_group_day_counts_df.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, horizon_view, mo, regime_type_view, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _summary = result.regime_summary_df[
            (result.regime_summary_df["regime_type"] == regime_type_view.value)
            & (result.regime_summary_df["horizon_key"] == horizon_view.value)
        ].copy()
        _group_summary = result.regime_group_summary_df[
            (result.regime_group_summary_df["regime_type"] == regime_type_view.value)
            & (result.regime_group_summary_df["horizon_key"] == horizon_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Regime Summary"),
                mo.Html(_summary.round(6).to_html(index=False)),
                mo.md("### Collapsed Group Summary"),
                mo.Html(_group_summary.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, horizon_view, metric_view, mo, regime_type_view, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _hypothesis = result.regime_hypothesis_df[
            (result.regime_hypothesis_df["regime_type"] == regime_type_view.value)
            & (result.regime_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.regime_hypothesis_df["metric_key"] == metric_view.value)
        ].copy()
        _group_hypothesis = result.regime_group_hypothesis_df[
            (result.regime_group_hypothesis_df["regime_type"] == regime_type_view.value)
            & (result.regime_group_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.regime_group_hypothesis_df["metric_key"] == metric_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Bucket-Level Hypotheses"),
                mo.Html(_hypothesis.round(6).to_html(index=False)),
                mo.md("### Group-Level Hypotheses"),
                mo.Html(_group_hypothesis.round(6).to_html(index=False)),
            ]
        )
    _view
    return


if __name__ == "__main__":
    app.run()
