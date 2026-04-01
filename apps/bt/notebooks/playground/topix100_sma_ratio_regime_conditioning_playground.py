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
    app_title="TOPIX100 SMA Ratio Regime Conditioning Playground",
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
    from src.domains.analytics.topix100_sma_ratio_regime_conditioning import (
        REGIME_TYPE_ORDER,
        get_topix100_sma_ratio_regime_conditioning_bundle_path_for_run_id,
        get_topix100_sma_ratio_regime_conditioning_latest_bundle_path,
        load_topix100_sma_ratio_regime_conditioning_research_bundle,
    )

    return (
        REGIME_TYPE_ORDER,
        build_bundle_viewer_controls,
        get_topix100_sma_ratio_regime_conditioning_bundle_path_for_run_id,
        get_latest_bundle_defaults,
        get_topix100_sma_ratio_regime_conditioning_latest_bundle_path,
        load_research_bundle_info,
        load_bundle_selection,
        load_topix100_sma_ratio_regime_conditioning_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix100_sma_ratio_regime_conditioning_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix100_sma_ratio_regime_conditioning_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix100_sma_ratio_regime_conditioning.py",
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix100_sma_ratio_regime_conditioning_bundle_path_for_run_id,
    run_id,
    resolve_selected_bundle_path,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix100_sma_ratio_regime_conditioning_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_research_bundle_info,
    load_bundle_selection,
    load_topix100_sma_ratio_regime_conditioning_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix100_sma_ratio_regime_conditioning_research_bundle,
        )
        error_message = None
    except Exception as exc:
        bundle_info = None
        result = None
        error_message = str(exc)
    return bundle_info, error_message, result


@app.cell
def _(REGIME_TYPE_ORDER, error_message, mo, result):
    if error_message or result is None:
        regime_type_view = mo.md("")
        horizon_view = mo.md("")
        metric_view = mo.md("")
    else:
        regime_type_view = mo.ui.dropdown(
            options={key: key for key in REGIME_TYPE_ORDER},
            value="topix_close",
            label="Regime Type",
        )
        horizon_view = mo.ui.dropdown(
            options={"t_plus_1": "t_plus_1", "t_plus_5": "t_plus_5", "t_plus_10": "t_plus_10"},
            value="t_plus_10",
            label="Horizon",
        )
        metric_view = mo.ui.dropdown(
            options={"future_return": "future_return", "future_close": "future_close"},
            value="future_return",
            label="Metric",
        )
    mo.hstack([regime_type_view, horizon_view, metric_view])
    return horizon_view, metric_view, regime_type_view


@app.cell
def _(bundle_info, error_message, mo, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _view = mo.md(
            "\n".join(
                [
                    "## TOPIX100 Price/Volume SMA Regime Conditioning",
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
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Latest TOPIX100 constituent count: **{result.universe_constituent_count}**",
                    f"- Valid dates after SMA warmup/filter: **{result.valid_date_count}**",
                    f"- Sigma thresholds: **{result.sigma_threshold_1} / {result.sigma_threshold_2}**",
                    "",
                    "Primary split is fixed to **price_sma_20_80 x volume_sma_20_80**.",
                    "Buckets are **Q1/Q10/Q4+Q5+Q6**, each split into **volume high/low** halves.",
                    "Regimes are collapsed to **Weak / Neutral / Strong** by combining the 5 sigma buckets.",
                ]
            )
        )
    if error_message:
        _view = mo.md(f"## Error\n\n{error_message}")
    _view
    return


@app.cell
def _(error_message, mo, pd, result):
    _view = mo.md("")
    if not error_message and result is not None:
        topix_stats_df = pd.DataFrame(
            [result.topix_close_stats.__dict__] if result.topix_close_stats else []
        )
        nt_stats_df = pd.DataFrame(
            [result.nt_ratio_stats.__dict__] if result.nt_ratio_stats else []
        )
        _view = mo.vstack(
            [
                mo.md("## Regime Stats"),
                mo.md("### TOPIX Close Return"),
                mo.Html(topix_stats_df.round(6).to_html(index=False)),
                mo.md("### NT Ratio Return"),
                mo.Html(nt_stats_df.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, mo, regime_type_view, result):
    if error_message or result is None:
        regime_group_view = mo.md("")
    else:
        bucket_df = result.regime_group_day_counts_df[
            result.regime_group_day_counts_df["regime_type"] == regime_type_view.value
        ].copy()
        options = {
            row["regime_group_key"]: row["regime_group_label"]
            for _, row in bucket_df.iterrows()
        }
        default_value = next(iter(options)) if options else ""
        regime_group_view = mo.ui.dropdown(
            options=options,
            value=default_value,
            label="Regime Group",
        )
    regime_group_view
    return (regime_group_view,)


@app.cell
def _(error_message, mo, regime_type_view, result):
    _view = mo.md("")
    if not error_message and result is not None:
        day_counts = result.regime_group_day_counts_df[
            result.regime_group_day_counts_df["regime_type"] == regime_type_view.value
        ].copy()
        _view = mo.vstack(
            [
                mo.md("## 3-Regime Day Counts"),
                mo.Html(day_counts.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, horizon_view, metric_view, mo, regime_type_view, result):
    _view = mo.md("")
    if not error_message and result is not None:
        summary_df = result.regime_group_summary_df[
            (result.regime_group_summary_df["regime_type"] == regime_type_view.value)
            & (result.regime_group_summary_df["horizon_key"] == horizon_view.value)
        ].copy()
        if metric_view.value == "future_close":
            summary_df = summary_df[
                [
                    "regime_group_label",
                    "combined_bucket_label",
                    "date_count",
                    "mean_group_size",
                    "mean_event_close",
                    "mean_future_close",
                ]
            ]
        else:
            summary_df = summary_df[
                [
                    "regime_group_label",
                    "combined_bucket_label",
                    "date_count",
                    "mean_group_size",
                    "mean_regime_return",
                    "mean_future_return",
                    "median_future_return",
                    "std_future_return",
                ]
            ]
        _view = mo.vstack(
            [
                mo.md("## 3-Regime Bucket Summary"),
                mo.Html(summary_df.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(
    error_message,
    horizon_view,
    metric_view,
    mo,
    regime_group_view,
    regime_type_view,
    result,
):
    _view = mo.md("")
    if not error_message and result is not None:
        hypothesis_df = result.regime_group_hypothesis_df[
            (result.regime_group_hypothesis_df["regime_type"] == regime_type_view.value)
            & (result.regime_group_hypothesis_df["regime_group_key"] == regime_group_view.value)
            & (result.regime_group_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.regime_group_hypothesis_df["metric_key"] == metric_view.value)
        ].copy()
        pairwise_df = result.regime_group_pairwise_significance_df[
            (result.regime_group_pairwise_significance_df["regime_type"] == regime_type_view.value)
            & (
                result.regime_group_pairwise_significance_df["regime_group_key"]
                == regime_group_view.value
            )
            & (result.regime_group_pairwise_significance_df["horizon_key"] == horizon_view.value)
            & (result.regime_group_pairwise_significance_df["metric_key"] == metric_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md("## Selected Regime Group Hypotheses"),
                mo.Html(hypothesis_df.round(6).to_html(index=False)),
                mo.md("## Selected Regime Group Pairwise Table"),
                mo.Html(pairwise_df.round(6).to_html(index=False)),
            ]
        )
    _view
    return


if __name__ == "__main__":
    app.run()
