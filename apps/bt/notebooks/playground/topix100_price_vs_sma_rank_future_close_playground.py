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
app = marimo.App(
    width="full",
    app_title="TOPIX100 Price vs SMA Rank / Future Close Playground",
)


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
    from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
        PRICE_FEATURE_LABEL_MAP,
        PRICE_FEATURE_ORDER,
        PRIMARY_VOLUME_FEATURE,
        TOPIX100_PRICE_VS_SMA_RESEARCH_EXPERIMENT_ID,
        VOLUME_FEATURE_LABEL_MAP,
        VOLUME_FEATURE_ORDER,
        VOLUME_SMA_WINDOW_ORDER,
        get_topix100_price_vs_sma_rank_future_close_bundle_path_for_run_id,
        get_topix100_price_vs_sma_rank_future_close_latest_bundle_path,
        load_topix100_price_vs_sma_rank_future_close_research_bundle,
    )

    return (
        PRICE_FEATURE_LABEL_MAP,
        PRICE_FEATURE_ORDER,
        PRIMARY_VOLUME_FEATURE,
        TOPIX100_PRICE_VS_SMA_RESEARCH_EXPERIMENT_ID,
        build_bundle_viewer_controls,
        VOLUME_FEATURE_LABEL_MAP,
        VOLUME_FEATURE_ORDER,
        VOLUME_SMA_WINDOW_ORDER,
        get_latest_bundle_defaults,
        get_topix100_price_vs_sma_rank_future_close_bundle_path_for_run_id,
        get_topix100_price_vs_sma_rank_future_close_latest_bundle_path,
        load_research_bundle_info,
        load_bundle_selection,
        load_topix100_price_vs_sma_rank_future_close_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix100_price_vs_sma_rank_future_close_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix100_price_vs_sma_rank_future_close_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix100_price_vs_sma_rank_future_close.py",
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix100_price_vs_sma_rank_future_close_bundle_path_for_run_id,
    run_id,
    resolve_selected_bundle_path,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix100_price_vs_sma_rank_future_close_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_research_bundle_info,
    load_bundle_selection,
    load_topix100_price_vs_sma_rank_future_close_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix100_price_vs_sma_rank_future_close_research_bundle,
        )
        error_message = None
    except Exception as exc:
        bundle_info = None
        result = None
        error_message = str(exc)
    return bundle_info, error_message, result


@app.cell
def _(
    PRICE_FEATURE_LABEL_MAP,
    PRICE_FEATURE_ORDER,
    TOPIX100_PRICE_VS_SMA_RESEARCH_EXPERIMENT_ID,
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    bundle_info,
    error_message,
    mo,
    parsed_inputs,
    result,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _labels = [
            f"`{feature}` = **{PRICE_FEATURE_LABEL_MAP[feature]}**"
            for feature in PRICE_FEATURE_ORDER
        ]
        _volume_labels = [
            f"`{feature}` = **{VOLUME_FEATURE_LABEL_MAP[feature]}**"
            for feature in VOLUME_FEATURE_ORDER
        ]
        _view = mo.md(
            "\n".join(
                [
                    "## TOPIX100 Price vs SMA Rank Research",
                    "",
                    f"- Experiment ID: **{TOPIX100_PRICE_VS_SMA_RESEARCH_EXPERIMENT_ID}**",
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
                    f"- Available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Effective analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Latest TOPIX100 constituent count: **{result.topix100_constituent_count}**",
                    f"- Stock-day rows after SMA warmup/filter: **{result.stock_day_count}**",
                    f"- Valid dates: **{result.valid_date_count}**",
                    "",
                    "Price features:",
                    *[f"- {_label}" for _label in _labels],
                    "",
                    "Volume lenses:",
                    *[f"- {_label}" for _label in _volume_labels],
                    "",
                    "Within each price bucket, names are split into high / low halves by the selected volume lens.",
                ]
            )
        )
    if error_message:
        _view = mo.md(f"## Error\n\n`{error_message}`")
    _view
    return


@app.cell
def _(
    PRICE_FEATURE_LABEL_MAP,
    PRICE_FEATURE_ORDER,
    PRIMARY_VOLUME_FEATURE,
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    error_message,
    mo,
    result,
):
    if error_message or result is None:
        price_feature_view = mo.md("")
        volume_feature_view = mo.md("")
        horizon_view = mo.md("")
        metric_view = mo.md("")
    else:
        price_feature_view = mo.ui.dropdown(
            options={
                feature: PRICE_FEATURE_LABEL_MAP[feature]
                for feature in PRICE_FEATURE_ORDER
            },
            value="price_vs_sma_50_gap",
            label="Price Feature",
        )
        volume_feature_view = mo.ui.dropdown(
            options={
                feature: VOLUME_FEATURE_LABEL_MAP[feature]
                for feature in VOLUME_FEATURE_ORDER
            },
            value=PRIMARY_VOLUME_FEATURE,
            label="Volume Lens",
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
    mo.hstack([price_feature_view, volume_feature_view, horizon_view, metric_view])
    return horizon_view, metric_view, price_feature_view, volume_feature_view


@app.cell
def _(error_message, mo, price_feature_view, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _decile_df = result.ranking_feature_summary_df[
            result.ranking_feature_summary_df["ranking_feature"] == price_feature_view.value
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Price Feature Deciles"),
                mo.Html(_decile_df.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, horizon_view, metric_view, mo, price_feature_view, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _global_df = result.global_significance_df[
            (result.global_significance_df["ranking_feature"] == price_feature_view.value)
            & (result.global_significance_df["horizon_key"] == horizon_view.value)
            & (result.global_significance_df["metric_key"] == metric_view.value)
        ].copy()
        _pairwise_df = result.pairwise_significance_df[
            (result.pairwise_significance_df["ranking_feature"] == price_feature_view.value)
            & (result.pairwise_significance_df["horizon_key"] == horizon_view.value)
            & (result.pairwise_significance_df["metric_key"] == metric_view.value)
            & (result.pairwise_significance_df["left_decile"] == "Q1")
            & (result.pairwise_significance_df["right_decile"] == "Q10")
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Decile Significance"),
                mo.Html(_global_df.round(6).to_html(index=False)),
                mo.md("### Q1 vs Q10"),
                mo.Html(_pairwise_df.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, horizon_view, metric_view, mo, price_feature_view, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _summary_df = result.price_bucket_summary_df[
            (result.price_bucket_summary_df["ranking_feature"] == price_feature_view.value)
            & (result.price_bucket_summary_df["horizon_key"] == horizon_view.value)
        ].copy()
        _hypothesis_df = result.group_hypothesis_df[
            (result.group_hypothesis_df["ranking_feature"] == price_feature_view.value)
            & (result.group_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.group_hypothesis_df["metric_key"] == metric_view.value)
        ].copy()
        _pairwise_df = result.price_bucket_pairwise_significance_df[
            (result.price_bucket_pairwise_significance_df["ranking_feature"] == price_feature_view.value)
            & (result.price_bucket_pairwise_significance_df["horizon_key"] == horizon_view.value)
            & (result.price_bucket_pairwise_significance_df["metric_key"] == metric_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Q1 / Q4+Q5+Q6 / Q10"),
                mo.Html(_summary_df.round(6).to_html(index=False)),
                mo.md("### Direct Hypotheses"),
                mo.Html(_hypothesis_df.round(6).to_html(index=False)),
                mo.md("### Group Pairwise Detail"),
                mo.Html(_pairwise_df.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    error_message,
    horizon_view,
    metric_view,
    mo,
    pd,
    price_feature_view,
    result,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _comparison_df = result.split_hypothesis_df[
            (result.split_hypothesis_df["price_feature"] == price_feature_view.value)
            & (result.split_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.split_hypothesis_df["metric_key"] == metric_view.value)
        ].copy()
        _comparison_df["volume_feature"] = pd.Categorical(
            _comparison_df["volume_feature"],
            categories=list(VOLUME_FEATURE_ORDER),
            ordered=True,
        )
        _comparison_df = _comparison_df.sort_values(
            ["volume_feature", "hypothesis_label"]
        ).reset_index(drop=True)
        _comparison_df["volume_feature"] = _comparison_df["volume_feature"].astype(str)
        _comparison_df["volume_feature_label"] = _comparison_df["volume_feature"].map(
            VOLUME_FEATURE_LABEL_MAP
        )
        _view = mo.vstack(
            [
                mo.md("### Volume Lens Comparison"),
                mo.Html(_comparison_df.round(6).to_html(index=False)),
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
    price_feature_view,
    result,
    volume_feature_view,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _summary_df = result.price_volume_split_summary_df[
            (result.price_volume_split_summary_df["price_feature"] == price_feature_view.value)
            & (
                result.price_volume_split_summary_df["volume_feature"]
                == volume_feature_view.value
            )
            & (result.price_volume_split_summary_df["horizon_key"] == horizon_view.value)
        ].copy()
        _hypothesis_df = result.split_hypothesis_df[
            (result.split_hypothesis_df["price_feature"] == price_feature_view.value)
            & (result.split_hypothesis_df["volume_feature"] == volume_feature_view.value)
            & (result.split_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.split_hypothesis_df["metric_key"] == metric_view.value)
        ].copy()
        _pairwise_df = result.price_volume_split_pairwise_significance_df[
            (result.price_volume_split_pairwise_significance_df["price_feature"] == price_feature_view.value)
            & (
                result.price_volume_split_pairwise_significance_df["volume_feature"]
                == volume_feature_view.value
            )
            & (result.price_volume_split_pairwise_significance_df["horizon_key"] == horizon_view.value)
            & (result.price_volume_split_pairwise_significance_df["metric_key"] == metric_view.value)
            & (
                result.price_volume_split_pairwise_significance_df["left_combined_bucket"].isin(
                    [
                        "q1_volume_high",
                        "q1_volume_low",
                        "q10_volume_low",
                    ]
                )
                | result.price_volume_split_pairwise_significance_df["right_combined_bucket"].isin(
                    [
                        "middle_volume_high",
                        "middle_volume_low",
                        "q10_volume_high",
                    ]
                )
            )
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Price Bucket x Volume Half-Split"),
                mo.Html(_summary_df.round(6).to_html(index=False)),
                mo.md("### Volume-Split Hypotheses"),
                mo.Html(_hypothesis_df.round(6).to_html(index=False)),
                mo.md("### Selected Pairwise Detail"),
                mo.Html(_pairwise_df.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(PRICE_FEATURE_LABEL_MAP, error_message, mo, plt, price_feature_view, result):
    _chart = mo.md("")
    if not error_message and result is not None:
        _summary_df = result.decile_future_summary_df[
            result.decile_future_summary_df["ranking_feature"] == price_feature_view.value
        ].copy()
        _fig, _ax = plt.subplots(figsize=(10, 4.5))
        _x_positions = [1, 5, 10]
        for _decile_key, _color in (("Q1", "#0f766e"), ("Q10", "#dc2626")):
            _decile_df = _summary_df[
                _summary_df["feature_decile"] == _decile_key
            ].copy()
            _decile_df = _decile_df.set_index("horizon_key").reindex(
                ["t_plus_1", "t_plus_5", "t_plus_10"]
            )
            _ax.plot(
                _x_positions,
                _decile_df["mean_future_return"].tolist(),
                marker="o",
                linewidth=2,
                label=_decile_key,
                color=_color,
            )
        _ax.set_title(
            f"Mean Future Return by Decile ({PRICE_FEATURE_LABEL_MAP[price_feature_view.value]})"
        )
        _ax.set_xlabel("Trading Days Ahead")
        _ax.set_ylabel("Mean Future Return")
        _ax.set_xticks(_x_positions)
        _ax.grid(alpha=0.2)
        _ax.legend()
        _fig.tight_layout()
        _chart = _fig
    _chart
    return


if __name__ == "__main__":
    app.run()
