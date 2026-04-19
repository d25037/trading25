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
    app_title="TOPIX100 SMA Ratio Rank / Future Close Playground",
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

    _project_root = ensure_bt_project_root_on_path(Path.cwd(), sys.path)
    from src.domains.analytics.research_bundle import load_research_bundle_info
    from src.domains.analytics.topix100_sma_ratio_rank_future_close import (
        DECILE_ORDER,
        HORIZON_ORDER,
        RANKING_FEATURE_ORDER,
        get_topix100_sma_ratio_rank_future_close_bundle_path_for_run_id,
        get_topix100_sma_ratio_rank_future_close_latest_bundle_path,
        load_topix100_sma_ratio_rank_future_close_research_bundle,
    )

    return (
        DECILE_ORDER,
        HORIZON_ORDER,
        RANKING_FEATURE_ORDER,
        build_bundle_viewer_controls,
        get_topix100_sma_ratio_rank_future_close_bundle_path_for_run_id,
        get_latest_bundle_defaults,
        get_topix100_sma_ratio_rank_future_close_latest_bundle_path,
        load_research_bundle_info,
        load_bundle_selection,
        load_topix100_sma_ratio_rank_future_close_research_bundle,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix100_sma_ratio_rank_future_close_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix100_sma_ratio_rank_future_close_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix100_sma_ratio_rank_future_close.py",
        docs_readme_path="apps/bt/docs/experiments/market-behavior/topix100-sma-ratio-lightgbm/README.md",
        extra_note_lines=[
            "- This notebook is baseline-bundle viewer only.",
            "- Reproduce the LightGBM study with `apps/bt/scripts/research/run_topix100_sma_ratio_rank_future_close_lightgbm.py`.",
        ],
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix100_sma_ratio_rank_future_close_bundle_path_for_run_id,
    run_id,
    resolve_selected_bundle_path,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix100_sma_ratio_rank_future_close_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_research_bundle_info,
    load_bundle_selection,
    load_topix100_sma_ratio_rank_future_close_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix100_sma_ratio_rank_future_close_research_bundle,
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
    result,
):
    lightgbm_result = None
    lightgbm_error_message = None
    if result is not None:
        lightgbm_error_message = (
            "LightGBM side analysis is disabled in viewer-only notebook mode."
        )
    return lightgbm_error_message, lightgbm_result


@app.cell
def _(error_message, lightgbm_error_message, mo, result):
    _note = mo.md("")
    if not error_message and result is not None and lightgbm_error_message:
        _note = mo.md(
            "\n".join(
                [
                    "## LightGBM Research Note",
                    "",
                    f"- `{lightgbm_error_message}`",
                    "- Baseline notebook sections remain available below.",
                ]
            )
        )
    _note
    return


@app.cell
def _(DECILE_ORDER, HORIZON_ORDER, RANKING_FEATURE_ORDER, error_message, mo, result):
    if error_message or result is None:
        ranking_feature_view = mo.md("")
        horizon_view = mo.md("")
        metric_view = mo.md("")
    else:
        ranking_feature_view = mo.ui.dropdown(
            options={key: key for key in RANKING_FEATURE_ORDER},
            value="price_sma_20_80",
            label="Ranking Feature",
        )
        horizon_view = mo.ui.dropdown(
            options={key: key for key in HORIZON_ORDER},
            value="t_plus_5",
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
    mo.hstack([ranking_feature_view, horizon_view, metric_view])
    return DECILE_ORDER, horizon_view, metric_view, ranking_feature_view


@app.cell
def _(bundle_info, error_message, mo, parsed_inputs, result):
    _summary_view = mo.md("")
    if not error_message and result is not None:
        _summary_view = mo.md(
            "\n".join(
                [
                    "## TOPIX100 SMA Ratio Rank / Future Close Research",
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
                    f"- Available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Effective analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Latest TOPIX100 constituent count: **{result.topix100_constituent_count}**",
                    f"- Stock-day rows after SMA warmup/filter: **{result.stock_day_count}**",
                    f"- Ranked events (`stock-day x 6 features`): **{result.ranked_event_count}**",
                    f"- Valid dates: **{result.valid_date_count}**",
                    f"- Discovery / validation split: **<= {result.discovery_end_date} / >= {result.validation_start_date}**",
                    "",
                    "`Q1` is the highest SMA-ratio decile and `Q10` is the lowest for the selected ranking feature.",
                    "`future_close` is provided as requested, but `future_return` is the cleaner interpretation.",
                ]
            )
        )
    _summary_view
    return


@app.cell
def _(HORIZON_ORDER, error_message, mo, result):
    if error_message or result is None:
        selection_horizon_view = mo.md("")
    else:
        selection_horizon_view = mo.ui.dropdown(
            options={key: key for key in HORIZON_ORDER},
            value="t_plus_10",
            label="Selection Horizon",
        )
    selection_horizon_view
    return (selection_horizon_view,)


@app.cell
def _(error_message, mo, result, selection_horizon_view):
    _view = mo.md("")
    if not error_message and result is not None:
        _selected_features = result.selected_feature_df[
            result.selected_feature_df["horizon_key"] == selection_horizon_view.value
        ].copy()
        _feature_selection = result.feature_selection_df[
            result.feature_selection_df["horizon_key"] == selection_horizon_view.value
        ].copy()
        _view = mo.vstack(
            [
                mo.md("## Discovery / Validation Feature Selection"),
                mo.md("### Selected Price / Volume Features"),
                mo.Html(_selected_features.round(6).to_html(index=False)),
                mo.md("### All Feature Scores"),
                mo.Html(_feature_selection.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, mo, result, selection_horizon_view):
    _view = mo.md("")
    if not error_message and result is not None:
        _candidates = result.composite_candidate_df[
            result.composite_candidate_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        _selected = result.selected_composite_df[
            result.selected_composite_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        _view = mo.vstack(
            [
                mo.md("## Price x Volume Composite Ranking"),
                mo.md("### Selected Composite"),
                mo.Html(_selected.round(6).to_html(index=False)),
                mo.md("### Composite Method Candidates"),
                mo.Html(_candidates.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, mo, result, selection_horizon_view):
    _view = mo.md("")
    if not error_message and result is not None:
        _selected = result.selected_composite_df[
            result.selected_composite_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        if not _selected.empty:
            _ranking_feature = _selected.iloc[0]["ranking_feature"]
            _ranking_summary = result.selected_composite_ranking_summary_df[
                result.selected_composite_ranking_summary_df["ranking_feature"]
                == _ranking_feature
            ].copy()
            _future_summary = result.selected_composite_future_summary_df[
                result.selected_composite_future_summary_df["ranking_feature"]
                == _ranking_feature
            ].copy()
            _view = mo.vstack(
                [
                    mo.md("### Selected Composite Deciles"),
                    mo.Html(_ranking_summary.round(6).to_html(index=False)),
                    mo.md("### Selected Composite Future Summary"),
                    mo.Html(_future_summary.round(6).to_html(index=False)),
                ]
            )
    _view
    return


@app.cell
def _(
    error_message,
    metric_view,
    mo,
    result,
    selection_horizon_view,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _selected = result.selected_composite_df[
            result.selected_composite_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        if not _selected.empty:
            _ranking_feature = _selected.iloc[0]["ranking_feature"]
            _global = result.selected_composite_global_significance_df[
                (result.selected_composite_global_significance_df["ranking_feature"] == _ranking_feature)
                & (result.selected_composite_global_significance_df["metric_key"] == metric_view.value)
            ].copy()
            _pairwise = result.selected_composite_pairwise_significance_df[
                (result.selected_composite_pairwise_significance_df["ranking_feature"] == _ranking_feature)
                & (result.selected_composite_pairwise_significance_df["metric_key"] == metric_view.value)
                & (result.selected_composite_pairwise_significance_df["left_decile"] == "Q1")
                & (result.selected_composite_pairwise_significance_df["right_decile"] == "Q10")
            ].copy()
            _view = mo.vstack(
                [
                    mo.md("### Selected Composite Significance"),
                    mo.Html(_global.round(6).to_html(index=False)),
                    mo.md("### Selected Composite Pairwise (Q1 vs Q10)"),
                    mo.Html(_pairwise.round(6).to_html(index=False)),
                ]
            )
    _view
    return


@app.cell
def _(error_message, mo, plt, result, selection_horizon_view):
    _chart = mo.md("")
    if not error_message and result is not None:
        _selected = result.selected_composite_df[
            result.selected_composite_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        if not _selected.empty:
            _ranking_feature = _selected.iloc[0]["ranking_feature"]
            _summary = result.selected_composite_future_summary_df[
                result.selected_composite_future_summary_df["ranking_feature"]
                == _ranking_feature
            ].copy()
            _x_positions = [1, 5, 10]
            _fig, _ax = plt.subplots(figsize=(10, 4.5))
            for _decile_key, _color in (("Q1", "#0f766e"), ("Q10", "#dc2626")):
                _decile_df = _summary[
                    _summary["feature_decile"] == _decile_key
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
                f"Selected Composite Mean Future Return ({selection_horizon_view.value}, Q1 vs Q10)"
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


@app.cell
def _(
    error_message,
    lightgbm_error_message,
    lightgbm_result,
    mo,
    selection_horizon_view,
):
    _view = mo.md("")
    if (
        not error_message
        and not lightgbm_error_message
        and lightgbm_result is not None
    ):
        _walkforward = lightgbm_result.walkforward
        _config = _walkforward.split_config_df.copy()
        _model = _walkforward.selected_model_df[
            _walkforward.selected_model_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        _gate = _walkforward.exploratory_gate_df[
            _walkforward.exploratory_gate_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        _comparison = _walkforward.comparison_summary_df[
            _walkforward.comparison_summary_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        _view = mo.vstack(
            [
                mo.md("## Walk-Forward LightGBM vs Baseline"),
                mo.md("### Split Config"),
                mo.Html(_config.round(6).to_html(index=False)),
                mo.md("### LightGBM OOS Model Summary"),
                mo.Html(_model.round(6).to_html(index=False)),
                mo.md("### Exploratory Gate"),
                mo.Html(_gate.round(6).to_html(index=False)),
                mo.md("### Baseline vs LightGBM OOS"),
                mo.Html(_comparison.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(
    error_message,
    lightgbm_error_message,
    lightgbm_result,
    mo,
    selection_horizon_view,
):
    _view = mo.md("")
    if (
        not error_message
        and not lightgbm_error_message
        and lightgbm_result is not None
    ):
        _walkforward = lightgbm_result.walkforward
        _coverage = _walkforward.split_coverage_df[
            _walkforward.split_coverage_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        _selected_features = _walkforward.baseline_selected_feature_df[
            _walkforward.baseline_selected_feature_df["horizon_key"]
            == selection_horizon_view.value
        ].copy()
        _selected_composites = _walkforward.baseline_selected_composite_df[
            _walkforward.baseline_selected_composite_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Split Coverage"),
                mo.Html(_coverage.round(6).to_html(index=False)),
                mo.md("### Split-Selected Baseline Features"),
                mo.Html(_selected_features.round(6).to_html(index=False)),
                mo.md("### Split-Selected Baseline Composite"),
                mo.Html(_selected_composites.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(
    error_message,
    lightgbm_error_message,
    lightgbm_result,
    metric_view,
    mo,
    selection_horizon_view,
):
    _view = mo.md("")
    if (
        not error_message
        and not lightgbm_error_message
        and lightgbm_result is not None
    ):
        _walkforward = lightgbm_result.walkforward
        _selected = _walkforward.selected_model_df[
            _walkforward.selected_model_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        if not _selected.empty:
            _ranking_feature = _selected.iloc[0]["ranking_feature"]
            _ranking_summary = _walkforward.ranking_feature_summary_df[
                _walkforward.ranking_feature_summary_df["ranking_feature"]
                == _ranking_feature
            ].copy()
            _future_summary = _walkforward.decile_future_summary_df[
                _walkforward.decile_future_summary_df["ranking_feature"]
                == _ranking_feature
            ].copy()
            _view = mo.vstack(
                [
                    mo.md("### Walk-Forward LightGBM Deciles"),
                    mo.Html(_ranking_summary.round(6).to_html(index=False)),
                    mo.md("### Walk-Forward LightGBM Future Summary"),
                    mo.Html(_future_summary.round(6).to_html(index=False)),
                ]
            )
    _view
    return


@app.cell
def _(
    error_message,
    lightgbm_error_message,
    lightgbm_result,
    metric_view,
    mo,
    plt,
    selection_horizon_view,
):
    _view = mo.md("")
    if (
        not error_message
        and not lightgbm_error_message
        and lightgbm_result is not None
    ):
        _walkforward = lightgbm_result.walkforward
        _selected = _walkforward.selected_model_df[
            _walkforward.selected_model_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        _diagnostic_block = mo.md("")
        if not _selected.empty:
            _ranking_feature = _selected.iloc[0]["ranking_feature"]
            _global = _walkforward.global_significance_df[
                (_walkforward.global_significance_df["ranking_feature"] == _ranking_feature)
                & (_walkforward.global_significance_df["metric_key"] == metric_view.value)
                & (_walkforward.global_significance_df["horizon_key"] == selection_horizon_view.value)
            ].copy()
            _pairwise = _walkforward.pairwise_significance_df[
                (_walkforward.pairwise_significance_df["ranking_feature"] == _ranking_feature)
                & (_walkforward.pairwise_significance_df["metric_key"] == metric_view.value)
                & (_walkforward.pairwise_significance_df["horizon_key"] == selection_horizon_view.value)
                & (_walkforward.pairwise_significance_df["left_decile"] == "Q1")
                & (_walkforward.pairwise_significance_df["right_decile"] == "Q10")
            ].copy()
            _diagnostic_parts = [
                mo.md("### Walk-Forward LightGBM Significance"),
                mo.Html(_global.round(6).to_html(index=False)),
                mo.md("### Walk-Forward LightGBM Pairwise (Q1 vs Q10)"),
                mo.Html(_pairwise.round(6).to_html(index=False)),
            ]
            if lightgbm_result.diagnostic is not None:
                _fixed_comparison = lightgbm_result.diagnostic.comparison_summary_df[
                    lightgbm_result.diagnostic.comparison_summary_df[
                        "selected_horizon_key"
                    ]
                    == selection_horizon_view.value
                ].copy()
                _diagnostic_parts.extend(
                    [
                        mo.md("### Fixed-Split Diagnostic"),
                        mo.Html(_fixed_comparison.round(6).to_html(index=False)),
                    ]
                )
            elif lightgbm_result.diagnostic_error_message:
                _diagnostic_parts.extend(
                    [
                        mo.md("### Fixed-Split Diagnostic"),
                        mo.md(f"- `{lightgbm_result.diagnostic_error_message}`"),
                    ]
                )
            _diagnostic_block = mo.vstack(_diagnostic_parts)
        _split_spread = _walkforward.split_spread_df[
            _walkforward.split_spread_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        _importance = _walkforward.feature_importance_df[
            _walkforward.feature_importance_df["selected_horizon_key"]
            == selection_horizon_view.value
        ].copy()
        _fig, _ax = plt.subplots(figsize=(10, 4.5))
        _ax.barh(
            _importance["feature_label"].tolist()[::-1],
            _importance["mean_importance_gain"].tolist()[::-1],
            color="#2563eb",
        )
        _ax.set_title(
            f"Walk-Forward Mean Gain Importance ({selection_horizon_view.value})"
        )
        _ax.set_xlabel("Mean Gain Importance")
        _ax.grid(axis="x", alpha=0.2)
        _fig.tight_layout()
        _view = mo.vstack(
            [
                _diagnostic_block,
                mo.md("### Split-by-Split OOS Spread"),
                mo.Html(_split_spread.round(6).to_html(index=False)),
                mo.md("### Mean Gain Importance"),
                mo.Html(_importance.round(6).to_html(index=False)),
                _fig,
            ]
        )
    _view
    return


@app.cell
def _(error_message, mo, ranking_feature_view, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _table = result.ranking_feature_summary_df[
            result.ranking_feature_summary_df["ranking_feature"]
            == ranking_feature_view.value
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Ranking Feature Deciles"),
                mo.Html(_table.round(6).to_html(index=False)),
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
    ranking_feature_view,
    result,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _global = result.global_significance_df[
            (result.global_significance_df["ranking_feature"] == ranking_feature_view.value)
            & (result.global_significance_df["horizon_key"] == horizon_view.value)
            & (result.global_significance_df["metric_key"] == metric_view.value)
        ].copy()
        _pairwise = result.pairwise_significance_df[
            (result.pairwise_significance_df["ranking_feature"] == ranking_feature_view.value)
            & (result.pairwise_significance_df["horizon_key"] == horizon_view.value)
            & (result.pairwise_significance_df["metric_key"] == metric_view.value)
            & (result.pairwise_significance_df["left_decile"] == "Q1")
            & (result.pairwise_significance_df["right_decile"] == "Q10")
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Significance"),
                mo.Html(_global.round(6).to_html(index=False)),
                mo.md("### Pairwise Significance (Q1 vs Q10)"),
                mo.Html(_pairwise.round(6).to_html(index=False)),
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
    ranking_feature_view,
    result,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _summary = result.extreme_vs_middle_summary_df[
            (result.extreme_vs_middle_summary_df["ranking_feature"] == ranking_feature_view.value)
            & (result.extreme_vs_middle_summary_df["horizon_key"] == horizon_view.value)
        ].copy()
        _significance = result.extreme_vs_middle_significance_df[
            (result.extreme_vs_middle_significance_df["ranking_feature"] == ranking_feature_view.value)
            & (result.extreme_vs_middle_significance_df["horizon_key"] == horizon_view.value)
            & (result.extreme_vs_middle_significance_df["metric_key"] == metric_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Q1+Q10 vs Q4+Q5+Q6"),
                mo.Html(_summary.round(6).to_html(index=False)),
                mo.md("### Q1+Q10 vs Q4+Q5+Q6 Significance"),
                mo.Html(_significance.round(6).to_html(index=False)),
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
    result,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _summary = result.nested_volume_split_summary_df[
            result.nested_volume_split_summary_df["horizon_key"] == horizon_view.value
        ].copy()
        _global = result.nested_volume_split_global_significance_df[
            (result.nested_volume_split_global_significance_df["horizon_key"] == horizon_view.value)
            & (result.nested_volume_split_global_significance_df["metric_key"] == metric_view.value)
        ].copy()
        _pairwise = result.nested_volume_split_pairwise_significance_df[
            (result.nested_volume_split_pairwise_significance_df["horizon_key"] == horizon_view.value)
            & (result.nested_volume_split_pairwise_significance_df["metric_key"] == metric_view.value)
            & (
                result.nested_volume_split_pairwise_significance_df[
                    "left_nested_combined_bucket"
                ].isin(["extreme_volume_high", "extreme_volume_low"])
            )
            & (
                result.nested_volume_split_pairwise_significance_df[
                    "right_nested_combined_bucket"
                ].isin(["middle_volume_high", "middle_volume_low"])
            )
        ].copy()
        _interaction = result.nested_volume_split_interaction_df[
            (result.nested_volume_split_interaction_df["horizon_key"] == horizon_view.value)
            & (result.nested_volume_split_interaction_df["metric_key"] == metric_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md(
                    "### Price SMA 20 / 80 Bucket x Volume SMA 20 / 80 Half-Split"
                ),
                mo.Html(_summary.round(6).to_html(index=False)),
                mo.md("### 4-Cell Global Significance"),
                mo.Html(_global.round(6).to_html(index=False)),
                mo.md("### Extreme vs Middle Within Same Volume Half"),
                mo.Html(_pairwise.round(6).to_html(index=False)),
                mo.md("### Volume-Split Interaction"),
                mo.Html(_interaction.round(6).to_html(index=False)),
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
    result,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _summary = result.q1_q10_volume_split_summary_df[
            result.q1_q10_volume_split_summary_df["horizon_key"] == horizon_view.value
        ].copy()
        _global = result.q1_q10_volume_split_global_significance_df[
            (result.q1_q10_volume_split_global_significance_df["horizon_key"] == horizon_view.value)
            & (result.q1_q10_volume_split_global_significance_df["metric_key"] == metric_view.value)
        ].copy()
        _pairwise = result.q1_q10_volume_split_pairwise_significance_df[
            (result.q1_q10_volume_split_pairwise_significance_df["horizon_key"] == horizon_view.value)
            & (result.q1_q10_volume_split_pairwise_significance_df["metric_key"] == metric_view.value)
            & (
                (
                    result.q1_q10_volume_split_pairwise_significance_df[
                        "left_q1_q10_combined_bucket"
                    ]
                    == "q1_volume_high"
                )
                & (
                    result.q1_q10_volume_split_pairwise_significance_df[
                        "right_q1_q10_combined_bucket"
                    ]
                    == "q10_volume_high"
                )
                | (
                    (
                        result.q1_q10_volume_split_pairwise_significance_df[
                            "left_q1_q10_combined_bucket"
                        ]
                        == "q1_volume_low"
                    )
                    & (
                        result.q1_q10_volume_split_pairwise_significance_df[
                            "right_q1_q10_combined_bucket"
                        ]
                        == "q10_volume_low"
                    )
                )
                | (
                    (
                        result.q1_q10_volume_split_pairwise_significance_df[
                            "left_q1_q10_combined_bucket"
                        ]
                        == "q1_volume_high"
                    )
                    & (
                        result.q1_q10_volume_split_pairwise_significance_df[
                            "right_q1_q10_combined_bucket"
                        ]
                        == "q1_volume_low"
                    )
                )
                | (
                    (
                        result.q1_q10_volume_split_pairwise_significance_df[
                            "left_q1_q10_combined_bucket"
                        ]
                        == "q10_volume_high"
                    )
                    & (
                        result.q1_q10_volume_split_pairwise_significance_df[
                            "right_q1_q10_combined_bucket"
                        ]
                        == "q10_volume_low"
                    )
                )
            )
        ].copy()
        _interaction = result.q1_q10_volume_split_interaction_df[
            (result.q1_q10_volume_split_interaction_df["horizon_key"] == horizon_view.value)
            & (result.q1_q10_volume_split_interaction_df["metric_key"] == metric_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Q1 / Q10 x Volume SMA 20 / 80 Half-Split"),
                mo.Html(_summary.round(6).to_html(index=False)),
                mo.md("### Q1 / Q10 4-Cell Global Significance"),
                mo.Html(_global.round(6).to_html(index=False)),
                mo.md("### Q1 vs Q10 / Within-Bucket Volume Split"),
                mo.Html(_pairwise.round(6).to_html(index=False)),
                mo.md("### Q1-Q10 Spread Interaction"),
                mo.Html(_interaction.round(6).to_html(index=False)),
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
    result,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _summary = result.q10_middle_volume_split_summary_df[
            result.q10_middle_volume_split_summary_df["horizon_key"] == horizon_view.value
        ].copy()
        _hypothesis = result.q10_low_hypothesis_df[
            (result.q10_low_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.q10_low_hypothesis_df["metric_key"] == metric_view.value)
        ].copy()
        _pairwise = result.q10_middle_volume_split_pairwise_significance_df[
            (result.q10_middle_volume_split_pairwise_significance_df["horizon_key"] == horizon_view.value)
            & (result.q10_middle_volume_split_pairwise_significance_df["metric_key"] == metric_view.value)
            & (
                result.q10_middle_volume_split_pairwise_significance_df[
                    "left_q10_middle_combined_bucket"
                ]
                == "q10_volume_low"
            )
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Q10 Low Hypothesis"),
                mo.Html(_summary.round(6).to_html(index=False)),
                mo.md("### Q10 Low Direct Comparisons"),
                mo.Html(_hypothesis.round(6).to_html(index=False)),
                mo.md("### Q10 Low Pairwise Detail"),
                mo.Html(_pairwise.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, horizon_view, mo, ranking_feature_view, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _summary = result.decile_future_summary_df[
            (result.decile_future_summary_df["ranking_feature"] == ranking_feature_view.value)
            & (result.decile_future_summary_df["horizon_key"] == horizon_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Future Target Summary"),
                mo.Html(_summary.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, mo, plt, ranking_feature_view, result):
    _chart = mo.md("")
    if not error_message and result is not None:
        _summary = result.ranking_feature_summary_df[
            result.ranking_feature_summary_df["ranking_feature"]
            == ranking_feature_view.value
        ].copy()
        _bar_colors = [
            "#0f766e",
            "#0d9488",
            "#14b8a6",
            "#2dd4bf",
            "#67e8f9",
            "#fcd34d",
            "#fbbf24",
            "#f59e0b",
            "#ea580c",
            "#dc2626",
        ]
        _fig, _ax = plt.subplots(figsize=(10, 4.5))
        _ax.bar(
            _summary["feature_decile"].tolist(),
            _summary["mean_ranking_value"].tolist(),
            color=_bar_colors[: len(_summary)],
        )
        _ax.set_title(f"Mean {ranking_feature_view.value} by Decile")
        _ax.set_xlabel("Ranking Decile")
        _ax.set_ylabel("Mean Ranking Value")
        _ax.grid(axis="y", alpha=0.2)
        _fig.tight_layout()
        _chart = _fig
    _chart
    return


@app.cell
def _(
    DECILE_ORDER,
    error_message,
    horizon_view,
    metric_view,
    mo,
    plt,
    ranking_feature_view,
    result,
):
    _chart = mo.md("")
    if not error_message and result is not None:
        _metric_column = (
            "group_mean_future_return"
            if metric_view.value == "future_return"
            else "group_mean_future_close"
        )
        _daily = result.daily_group_means_df[
            (result.daily_group_means_df["ranking_feature"] == ranking_feature_view.value)
            & (result.daily_group_means_df["horizon_key"] == horizon_view.value)
        ].copy()
        _grouped_values = [
            _daily.loc[_daily["feature_decile"] == decile_key, _metric_column].dropna()
            for decile_key in DECILE_ORDER
        ]
        _fig, _ax = plt.subplots(figsize=(12, 4.5))
        _ax.boxplot(_grouped_values, labels=list(DECILE_ORDER))
        _ax.set_title(
            f"Daily Mean {metric_view.value} by Decile ({ranking_feature_view.value}, {horizon_view.value})"
        )
        _ax.set_xlabel("Ranking Decile")
        _ax.set_ylabel(metric_view.value)
        _ax.grid(axis="y", alpha=0.2)
        _fig.tight_layout()
        _chart = _fig
    _chart
    return


@app.cell
def _(error_message, mo, plt, ranking_feature_view, result):
    _chart = mo.md("")
    if not error_message and result is not None:
        _summary = result.decile_future_summary_df[
            result.decile_future_summary_df["ranking_feature"] == ranking_feature_view.value
        ].copy()
        _x_positions = [1, 5, 10]
        _fig, _ax = plt.subplots(figsize=(10, 4.5))
        for _decile_key, _color in (("Q1", "#0f766e"), ("Q10", "#dc2626")):
            _decile_df = _summary[
                _summary["feature_decile"] == _decile_key
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
            f"Mean Future Return by Decile ({ranking_feature_view.value}, Q1 vs Q10)"
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


@app.cell
def _(error_message, mo, ranking_feature_view, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _sample = result.ranked_panel_df[
            result.ranked_panel_df["ranking_feature"] == ranking_feature_view.value
        ][
            [
                "date",
                "code",
                "close",
                "ranking_feature",
                "ranking_value",
                "feature_rank_desc",
                "feature_decile",
                "t_plus_1_close",
                "t_plus_5_close",
                "t_plus_10_close",
            ]
        ].tail(80)
        _view = mo.vstack(
            [
                mo.md("### Ranked Event Sample (tail 80 rows)"),
                mo.Html(_sample.round(6).to_html(index=False)),
            ]
        )
    _view
    return

if __name__ == "__main__":
    app.run()
