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
    app_title="TOPIX100 Price/SMA50 Decile Partitions Playground",
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
    from src.domains.analytics.topix100_price_to_sma50_decile_partitions import (
        TOPIX100_PRICE_TO_SMA50_DECILE_PARTITIONS_RESEARCH_EXPERIMENT_ID,
        get_topix100_price_to_sma50_decile_partitions_bundle_path_for_run_id,
        get_topix100_price_to_sma50_decile_partitions_latest_bundle_path,
        load_topix100_price_to_sma50_decile_partitions_research_bundle,
    )

    return (
        TOPIX100_PRICE_TO_SMA50_DECILE_PARTITIONS_RESEARCH_EXPERIMENT_ID,
        build_bundle_viewer_controls,
        get_latest_bundle_defaults,
        get_topix100_price_to_sma50_decile_partitions_bundle_path_for_run_id,
        get_topix100_price_to_sma50_decile_partitions_latest_bundle_path,
        load_bundle_selection,
        load_research_bundle_info,
        load_topix100_price_to_sma50_decile_partitions_research_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_topix100_price_to_sma50_decile_partitions_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix100_price_to_sma50_decile_partitions_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix100_price_to_sma50_decile_partitions.py",
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix100_price_to_sma50_decile_partitions_bundle_path_for_run_id,
    resolve_selected_bundle_path,
    run_id,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix100_price_to_sma50_decile_partitions_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_bundle_selection,
    load_research_bundle_info,
    load_topix100_price_to_sma50_decile_partitions_research_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix100_price_to_sma50_decile_partitions_research_bundle,
        )
        error_message = None
    except Exception as exc:
        bundle_info = None
        result = None
        error_message = str(exc)
    return bundle_info, error_message, result


@app.cell
def _(
    TOPIX100_PRICE_TO_SMA50_DECILE_PARTITIONS_RESEARCH_EXPERIMENT_ID,
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
                    "## TOPIX100 Price/SMA50 Decile Partitions",
                    "",
                    f"- Experiment ID: **{TOPIX100_PRICE_TO_SMA50_DECILE_PARTITIONS_RESEARCH_EXPERIMENT_ID}**",
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
                    f"- Feature pair: **{result.price_feature_label} x {result.volume_feature_label}**",
                    f"- Latest TOPIX100 constituent count: **{result.topix100_constituent_count}**",
                    f"- Stock-day rows after warmup/filter: **{result.stock_day_count}**",
                    f"- Valid dates: **{result.valid_date_count}**",
                    f"- Candidate partitions searched: **{result.candidate_count}**",
                    "",
                    "This notebook does not reuse the old `Q1 / Q4+Q5+Q6 / Q10` shortcut.",
                    "Instead it scores **all 36 contiguous three-way partitions that cover all 10 deciles**.",
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
        horizon_view = mo.md("")
        metric_view = mo.md("")
        sort_key_view = mo.md("")
    else:
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
        sort_key_view = mo.ui.dropdown(
            options={
                "Price Wilcoxon Hits": "price_wilcoxon_significant_pair_count",
                "Extreme vs Middle Spread": "abs_extreme_vs_middle_mean_difference",
                "Volume Wilcoxon Hits": "volume_wilcoxon_significant_pair_count",
                "LowVol Low vs Middle Low": "low_volume_low_vs_middle_volume_low",
                "Min Group Size": "min_mean_group_size",
            },
            value="price_wilcoxon_significant_pair_count",
            label="Sort Lens",
        )
    mo.hstack([horizon_view, metric_view, sort_key_view])
    return horizon_view, metric_view, sort_key_view


@app.cell
def _(error_message, horizon_view, metric_view, result, sort_key_view):
    if error_message or result is None:
        candidate_scorecard_df = None
    else:
        candidate_scorecard_df = result.candidate_overall_scorecard_df[
            (result.candidate_overall_scorecard_df["horizon_key"] == horizon_view.value)
            & (result.candidate_overall_scorecard_df["metric_key"] == metric_view.value)
        ].copy()
        if sort_key_view.value in candidate_scorecard_df.columns:
            candidate_scorecard_df = candidate_scorecard_df.sort_values(
                by=[sort_key_view.value, "min_mean_group_size", "candidate_label"],
                ascending=[False, False, True],
                na_position="last",
            ).reset_index(drop=True)
    return (candidate_scorecard_df,)


@app.cell
def _(candidate_scorecard_df, error_message, mo):
    if error_message or candidate_scorecard_df is None or candidate_scorecard_df.empty:
        candidate_view = mo.md("")
    else:
        options = {
            (
                f"{row['candidate_label']} | "
                f"price_hits={int(row['price_wilcoxon_significant_pair_count'] or 0)} | "
                f"spread={float(row['abs_extreme_vs_middle_mean_difference'] or 0.0):+.4f}"
            ): row["candidate_key"]
            for _, row in candidate_scorecard_df.iterrows()
        }
        default_value = next(iter(options.values()))
        candidate_view = mo.ui.dropdown(
            options=options,
            value=default_value,
            label="Candidate Partition",
        )
    candidate_view
    return (candidate_view,)


@app.cell
def _(candidate_scorecard_df, candidate_view, error_message):
    if (
        error_message
        or candidate_scorecard_df is None
        or candidate_scorecard_df.empty
        or not hasattr(candidate_view, "value")
    ):
        selected_candidate_row = None
    else:
        _selected = candidate_scorecard_df[
            candidate_scorecard_df["candidate_key"] == candidate_view.value
        ]
        selected_candidate_row = _selected.iloc[0] if not _selected.empty else None
    return (selected_candidate_row,)


@app.cell
def _(candidate_scorecard_df, error_message, mo):
    _view = mo.md("")
    if not error_message and candidate_scorecard_df is not None:
        _cols = [
            "candidate_label",
            "high_deciles_label",
            "middle_deciles_label",
            "low_deciles_label",
            "price_wilcoxon_significant_pair_count",
            "abs_extreme_vs_middle_mean_difference",
            "volume_wilcoxon_significant_pair_count",
            "low_volume_low_vs_middle_volume_low",
            "min_mean_group_size",
        ]
        available_cols = [col for col in _cols if col in candidate_scorecard_df.columns]
        _view = mo.vstack(
            [
                mo.md("### Candidate Ranking"),
                mo.Html(candidate_scorecard_df[available_cols].round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, mo, selected_candidate_row):
    _view = mo.md("")
    if not error_message and selected_candidate_row is not None:
        _view = mo.md(
            "\n".join(
                [
                    "### Selected Candidate",
                    "",
                    f"- Candidate: **{selected_candidate_row['candidate_label']}**",
                    f"- High bucket: **{selected_candidate_row['high_deciles_label']}**",
                    f"- Middle bucket: **{selected_candidate_row['middle_deciles_label']}**",
                    f"- Low bucket: **{selected_candidate_row['low_deciles_label']}**",
                    f"- Price Wilcoxon hits: **{int(selected_candidate_row.get('price_wilcoxon_significant_pair_count', 0) or 0)}**",
                    f"- Volume Wilcoxon hits: **{int(selected_candidate_row.get('volume_wilcoxon_significant_pair_count', 0) or 0)}**",
                    f"- Extreme vs Middle spread: **{float(selected_candidate_row.get('abs_extreme_vs_middle_mean_difference', 0.0) or 0.0):+.4f}**",
                    f"- LowVol Low vs Middle Low: **{float(selected_candidate_row.get('low_volume_low_vs_middle_volume_low', 0.0) or 0.0):+.4f}**",
                    f"- Min mean group size: **{float(selected_candidate_row.get('min_mean_group_size', 0.0) or 0.0):.2f}**",
                ]
            )
        )
    _view
    return


@app.cell
def _(error_message, horizon_view, mo, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _profile_df = result.decile_profile_df[
            result.decile_profile_df["horizon_key"] == horizon_view.value
        ].copy()
        _threshold_df = result.decile_threshold_summary_df.copy()
        _view = mo.vstack(
            [
                mo.md("### Decile Baseline"),
                mo.md("#### Mean Future Return by Decile"),
                mo.Html(_profile_df.round(6).to_html(index=False)),
                mo.md("#### Price / SMA50 and Volume SMA 5/20 Distribution"),
                mo.Html(_threshold_df.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, horizon_view, mo, plt, result):
    _chart = mo.md("")
    if not error_message and result is not None:
        _profile_df = result.decile_profile_df[
            result.decile_profile_df["horizon_key"] == horizon_view.value
        ].copy()
        if not _profile_df.empty:
            _fig, _ax = plt.subplots(figsize=(10, 4.5))
            _ax.bar(
                _profile_df["feature_decile"].tolist(),
                _profile_df["mean_future_return"].tolist(),
                color="#0f766e",
                alpha=0.85,
            )
            _ax.set_title(f"Mean Future Return by Decile ({horizon_view.value})")
            _ax.set_xlabel("Price / SMA50 Decile")
            _ax.set_ylabel("Mean Future Return")
            _ax.grid(axis="y", alpha=0.2)
            _fig.tight_layout()
            _chart = _fig
    _chart
    return


@app.cell
def _(error_message, horizon_view, mo, result, selected_candidate_row):
    _view = mo.md("")
    if not error_message and result is not None and selected_candidate_row is not None:
        _summary_df = result.candidate_price_group_summary_df[
            (result.candidate_price_group_summary_df["candidate_key"] == selected_candidate_row["candidate_key"])
            & (result.candidate_price_group_summary_df["horizon_key"] == horizon_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Selected Candidate Group Summary"),
                mo.Html(_summary_df.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(error_message, mo, plt, result, selected_candidate_row):
    _chart = mo.md("")
    if not error_message and result is not None and selected_candidate_row is not None:
        _summary_df = result.candidate_price_group_summary_df[
            result.candidate_price_group_summary_df["candidate_key"]
            == selected_candidate_row["candidate_key"]
        ].copy()
        if not _summary_df.empty:
            _fig, _ax = plt.subplots(figsize=(10, 4.5))
            for _group_key, _color in (
                ("high", "#2563eb"),
                ("middle", "#64748b"),
                ("low", "#dc2626"),
            ):
                _group_df = _summary_df[
                    _summary_df["price_group"] == _group_key
                ].copy()
                _group_df = _group_df.set_index("horizon_key").reindex(
                    ["t_plus_1", "t_plus_5", "t_plus_10"]
                )
                _ax.plot(
                    [1, 5, 10],
                    _group_df["mean_future_return"].tolist(),
                    marker="o",
                    linewidth=2,
                    label=_group_df["price_group_label"].iloc[0]
                    if not _group_df.empty
                    else _group_key,
                    color=_color,
                )
            _ax.set_title(
                f"Selected Candidate Mean Future Return ({selected_candidate_row['candidate_label']})"
            )
            _ax.set_xlabel("Trading Days Ahead")
            _ax.set_ylabel("Mean Future Return")
            _ax.set_xticks([1, 5, 10])
            _ax.grid(alpha=0.2)
            _ax.legend()
            _fig.tight_layout()
            _chart = _fig
    _chart
    return


@app.cell
def _(error_message, horizon_view, metric_view, mo, result, selected_candidate_row):
    _view = mo.md("")
    if not error_message and result is not None and selected_candidate_row is not None:
        _price_hypothesis_df = result.candidate_price_hypothesis_df[
            (result.candidate_price_hypothesis_df["candidate_key"] == selected_candidate_row["candidate_key"])
            & (result.candidate_price_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.candidate_price_hypothesis_df["metric_key"] == metric_view.value)
        ].copy()
        _volume_hypothesis_df = result.candidate_low_volume_hypothesis_df[
            (result.candidate_low_volume_hypothesis_df["candidate_key"] == selected_candidate_row["candidate_key"])
            & (result.candidate_low_volume_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.candidate_low_volume_hypothesis_df["metric_key"] == metric_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Price-Only Hypotheses"),
                mo.Html(_price_hypothesis_df.round(6).to_html(index=False)),
                mo.md("### Low-Volume Hypotheses"),
                mo.Html(_volume_hypothesis_df.round(6).to_html(index=False)),
            ]
        )
    _view
    return


if __name__ == "__main__":
    app.run()
