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
    app_title="PRIME ex TOPIX500 SMA Ratio Rank / Future Close Playground",
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

    _project_root = ensure_bt_project_root_on_path(Path.cwd(), sys.path)
    from src.domains.analytics.research_bundle import load_research_bundle_info
    from src.domains.analytics.prime_ex_topix500_sma_ratio_rank_future_close import (
        HORIZON_ORDER,
        METRIC_ORDER,
        get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id,
        get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path,
        load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
    )

    return (
        HORIZON_ORDER,
        METRIC_ORDER,
        build_bundle_viewer_controls,
        get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id,
        get_latest_bundle_defaults,
        get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path,
        load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
        load_research_bundle_info,
        load_bundle_selection,
        _project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(get_latest_bundle_defaults, get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_prime_ex_topix500_sma_ratio_rank_future_close.py",
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id,
    run_id,
    resolve_selected_bundle_path,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
    load_research_bundle_info,
    load_bundle_selection,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
        )
        error_message = None
    except Exception as exc:
        bundle_info = None
        result = None
        error_message = str(exc)
    return bundle_info, error_message, result


@app.cell
def _(HORIZON_ORDER, METRIC_ORDER, error_message, mo):
    if error_message:
        horizon_view = mo.md("")
        metric_view = mo.md("")
        price_feature_view = mo.md("")
    else:
        price_feature_view = mo.ui.dropdown(
            options={
                "price_sma_5_20": "price_sma_5_20",
                "price_sma_20_80": "price_sma_20_80",
                "price_sma_50_150": "price_sma_50_150",
            },
            value="price_sma_20_80",
            label="Price Feature",
        )
        horizon_view = mo.ui.dropdown(
            options={key: key for key in HORIZON_ORDER},
            value="t_plus_10",
            label="Horizon",
        )
        metric_view = mo.ui.dropdown(
            options={key: key for key in METRIC_ORDER},
            value="future_return",
            label="Metric",
        )
    mo.hstack([price_feature_view, horizon_view, metric_view])
    return horizon_view, metric_view, price_feature_view


@app.cell
def _(bundle_info, error_message, mo, result):
    summary_view = mo.md("")
    if not error_message and result is not None:
        summary_view = mo.md(
            "\n".join(
                [
                    "## PRIME ex TOPIX500 SMA Ratio Rank / Future Close Research",
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
                    f"- Universe: **{result.universe_label}**",
                    f"- Available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Effective analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Latest constituent count: **{result.universe_constituent_count}**",
                    f"- Stock-day rows after SMA warmup/filter: **{result.stock_day_count}**",
                    f"- Ranked events (`stock-day x 6 features`): **{result.ranked_event_count}**",
                    f"- Valid dates: **{result.valid_date_count}**",
                    "",
                    "`Q1` is the highest decile and `Q10` is the lowest for the selected SMA-ratio feature.",
                    "`Q10 low hypothesis` below is fixed to `price_sma_20_80 x volume_sma_20_80`.",
                ]
            )
        )
    if error_message:
        summary_view = mo.md(f"## Error\n\n{error_message}")
    summary_view
    return


@app.cell
def _(error_message, horizon_view, metric_view, mo, price_feature_view, result):
    view = mo.md("")
    if not error_message and result is not None:
        feature_key = price_feature_view.value
        horizon_key = horizon_view.value
        metric_key = metric_view.value

        ranking_summary = result.ranking_feature_summary_df[
            result.ranking_feature_summary_df["ranking_feature"] == feature_key
        ].copy()
        future_summary = result.decile_future_summary_df[
            (result.decile_future_summary_df["ranking_feature"] == feature_key)
            & (result.decile_future_summary_df["horizon_key"] == horizon_key)
            & (result.decile_future_summary_df["metric_key"] == metric_key)
        ].copy()
        extreme_summary = result.extreme_vs_middle_summary_df[
            (result.extreme_vs_middle_summary_df["ranking_feature"] == feature_key)
            & (result.extreme_vs_middle_summary_df["horizon_key"] == horizon_key)
            & (result.extreme_vs_middle_summary_df["metric_key"] == metric_key)
        ].copy()
        q10_summary = result.q10_middle_volume_split_summary_df[
            (result.q10_middle_volume_split_summary_df["horizon_key"] == horizon_key)
            & (
                result.q10_middle_volume_split_summary_df["q10_middle_combined_bucket"].isin(
                    [
                        "q10_volume_low",
                        "q10_volume_high",
                        "middle_volume_low",
                        "middle_volume_high",
                    ]
                )
            )
        ].copy()
        q10_hypothesis = result.q10_low_hypothesis_df[
            (result.q10_low_hypothesis_df["horizon_key"] == horizon_key)
            & (result.q10_low_hypothesis_df["metric_key"] == metric_key)
        ].copy()

        view = mo.vstack(
            [
                mo.md("## Price Feature Deciles"),
                mo.md("### Ratio Level by Decile"),
                mo.Html(ranking_summary.round(6).to_html(index=False)),
                mo.md("### Future Outcome by Decile"),
                mo.Html(future_summary.round(6).to_html(index=False)),
                mo.md("## Extreme vs Middle"),
                mo.Html(extreme_summary.round(6).to_html(index=False)),
                mo.md("## Q10 Low Hypothesis (`price_sma_20_80 x volume_sma_20_80`)"),
                mo.md("### 4 Buckets"),
                mo.Html(q10_summary.round(6).to_html(index=False)),
                mo.md("### Direct Comparisons"),
                mo.Html(q10_hypothesis.round(6).to_html(index=False)),
            ]
        )
    view
    return


if __name__ == "__main__":
    app.run()
