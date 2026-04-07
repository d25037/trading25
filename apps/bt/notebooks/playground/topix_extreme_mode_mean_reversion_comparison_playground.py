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
    app_title="TOPIX Extreme Mode Mean-Reversion Comparison Playground",
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
    from src.domains.analytics.topix_extreme_mode_mean_reversion_comparison import (
        get_topix_extreme_mode_mean_reversion_comparison_bundle_path_for_run_id,
        get_topix_extreme_mode_mean_reversion_comparison_latest_bundle_path,
        load_topix_extreme_mode_mean_reversion_comparison_bundle,
    )

    return (
        build_bundle_viewer_controls,
        get_latest_bundle_defaults,
        get_topix_extreme_mode_mean_reversion_comparison_bundle_path_for_run_id,
        get_topix_extreme_mode_mean_reversion_comparison_latest_bundle_path,
        load_bundle_selection,
        load_research_bundle_info,
        load_topix_extreme_mode_mean_reversion_comparison_bundle,
        project_root,
        resolve_selected_bundle_path,
    )


@app.cell
def _(
    get_latest_bundle_defaults,
    get_topix_extreme_mode_mean_reversion_comparison_latest_bundle_path,
):
    latest_bundle_path_str, latest_run_id = get_latest_bundle_defaults(
        get_topix_extreme_mode_mean_reversion_comparison_latest_bundle_path
    )
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(build_bundle_viewer_controls, latest_bundle_path_str, latest_run_id, mo):
    run_id, bundle_path, controls_view = build_bundle_viewer_controls(
        mo,
        latest_run_id=latest_run_id,
        latest_bundle_path_str=latest_bundle_path_str,
        runner_path="apps/bt/scripts/research/run_topix_extreme_mode_mean_reversion_comparison.py",
        extra_note_lines=[
            "- compare the original daily extreme mode and the streak-candle extreme mode",
            "- signals are observed at close, entered next open, exited at N-day close",
            "- overlapping signals are ignored while a trade is open",
        ],
    )
    controls_view
    return bundle_path, run_id


@app.cell
def _(
    bundle_path,
    get_topix_extreme_mode_mean_reversion_comparison_bundle_path_for_run_id,
    resolve_selected_bundle_path,
    run_id,
):
    run_id_value = run_id.value.strip()
    parsed_inputs = {
        "run_id": run_id_value or None,
        "selected_bundle_path": resolve_selected_bundle_path(
            bundle_path.value,
            run_id_value,
            get_topix_extreme_mode_mean_reversion_comparison_bundle_path_for_run_id,
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_bundle_selection,
    load_research_bundle_info,
    load_topix_extreme_mode_mean_reversion_comparison_bundle,
    parsed_inputs,
):
    try:
        bundle_info, result = load_bundle_selection(
            selected_bundle_path=parsed_inputs["selected_bundle_path"],
            load_research_bundle_info=load_research_bundle_info,
            load_research_bundle=load_topix_extreme_mode_mean_reversion_comparison_bundle,
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
                    "## TOPIX Extreme Mode Mean-Reversion Comparison",
                    "",
                    f"- Common comparison range: **{result.common_start_date} -> {result.common_end_date}**",
                    f"- Selected normal X: **{result.selected_normal_window_days} days**",
                    f"- Selected streak X: **{result.selected_streak_window_streaks} streaks**",
                    f"- Hold days: **{', '.join(str(value) for value in result.hold_days)}**",
                    f"- Validation ratio: **{result.validation_ratio:.2f}**",
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
        split_view = mo.md("")
        strategy_view = mo.md("")
    else:
        split_view = mo.ui.dropdown(
            options={"validation": "validation", "full": "full", "discovery": "discovery"},
            value="validation",
            label="Split",
        )
        strategy_view = mo.ui.dropdown(
            options={
                "long_on_bearish": "long_on_bearish",
                "short_on_bullish": "short_on_bullish",
                "long_bear_short_bull": "long_bear_short_bull",
            },
            value="long_on_bearish",
            label="Strategy",
        )
    mo.hstack([split_view, strategy_view])
    return split_view, strategy_view


@app.cell
def _(error_message, mo, result):
    overview_view = mo.md("")
    if not error_message and result is not None:
        overview_view = mo.vstack(
            [
                mo.md("## Model Overview"),
                mo.Html(result.model_overview_df.round(4).to_html(index=False)),
            ]
        )
    overview_view
    return


@app.cell
def _(error_message, mo, result, split_view, strategy_view):
    signal_summary_view = mo.md("")
    backtest_summary_view = mo.md("")
    leaderboard_view = mo.md("")
    trades_view = mo.md("")
    if not error_message and result is not None:
        split_name = split_view.value
        strategy_name = strategy_view.value

        signal_summary_df = result.signal_summary_df[
            result.signal_summary_df["sample_split"] == split_name
        ].copy()
        signal_summary_df = signal_summary_df.sort_values(
            ["model", "mode"],
            kind="stable",
        )
        signal_summary_view = mo.vstack(
            [
                mo.md(f"## Signal Summary ({split_name})"),
                mo.Html(signal_summary_df.round(4).to_html(index=False)),
            ]
        )

        strategy_summary_df = result.backtest_summary_df[
            (result.backtest_summary_df["sample_split"] == split_name)
            & (result.backtest_summary_df["strategy"] == strategy_name)
        ].copy()
        strategy_summary_df = strategy_summary_df.sort_values(
            ["hold_days", "model"],
            kind="stable",
        )
        backtest_summary_view = mo.vstack(
            [
                mo.md(f"## Backtest Summary ({split_name} / {strategy_name})"),
                mo.Html(strategy_summary_df.round(4).to_html(index=False)),
            ]
        )

        leaderboard_df = result.validation_leaderboard_df.copy()
        leaderboard_view = mo.vstack(
            [
                mo.md("## Validation Leaderboard"),
                mo.Html(leaderboard_df.round(4).to_html(index=False)),
            ]
        )

        recent_trades_df = result.backtest_trade_df[
            (result.backtest_trade_df["sample_split"] == split_name)
            & (result.backtest_trade_df["strategy"] == strategy_name)
        ].copy()
        recent_trades_df = recent_trades_df.sort_values(
            ["entry_date", "model"],
            ascending=[False, True],
            kind="stable",
        ).head(20)
        trades_view = mo.vstack(
            [
                mo.md(f"## Recent Trades ({split_name} / {strategy_name})"),
                mo.Html(recent_trades_df.round(4).to_html(index=False)),
            ]
        )

    mo.vstack([signal_summary_view, backtest_summary_view, leaderboard_view, trades_view])
    return


if __name__ == "__main__":
    app.run()
