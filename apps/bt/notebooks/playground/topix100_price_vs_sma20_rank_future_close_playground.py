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
    app_title="TOPIX100 Price vs SMA20 Rank / Future Close Playground",
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
    project_root = Path.cwd()
    if project_root.name == "playground":
        project_root = project_root.parent.parent
    elif project_root.name == "notebooks":
        project_root = project_root.parent

    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from src.shared.config.settings import get_settings
    from src.domains.analytics.topix100_price_vs_sma20_rank_future_close import (
        PRIMARY_PRICE_FEATURE,
        PRIMARY_PRICE_FEATURE_LABEL,
        PRIMARY_VOLUME_FEATURE,
        PRIMARY_VOLUME_FEATURE_LABEL,
        get_topix100_price_vs_sma20_rank_future_close_available_date_range,
        run_topix100_price_vs_sma20_rank_future_close_research,
    )

    default_db_path = get_settings().market_db_path
    return (
        PRIMARY_PRICE_FEATURE,
        PRIMARY_PRICE_FEATURE_LABEL,
        PRIMARY_VOLUME_FEATURE,
        PRIMARY_VOLUME_FEATURE_LABEL,
        default_db_path,
        get_topix100_price_vs_sma20_rank_future_close_available_date_range,
        run_topix100_price_vs_sma20_rank_future_close_research,
    )


@app.cell
def _(
    default_db_path,
    get_topix100_price_vs_sma20_rank_future_close_available_date_range,
):
    try:
        initial_range = get_topix100_price_vs_sma20_rank_future_close_available_date_range(
            default_db_path
        )
    except Exception:
        initial_range = (None, None)
    return (initial_range,)


@app.cell
def _(default_db_path, initial_range, mo, pd):
    available_start_date, available_end_date = initial_range
    default_start_date = available_start_date or ""
    if available_end_date:
        candidate = (
            pd.Timestamp(available_end_date)
            - pd.DateOffset(years=10)
            + pd.Timedelta(days=1)
        ).strftime("%Y-%m-%d")
        if available_start_date:
            default_start_date = max(available_start_date, candidate)
        else:
            default_start_date = candidate

    db_path = mo.ui.text(value=default_db_path, label="DuckDB Path")
    start_date = mo.ui.text(
        value=default_start_date,
        label="Analysis Start Date (YYYY-MM-DD)",
    )
    end_date = mo.ui.text(
        value=available_end_date or "",
        label="Analysis End Date (YYYY-MM-DD)",
    )
    lookback_years = mo.ui.number(value=10, start=1, step=1, label="Lookback Years")
    min_constituents_per_day = mo.ui.number(
        value=80,
        start=4,
        step=1,
        label="Min Constituents / Day",
    )

    mo.vstack(
        [
            db_path,
            mo.hstack([start_date, end_date]),
            mo.hstack([lookback_years, min_constituents_per_day]),
        ]
    )
    return db_path, end_date, lookback_years, min_constituents_per_day, start_date


@app.cell
def _(db_path, end_date, lookback_years, min_constituents_per_day, start_date):
    parsed_inputs = {
        "selected_db_path": db_path.value.strip(),
        "selected_start": start_date.value.strip() or None,
        "selected_end": end_date.value.strip() or None,
        "lookback_years": int(lookback_years.value),
        "min_constituents_per_day": int(min_constituents_per_day.value),
    }
    return (parsed_inputs,)


@app.cell
def _(parsed_inputs, run_topix100_price_vs_sma20_rank_future_close_research):
    try:
        result = run_topix100_price_vs_sma20_rank_future_close_research(
            parsed_inputs["selected_db_path"],
            start_date=parsed_inputs["selected_start"],
            end_date=parsed_inputs["selected_end"],
            lookback_years=parsed_inputs["lookback_years"],
            min_constituents_per_day=parsed_inputs["min_constituents_per_day"],
        )
        error_message = None
    except Exception as exc:
        result = None
        error_message = str(exc)
    return error_message, result


@app.cell
def _(error_message, mo, parsed_inputs, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _view = mo.md(
            "\n".join(
                [
                    "## TOPIX100 Price vs SMA20 Rank Research",
                    "",
                    f"- Source mode: **{result.source_mode}**",
                    f"- Source detail: **{result.source_detail}**",
                    f"- Available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Requested range: **{parsed_inputs['selected_start'] or result.default_start_date} -> {parsed_inputs['selected_end'] or result.available_end_date}**",
                    f"- Effective analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Latest TOPIX100 constituent count: **{result.topix100_constituent_count}**",
                    f"- Stock-day rows after SMA warmup/filter: **{result.stock_day_count}**",
                    f"- Valid dates: **{result.valid_date_count}**",
                    "",
                    "Primary price feature is **(close / 20SMA) - 1**.",
                    "Q1 is the highest positive deviation decile and Q10 is the lowest deviation decile.",
                    "Volume split is fixed to **volume_sma_20_80** high / low halves inside each price bucket.",
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
    mo.hstack([horizon_view, metric_view])
    return horizon_view, metric_view


@app.cell
def _(PRIMARY_PRICE_FEATURE, error_message, mo, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _decile_df = result.ranking_feature_summary_df[
            result.ranking_feature_summary_df["ranking_feature"] == PRIMARY_PRICE_FEATURE
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
def _(PRIMARY_PRICE_FEATURE, error_message, horizon_view, metric_view, mo, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _global_df = result.global_significance_df[
            (result.global_significance_df["ranking_feature"] == PRIMARY_PRICE_FEATURE)
            & (result.global_significance_df["horizon_key"] == horizon_view.value)
            & (result.global_significance_df["metric_key"] == metric_view.value)
        ].copy()
        _pairwise_df = result.pairwise_significance_df[
            (result.pairwise_significance_df["ranking_feature"] == PRIMARY_PRICE_FEATURE)
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
def _(error_message, horizon_view, metric_view, mo, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _summary_df = result.price_bucket_summary_df[
            result.price_bucket_summary_df["horizon_key"] == horizon_view.value
        ].copy()
        _hypothesis_df = result.group_hypothesis_df[
            (result.group_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.group_hypothesis_df["metric_key"] == metric_view.value)
        ].copy()
        _pairwise_df = result.price_bucket_pairwise_significance_df[
            (result.price_bucket_pairwise_significance_df["horizon_key"] == horizon_view.value)
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
def _(error_message, horizon_view, metric_view, mo, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _summary_df = result.price_volume_split_summary_df[
            result.price_volume_split_summary_df["horizon_key"] == horizon_view.value
        ].copy()
        _hypothesis_df = result.split_hypothesis_df[
            (result.split_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.split_hypothesis_df["metric_key"] == metric_view.value)
        ].copy()
        _pairwise_df = result.price_volume_split_pairwise_significance_df[
            (result.price_volume_split_pairwise_significance_df["horizon_key"] == horizon_view.value)
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
def _(PRIMARY_PRICE_FEATURE_LABEL, error_message, mo, plt, result):
    _chart = mo.md("")
    if not error_message and result is not None:
        _summary_df = result.decile_future_summary_df[
            result.decile_future_summary_df["ranking_feature_label"]
            == PRIMARY_PRICE_FEATURE_LABEL
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
        _ax.set_title("Mean Future Return by Decile (Q1 vs Q10)")
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
