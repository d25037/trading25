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
    app_title="TOPIX100 Price vs SMA Q10 Bounce Playground",
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
    from src.domains.analytics.research_bundle import load_research_bundle_info
    from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
        PRICE_FEATURE_LABEL_MAP,
        VOLUME_FEATURE_LABEL_MAP,
        get_topix100_price_vs_sma_rank_future_close_available_date_range,
    )
    from src.domains.analytics.topix100_price_vs_sma_q10_bounce import (
        Q10_LOW_HYPOTHESIS_LABELS,
        TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
        get_topix100_price_vs_sma_q10_bounce_bundle_path_for_run_id,
        get_topix100_price_vs_sma_q10_bounce_latest_bundle_path,
        load_topix100_price_vs_sma_q10_bounce_research_bundle,
        run_topix100_price_vs_sma_q10_bounce_research,
    )

    default_db_path = get_settings().market_db_path
    return (
        PRICE_FEATURE_LABEL_MAP,
        Q10_LOW_HYPOTHESIS_LABELS,
        TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
        VOLUME_FEATURE_LABEL_MAP,
        default_db_path,
        get_topix100_price_vs_sma_q10_bounce_bundle_path_for_run_id,
        get_topix100_price_vs_sma_q10_bounce_latest_bundle_path,
        get_topix100_price_vs_sma_rank_future_close_available_date_range,
        load_research_bundle_info,
        load_topix100_price_vs_sma_q10_bounce_research_bundle,
        run_topix100_price_vs_sma_q10_bounce_research,
    )


@app.cell
def _(get_topix100_price_vs_sma_q10_bounce_latest_bundle_path):
    try:
        latest_bundle_path = get_topix100_price_vs_sma_q10_bounce_latest_bundle_path()
    except Exception:
        latest_bundle_path = None
    latest_run_id = latest_bundle_path.name if latest_bundle_path else ""
    latest_bundle_path_str = str(latest_bundle_path) if latest_bundle_path else ""
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(default_db_path, get_topix100_price_vs_sma_rank_future_close_available_date_range):
    try:
        initial_range = get_topix100_price_vs_sma_rank_future_close_available_date_range(
            default_db_path
        )
    except Exception:
        initial_range = (None, None)
    return (initial_range,)


@app.cell
def _(default_db_path, initial_range, latest_bundle_path_str, latest_run_id, mo, pd):
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

    mode = mo.ui.dropdown(
        options={
            "bundle": "Load Existing Bundle",
            "recompute": "Run Fresh Analysis",
        },
        value="bundle",
        label="Mode",
    )
    run_id = mo.ui.text(value=latest_run_id, label="Run ID")
    bundle_path = mo.ui.text(
        value=latest_bundle_path_str,
        label="Bundle Path (optional)",
    )
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

    recompute_controls = mo.vstack(
        [
            db_path,
            mo.hstack([start_date, end_date]),
            mo.hstack([lookback_years, min_constituents_per_day]),
        ]
    )
    mo.vstack(
        [
            mo.md(
                "\n".join(
                    [
                        "### Research Runner",
                        "",
                        "- Default path is **viewer-first**: load an existing bundle by `Run ID` or `Bundle Path`.",
                        "- Fresh analysis only runs when `Mode = Run Fresh Analysis`.",
                        "- Canonical runner: `apps/bt/scripts/research/run_topix100_price_vs_sma_q10_bounce.py`",
                    ]
                )
            ),
            mo.hstack([mode, run_id]),
            bundle_path,
            recompute_controls if mode.value == "recompute" else mo.md(""),
        ]
    )
    return (
        bundle_path,
        db_path,
        end_date,
        lookback_years,
        min_constituents_per_day,
        mode,
        run_id,
        start_date,
    )


@app.cell
def _(
    bundle_path,
    db_path,
    end_date,
    get_topix100_price_vs_sma_q10_bounce_bundle_path_for_run_id,
    lookback_years,
    min_constituents_per_day,
    mode,
    run_id,
    start_date,
):
    run_id_value = run_id.value.strip()
    bundle_path_value = bundle_path.value.strip()
    resolved_bundle_path = bundle_path_value
    if not resolved_bundle_path and run_id_value:
        resolved_bundle_path = str(
            get_topix100_price_vs_sma_q10_bounce_bundle_path_for_run_id(run_id_value)
        )
    parsed_inputs = {
        "mode": mode.value,
        "run_id": run_id_value or None,
        "selected_bundle_path": resolved_bundle_path or None,
        "selected_db_path": db_path.value.strip(),
        "selected_start": start_date.value.strip() or None,
        "selected_end": end_date.value.strip() or None,
        "lookback_years": int(lookback_years.value),
        "min_constituents_per_day": int(min_constituents_per_day.value),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_research_bundle_info,
    load_topix100_price_vs_sma_q10_bounce_research_bundle,
    parsed_inputs,
    run_topix100_price_vs_sma_q10_bounce_research,
):
    try:
        if parsed_inputs["mode"] == "bundle":
            selected_bundle_path = parsed_inputs["selected_bundle_path"]
            if not selected_bundle_path:
                raise ValueError(
                    "Set a bundle path or run id, or switch Mode to Run Fresh Analysis."
                )
            bundle_info = load_research_bundle_info(selected_bundle_path)
            result = load_topix100_price_vs_sma_q10_bounce_research_bundle(
                selected_bundle_path
            )
        else:
            bundle_info = None
            result = run_topix100_price_vs_sma_q10_bounce_research(
                parsed_inputs["selected_db_path"],
                start_date=parsed_inputs["selected_start"],
                end_date=parsed_inputs["selected_end"],
                lookback_years=parsed_inputs["lookback_years"],
                min_constituents_per_day=parsed_inputs["min_constituents_per_day"],
            )
        error_message = None
    except Exception as exc:
        bundle_info = None
        result = None
        error_message = str(exc)
    return bundle_info, error_message, result


@app.cell
def _(
    TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
    bundle_info,
    error_message,
    mo,
    parsed_inputs,
    result,
):
    _view = mo.md("")
    if not error_message and result is not None:
        base = result.base_result
        feature_labels = ", ".join(result.price_feature_order)
        volume_labels = ", ".join(result.volume_feature_order)
        _view = mo.md(
            "\n".join(
                [
                    "## TOPIX100 Price vs SMA Q10 Bounce Research",
                    "",
                    f"- Experiment ID: **{TOPIX100_PRICE_VS_SMA_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID}**",
                    f"- Mode: **{parsed_inputs['mode']}**",
                    *(
                        [
                            f"- Bundle run id: **{bundle_info.run_id}**",
                            f"- Bundle created at: **{bundle_info.created_at}**",
                            f"- Bundle path: **{bundle_info.bundle_dir}**",
                        ]
                        if bundle_info is not None
                        else []
                    ),
                    f"- Source mode: **{base.source_mode}**",
                    f"- Source detail: **{base.source_detail}**",
                    f"- Available range: **{base.available_start_date} -> {base.available_end_date}**",
                    f"- Requested range: **{parsed_inputs['selected_start'] or base.default_start_date} -> {parsed_inputs['selected_end'] or base.available_end_date}**",
                    f"- Effective analysis range: **{base.analysis_start_date} -> {base.analysis_end_date}**",
                    f"- Latest TOPIX100 constituent count: **{base.topix100_constituent_count}**",
                    f"- Stock-day rows after warmup/filter: **{base.stock_day_count}**",
                    f"- Valid dates: **{base.valid_date_count}**",
                    f"- Price features: **{feature_labels}**",
                    f"- Volume features: **{volume_labels}**",
                    "",
                    "This playground narrows the broader `price / SMA` study to the bounce slice.",
                    "The main question is whether **Q10 Low** beats `Q10 High`, `Middle Low`, and `Middle High`.",
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
    Q10_LOW_HYPOTHESIS_LABELS,
    VOLUME_FEATURE_LABEL_MAP,
    error_message,
    mo,
    result,
):
    if error_message or result is None:
        price_feature_view = mo.md("")
        volume_feature_view = mo.md("")
        horizon_view = mo.md("")
        metric_view = mo.md("")
        hypothesis_view = mo.md("")
    else:
        price_feature_view = mo.ui.dropdown(
            options={
                PRICE_FEATURE_LABEL_MAP[feature]: feature
                for feature in result.price_feature_order
            },
            value="price_vs_sma_50_gap"
            if "price_vs_sma_50_gap" in result.price_feature_order
            else result.price_feature_order[0],
            label="Price Feature",
        )
        volume_feature_view = mo.ui.dropdown(
            options={
                VOLUME_FEATURE_LABEL_MAP[feature]: feature
                for feature in result.volume_feature_order
            },
            value=result.volume_feature_order[0],
            label="Volume Feature",
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
        hypothesis_view = mo.ui.dropdown(
            options={label: label for _, _, label in Q10_LOW_HYPOTHESIS_LABELS},
            value="Q10 Low vs Middle Low",
            label="Hypothesis",
        )
    mo.vstack(
        [
            mo.hstack([price_feature_view, volume_feature_view]),
            mo.hstack([horizon_view, metric_view]),
            mo.hstack([hypothesis_view]),
        ]
    )
    return (
        hypothesis_view,
        horizon_view,
        metric_view,
        price_feature_view,
        volume_feature_view,
    )


@app.cell
def _(error_message, metric_view, mo, result):
    _view = mo.md("")
    if not error_message and result is not None:
        _scorecard = result.q10_low_scorecard_df[
            result.q10_low_scorecard_df["metric_key"] == metric_view.value
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Q10 Bounce Scorecard"),
                mo.Html(_scorecard.round(6).to_html(index=False)),
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
        _summary = result.q10_middle_volume_split_summary_df[
            (result.q10_middle_volume_split_summary_df["price_feature"] == price_feature_view.value)
            & (result.q10_middle_volume_split_summary_df["volume_feature"] == volume_feature_view.value)
            & (result.q10_middle_volume_split_summary_df["horizon_key"] == horizon_view.value)
        ].copy()
        _pairwise = result.q10_middle_volume_split_pairwise_significance_df[
            (result.q10_middle_volume_split_pairwise_significance_df["price_feature"] == price_feature_view.value)
            & (result.q10_middle_volume_split_pairwise_significance_df["volume_feature"] == volume_feature_view.value)
            & (result.q10_middle_volume_split_pairwise_significance_df["horizon_key"] == horizon_view.value)
            & (result.q10_middle_volume_split_pairwise_significance_df["metric_key"] == metric_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Q10 / Middle Volume Split Summary"),
                mo.Html(_summary.round(6).to_html(index=False)),
                mo.md("### Pairwise Significance"),
                mo.Html(_pairwise.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(
    error_message,
    horizon_view,
    hypothesis_view,
    metric_view,
    mo,
    price_feature_view,
    result,
    volume_feature_view,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _hypothesis = result.q10_low_hypothesis_df[
            (result.q10_low_hypothesis_df["price_feature"] == price_feature_view.value)
            & (result.q10_low_hypothesis_df["volume_feature"] == volume_feature_view.value)
            & (result.q10_low_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.q10_low_hypothesis_df["metric_key"] == metric_view.value)
            & (result.q10_low_hypothesis_df["hypothesis_label"] == hypothesis_view.value)
        ].copy()
        _scorecard = result.q10_low_scorecard_df[
            (result.q10_low_scorecard_df["price_feature"] == price_feature_view.value)
            & (result.q10_low_scorecard_df["volume_feature"] == volume_feature_view.value)
            & (result.q10_low_scorecard_df["horizon_key"] == horizon_view.value)
            & (result.q10_low_scorecard_df["metric_key"] == metric_view.value)
            & (result.q10_low_scorecard_df["hypothesis_label"] == hypothesis_view.value)
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Selected Bounce Hypothesis"),
                mo.Html(_hypothesis.round(6).to_html(index=False)),
                mo.md("### Daily Spread Scorecard"),
                mo.Html(_scorecard.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(
    error_message,
    horizon_view,
    hypothesis_view,
    metric_view,
    pd,
    plt,
    price_feature_view,
    result,
    volume_feature_view,
):
    if not error_message and result is not None:
        _chart_df = result.q10_low_spread_daily_df[
            (result.q10_low_spread_daily_df["price_feature"] == price_feature_view.value)
            & (result.q10_low_spread_daily_df["volume_feature"] == volume_feature_view.value)
            & (result.q10_low_spread_daily_df["horizon_key"] == horizon_view.value)
            & (result.q10_low_spread_daily_df["metric_key"] == metric_view.value)
            & (result.q10_low_spread_daily_df["hypothesis_label"] == hypothesis_view.value)
        ].copy()
        if not _chart_df.empty:
            _chart_df["date"] = pd.to_datetime(_chart_df["date"])
            _chart_df = _chart_df.sort_values("date").reset_index(drop=True)
            _chart_df["rolling_mean_difference"] = _chart_df[
                "mean_difference"
            ].rolling(
                window=63,
                min_periods=10,
            ).mean()

            fig, ax = plt.subplots(figsize=(10, 4))
            ax.plot(
                _chart_df["date"],
                _chart_df["mean_difference"],
                alpha=0.25,
                label="Daily spread",
            )
            ax.plot(
                _chart_df["date"],
                _chart_df["rolling_mean_difference"],
                linewidth=2,
                label="63D rolling mean",
            )
            ax.axhline(0.0, color="black", linewidth=1, linestyle="--")
            ax.set_title(
                f"{price_feature_view.value} / {horizon_view.value} / {hypothesis_view.value}"
            )
            ax.set_ylabel(metric_view.value)
            ax.legend()
            fig


if __name__ == "__main__":
    app.run()
