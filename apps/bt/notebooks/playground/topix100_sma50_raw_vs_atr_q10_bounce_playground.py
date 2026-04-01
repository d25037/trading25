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
    app_title="TOPIX100 SMA50 Raw vs ATR Q10 Bounce Playground",
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

    from src.domains.analytics.research_bundle import load_research_bundle_info
    from src.domains.analytics.topix100_sma50_raw_vs_atr_q10_bounce import (
        DEFAULT_SIGNAL_VARIANT,
        DEFAULT_VOLUME_FEATURE,
        SAMPLE_LOOKBACK_DAYS,
        SIGNAL_VARIANT_LABEL_MAP,
        TOPIX100_SMA50_RAW_VS_ATR_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
        get_topix100_sma50_raw_vs_atr_q10_bounce_bundle_path_for_run_id,
        get_topix100_sma50_raw_vs_atr_q10_bounce_latest_bundle_path,
        load_topix100_sma50_raw_vs_atr_q10_bounce_research_bundle,
        run_topix100_sma50_raw_vs_atr_q10_bounce_research,
    )
    from src.domains.analytics.topix100_price_vs_sma_q10_bounce import (
        Q10_LOW_HYPOTHESIS_LABELS,
        Q10_MIDDLE_COMBINED_BUCKET_ORDER,
    )
    from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
        COMBINED_BUCKET_LABEL_MAP,
        VOLUME_FEATURE_LABEL_MAP,
    )
    from src.shared.config.settings import get_settings

    default_db_path = get_settings().market_db_path
    return (
        COMBINED_BUCKET_LABEL_MAP,
        DEFAULT_SIGNAL_VARIANT,
        DEFAULT_VOLUME_FEATURE,
        Q10_LOW_HYPOTHESIS_LABELS,
        Q10_MIDDLE_COMBINED_BUCKET_ORDER,
        SAMPLE_LOOKBACK_DAYS,
        SIGNAL_VARIANT_LABEL_MAP,
        TOPIX100_SMA50_RAW_VS_ATR_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
        VOLUME_FEATURE_LABEL_MAP,
        default_db_path,
        get_topix100_sma50_raw_vs_atr_q10_bounce_bundle_path_for_run_id,
        get_topix100_sma50_raw_vs_atr_q10_bounce_latest_bundle_path,
        load_research_bundle_info,
        load_topix100_sma50_raw_vs_atr_q10_bounce_research_bundle,
        run_topix100_sma50_raw_vs_atr_q10_bounce_research,
    )


@app.cell
def _(get_topix100_sma50_raw_vs_atr_q10_bounce_latest_bundle_path):
    try:
        latest_bundle_path = (
            get_topix100_sma50_raw_vs_atr_q10_bounce_latest_bundle_path()
        )
    except Exception:
        latest_bundle_path = None
    latest_run_id = latest_bundle_path.name if latest_bundle_path else ""
    latest_bundle_path_str = str(latest_bundle_path) if latest_bundle_path else ""
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(default_db_path, latest_bundle_path_str, latest_run_id, mo, pd):
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
        value=latest_bundle_path_str, label="Bundle Path (optional)"
    )
    db_path = mo.ui.text(value=default_db_path, label="DuckDB Path")
    start_date = mo.ui.text(value="", label="Analysis Start Date (YYYY-MM-DD)")
    end_date = mo.ui.text(value="", label="Analysis End Date (YYYY-MM-DD)")
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
                        "- Canonical runner: `apps/bt/scripts/research/run_topix100_sma50_raw_vs_atr_q10_bounce.py`",
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
        pd,
        run_id,
        start_date,
    )


@app.cell
def _(
    DEFAULT_VOLUME_FEATURE,
    bundle_path,
    db_path,
    end_date,
    get_topix100_sma50_raw_vs_atr_q10_bounce_bundle_path_for_run_id,
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
            get_topix100_sma50_raw_vs_atr_q10_bounce_bundle_path_for_run_id(
                run_id_value
            )
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
        "volume_feature": DEFAULT_VOLUME_FEATURE,
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_research_bundle_info,
    load_topix100_sma50_raw_vs_atr_q10_bounce_research_bundle,
    parsed_inputs,
    run_topix100_sma50_raw_vs_atr_q10_bounce_research,
):
    try:
        if parsed_inputs["mode"] == "bundle":
            selected_bundle_path = parsed_inputs["selected_bundle_path"]
            if not selected_bundle_path:
                raise ValueError(
                    "Set a bundle path or run id, or switch Mode to Run Fresh Analysis."
                )
            bundle_info = load_research_bundle_info(selected_bundle_path)
            result = load_topix100_sma50_raw_vs_atr_q10_bounce_research_bundle(
                selected_bundle_path
            )
        else:
            bundle_info = None
            result = run_topix100_sma50_raw_vs_atr_q10_bounce_research(
                parsed_inputs["selected_db_path"],
                start_date=parsed_inputs["selected_start"],
                end_date=parsed_inputs["selected_end"],
                lookback_years=parsed_inputs["lookback_years"],
                min_constituents_per_day=parsed_inputs["min_constituents_per_day"],
                volume_feature=parsed_inputs["volume_feature"],
            )
        error_message = None
    except Exception as exc:
        bundle_info = None
        result = None
        error_message = str(exc)
    return bundle_info, error_message, result


@app.cell
def _(
    TOPIX100_SMA50_RAW_VS_ATR_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID,
    bundle_info,
    error_message,
    mo,
    result,
):
    _view = mo.md("")
    if not error_message and result is not None:
        variant_labels = ", ".join(result.signal_variant_order)
        _view = mo.md(
            "\n".join(
                [
                    "## Research Snapshot",
                    "",
                    f"- Experiment ID: **{TOPIX100_SMA50_RAW_VS_ATR_Q10_BOUNCE_RESEARCH_EXPERIMENT_ID}**",
                    f"- Bundle run: **{bundle_info.run_id if bundle_info else 'fresh analysis'}**",
                    f"- Source mode: **{result.source_mode}**",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Signal variants: **{variant_labels}**",
                    f"- Volume feature: **{result.volume_feature}**",
                    f"- Stock-day rows after warmup/filter: **{result.stock_day_count}**",
                    f"- Valid dates: **{result.valid_date_count}**",
                    "",
                    "This playground compares the plain SMA50 gap with the ATR14-standardized gap inside the same `Q10 / middle x volume` bounce frame.",
                ]
            )
        )
    if error_message:
        _view = mo.md(f"## Error\n\n`{error_message}`")
    _view
    return


@app.cell
def _(
    COMBINED_BUCKET_LABEL_MAP,
    DEFAULT_SIGNAL_VARIANT,
    Q10_LOW_HYPOTHESIS_LABELS,
    Q10_MIDDLE_COMBINED_BUCKET_ORDER,
    SIGNAL_VARIANT_LABEL_MAP,
    VOLUME_FEATURE_LABEL_MAP,
    error_message,
    mo,
    result,
):
    if error_message or result is None:
        signal_variant_view = mo.md("")
        volume_feature_view = mo.md("")
        horizon_view = mo.md("")
        metric_view = mo.md("")
        hypothesis_view = mo.md("")
        sample_bucket_view = mo.md("")
    else:
        signal_variant_view = mo.ui.dropdown(
            options={
                SIGNAL_VARIANT_LABEL_MAP[variant]: variant
                for variant in result.signal_variant_order
            },
            value=DEFAULT_SIGNAL_VARIANT
            if DEFAULT_SIGNAL_VARIANT in result.signal_variant_order
            else result.signal_variant_order[0],
            label="Signal Variant",
        )
        volume_feature_values = (
            result.q10_middle_volume_split_summary_df["volume_feature"]
            .drop_duplicates()
            .tolist()
        )
        volume_feature_view = mo.ui.dropdown(
            options={
                VOLUME_FEATURE_LABEL_MAP[feature]: feature
                for feature in volume_feature_values
            },
            value=volume_feature_values[0],
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
            value="Q10 Low vs Middle High",
            label="Hypothesis",
        )
        sample_bucket_view = mo.ui.dropdown(
            options={
                COMBINED_BUCKET_LABEL_MAP[bucket]: bucket
                for bucket in Q10_MIDDLE_COMBINED_BUCKET_ORDER
            },
            value="middle_volume_high",
            label="Sample Bucket",
        )
    mo.vstack(
        [
            mo.hstack([signal_variant_view, volume_feature_view]),
            mo.hstack([horizon_view, metric_view]),
            mo.hstack([hypothesis_view, sample_bucket_view]),
        ]
    )
    return (
        horizon_view,
        hypothesis_view,
        metric_view,
        sample_bucket_view,
        signal_variant_view,
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
                mo.md("### Full Scorecard"),
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
    result,
    signal_variant_view,
    volume_feature_view,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _summary = result.q10_middle_volume_split_summary_df[
            (
                result.q10_middle_volume_split_summary_df["signal_variant"]
                == signal_variant_view.value
            )
            & (
                result.q10_middle_volume_split_summary_df["volume_feature"]
                == volume_feature_view.value
            )
            & (
                result.q10_middle_volume_split_summary_df["horizon_key"]
                == horizon_view.value
            )
        ].copy()
        _pairwise = result.q10_middle_volume_split_pairwise_significance_df[
            (
                result.q10_middle_volume_split_pairwise_significance_df[
                    "signal_variant"
                ]
                == signal_variant_view.value
            )
            & (
                result.q10_middle_volume_split_pairwise_significance_df[
                    "volume_feature"
                ]
                == volume_feature_view.value
            )
            & (
                result.q10_middle_volume_split_pairwise_significance_df["horizon_key"]
                == horizon_view.value
            )
            & (
                result.q10_middle_volume_split_pairwise_significance_df["metric_key"]
                == metric_view.value
            )
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
    result,
    signal_variant_view,
    volume_feature_view,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _hypothesis = result.q10_low_hypothesis_df[
            (
                result.q10_low_hypothesis_df["signal_variant"]
                == signal_variant_view.value
            )
            & (
                result.q10_low_hypothesis_df["volume_feature"]
                == volume_feature_view.value
            )
            & (result.q10_low_hypothesis_df["horizon_key"] == horizon_view.value)
            & (result.q10_low_hypothesis_df["metric_key"] == metric_view.value)
            & (
                result.q10_low_hypothesis_df["hypothesis_label"]
                == hypothesis_view.value
            )
        ].copy()
        _scorecard = result.q10_low_scorecard_df[
            (result.q10_low_scorecard_df["signal_variant"] == signal_variant_view.value)
            & (
                result.q10_low_scorecard_df["volume_feature"]
                == volume_feature_view.value
            )
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
    result,
    signal_variant_view,
    volume_feature_view,
):
    if not error_message and result is not None:
        _chart_df = result.q10_low_spread_daily_df[
            (
                result.q10_low_spread_daily_df["signal_variant"]
                == signal_variant_view.value
            )
            & (
                result.q10_low_spread_daily_df["volume_feature"]
                == volume_feature_view.value
            )
            & (result.q10_low_spread_daily_df["horizon_key"] == horizon_view.value)
            & (result.q10_low_spread_daily_df["metric_key"] == metric_view.value)
            & (
                result.q10_low_spread_daily_df["hypothesis_label"]
                == hypothesis_view.value
            )
        ].copy()
        if not _chart_df.empty:
            _chart_df["date"] = pd.to_datetime(_chart_df["date"])
            _chart_df = _chart_df.sort_values("date").reset_index(drop=True)
            _chart_df["rolling_mean_difference"] = (
                _chart_df["mean_difference"]
                .rolling(
                    window=63,
                    min_periods=10,
                )
                .mean()
            )

            _spread_fig, _spread_ax = plt.subplots(figsize=(10, 4))
            _spread_ax.plot(
                _chart_df["date"],
                _chart_df["mean_difference"],
                alpha=0.25,
                label="Daily spread",
            )
            _spread_ax.plot(
                _chart_df["date"],
                _chart_df["rolling_mean_difference"],
                linewidth=2,
                label="63D rolling mean",
            )
            _spread_ax.axhline(0.0, color="black", linewidth=1, linestyle="--")
            _spread_ax.set_title(
                f"{signal_variant_view.value} / {horizon_view.value} / {hypothesis_view.value}"
            )
            _spread_ax.set_ylabel(metric_view.value)
            _spread_ax.legend()
            _spread_fig


@app.cell
def _(
    error_message,
    mo,
    result,
    sample_bucket_view,
    signal_variant_view,
    volume_feature_view,
):
    _view = mo.md("")
    if not error_message and result is not None:
        _samples = result.sample_chart_candidates_df[
            (
                result.sample_chart_candidates_df["signal_variant"]
                == signal_variant_view.value
            )
            & (
                result.sample_chart_candidates_df["volume_feature"]
                == volume_feature_view.value
            )
            & (
                result.sample_chart_candidates_df["combined_bucket"]
                == sample_bucket_view.value
            )
        ].copy()
        _view = mo.vstack(
            [
                mo.md("### Representative Sample Candidates"),
                mo.Html(_samples.round(6).to_html(index=False)),
            ]
        )
    _view
    return


@app.cell
def _(
    SAMPLE_LOOKBACK_DAYS,
    error_message,
    pd,
    plt,
    result,
    sample_bucket_view,
    signal_variant_view,
    volume_feature_view,
):
    if not error_message and result is not None:
        _samples = result.sample_chart_candidates_df[
            (
                result.sample_chart_candidates_df["signal_variant"]
                == signal_variant_view.value
            )
            & (
                result.sample_chart_candidates_df["volume_feature"]
                == volume_feature_view.value
            )
            & (
                result.sample_chart_candidates_df["combined_bucket"]
                == sample_bucket_view.value
            )
        ].copy()
        if not _samples.empty:
            _samples = _samples.sort_values(["sample_rank", "date"]).reset_index(
                drop=True
            )
            _rows = len(_samples)
            _audit_fig, _audit_axes = plt.subplots(
                _rows * 3, 1, figsize=(12, max(8, _rows * 7)), sharex=False
            )
            if _rows == 1:
                _audit_axes = [_audit_axes[0], _audit_axes[1], _audit_axes[2]]
            for _sample_index, (_, _sample_row) in enumerate(_samples.iterrows()):
                _history = result.event_panel_df[
                    result.event_panel_df["code"] == _sample_row["code"]
                ].copy()
                _history["date"] = pd.to_datetime(_history["date"])
                _event_date = pd.Timestamp(_sample_row["date"])
                _history = (
                    _history[_history["date"] <= _event_date]
                    .sort_values("date")
                    .tail(SAMPLE_LOOKBACK_DAYS)
                )
                _base = _sample_index * 3
                _ax_price = _audit_axes[_base]
                _ax_raw = _audit_axes[_base + 1]
                _ax_atr = _audit_axes[_base + 2]

                _ax_price.plot(_history["date"], _history["close"], label="Close")
                _ax_price.plot(_history["date"], _history["sma50"], label="SMA50")
                _ax_price.axvline(
                    _event_date, color="black", linestyle="--", linewidth=1
                )
                _ax_price.set_title(
                    f"{_sample_row['sample_rank']}. {_sample_row['code']} {_sample_row['company_name']} "
                    f"({_sample_row['period_segment_label']}) | +5={_sample_row['t_plus_5_return']:+.2%} "
                    f"+10={_sample_row['t_plus_10_return']:+.2%}"
                )
                _ax_price.legend(loc="upper left")

                _ax_raw.plot(_history["date"], _history["raw_gap"], color="#1f77b4")
                _ax_raw.axhline(0.0, color="black", linestyle="--", linewidth=1)
                _ax_raw.axvline(_event_date, color="black", linestyle="--", linewidth=1)
                _ax_raw.set_ylabel("raw_gap")

                _ax_atr.plot(
                    _history["date"],
                    _history["atr_gap_14"],
                    label="atr_gap_14",
                    color="#d62728",
                )
                _ax_atr.plot(
                    _history["date"],
                    _history["atr14"],
                    label="atr14",
                    color="#2ca02c",
                    alpha=0.8,
                )
                _ax_atr.axhline(0.0, color="black", linestyle="--", linewidth=1)
                _ax_atr.axvline(_event_date, color="black", linestyle="--", linewidth=1)
                _ax_atr.set_ylabel("atr")
                _ax_atr.legend(loc="upper left")
            _audit_fig.tight_layout()
            _audit_fig


if __name__ == "__main__":
    app.run()
