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
    _project_root = Path.cwd()
    if _project_root.name == "playground":
        _project_root = _project_root.parent.parent
    elif _project_root.name == "notebooks":
        _project_root = _project_root.parent

    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))

    from src.shared.config.settings import get_settings
    from src.domains.analytics.research_bundle import load_research_bundle_info
    from src.domains.analytics.prime_ex_topix500_sma_ratio_rank_future_close import (
        HORIZON_ORDER,
        METRIC_ORDER,
        get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id,
        get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range,
        get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path,
        load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
        run_prime_ex_topix500_sma_ratio_rank_future_close_research,
    )

    default_db_path = get_settings().market_db_path
    return (
        HORIZON_ORDER,
        METRIC_ORDER,
        default_db_path,
        get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id,
        get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range,
        get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path,
        load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
        load_research_bundle_info,
        run_prime_ex_topix500_sma_ratio_rank_future_close_research,
    )


@app.cell
def _(get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path):
    try:
        latest_bundle_path = (
            get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path()
        )
    except Exception:
        latest_bundle_path = None
    latest_run_id = latest_bundle_path.name if latest_bundle_path else ""
    latest_bundle_path_str = str(latest_bundle_path) if latest_bundle_path else ""
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(
    default_db_path,
    get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range,
):
    try:
        initial_range = get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range(
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
        default_start_date = max(available_start_date or candidate, candidate)

    mode = mo.ui.dropdown(
        options={"bundle": "Load Existing Bundle", "recompute": "Run Fresh Analysis"},
        value="bundle",
        label="Mode",
    )
    run_id = mo.ui.text(value=latest_run_id, label="Run ID")
    bundle_path = mo.ui.text(value=latest_bundle_path_str, label="Bundle Path (optional)")
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
        value=400,
        start=50,
        step=10,
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
                        "- Canonical runner: `apps/bt/scripts/research/run_prime_ex_topix500_sma_ratio_rank_future_close.py`",
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
    get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id,
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
            get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id(
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
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
    load_research_bundle_info,
    parsed_inputs,
    run_prime_ex_topix500_sma_ratio_rank_future_close_research,
):
    try:
        if parsed_inputs["mode"] == "bundle":
            selected_bundle_path = parsed_inputs["selected_bundle_path"]
            if not selected_bundle_path:
                raise ValueError(
                    "Set a bundle path or run id, or switch Mode to Run Fresh Analysis."
                )
            bundle_info = load_research_bundle_info(selected_bundle_path)
            result = load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle(
                selected_bundle_path
            )
        else:
            bundle_info = None
            result = run_prime_ex_topix500_sma_ratio_rank_future_close_research(
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
def _(bundle_info, error_message, mo, parsed_inputs, result):
    summary_view = mo.md("")
    if not error_message and result is not None:
        summary_view = mo.md(
            "\n".join(
                [
                    "## PRIME ex TOPIX500 SMA Ratio Rank / Future Close Research",
                    "",
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
                    f"- Source mode: **{result.source_mode}**",
                    f"- Source detail: **{result.source_detail}**",
                    f"- Universe: **{result.universe_label}**",
                    f"- Available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Requested range: **{parsed_inputs['selected_start'] or result.default_start_date} -> {parsed_inputs['selected_end'] or result.available_end_date}**",
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
