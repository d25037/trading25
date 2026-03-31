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
    app_title="TOPIX100 VI Change Regime Conditioning Playground",
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
    project_root = Path.cwd()
    if project_root.name == "playground":
        project_root = project_root.parent.parent
    elif project_root.name == "notebooks":
        project_root = project_root.parent

    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from src.domains.analytics.research_bundle import load_research_bundle_info
    from src.domains.analytics.topix100_vi_change_regime_conditioning import (
        get_topix100_vi_change_available_date_range,
        get_topix100_vi_change_regime_conditioning_bundle_path_for_run_id,
        get_topix100_vi_change_regime_conditioning_latest_bundle_path,
        load_topix100_vi_change_regime_conditioning_research_bundle,
        run_topix100_vi_change_regime_conditioning_research,
    )
    from src.shared.config.settings import get_settings

    default_db_path = get_settings().market_db_path
    return (
        default_db_path,
        get_topix100_vi_change_available_date_range,
        get_topix100_vi_change_regime_conditioning_bundle_path_for_run_id,
        get_topix100_vi_change_regime_conditioning_latest_bundle_path,
        load_research_bundle_info,
        load_topix100_vi_change_regime_conditioning_research_bundle,
        run_topix100_vi_change_regime_conditioning_research,
    )


@app.cell
def _(get_topix100_vi_change_regime_conditioning_latest_bundle_path):
    try:
        latest_bundle_path = get_topix100_vi_change_regime_conditioning_latest_bundle_path()
    except Exception:
        latest_bundle_path = None
    latest_run_id = latest_bundle_path.name if latest_bundle_path else ""
    latest_bundle_path_str = str(latest_bundle_path) if latest_bundle_path else ""
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(default_db_path, get_topix100_vi_change_available_date_range, latest_bundle_path_str, latest_run_id, mo):
    try:
        initial_range = get_topix100_vi_change_available_date_range(default_db_path)
    except Exception:
        initial_range = (None, None)
    available_start_date, available_end_date = initial_range

    mode = mo.ui.dropdown(
        options={"bundle": "Load Existing Bundle", "recompute": "Run Fresh Analysis"},
        value="bundle",
        label="Mode",
    )
    run_id = mo.ui.text(value=latest_run_id, label="Run ID")
    bundle_path = mo.ui.text(value=latest_bundle_path_str, label="Bundle Path (optional)")
    db_path = mo.ui.text(value=default_db_path, label="DuckDB Path")
    start_date = mo.ui.text(
        value=available_start_date or "",
        label="Analysis Start Date (YYYY-MM-DD)",
    )
    end_date = mo.ui.text(
        value=available_end_date or "",
        label="Analysis End Date (YYYY-MM-DD)",
    )
    lookback_years = mo.ui.number(value=10, start=1, step=1, label="Lookback Years")
    min_constituents_per_day = mo.ui.number(
        value=80,
        start=10,
        step=1,
        label="Min Constituents / Day",
    )
    sigma_threshold_1 = mo.ui.number(value=1.0, start=0.5, step=0.25, label="Sigma 1")
    sigma_threshold_2 = mo.ui.number(value=2.0, start=1.0, step=0.25, label="Sigma 2")

    recompute_controls = mo.vstack(
        [
            db_path,
            mo.hstack([start_date, end_date]),
            mo.hstack([lookback_years, min_constituents_per_day]),
            mo.hstack([sigma_threshold_1, sigma_threshold_2]),
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
                        "- Canonical runner: `apps/bt/scripts/research/run_topix100_vi_change_regime_conditioning.py`",
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
        sigma_threshold_1,
        sigma_threshold_2,
        start_date,
    )


@app.cell
def _(
    bundle_path,
    db_path,
    end_date,
    get_topix100_vi_change_regime_conditioning_bundle_path_for_run_id,
    lookback_years,
    min_constituents_per_day,
    mode,
    run_id,
    sigma_threshold_1,
    sigma_threshold_2,
    start_date,
):
    run_id_value = run_id.value.strip()
    bundle_path_value = bundle_path.value.strip()
    resolved_bundle_path = bundle_path_value
    if not resolved_bundle_path and run_id_value:
        resolved_bundle_path = str(
            get_topix100_vi_change_regime_conditioning_bundle_path_for_run_id(
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
        "sigma_threshold_1": float(sigma_threshold_1.value),
        "sigma_threshold_2": float(sigma_threshold_2.value),
    }
    return (parsed_inputs,)


@app.cell
def _(
    load_research_bundle_info,
    load_topix100_vi_change_regime_conditioning_research_bundle,
    parsed_inputs,
    run_topix100_vi_change_regime_conditioning_research,
):
    try:
        if parsed_inputs["mode"] == "bundle":
            selected_bundle_path = parsed_inputs["selected_bundle_path"]
            if not selected_bundle_path:
                raise ValueError(
                    "Set a bundle path or run id, or switch Mode to Run Fresh Analysis."
                )
            bundle_info = load_research_bundle_info(selected_bundle_path)
            result = load_topix100_vi_change_regime_conditioning_research_bundle(
                selected_bundle_path
            )
        else:
            bundle_info = None
            result = run_topix100_vi_change_regime_conditioning_research(
                parsed_inputs["selected_db_path"],
                start_date=parsed_inputs["selected_start"],
                end_date=parsed_inputs["selected_end"],
                lookback_years=parsed_inputs["lookback_years"],
                min_constituents_per_day=parsed_inputs["min_constituents_per_day"],
                sigma_threshold_1=parsed_inputs["sigma_threshold_1"],
                sigma_threshold_2=parsed_inputs["sigma_threshold_2"],
            )
        error_message = None
    except Exception as exc:
        bundle_info = None
        result = None
        error_message = str(exc)
    return bundle_info, error_message, result


@app.cell
def _(bundle_info, error_message, mo, parsed_inputs, result):
    summary_view = mo.md("")
    if not error_message and result is not None:
        summary_view = mo.md(
            "\n".join(
                [
                    "## TOPIX100 VI Change Regime Conditioning",
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
                    f"- VI available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Latest TOPIX100 constituent count: **{result.universe_constituent_count}**",
                    f"- Valid dates with VI regime: **{result.valid_date_count}**",
                    f"- Sigma thresholds: **{parsed_inputs['sigma_threshold_1']} / {parsed_inputs['sigma_threshold_2']}**",
                    "",
                    "Primary split is fixed to **price vs 20SMA gap x volume_sma_20_80**.",
                    "Regimes are bucketed on **same-day Nikkei VI change**, then collapsed to **Weak / Neutral / Strong**.",
                ]
            )
        )
    summary_view
    return


@app.cell
def _(error_message, mo, pd, result):
    stats_view = mo.md("")
    if not error_message and result is not None:
        vi_stats_df = pd.DataFrame(
            [result.vi_change_stats.__dict__] if result.vi_change_stats else []
        )
        stats_view = mo.vstack(
            [
                mo.md("## Regime Stats"),
                mo.md("### Nikkei VI Change"),
                mo.Html(vi_stats_df.round(6).to_html(index=False)),
            ]
        )
    stats_view
    return


@app.cell
def _(error_message, mo, result):
    day_count_view = mo.md("")
    if not error_message and result is not None:
        day_count_view = mo.vstack(
            [
                mo.md("## Bucket Coverage"),
                mo.Html(result.regime_day_counts_df.round(6).to_html(index=False)),
                mo.md("## Collapsed Coverage"),
                mo.Html(result.regime_group_day_counts_df.round(6).to_html(index=False)),
            ]
        )
    day_count_view
    return


@app.cell
def _(error_message, mo, result):
    summary_tables_view = mo.md("")
    if not error_message and result is not None:
        summary_tables_view = mo.vstack(
            [
                mo.md("## Detailed Summary"),
                mo.Html(result.regime_summary_df.round(6).to_html(index=False)),
                mo.md("## Collapsed Summary"),
                mo.Html(result.regime_group_summary_df.round(6).to_html(index=False)),
            ]
        )
    summary_tables_view
    return


@app.cell
def _(error_message, mo, result):
    hypothesis_view = mo.md("")
    if not error_message and result is not None:
        hypothesis_view = mo.vstack(
            [
                mo.md("## Detailed Hypothesis Table"),
                mo.Html(result.regime_hypothesis_df.round(6).to_html(index=False)),
                mo.md("## Collapsed Hypothesis Table"),
                mo.Html(result.regime_group_hypothesis_df.round(6).to_html(index=False)),
            ]
        )
    hypothesis_view
    return


if __name__ == "__main__":
    app.run()
