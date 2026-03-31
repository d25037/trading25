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
    app_title="Stock Intraday / Overnight Share Playground",
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
    _project_root = Path.cwd()
    if _project_root.name == "playground":
        _project_root = _project_root.parent.parent
    elif _project_root.name == "notebooks":
        _project_root = _project_root.parent

    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))

    from src.shared.config.settings import get_settings
    from src.domains.analytics.research_bundle import load_research_bundle_info
    from src.domains.analytics.stock_intraday_overnight_share import (
        STOCK_INTRADAY_OVERNIGHT_SHARE_RESEARCH_EXPERIMENT_ID,
        STOCK_GROUP_ORDER,
        get_stock_intraday_overnight_share_bundle_path_for_run_id,
        get_stock_intraday_overnight_share_latest_bundle_path,
        get_stock_available_date_range,
        load_stock_intraday_overnight_share_research_bundle,
        run_stock_intraday_overnight_share_analysis,
    )

    default_db_path = get_settings().market_db_path
    return (
        STOCK_INTRADAY_OVERNIGHT_SHARE_RESEARCH_EXPERIMENT_ID,
        STOCK_GROUP_ORDER,
        default_db_path,
        get_stock_intraday_overnight_share_bundle_path_for_run_id,
        get_stock_intraday_overnight_share_latest_bundle_path,
        get_stock_available_date_range,
        load_research_bundle_info,
        load_stock_intraday_overnight_share_research_bundle,
        run_stock_intraday_overnight_share_analysis,
    )


@app.cell
def _(get_stock_intraday_overnight_share_latest_bundle_path):
    try:
        latest_bundle_path = get_stock_intraday_overnight_share_latest_bundle_path()
    except Exception:
        latest_bundle_path = None
    latest_run_id = latest_bundle_path.name if latest_bundle_path else ""
    latest_bundle_path_str = str(latest_bundle_path) if latest_bundle_path else ""
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(default_db_path, get_stock_available_date_range):
    try:
        initial_range = get_stock_available_date_range(default_db_path)
    except Exception:
        initial_range = (None, None)
    return (initial_range,)


@app.cell
def _(
    STOCK_GROUP_ORDER,
    default_db_path,
    initial_range,
    latest_bundle_path_str,
    latest_run_id,
    mo,
):
    _available_start_date, _available_end_date = initial_range

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
        value=_available_start_date or "",
        label="Event Start Date (YYYY-MM-DD)",
    )
    end_date = mo.ui.text(
        value=_available_end_date or "",
        label="Event End Date (YYYY-MM-DD)",
    )
    selected_groups = mo.ui.text(
        value=", ".join(STOCK_GROUP_ORDER),
        label="Groups (comma separated)",
    )
    min_session_count = mo.ui.number(
        value=60,
        start=0,
        step=10,
        label="Minimum Sessions Per Stock",
    )
    top_n = mo.ui.number(
        value=20,
        start=1,
        step=1,
        label="Focus Group Top N",
    )
    rolling_window = mo.ui.slider(
        1,
        60,
        value=20,
        step=1,
        label="Daily Share Rolling Window",
    )
    focus_metric = mo.ui.dropdown(
        options=[
            "Overnight Share",
            "Intraday Share",
            "Total Abs Move",
        ],
        value="Overnight Share",
        label="Focus Group Ranking Metric",
    )

    recompute_controls = mo.vstack(
        [
            db_path,
            mo.hstack([start_date, end_date]),
            selected_groups,
            mo.hstack([min_session_count, top_n]),
            rolling_window,
            focus_metric,
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
                        "- Canonical runner: `apps/bt/scripts/research/run_stock_intraday_overnight_share.py`",
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
        focus_metric,
        min_session_count,
        mode,
        run_id,
        rolling_window,
        selected_groups,
        start_date,
        top_n,
    )


@app.cell
def _(
    STOCK_GROUP_ORDER,
    bundle_path,
    db_path,
    end_date,
    focus_metric,
    get_stock_intraday_overnight_share_bundle_path_for_run_id,
    min_session_count,
    mode,
    run_id,
    rolling_window,
    selected_groups,
    start_date,
    top_n,
):
    _focus_metric_map = {
        "Overnight Share": "overnight_share",
        "Intraday Share": "intraday_share",
        "Total Abs Move": "total_abs_log_return_sum",
    }
    _requested_groups = [
        value.strip()
        for value in selected_groups.value.split(",")
        if value.strip()
    ]
    if not _requested_groups:
        _requested_groups = list(STOCK_GROUP_ORDER)

    run_id_value = run_id.value.strip()
    bundle_path_value = bundle_path.value.strip()
    resolved_bundle_path = bundle_path_value
    if not resolved_bundle_path and run_id_value:
        resolved_bundle_path = str(
            get_stock_intraday_overnight_share_bundle_path_for_run_id(run_id_value)
        )
    parsed_inputs = {
        "mode": mode.value,
        "run_id": run_id_value or None,
        "selected_bundle_path": resolved_bundle_path or None,
        "focus_metric": _focus_metric_map[focus_metric.value],
        "min_session_count": int(min_session_count.value),
        "requested_groups": _requested_groups,
        "rolling_window": int(rolling_window.value),
        "selected_db_path": db_path.value.strip(),
        "selected_end": end_date.value.strip() or None,
        "selected_start": start_date.value.strip() or None,
        "top_n": int(top_n.value),
        "focus_metric_label": focus_metric.value,
    }
    return (parsed_inputs,)


@app.cell
def _(mo, parsed_inputs):
    _options = {group: group for group in parsed_inputs["requested_groups"]}
    _first_group = parsed_inputs["requested_groups"][0]
    focus_group = mo.ui.dropdown(
        options=_options,
        value=_first_group,
        label="Focus Group",
    )
    focus_group
    return (focus_group,)


@app.cell
def _(
    load_research_bundle_info,
    load_stock_intraday_overnight_share_research_bundle,
    parsed_inputs,
    run_stock_intraday_overnight_share_analysis,
):
    try:
        if parsed_inputs["mode"] == "bundle":
            selected_bundle_path = parsed_inputs["selected_bundle_path"]
            if not selected_bundle_path:
                raise ValueError(
                    "Set a bundle path or run id, or switch Mode to Run Fresh Analysis."
                )
            bundle_info = load_research_bundle_info(selected_bundle_path)
            result = load_stock_intraday_overnight_share_research_bundle(
                selected_bundle_path
            )
        else:
            bundle_info = None
            result = run_stock_intraday_overnight_share_analysis(
                parsed_inputs["selected_db_path"],
                start_date=parsed_inputs["selected_start"],
                end_date=parsed_inputs["selected_end"],
                selected_groups=parsed_inputs["requested_groups"],
                min_session_count=parsed_inputs["min_session_count"],
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
    STOCK_INTRADAY_OVERNIGHT_SHARE_RESEARCH_EXPERIMENT_ID,
    bundle_info,
    error_message,
    mo,
    result,
):
    _summary_view = mo.md("")
    if not error_message and result is not None:
        _total_stocks = int(result.stock_metrics_df["code"].nunique())
        _bundle_lines = []
        if bundle_info is not None:
            _bundle_lines = [
                f"- Experiment: **{STOCK_INTRADAY_OVERNIGHT_SHARE_RESEARCH_EXPERIMENT_ID}**",
                f"- Bundle run: **{bundle_info.run_id}**",
                "",
            ]
        _summary_view = mo.md(
            "\n".join(
                [
                    "## Stock Intraday / Overnight Share Playground",
                    "",
                    "- Definition: `intraday_share = Σ|log(C_t / O_t)| / Σ(|log(C_t / O_t)| + |log(O_{t+1} / C_t)|)`",
                    "- Definition: `overnight_share = Σ|log(O_{t+1} / C_t)| / Σ(|log(C_t / O_t)| + |log(O_{t+1} / C_t)|)`",
                    "- Event unit: current-session intraday + next-session overnight pair",
                    "- Group note: `TOPIX500` includes `TOPIX100`; `PRIME ex TOPIX500` excludes those constituents",
                    "",
                    f"- Source mode: **{result.source_mode}**",
                    f"- Source detail: **{result.source_detail}**",
                    f"- Available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Selected groups: **{', '.join(result.selected_groups)}**",
                    f"- Minimum sessions per stock: **{result.min_session_count}**",
                    f"- Included stocks after filter: **{_total_stocks}**",
                    "",
                    *_bundle_lines,
                ]
            )
        )
    _summary_view
    return


@app.cell
def _(error_message, mo, pd, result):
    _table_view = mo.md("")
    if not error_message and result is not None:
        _group_summary_df = result.group_summary_df.copy()
        _share_columns = [
            "mean_intraday_share",
            "median_intraday_share",
            "p25_intraday_share",
            "p75_intraday_share",
            "mean_overnight_share",
            "median_overnight_share",
            "p25_overnight_share",
            "p75_overnight_share",
        ]
        for _column in _share_columns:
            _group_summary_df[_column] = _group_summary_df[_column].map(
                lambda value: f"{value * 100:.2f}%" if pd.notna(value) else ""
            )

        _table_view = mo.vstack(
            [
                mo.md("### Group Summary"),
                mo.Html(
                    _group_summary_df.to_html(
                        index=False,
                        float_format=lambda value: f"{value:.4f}",
                    )
                ),
            ]
        )
    _table_view
    return


@app.cell
def _(error_message, mo, plt, result):
    _median_share_chart = mo.md("")
    if not error_message and result is not None:
        _plot_df = result.group_summary_df.dropna(
            subset=["median_intraday_share", "median_overnight_share"]
        )
        if _plot_df.empty:
            _median_share_chart = mo.md("No median-share rows to plot.")
        else:
            _fig, _ax = plt.subplots(figsize=(10, 4.5))
            _groups = _plot_df["stock_group"].tolist()
            _intraday = (_plot_df["median_intraday_share"] * 100.0).tolist()
            _overnight = (_plot_df["median_overnight_share"] * 100.0).tolist()

            _ax.barh(_groups, _intraday, label="Median Intraday Share", color="#e07a5f")
            _ax.barh(
                _groups,
                _overnight,
                left=_intraday,
                label="Median Overnight Share",
                color="#3d405b",
            )
            _ax.set_xlim(0.0, 100.0)
            _ax.set_xlabel("Share (%)")
            _ax.set_title("Median Share Composition By Group")
            _ax.legend(loc="lower right")
            _ax.grid(axis="x", alpha=0.2)
            _fig.tight_layout()
            _median_share_chart = _fig
    _median_share_chart
    return


@app.cell
def _(error_message, mo, plt, result):
    _boxplot_chart = mo.md("")
    if not error_message and result is not None:
        _plot_df = result.stock_metrics_df.dropna(subset=["overnight_share"])
        _data = []
        _labels = []
        for _group in result.selected_groups:
            _group_values = _plot_df.loc[
                _plot_df["stock_group"] == _group,
                "overnight_share",
            ].tolist()
            if _group_values:
                _data.append([value * 100.0 for value in _group_values])
                _labels.append(_group)

        if not _data:
            _boxplot_chart = mo.md("No stock-level overnight-share rows to plot.")
        else:
            _fig, _ax = plt.subplots(figsize=(11, 5))
            _box = _ax.boxplot(_data, patch_artist=True, labels=_labels)
            _palette = ["#3d405b", "#81b29a", "#e07a5f", "#f2cc8f", "#5b8e7d"]
            for _patch, _color in zip(_box["boxes"], _palette, strict=False):
                _patch.set_facecolor(_color)
                _patch.set_alpha(0.8)

            _ax.set_ylabel("Overnight Share (%)")
            _ax.set_title("Stock-Level Overnight Share Distribution")
            _ax.grid(axis="y", alpha=0.2)
            plt.xticks(rotation=10, ha="right")
            _fig.tight_layout()
            _boxplot_chart = _fig
    _boxplot_chart
    return


@app.cell
def _(error_message, mo, parsed_inputs, plt, result):
    _daily_chart = mo.md("")
    if not error_message and result is not None:
        _daily_df = result.daily_group_shares_df.copy()
        if _daily_df.empty:
            _daily_chart = mo.md("No daily group-share rows to plot.")
        else:
            _window = parsed_inputs["rolling_window"]
            _daily_df["overnight_share_rolling"] = _daily_df.groupby("stock_group")[
                "overnight_share"
            ].transform(
                lambda values: values.rolling(window=_window, min_periods=1).mean()
            )

            _fig, _ax = plt.subplots(figsize=(12, 5))
            for _group in result.selected_groups:
                _group_df = _daily_df.loc[_daily_df["stock_group"] == _group]
                if _group_df.empty:
                    continue
                _ax.plot(
                    _group_df["date"],
                    _group_df["overnight_share_rolling"] * 100.0,
                    label=_group,
                    linewidth=1.8,
                )

            _ax.set_ylabel("Overnight Share (%)")
            _ax.set_title(f"Daily Overnight Share ({_window}-day rolling mean)")
            _ax.grid(alpha=0.2)
            _ax.legend(loc="best")
            plt.xticks(rotation=20, ha="right")
            _fig.tight_layout()
            _daily_chart = _fig
    _daily_chart
    return


@app.cell
def _(error_message, focus_group, mo, parsed_inputs, plt, result):
    _focus_chart = mo.md("")
    if not error_message and result is not None:
        _focus_metric = parsed_inputs["focus_metric"]
        _focus_df = result.stock_metrics_df.loc[
            result.stock_metrics_df["stock_group"] == focus_group.value
        ].copy()

        if _focus_df.empty:
            _focus_chart = mo.md("No stock rows available for the selected focus group.")
        else:
            _focus_df = _focus_df.sort_values(
                by=[_focus_metric, "total_abs_log_return_sum", "code"],
                ascending=[False, False, True],
                na_position="last",
            ).head(parsed_inputs["top_n"])

            _labels = [
                f"{code} {name}"
                for code, name in zip(
                    _focus_df["code"],
                    _focus_df["company_name"],
                    strict=True,
                )
            ]
            _values = _focus_df[_focus_metric]
            _is_share_metric = _focus_metric.endswith("_share")
            _plot_values = _values * 100.0 if _is_share_metric else _values

            _fig, _ax = plt.subplots(figsize=(12, max(4, len(_focus_df) * 0.35)))
            _ax.barh(_labels, _plot_values, color="#3d405b")
            _ax.invert_yaxis()
            _ax.set_title(
                f"{focus_group.value}: top {parsed_inputs['top_n']} by {parsed_inputs['focus_metric_label']}"
            )
            _ax.set_xlabel("Percent" if _is_share_metric else "Absolute log-return sum")
            _ax.grid(axis="x", alpha=0.2)
            _fig.tight_layout()
            _focus_chart = _fig
    _focus_chart
    return


@app.cell
def _(error_message, focus_group, mo, parsed_inputs, pd, result):
    _table_view = mo.md("")
    if not error_message and result is not None:
        _focus_metric = parsed_inputs["focus_metric"]
        _focus_df = result.stock_metrics_df.loc[
            result.stock_metrics_df["stock_group"] == focus_group.value
        ].copy()
        _focus_df = _focus_df.sort_values(
            by=[_focus_metric, "total_abs_log_return_sum", "code"],
            ascending=[False, False, True],
            na_position="last",
        ).head(parsed_inputs["top_n"])

        for _column in ("intraday_share", "overnight_share"):
            _focus_df[_column] = _focus_df[_column].map(
                lambda value: f"{value * 100:.2f}%" if pd.notna(value) else ""
            )

        _daily_tail_df = result.daily_group_shares_df.copy().tail(100)
        for _column in ("intraday_share", "overnight_share"):
            _daily_tail_df[_column] = _daily_tail_df[_column].map(
                lambda value: f"{value * 100:.2f}%" if pd.notna(value) else ""
            )

        _table_view = mo.vstack(
            [
                mo.md(f"### Focus Group Stocks: {focus_group.value}"),
                mo.Html(
                    _focus_df.to_html(
                        index=False,
                        float_format=lambda value: f"{value:.6f}",
                    )
                ),
                mo.md("### Daily Group Shares (latest 100 rows)"),
                mo.Html(
                    _daily_tail_df.to_html(
                        index=False,
                        float_format=lambda value: f"{value:.6f}",
                    )
                ),
            ]
        )
    _table_view
    return


if __name__ == "__main__":
    app.run()
