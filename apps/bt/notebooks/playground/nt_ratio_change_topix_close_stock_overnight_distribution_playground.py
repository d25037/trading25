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
    app_title="NT Ratio Change x TOPIX Close / Stock Overnight Distribution Playground",
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
    from src.domains.analytics.nt_ratio_change_topix_close_stock_overnight_distribution import (
        NT_RATIO_CHANGE_TOPIX_CLOSE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
        STOCK_GROUP_ORDER,
        get_nt_ratio_change_topix_close_stock_overnight_distribution_bundle_path_for_run_id,
        get_nt_ratio_change_topix_close_stock_overnight_distribution_latest_bundle_path,
        get_nt_ratio_change_topix_close_available_date_range,
        load_nt_ratio_change_topix_close_stock_overnight_distribution_research_bundle,
        run_nt_ratio_change_topix_close_stock_overnight_distribution,
    )
    from src.domains.analytics.topix_close_stock_overnight_distribution import (
        get_topix_close_return_stats,
    )

    default_db_path = get_settings().market_db_path
    return (
        NT_RATIO_CHANGE_TOPIX_CLOSE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
        STOCK_GROUP_ORDER,
        default_db_path,
        get_nt_ratio_change_topix_close_stock_overnight_distribution_bundle_path_for_run_id,
        get_nt_ratio_change_topix_close_stock_overnight_distribution_latest_bundle_path,
        get_nt_ratio_change_topix_close_available_date_range,
        get_topix_close_return_stats,
        load_nt_ratio_change_topix_close_stock_overnight_distribution_research_bundle,
        load_research_bundle_info,
        run_nt_ratio_change_topix_close_stock_overnight_distribution,
    )


@app.cell
def _(get_nt_ratio_change_topix_close_stock_overnight_distribution_latest_bundle_path):
    try:
        latest_bundle_path = (
            get_nt_ratio_change_topix_close_stock_overnight_distribution_latest_bundle_path()
        )
    except Exception:
        latest_bundle_path = None
    latest_run_id = latest_bundle_path.name if latest_bundle_path else ""
    latest_bundle_path_str = str(latest_bundle_path) if latest_bundle_path else ""
    return latest_bundle_path_str, latest_run_id


@app.cell
def _(default_db_path, get_nt_ratio_change_topix_close_available_date_range):
    try:
        initial_range = get_nt_ratio_change_topix_close_available_date_range(
            default_db_path
        )
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
    sigma_threshold_1 = mo.ui.number(
        value=1.0,
        start=0.1,
        stop=10.0,
        step=0.1,
        label="NT Sigma Threshold 1",
    )
    sigma_threshold_2 = mo.ui.number(
        value=2.0,
        start=0.2,
        stop=20.0,
        step=0.1,
        label="NT Sigma Threshold 2",
    )
    topix_sigma_threshold_1 = mo.ui.number(
        value=1.0,
        start=0.1,
        stop=10.0,
        step=0.1,
        label="TOPIX Close Sigma Threshold 1",
    )
    topix_sigma_threshold_2 = mo.ui.number(
        value=2.0,
        start=0.2,
        stop=20.0,
        step=0.1,
        label="TOPIX Close Sigma Threshold 2",
    )
    sample_size = mo.ui.number(
        value=1000,
        start=0,
        step=100,
        label="Sample Size Per Group/Joint Bucket",
    )
    clip_lower = mo.ui.number(
        value=1.0,
        start=0.0,
        stop=49.0,
        step=0.5,
        label="Clip Lower Percentile",
    )
    clip_upper = mo.ui.number(
        value=99.0,
        start=51.0,
        stop=100.0,
        step=0.5,
        label="Clip Upper Percentile",
    )

    recompute_controls = mo.vstack(
        [
            db_path,
            mo.hstack([start_date, end_date]),
            selected_groups,
            mo.hstack([sigma_threshold_1, sigma_threshold_2]),
            mo.hstack([topix_sigma_threshold_1, topix_sigma_threshold_2]),
            mo.hstack([sample_size, clip_lower, clip_upper]),
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
                        "- Canonical runner: `apps/bt/scripts/research/run_nt_ratio_change_topix_close_stock_overnight_distribution.py`",
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
        clip_lower,
        clip_upper,
        db_path,
        end_date,
        mode,
        run_id,
        sample_size,
        selected_groups,
        sigma_threshold_1,
        sigma_threshold_2,
        start_date,
        topix_sigma_threshold_1,
        topix_sigma_threshold_2,
    )


@app.cell
def _(
    STOCK_GROUP_ORDER,
    bundle_path,
    clip_lower,
    clip_upper,
    db_path,
    end_date,
    get_nt_ratio_change_topix_close_stock_overnight_distribution_bundle_path_for_run_id,
    mode,
    run_id,
    sample_size,
    selected_groups,
    sigma_threshold_1,
    sigma_threshold_2,
    start_date,
    topix_sigma_threshold_1,
    topix_sigma_threshold_2,
):
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
            get_nt_ratio_change_topix_close_stock_overnight_distribution_bundle_path_for_run_id(
                run_id_value
            )
        )
    parsed_inputs = {
        "mode": mode.value,
        "run_id": run_id_value or None,
        "selected_bundle_path": resolved_bundle_path or None,
        "requested_groups": _requested_groups,
        "selected_clip": (float(clip_lower.value), float(clip_upper.value)),
        "selected_db_path": db_path.value.strip(),
        "selected_end": end_date.value.strip() or None,
        "selected_sample_size": int(sample_size.value),
        "selected_start": start_date.value.strip() or None,
        "sigma_thresholds": (
            float(sigma_threshold_1.value),
            float(sigma_threshold_2.value),
        ),
        "topix_sigma_thresholds": (
            float(topix_sigma_threshold_1.value),
            float(topix_sigma_threshold_2.value),
        ),
    }
    return (parsed_inputs,)


@app.cell
def _(mo, parsed_inputs):
    _options = {group: group for group in parsed_inputs["requested_groups"]}
    _first_group = parsed_inputs["requested_groups"][0]
    focus_group = mo.ui.dropdown(
        options=_options,
        value=_first_group,
        label="Focus Group For Distribution",
    )
    focus_group
    return (focus_group,)


@app.cell
def _(
    get_topix_close_return_stats,
    load_nt_ratio_change_topix_close_stock_overnight_distribution_research_bundle,
    load_research_bundle_info,
    parsed_inputs,
    run_nt_ratio_change_topix_close_stock_overnight_distribution,
):
    try:
        if parsed_inputs["mode"] == "bundle":
            selected_bundle_path = parsed_inputs["selected_bundle_path"]
            if not selected_bundle_path:
                raise ValueError(
                    "Set a bundle path or run id, or switch Mode to Run Fresh Analysis."
                )
            bundle_info = load_research_bundle_info(selected_bundle_path)
            result = load_nt_ratio_change_topix_close_stock_overnight_distribution_research_bundle(
                selected_bundle_path
            )
        else:
            _sigma_threshold_1, _sigma_threshold_2 = parsed_inputs["sigma_thresholds"]
            _topix_sigma_threshold_1, _topix_sigma_threshold_2 = parsed_inputs[
                "topix_sigma_thresholds"
            ]
            _topix_close_stats = get_topix_close_return_stats(
                parsed_inputs["selected_db_path"],
                start_date=parsed_inputs["selected_start"],
                end_date=parsed_inputs["selected_end"],
                sigma_threshold_1=_topix_sigma_threshold_1,
                sigma_threshold_2=_topix_sigma_threshold_2,
            )
            if _topix_close_stats is None:
                raise ValueError("No analyzable TOPIX close rows in selected range.")
            bundle_info = None
            result = run_nt_ratio_change_topix_close_stock_overnight_distribution(
                parsed_inputs["selected_db_path"],
                start_date=parsed_inputs["selected_start"],
                end_date=parsed_inputs["selected_end"],
                sigma_threshold_1=_sigma_threshold_1,
                sigma_threshold_2=_sigma_threshold_2,
                topix_close_threshold_1=_topix_close_stats.threshold_1,
                topix_close_threshold_2=_topix_close_stats.threshold_2,
                topix_close_stats=_topix_close_stats,
                selected_groups=parsed_inputs["requested_groups"],
                sample_size=parsed_inputs["selected_sample_size"],
                clip_percentiles=parsed_inputs["selected_clip"],
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
def _(error_message, result):
    if error_message or result is None:
        nt_bucket_order = []
        topix_bucket_order = []
        nt_bucket_labels = {}
        topix_bucket_labels = {}
    else:
        nt_bucket_order = list(
            result.joint_day_counts_df["nt_ratio_bucket_key"].drop_duplicates()
        )
        topix_bucket_order = list(
            result.joint_day_counts_df["topix_close_bucket_key"].drop_duplicates()
        )
        nt_bucket_labels = {
            row["nt_ratio_bucket_key"]: row["nt_ratio_bucket_label"]
            for _, row in result.joint_day_counts_df.iterrows()
        }
        topix_bucket_labels = {
            row["topix_close_bucket_key"]: row["topix_close_bucket_label"]
            for _, row in result.joint_day_counts_df.iterrows()
        }
    return (
        nt_bucket_labels,
        nt_bucket_order,
        topix_bucket_labels,
        topix_bucket_order,
    )


@app.function
def build_matrix(
    df,
    *,
    row_key,
    col_key,
    value_key,
    row_order,
    col_order,
    fill_value=0.0,
):
    _pivot = df.pivot(index=row_key, columns=col_key, values=value_key)
    _pivot = _pivot.reindex(index=row_order, columns=col_order)
    return _pivot.fillna(fill_value)


@app.cell
def _(
    NT_RATIO_CHANGE_TOPIX_CLOSE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
    bundle_info,
    error_message,
    mo,
    result,
):
    _summary_view = mo.md("")
    if not error_message and result is not None:
        _nt_stats = result.nt_ratio_stats
        _topix_stats = result.topix_close_stats
        _nt_stats_lines = [
            "- NT ratio return stats: **no analyzable rows in range**",
        ]
        if _nt_stats is not None:
            _nt_stats_lines = [
                f"- NT ratio return sample count: **{_nt_stats.sample_count}**",
                f"- Mean / Std: **{_nt_stats.mean_return * 100:.4f}% / {_nt_stats.std_return * 100:.4f}%**",
                f"- NT bucket boundaries: **μ-{result.sigma_threshold_2:g}σ = {_nt_stats.lower_threshold_2 * 100:.4f}%**, **μ-{result.sigma_threshold_1:g}σ = {_nt_stats.lower_threshold_1 * 100:.4f}%**, **μ+{result.sigma_threshold_1:g}σ = {_nt_stats.upper_threshold_1 * 100:.4f}%**, **μ+{result.sigma_threshold_2:g}σ = {_nt_stats.upper_threshold_2 * 100:.4f}%**",
                f"- Min / Q25 / Median / Q75 / Max: **{_nt_stats.min_return * 100:.4f}% / {_nt_stats.q25_return * 100:.4f}% / {_nt_stats.median_return * 100:.4f}% / {_nt_stats.q75_return * 100:.4f}% / {_nt_stats.max_return * 100:.4f}%**",
            ]
        _topix_stats_lines = [
            "- TOPIX close return stats: **no analyzable rows in range**",
        ]
        if _topix_stats is not None:
            _topix_stats_lines = [
                f"- TOPIX close return sample count: **{_topix_stats.sample_count}**",
                f"- Mean / Std: **{_topix_stats.mean_return * 100:.4f}% / {_topix_stats.std_return * 100:.4f}%**",
                f"- TOPIX sigma thresholds: **{_topix_stats.sigma_threshold_1:g}σ / {_topix_stats.sigma_threshold_2:g}σ**",
                f"- TOPIX derived thresholds: **{_topix_stats.threshold_1 * 100:.4f}% / {_topix_stats.threshold_2 * 100:.4f}%**",
                f"- Min / Q25 / Median / Q75 / Max: **{_topix_stats.min_return * 100:.4f}% / {_topix_stats.q25_return * 100:.4f}% / {_topix_stats.median_return * 100:.4f}% / {_topix_stats.q75_return * 100:.4f}% / {_topix_stats.max_return * 100:.4f}%**",
            ]
        _bundle_lines = []
        if bundle_info is not None:
            _bundle_lines = [
                f"- Experiment: **{NT_RATIO_CHANGE_TOPIX_CLOSE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID}**",
                f"- Bundle run: **{bundle_info.run_id}**",
            ]

        _summary_view = mo.md(
            "\n".join(
                [
                    "## NT Ratio Change x TOPIX Close / Stock Overnight Distribution Playground",
                    "",
                    f"- Source mode: **{result.source_mode}**",
                    f"- Source detail: **{result.source_detail}**",
                    f"- Available range: **{result.available_start_date} -> {result.available_end_date}**",
                    f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
                    f"- Selected groups: **{', '.join(result.selected_groups)}**",
                    f"- NT sigma thresholds: **{result.sigma_threshold_1:g}σ / {result.sigma_threshold_2:g}σ**",
                    f"- Sample size per group/joint bucket: **{result.sample_size}**",
                    f"- Plot clip: **{result.clip_percentiles[0]:.1f}% -> {result.clip_percentiles[1]:.1f}%**",
                    f"- Excluded NT ratio days without previous ratio: **{result.excluded_nt_ratio_days_without_prev_ratio}**",
                    f"- Excluded TOPIX days without previous close: **{result.excluded_topix_days_without_prev_close}**",
                    f"- Excluded joint days without next session: **{result.excluded_joint_days_without_next_session}**",
                    "- Event definition: **nt_ratio = N225_UNDERPX close / TOPIX close**",
                    "- Event definition: **nt_ratio_return = (nt_ratio - prev_nt_ratio) / prev_nt_ratio**",
                    "- Event definition: **topix_close_return = (topix_close - prev_topix_close) / prev_topix_close**",
                    "- Trade definition: **stock overnight_return = (next_open - event_close) / event_close**",
                    "",
                    *_bundle_lines,
                    *([""] if _bundle_lines else []),
                    *_nt_stats_lines,
                    "",
                    *_topix_stats_lines,
                ]
            )
        )
    _summary_view
    return


@app.cell
def _(error_message, mo, pd, result):
    _stats_table = mo.md("")
    if not error_message and result is not None:
        _views = []
        if result.nt_ratio_stats is not None:
            _nt_stats = result.nt_ratio_stats
            _nt_stats_df = pd.DataFrame(
                [
                    {
                        "sample_count": _nt_stats.sample_count,
                        "mean_return": _nt_stats.mean_return,
                        "std_return": _nt_stats.std_return,
                        "lower_threshold_2": _nt_stats.lower_threshold_2,
                        "lower_threshold_1": _nt_stats.lower_threshold_1,
                        "upper_threshold_1": _nt_stats.upper_threshold_1,
                        "upper_threshold_2": _nt_stats.upper_threshold_2,
                        "min_return": _nt_stats.min_return,
                        "q25_return": _nt_stats.q25_return,
                        "median_return": _nt_stats.median_return,
                        "q75_return": _nt_stats.q75_return,
                        "max_return": _nt_stats.max_return,
                    }
                ]
            )
            _views.extend(
                [
                    mo.md("### NT Ratio Return Stats"),
                    mo.Html(
                        _nt_stats_df.to_html(
                            index=False,
                            float_format=lambda value: f"{value:.6f}",
                        )
                    ),
                ]
            )
        if result.topix_close_stats is not None:
            _topix_close_stats = result.topix_close_stats
            _topix_stats_df = pd.DataFrame(
                [
                    {
                        "sample_count": _topix_close_stats.sample_count,
                        "mean_return": _topix_close_stats.mean_return,
                        "std_return": _topix_close_stats.std_return,
                        "threshold_1": _topix_close_stats.threshold_1,
                        "threshold_2": _topix_close_stats.threshold_2,
                        "min_return": _topix_close_stats.min_return,
                        "q25_return": _topix_close_stats.q25_return,
                        "median_return": _topix_close_stats.median_return,
                        "q75_return": _topix_close_stats.q75_return,
                        "max_return": _topix_close_stats.max_return,
                    }
                ]
            )
            _views.extend(
                [
                    mo.md("### TOPIX Close Return Stats"),
                    mo.Html(
                        _topix_stats_df.to_html(
                            index=False,
                            float_format=lambda value: f"{value:.6f}",
                        )
                    ),
                ]
            )
        if _views:
            _stats_table = mo.vstack(_views)
    _stats_table
    return


@app.cell
def _(
    error_message,
    mo,
    nt_bucket_labels,
    nt_bucket_order,
    plt,
    result,
    topix_bucket_labels,
    topix_bucket_order,
):
    _joint_day_count_chart = mo.md("")
    if not error_message and result is not None:
        _matrix = build_matrix(
            result.joint_day_counts_df,
            row_key="nt_ratio_bucket_key",
            col_key="topix_close_bucket_key",
            value_key="day_count",
            row_order=nt_bucket_order,
            col_order=topix_bucket_order,
        )
        _fig, _ax = plt.subplots(figsize=(12, 5))
        _image = _ax.imshow(_matrix.values, cmap="Blues")
        _ax.set_title("Joint Event Day Counts")
        _ax.set_ylabel("NT Ratio Return Bucket")
        _ax.set_xlabel("TOPIX Close Return Bucket")
        _ax.set_yticks(range(len(nt_bucket_order)))
        _ax.set_yticklabels([nt_bucket_labels[key] for key in nt_bucket_order])
        _ax.set_xticks(range(len(topix_bucket_order)))
        _ax.set_xticklabels(
            [topix_bucket_labels[key] for key in topix_bucket_order],
            rotation=25,
            ha="right",
        )
        for _row_index, _row_key in enumerate(nt_bucket_order):
            for _col_index, _col_key in enumerate(topix_bucket_order):
                _value = int(_matrix.loc[_row_key, _col_key])
                _ax.text(
                    _col_index,
                    _row_index,
                    str(_value),
                    ha="center",
                    va="center",
                    color="#111111",
                )
        _fig.colorbar(_image, ax=_ax, shrink=0.9)
        _fig.tight_layout()
        _joint_day_count_chart = _fig
    _joint_day_count_chart
    return


@app.cell
def _(
    error_message,
    mo,
    nt_bucket_labels,
    nt_bucket_order,
    plt,
    result,
    topix_bucket_labels,
    topix_bucket_order,
):
    _expected_return_heatmap = mo.md("")
    if not error_message and result is not None:
        _summary_df = result.summary_df.copy()
        _summary_df["mean_overnight_return_pct"] = (
            _summary_df["mean_overnight_return"] * 100.0
        )
        _groups = list(result.selected_groups)
        _fig, _axes = plt.subplots(2, 2, figsize=(16, 12), sharex=True, sharey=True)
        _axes_flat = _axes.flatten()
        _max_abs = max(
            float(_summary_df["mean_overnight_return_pct"].abs().max()),
            0.01,
        )

        for _ax, _group in zip(_axes_flat, _groups, strict=False):
            _group_df = _summary_df[_summary_df["stock_group"] == _group]
            _matrix = build_matrix(
                _group_df,
                row_key="nt_ratio_bucket_key",
                col_key="topix_close_bucket_key",
                value_key="mean_overnight_return_pct",
                row_order=nt_bucket_order,
                col_order=topix_bucket_order,
            )
            _image = _ax.imshow(
                _matrix.values,
                cmap="RdYlGn",
                vmin=-_max_abs,
                vmax=_max_abs,
            )
            _ax.set_title(_group)
            _ax.set_yticks(range(len(nt_bucket_order)))
            _ax.set_yticklabels([nt_bucket_labels[key] for key in nt_bucket_order])
            _ax.set_xticks(range(len(topix_bucket_order)))
            _ax.set_xticklabels(
                [topix_bucket_labels[key] for key in topix_bucket_order],
                rotation=25,
                ha="right",
            )
            for _row_index, _row_key in enumerate(nt_bucket_order):
                for _col_index, _col_key in enumerate(topix_bucket_order):
                    _value = float(_matrix.loc[_row_key, _col_key])
                    _ax.text(
                        _col_index,
                        _row_index,
                        f"{_value:.2f}",
                        ha="center",
                        va="center",
                        color="#111111",
                        fontsize=9,
                    )

        for _ax in _axes_flat[len(_groups) :]:
            _ax.axis("off")

        _fig.suptitle("Mean Stock Overnight Return (%) by NT Bucket x TOPIX Bucket")
        _fig.colorbar(_image, ax=_axes_flat.tolist(), shrink=0.9)
        _fig.tight_layout()
        _expected_return_heatmap = _fig
    _expected_return_heatmap
    return


@app.cell
def _(
    error_message,
    mo,
    nt_bucket_labels,
    nt_bucket_order,
    plt,
    result,
    topix_bucket_labels,
    topix_bucket_order,
):
    _direction_heatmap = mo.md("")
    if not error_message and result is not None:
        _summary_df = result.summary_df.copy()
        _summary_df["up_minus_down_pp"] = (
            (_summary_df["up_ratio"] - _summary_df["down_ratio"]) * 100.0
        )
        _groups = list(result.selected_groups)
        _fig, _axes = plt.subplots(2, 2, figsize=(16, 12), sharex=True, sharey=True)
        _axes_flat = _axes.flatten()
        _max_abs = max(float(_summary_df["up_minus_down_pp"].abs().max()), 0.5)

        for _ax, _group in zip(_axes_flat, _groups, strict=False):
            _group_df = _summary_df[_summary_df["stock_group"] == _group]
            _matrix = build_matrix(
                _group_df,
                row_key="nt_ratio_bucket_key",
                col_key="topix_close_bucket_key",
                value_key="up_minus_down_pp",
                row_order=nt_bucket_order,
                col_order=topix_bucket_order,
            )
            _image = _ax.imshow(
                _matrix.values,
                cmap="PiYG",
                vmin=-_max_abs,
                vmax=_max_abs,
            )
            _ax.set_title(_group)
            _ax.set_yticks(range(len(nt_bucket_order)))
            _ax.set_yticklabels([nt_bucket_labels[key] for key in nt_bucket_order])
            _ax.set_xticks(range(len(topix_bucket_order)))
            _ax.set_xticklabels(
                [topix_bucket_labels[key] for key in topix_bucket_order],
                rotation=25,
                ha="right",
            )
            for _row_index, _row_key in enumerate(nt_bucket_order):
                for _col_index, _col_key in enumerate(topix_bucket_order):
                    _value = float(_matrix.loc[_row_key, _col_key])
                    _ax.text(
                        _col_index,
                        _row_index,
                        f"{_value:.1f}",
                        ha="center",
                        va="center",
                        color="#111111",
                        fontsize=9,
                    )

        for _ax in _axes_flat[len(_groups) :]:
            _ax.axis("off")

        _fig.suptitle("Up Ratio - Down Ratio (pp) by NT Bucket x TOPIX Bucket")
        _fig.colorbar(_image, ax=_axes_flat.tolist(), shrink=0.9)
        _fig.tight_layout()
        _direction_heatmap = _fig
    _direction_heatmap
    return


@app.cell
def _(
    error_message,
    focus_group,
    mo,
    nt_bucket_labels,
    nt_bucket_order,
    plt,
    result,
    topix_bucket_labels,
    topix_bucket_order,
):
    _distribution_chart = mo.md("")
    if not error_message and result is not None:
        _plot_df = result.clipped_samples_df[
            result.clipped_samples_df["stock_group"] == focus_group.value
        ]
        _fig, _axes = plt.subplots(
            len(nt_bucket_order),
            1,
            figsize=(14, 4 * len(nt_bucket_order)),
            sharex=False,
        )
        if len(nt_bucket_order) == 1:
            _axes = [_axes]

        for _ax, _nt_bucket_key in zip(_axes, nt_bucket_order, strict=True):
            _bucket_df = _plot_df[_plot_df["nt_ratio_bucket_key"] == _nt_bucket_key]
            _boxplot_data = [
                _bucket_df.loc[
                    _bucket_df["topix_close_bucket_key"] == _topix_bucket_key,
                    "overnight_diff",
                ].tolist()
                for _topix_bucket_key in topix_bucket_order
            ]
            if any(_boxplot_data):
                _ax.boxplot(
                    _boxplot_data,
                    labels=[topix_bucket_labels[key] for key in topix_bucket_order],
                    patch_artist=True,
                )
                _ax.tick_params(axis="x", rotation=25)
            else:
                _ax.text(
                    0.5,
                    0.5,
                    "No sampled rows in this NT bucket",
                    ha="center",
                    va="center",
                )
                _ax.set_xticks([])

            _ax.set_title(nt_bucket_labels[_nt_bucket_key])
            _ax.set_ylabel("next_open - close")
            _ax.grid(axis="y", alpha=0.2)

        _fig.suptitle(
            f"{focus_group.value}: Sampled Overnight Diff Distribution by NT Bucket x TOPIX Bucket",
            y=1.01,
        )
        _fig.tight_layout()
        _distribution_chart = _fig
    _distribution_chart
    return


@app.cell
def _(error_message, mo, result):
    _table_view = mo.md("")
    if not error_message and result is not None:
        _summary_columns = [
            "stock_group",
            "nt_ratio_bucket_label",
            "topix_close_bucket_label",
            "sample_count",
            "up_count",
            "down_count",
            "flat_count",
            "up_ratio",
            "down_ratio",
            "flat_ratio",
            "mean_nt_ratio_return",
            "mean_topix_close_return",
            "mean_overnight_return",
            "mean_overnight_diff",
            "median_overnight_diff",
            "p05_overnight_diff",
            "p25_overnight_diff",
            "p50_overnight_diff",
            "p75_overnight_diff",
            "p95_overnight_diff",
        ]
        _sample_columns = [
            "stock_group",
            "nt_ratio_bucket_label",
            "topix_close_bucket_label",
            "date",
            "next_date",
            "code",
            "nt_ratio_return",
            "topix_close_return",
            "overnight_diff",
            "overnight_return",
            "direction",
            "sample_rank",
        ]
        _daily_columns = [
            "stock_group",
            "date",
            "next_date",
            "nt_ratio_bucket_label",
            "topix_close_bucket_label",
            "nt_ratio_return",
            "topix_close_return",
            "day_mean_overnight_return",
            "day_up_ratio",
            "day_down_ratio",
            "constituent_count",
        ]
        _table_view = mo.vstack(
            [
                mo.md("### Joint Event Day Counts"),
                mo.Html(result.joint_day_counts_df.to_html(index=False)),
                mo.md(
                    "### Exact Summary\n\n"
                    "`mean_overnight_return = average((next_open-event_close)/event_close)`"
                ),
                mo.Html(
                    result.summary_df[_summary_columns].to_html(
                        index=False,
                        float_format=lambda value: f"{value:.4f}",
                    )
                ),
                mo.md("### Sampled Rows (first 100)"),
                mo.Html(
                    result.samples_df[_sample_columns].head(100).to_html(
                        index=False,
                        float_format=lambda value: f"{value:.4f}",
                    )
                ),
                mo.md("### Daily Group Returns (latest 120)"),
                mo.Html(
                    result.daily_group_returns_df[_daily_columns].tail(120).to_html(
                        index=False,
                        float_format=lambda value: f"{value:.4f}",
                    )
                ),
            ]
        )
    _table_view
    return
if __name__ == "__main__":
    app.run()
