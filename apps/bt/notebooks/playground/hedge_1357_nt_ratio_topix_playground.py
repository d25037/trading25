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
    app_title="1357 x NT Ratio / TOPIX Hedge Research Playground",
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
    from src.domains.analytics.hedge_1357_nt_ratio_topix import (
        RULE_ORDER,
        STOCK_GROUP_ORDER,
        TARGET_ORDER,
        get_1357_nt_ratio_topix_available_date_range,
        run_1357_nt_ratio_topix_hedge_research,
    )

    default_db_path = get_settings().market_db_path
    return (
        RULE_ORDER,
        STOCK_GROUP_ORDER,
        TARGET_ORDER,
        default_db_path,
        get_1357_nt_ratio_topix_available_date_range,
        run_1357_nt_ratio_topix_hedge_research,
    )


@app.cell
def _(default_db_path, get_1357_nt_ratio_topix_available_date_range):
    try:
        initial_range = get_1357_nt_ratio_topix_available_date_range(default_db_path)
    except Exception:
        initial_range = (None, None)
    return (initial_range,)


@app.cell
def _(STOCK_GROUP_ORDER, default_db_path, initial_range, mo):
    _available_start_date, _available_end_date = initial_range

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
        label="Sigma Threshold 1",
    )
    sigma_threshold_2 = mo.ui.number(
        value=2.0,
        start=0.2,
        stop=20.0,
        step=0.1,
        label="Sigma Threshold 2",
    )
    fixed_weights = mo.ui.text(
        value="0.1, 0.2, 0.3, 0.4, 0.5",
        label="Fixed Hedge Weights",
    )

    mo.vstack(
        [
            db_path,
            mo.hstack([start_date, end_date]),
            selected_groups,
            mo.hstack([sigma_threshold_1, sigma_threshold_2]),
            fixed_weights,
        ]
    )
    return (
        db_path,
        end_date,
        fixed_weights,
        selected_groups,
        sigma_threshold_1,
        sigma_threshold_2,
        start_date,
    )


@app.cell
def _(
    STOCK_GROUP_ORDER,
    db_path,
    end_date,
    fixed_weights,
    selected_groups,
    sigma_threshold_1,
    sigma_threshold_2,
    start_date,
):
    _requested_groups = [
        value.strip()
        for value in selected_groups.value.split(",")
        if value.strip()
    ]
    if not _requested_groups:
        _requested_groups = list(STOCK_GROUP_ORDER)

    _parsed_weights = [
        float(value.strip())
        for value in fixed_weights.value.split(",")
        if value.strip()
    ]
    parsed_inputs = {
        "requested_groups": _requested_groups,
        "selected_db_path": db_path.value.strip(),
        "selected_end": end_date.value.strip() or None,
        "selected_start": start_date.value.strip() or None,
        "sigma_threshold_1": float(sigma_threshold_1.value),
        "sigma_threshold_2": float(sigma_threshold_2.value),
        "fixed_weights": _parsed_weights,
    }
    return (parsed_inputs,)


@app.cell
def _(parsed_inputs, run_1357_nt_ratio_topix_hedge_research):
    try:
        result = run_1357_nt_ratio_topix_hedge_research(
            parsed_inputs["selected_db_path"],
            start_date=parsed_inputs["selected_start"],
            end_date=parsed_inputs["selected_end"],
            sigma_threshold_1=parsed_inputs["sigma_threshold_1"],
            sigma_threshold_2=parsed_inputs["sigma_threshold_2"],
            selected_groups=parsed_inputs["requested_groups"],
            fixed_weights=parsed_inputs["fixed_weights"],
        )
        error_message = None
    except Exception as exc:
        result = None
        error_message = str(exc)
    return error_message, result


@app.cell
def _(error_message, mo):
    _error_view = mo.md("")
    if error_message:
        _error_view = mo.md(f"## Input Error\n\n`{error_message}`")
    _error_view
    return


@app.cell
def _(RULE_ORDER, TARGET_ORDER, error_message, mo, result):
    _controls = mo.md("")
    if error_message or result is None:
        split_view = mo.md("")
        target_view = mo.md("")
        rule_view = mo.md("")
        weight_view = mo.md("")
        metric_view = mo.md("")
    else:
        split_view = mo.ui.dropdown(
            options={"overall": "overall", "discovery": "discovery", "validation": "validation"},
            value="overall",
            label="Split",
        )
        target_view = mo.ui.dropdown(
            options={target: target for target in TARGET_ORDER},
            value="next_close_to_close",
            label="Target",
        )
        rule_view = mo.ui.dropdown(
            options={rule: rule for rule in RULE_ORDER},
            value="shock_joint_adverse",
            label="Rule",
        )
        weight_options = {
            label: label
            for label in result.hedge_metrics_df["weight_label"].drop_duplicates().tolist()
        }
        weight_view = mo.ui.dropdown(
            options=weight_options,
            value="fixed_0.30" if "fixed_0.30" in weight_options else next(iter(weight_options)),
            label="Weight",
        )
        metric_view = mo.ui.dropdown(
            options={
                "stress_mean_loss_improvement": "stress_mean_loss_improvement",
                "expected_shortfall_improvement": "expected_shortfall_improvement",
                "max_drawdown_improvement": "max_drawdown_improvement",
            },
            value="expected_shortfall_improvement",
            label="Downside Metric",
        )
        _controls = mo.hstack([split_view, target_view, rule_view, weight_view, metric_view])
    _controls
    return metric_view, rule_view, split_view, target_view, weight_view


@app.function
def build_matrix(
    df,
    *,
    row_key,
    col_key,
    value_key,
    row_order,
    col_order,
):
    _pivot = df.pivot(index=row_key, columns=col_key, values=value_key)
    return _pivot.reindex(index=row_order, columns=col_order)


@app.cell
def _(error_message, mo, parsed_inputs, result):
    _summary_view = mo.md("")
    if not error_message and result is not None:
        _topix_stats = result.topix_close_stats
        _nt_stats = result.nt_ratio_stats
        _summary_lines = [
            "## 1357 x NT Ratio / TOPIX Hedge Research",
            "",
            f"- Source mode: **{result.source_mode}**",
            f"- Source detail: **{result.source_detail}**",
            f"- Available range: **{result.available_start_date} -> {result.available_end_date}**",
            f"- Analysis range: **{result.analysis_start_date} -> {result.analysis_end_date}**",
            f"- Discovery / Validation split: **<= {result.discovery_end_date} / >= {result.validation_start_date}**",
            f"- Selected groups: **{', '.join(parsed_inputs['requested_groups'])}**",
            f"- Fixed weights: **{', '.join(f'{value:.2f}' for value in parsed_inputs['fixed_weights'])}**",
            (
                f"- MACD trend rule: **{result.macd_basis} "
                f"({result.macd_fast_period}/{result.macd_slow_period}/{result.macd_signal_period}) "
                "on TOPIX histogram < 0**"
            ),
            "- Targets: **next_overnight / next_intraday / next_close_to_close / forward_3d_close_to_close / forward_5d_close_to_close**",
            "- Notes: **3d/5d drawdown metrics are event-indexed diagnostics with overlapping windows, not tradable path-equity curves.**",
            "- Notes: **Turnover and leveraged ETF decay are diagnostic warnings, not modeled costs.**",
        ]
        if _topix_stats is not None:
            _summary_lines.extend(
                [
                    f"- TOPIX mean / std: **{_topix_stats.mean_return * 100:.4f}% / {_topix_stats.std_return * 100:.4f}%**",
                    f"- TOPIX thresholds: **{_topix_stats.threshold_1 * 100:.4f}% / {_topix_stats.threshold_2 * 100:.4f}%**",
                ]
            )
        if _nt_stats is not None:
            _summary_lines.extend(
                [
                    f"- NT ratio mean / std: **{_nt_stats.mean_return * 100:.4f}% / {_nt_stats.std_return * 100:.4f}%**",
                    f"- NT thresholds: **{_nt_stats.lower_threshold_1 * 100:.4f}% / {_nt_stats.upper_threshold_1 * 100:.4f}%**",
                ]
            )
        _summary_view = mo.md("\n".join(_summary_lines))
    _summary_view
    return


@app.cell
def _(error_message, mo, pd, result):
    _stats_table = mo.md("")
    if not error_message and result is not None:
        _views = []
        if result.topix_close_stats is not None:
            _views.extend(
                [
                    mo.md("### TOPIX Close Return Stats"),
                    mo.Html(
                        pd.DataFrame(
                            [
                                {
                                    "sample_count": result.topix_close_stats.sample_count,
                                    "mean_return": result.topix_close_stats.mean_return,
                                    "std_return": result.topix_close_stats.std_return,
                                    "threshold_1": result.topix_close_stats.threshold_1,
                                    "threshold_2": result.topix_close_stats.threshold_2,
                                    "min_return": result.topix_close_stats.min_return,
                                    "q25_return": result.topix_close_stats.q25_return,
                                    "median_return": result.topix_close_stats.median_return,
                                    "q75_return": result.topix_close_stats.q75_return,
                                    "max_return": result.topix_close_stats.max_return,
                                }
                            ]
                        ).to_html(index=False, float_format=lambda value: f"{value:.6f}")
                    ),
                ]
            )
        if result.nt_ratio_stats is not None:
            _views.extend(
                [
                    mo.md("### NT Ratio Return Stats"),
                    mo.Html(
                        pd.DataFrame(
                            [
                                {
                                    "sample_count": result.nt_ratio_stats.sample_count,
                                    "mean_return": result.nt_ratio_stats.mean_return,
                                    "std_return": result.nt_ratio_stats.std_return,
                                    "lower_threshold_2": result.nt_ratio_stats.lower_threshold_2,
                                    "lower_threshold_1": result.nt_ratio_stats.lower_threshold_1,
                                    "upper_threshold_1": result.nt_ratio_stats.upper_threshold_1,
                                    "upper_threshold_2": result.nt_ratio_stats.upper_threshold_2,
                                    "min_return": result.nt_ratio_stats.min_return,
                                    "q25_return": result.nt_ratio_stats.q25_return,
                                    "median_return": result.nt_ratio_stats.median_return,
                                    "q75_return": result.nt_ratio_stats.q75_return,
                                    "max_return": result.nt_ratio_stats.max_return,
                                }
                            ]
                        ).to_html(index=False, float_format=lambda value: f"{value:.6f}")
                    ),
                ]
            )
        if _views:
            _stats_table = mo.vstack(_views)
    _stats_table
    return


@app.cell
def _(error_message, mo, pd, plt, result, split_view, target_view):
    _heatmap_view = mo.md("")
    if not error_message and result is not None:
        _plot_df = result.joint_forward_summary_df[
            (result.joint_forward_summary_df["split"] == split_view.value)
            & (result.joint_forward_summary_df["target_name"] == target_view.value)
        ].copy()
        _nt_order = list(result.joint_forward_summary_df["nt_ratio_bucket_key"].drop_duplicates())
        _topix_order = list(result.joint_forward_summary_df["topix_close_bucket_key"].drop_duplicates())
        _nt_labels = {
            row["nt_ratio_bucket_key"]: row["nt_ratio_bucket_label"]
            for _, row in _plot_df.drop_duplicates(subset=["nt_ratio_bucket_key"]).iterrows()
        }
        _topix_labels = {
            row["topix_close_bucket_key"]: row["topix_close_bucket_label"]
            for _, row in _plot_df.drop_duplicates(subset=["topix_close_bucket_key"]).iterrows()
        }
        _matrix = build_matrix(
            _plot_df,
            row_key="nt_ratio_bucket_key",
            col_key="topix_close_bucket_key",
            value_key="mean_etf_return",
            row_order=_nt_order,
            col_order=_topix_order,
        )
        _fig, _ax = plt.subplots(figsize=(12, 5))
        _image = _ax.imshow(_matrix.values * 100.0, cmap="RdYlGn")
        _ax.set_title(f"1357 Mean Return Heatmap ({split_view.value} / {target_view.value})")
        _ax.set_ylabel("NT Ratio Return Bucket")
        _ax.set_xlabel("TOPIX Close Return Bucket")
        _ax.set_yticks(range(len(_nt_order)))
        _ax.set_yticklabels([_nt_labels.get(key, key) for key in _nt_order])
        _ax.set_xticks(range(len(_topix_order)))
        _ax.set_xticklabels(
            [_topix_labels.get(key, key) for key in _topix_order],
            rotation=25,
            ha="right",
        )
        for _row_index, _row_key in enumerate(_nt_order):
            for _col_index, _col_key in enumerate(_topix_order):
                _value = _matrix.loc[_row_key, _col_key]
                _text = "N/A" if pd.isna(_value) else f"{_value * 100:.2f}%"
                _ax.text(_col_index, _row_index, _text, ha="center", va="center", color="#111111")
        _fig.colorbar(_image, ax=_ax, shrink=0.9, label="Mean 1357 return (%)")
        _fig.tight_layout()
        _heatmap_view = _fig
    _heatmap_view
    return


@app.cell
def _(error_message, mo, result, rule_view, split_view, target_view):
    _comparison_view = mo.md("")
    if not error_message and result is not None:
        _table = result.hedge_metrics_df[
            (result.hedge_metrics_df["split"] == split_view.value)
            & (result.hedge_metrics_df["target_name"] == target_view.value)
            & (result.hedge_metrics_df["rule_name"] == rule_view.value)
        ][
            [
                "stock_group",
                "weight_label",
                "sample_count",
                "active_day_count",
                "mean_weight_when_active",
                "stress_mean_loss_improvement",
                "expected_shortfall_improvement",
                "max_drawdown_improvement",
                "down_day_hit_rate",
                "carry_cost_non_stress",
            ]
        ].sort_values(["stock_group", "weight_label"])
        _comparison_view = mo.vstack(
            [
                mo.md("### Proxy Basket Hedge Comparison"),
                mo.Html(
                    _table.to_html(
                        index=False,
                        float_format=lambda value: f"{value:.6f}",
                    )
                ),
            ]
        )
    _comparison_view
    return


@app.cell
def _(error_message, mo, result, rule_view, target_view):
    _etf_strategy_view = mo.md("")
    if not error_message and result is not None:
        _table = result.etf_strategy_metrics_df[
            (result.etf_strategy_metrics_df["target_name"] == target_view.value)
            & (result.etf_strategy_metrics_df["rule_name"] == rule_view.value)
        ][
            [
                "split",
                "sample_count",
                "active_day_count",
                "active_ratio",
                "mean_return_when_active",
                "strategy_mean_return",
                "strategy_total_return",
                "expected_shortfall_5",
                "max_drawdown",
                "positive_rate_when_active",
            ]
        ].sort_values("split")
        _etf_strategy_view = mo.vstack(
            [
                mo.md("### 1357 Standalone Rule Return"),
                mo.Html(
                    _table.to_html(
                        index=False,
                        float_format=lambda value: f"{value:.6f}",
                    )
                ),
            ]
        )
    _etf_strategy_view
    return


@app.cell
def _(
    error_message,
    metric_view,
    mo,
    pd,
    plt,
    result,
    rule_view,
    split_view,
    target_view,
):
    _downside_view = mo.md("")
    if not error_message and result is not None:
        _plot_df = result.hedge_metrics_df[
            (result.hedge_metrics_df["split"] == split_view.value)
            & (result.hedge_metrics_df["target_name"] == target_view.value)
            & (result.hedge_metrics_df["rule_name"] == rule_view.value)
        ].copy()
        _weight_order = _plot_df["weight_label"].drop_duplicates().tolist()
        _matrix = build_matrix(
            _plot_df,
            row_key="stock_group",
            col_key="weight_label",
            value_key=metric_view.value,
            row_order=list(result.selected_groups),
            col_order=_weight_order,
        )
        _fig, _ax = plt.subplots(figsize=(10, 4))
        _image = _ax.imshow(_matrix.values, cmap="RdYlGn")
        _ax.set_title(
            f"{metric_view.value} ({split_view.value} / {target_view.value} / {rule_view.value})"
        )
        _ax.set_ylabel("Stock Group")
        _ax.set_xlabel("Weight")
        _ax.set_yticks(range(len(result.selected_groups)))
        _ax.set_yticklabels(list(result.selected_groups))
        _ax.set_xticks(range(len(_weight_order)))
        _ax.set_xticklabels(_weight_order, rotation=25, ha="right")
        for _row_index, _stock_group in enumerate(result.selected_groups):
            for _col_index, _weight_label in enumerate(_weight_order):
                _value = _matrix.loc[_stock_group, _weight_label]
                _text = "N/A" if pd.isna(_value) else f"{_value:.4f}"
                _ax.text(_col_index, _row_index, _text, ha="center", va="center", color="#111111")
        _fig.colorbar(_image, ax=_ax, shrink=0.9)
        _fig.tight_layout()
        _downside_view = _fig
    _downside_view
    return


@app.cell
def _(error_message, mo, result, rule_view, target_view):
    _split_comparison_view = mo.md("")
    if not error_message and result is not None:
        _table = result.split_comparison_df[
            (result.split_comparison_df["target_name"] == target_view.value)
            & (result.split_comparison_df["rule_name"] == rule_view.value)
        ].sort_values(["stock_group", "weight_label"])
        _split_comparison_view = mo.vstack(
            [
                mo.md("### Discovery / Validation Comparison"),
                mo.Html(
                    _table.to_html(
                        index=False,
                        float_format=lambda value: f"{value:.6f}",
                    )
                ),
            ]
        )
    _split_comparison_view
    return


@app.cell
def _(error_message, mo, result, rule_view, split_view):
    _signal_summary_view = mo.md("")
    if not error_message and result is not None:
        _rule_summary = result.rule_signal_summary_df[
            (result.rule_signal_summary_df["split"] == split_view.value)
            & (result.rule_signal_summary_df["rule_name"] == rule_view.value)
        ]
        _signal_summary_view = mo.vstack(
            [
                mo.md("### Rule Turnover Diagnostics"),
                mo.Html(_rule_summary.to_html(index=False, float_format=lambda value: f"{value:.6f}")),
                mo.md(
                    "\n".join(
                        [
                            "### Practical Cautions",
                            "",
                            "- 1357 is a leveraged inverse ETF, so multi-day decay and path dependency matter.",
                            "- `transitions` and `average_run_length` are turnover diagnostics only; transaction costs are not modeled here.",
                            "- Use `fixed_0.30` or `beta_neutral_60d` as starting references, not final production weights.",
                        ]
                    )
                ),
            ]
        )
    _signal_summary_view
    return


@app.cell
def _(error_message, mo, result):
    _shortlist_view = mo.md("")
    if not error_message and result is not None:
        _shortlist_view = mo.vstack(
            [
                mo.md("### Shortlist"),
                mo.Html(
                    result.shortlist_df.to_html(
                        index=False,
                        float_format=lambda value: f"{value:.6f}",
                    )
                ),
            ]
        )
    _shortlist_view
    return


@app.cell
def _(error_message, mo, result, rule_view, target_view, weight_view):
    _annual_view = mo.md("")
    if not error_message and result is not None:
        _table = result.annual_rule_summary_df[
            (result.annual_rule_summary_df["target_name"] == target_view.value)
            & (result.annual_rule_summary_df["rule_name"] == rule_view.value)
            & (result.annual_rule_summary_df["weight_label"] == weight_view.value)
        ][
            [
                "calendar_year",
                "stock_group",
                "sample_count",
                "active_day_count",
                "stress_mean_loss_improvement",
                "expected_shortfall_improvement",
                "max_drawdown_improvement",
                "carry_cost_non_stress",
            ]
        ].sort_values(["calendar_year", "stock_group"])
        _annual_view = mo.vstack(
            [
                mo.md("### Annual Stability Summary"),
                mo.Html(
                    _table.to_html(
                        index=False,
                        float_format=lambda value: f"{value:.6f}",
                    )
                ),
            ]
        )
    _annual_view
    return

if __name__ == "__main__":
    app.run()
