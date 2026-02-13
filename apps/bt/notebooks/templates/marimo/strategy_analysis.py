# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "pandas>=2.0.0",
#     "numpy>=1.20.0",
#     "matplotlib>=3.0.0",
#     "vectorbt>=0.26.0",
#     "pydantic>=2.0.0",
# ]
# ///

"""
Strategy Analysis Template (Marimo版)

バックテスト戦略分析・可視化テンプレート
CLI引数経由でパラメータを受け取り、静的HTMLとして出力
"""

import marimo

app = marimo.App(width="full", app_title="Strategy Analysis")


@app.cell
def imports():
    import marimo as mo
    import json
    import sys
    from pathlib import Path
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    return mo, json, sys, Path, pd, np, plt


@app.cell
def load_parameters(mo, json, sys, Path):
    # プロジェクトルートをパスに追加
    project_root = Path.cwd()
    # notebooks/templates/marimo から実行された場合の対応
    if project_root.name == "marimo":
        project_root = project_root.parent.parent.parent
    elif project_root.name == "templates":
        project_root = project_root.parent.parent
    elif project_root.name == "notebooks":
        project_root = project_root.parent

    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    # CLI引数からパラメータを取得
    cli_args = mo.cli_args()
    params_json_path = cli_args.get("params-json", "")

    if params_json_path and Path(params_json_path).exists():
        with open(params_json_path, "r", encoding="utf-8") as f:
            _params = json.load(f)
    else:
        # デフォルト値（開発時用）
        _params = {
            "shared_config": {},
            "entry_filter_params": {},
            "exit_trigger_params": {},
        }

    shared_config = _params.get("shared_config", {})
    entry_filter_params = _params.get("entry_filter_params", {})
    exit_trigger_params = _params.get("exit_trigger_params", {})
    execution_meta = _params.get("_execution", {})
    output_html_path = execution_meta.get("html_path", "")

    return (
        shared_config,
        entry_filter_params,
        exit_trigger_params,
        project_root,
        output_html_path,
    )


@app.cell
def show_header(mo, shared_config):
    _header = mo.md(f"""
# Strategy Analysis

**Dataset**: {shared_config.get('dataset', 'N/A')}
**Initial Cash**: {shared_config.get('initial_cash', 0):,}
""")
    _header


@app.cell
def show_signal_params(mo, entry_filter_params, exit_trigger_params):
    """シグナルパラメータをHTML出力"""
    _output = []

    def _format_params_table(params: dict, title: str) -> str:
        """パラメータを読みやすいテーブル形式に変換"""
        if not params:
            return f"### {title}\n\nNo parameters configured."

        lines = [f"### {title}\n"]
        lines.append("| Signal | Parameters |")
        lines.append("|--------|------------|")

        for signal_name, signal_params in params.items():
            if isinstance(signal_params, dict):
                # enabled=Falseのシグナルはスキップ
                if not signal_params.get("enabled", False):
                    continue

                # パラメータをフォーマット
                param_strs = []
                for key, value in signal_params.items():
                    if key == "enabled":
                        continue
                    if isinstance(value, dict):
                        # ネストされたパラメータ（fundamentalのサブシグナル等）
                        if value.get("enabled", False):
                            nested_params = ", ".join(
                                f"{k}={v}" for k, v in value.items() if k != "enabled"
                            )
                            param_strs.append(f"**{key}**: {nested_params}")
                    else:
                        param_strs.append(f"{key}={value}")

                if param_strs:
                    lines.append(f"| {signal_name} | {', '.join(param_strs)} |")

        if len(lines) == 3:  # ヘッダーのみ（有効なシグナルなし）
            return f"### {title}\n\nNo active signals."

        return "\n".join(lines)

    _entry_table = _format_params_table(entry_filter_params, "Entry Filter Parameters")
    _exit_table = _format_params_table(exit_trigger_params, "Exit Trigger Parameters")

    _output.append(mo.md(f"""
## Signal Parameters

{_entry_table}

{_exit_table}
"""))

    mo.vstack(_output)


@app.cell
def validate_parameters(mo, shared_config, entry_filter_params, exit_trigger_params):
    from src.models.config import SharedConfig
    from src.models.signals import SignalParams
    from pydantic import ValidationError

    _validation_errors = []

    try:
        if shared_config:
            SharedConfig(**shared_config)
    except ValidationError as e:
        _validation_errors.append(f"SharedConfig: {e}")

    try:
        if entry_filter_params:
            SignalParams(**entry_filter_params)
    except ValidationError as e:
        _validation_errors.append(f"EntryFilterParams: {e}")

    try:
        if exit_trigger_params:
            SignalParams(**exit_trigger_params)
    except ValidationError as e:
        _validation_errors.append(f"ExitTriggerParams: {e}")

    if _validation_errors:
        mo.md("**Validation Errors:**\n" + "\n".join(_validation_errors))
    else:
        mo.md("Parameters validated successfully")


@app.cell
def execute_strategy(mo, shared_config, entry_filter_params, exit_trigger_params):
    from src.strategies.core.factory import StrategyFactory

    if not shared_config:
        mo.md("**Error**: No shared_config provided. Skipping strategy execution.")
        initial_portfolio = None
        kelly_portfolio = None
        allocation_info = None
        all_entries = None
    else:
        _result = StrategyFactory.execute_strategy_with_config(
            shared_config, entry_filter_params, exit_trigger_params
        )
        initial_portfolio = _result["initial_portfolio"]
        kelly_portfolio = _result["kelly_portfolio"]
        _max_concurrent = _result["max_concurrent"]
        allocation_info = _result.get("allocation_info", _max_concurrent)
        all_entries = _result.get("all_entries", None)

        mo.md("Strategy execution completed")

    return initial_portfolio, kelly_portfolio, allocation_info, all_entries


@app.cell
def show_allocation_info(mo, allocation_info):
    _output = []
    if allocation_info is None:
        _output.append(mo.md("### Kelly Allocation Info\n\nNo allocation data available"))
    elif hasattr(allocation_info, '_repr_html_'):
        # AllocationInfoオブジェクトの場合はHTML表示
        _output.append(mo.Html(allocation_info._repr_html_()))
    else:
        # フォールバック（max_concurrentなどの数値の場合）
        _output.append(mo.md(f"### Kelly Allocation Info\n\nMax Concurrent: {allocation_info}"))
    mo.vstack(_output)


@app.cell
def entry_signal_statistics(mo, pd, all_entries):
    _stats_output = []

    if all_entries is None:
        _stats_output.append(mo.md("Entry signal data not available"))
    elif hasattr(all_entries, 'empty') and all_entries.empty:
        _stats_output.append(mo.md("Entry signal data is empty"))
    else:
        _entries_per_day = all_entries.sum(axis=1)
        _entries_per_day_nonzero = _entries_per_day[_entries_per_day > 0]

        if len(_entries_per_day_nonzero) == 0:
            _stats_output.append(mo.md("No entry signals found"))
        else:
            _stats_dict = {
                "Statistic": ["Min", "Max", "Mean", "Median", "Mode", "Variance", "Std Dev"],
                "All Period (incl. 0)": [
                    int(_entries_per_day.min()),
                    int(_entries_per_day.max()),
                    f"{_entries_per_day.mean():.2f}",
                    f"{_entries_per_day.median():.2f}",
                    int(_entries_per_day.mode().iloc[0]) if len(_entries_per_day.mode()) > 0 else 0,
                    f"{_entries_per_day.var():.2f}",
                    f"{_entries_per_day.std():.2f}",
                ],
                "Signal Days Only": [
                    int(_entries_per_day_nonzero.min()),
                    int(_entries_per_day_nonzero.max()),
                    f"{_entries_per_day_nonzero.mean():.2f}",
                    f"{_entries_per_day_nonzero.median():.2f}",
                    int(_entries_per_day_nonzero.mode().iloc[0]) if len(_entries_per_day_nonzero.mode()) > 0 else 0,
                    f"{_entries_per_day_nonzero.var():.2f}",
                    f"{_entries_per_day_nonzero.std():.2f}",
                ],
            }

            _stats_df = pd.DataFrame(_stats_dict)

            _stats_output.append(mo.md(f"""
## Entry Signal Statistics

- **Total Days**: {len(_entries_per_day)}
- **Signal Days**: {len(_entries_per_day_nonzero)} ({len(_entries_per_day_nonzero)/len(_entries_per_day)*100:.1f}%)
- **Total Signals**: {int(_entries_per_day.sum())}
"""))
            _stats_output.append(mo.Html(_stats_df.to_html(index=False)))

    mo.vstack(_stats_output)


@app.cell
def entry_signal_timeseries(plt, all_entries):
    _fig1 = None
    if all_entries is not None and not (hasattr(all_entries, 'empty') and all_entries.empty):
        _entries_per_day = all_entries.sum(axis=1)

        if _entries_per_day.sum() > 0:
            _fig1, _ax1 = plt.subplots(figsize=(14, 6))
            _entries_per_day.plot(ax=_ax1, color="green", alpha=0.7, linewidth=1.5)
            _ax1.axhline(_entries_per_day.mean(), color="red", linestyle="--", linewidth=1,
                       label=f"Mean: {_entries_per_day.mean():.2f}", alpha=0.7)
            _ax1.axhline(_entries_per_day.median(), color="blue", linestyle="--", linewidth=1,
                       label=f"Median: {_entries_per_day.median():.2f}", alpha=0.7)
            _ax1.set_title("Daily Entry Signal Count", fontsize=14, fontweight="bold")
            _ax1.set_xlabel("Date")
            _ax1.set_ylabel("Number of Entry Signals")
            _ax1.legend(loc="best")
            _ax1.grid(True, alpha=0.3)
            plt.tight_layout()
    _fig1


@app.cell
def entry_signal_histogram(plt, all_entries):
    _fig2 = None
    if all_entries is not None and not (hasattr(all_entries, 'empty') and all_entries.empty):
        _entries_per_day = all_entries.sum(axis=1)
        _entries_per_day_nonzero = _entries_per_day[_entries_per_day > 0]

        if len(_entries_per_day_nonzero) > 0:
            _fig2, _axes2 = plt.subplots(1, 2, figsize=(14, 5))

            # All period
            _entries_per_day.plot.hist(bins=30, ax=_axes2[0], color="green", alpha=0.7, edgecolor="black")
            _axes2[0].axvline(_entries_per_day.mean(), color="red", linestyle="--", linewidth=2,
                            label=f"Mean: {_entries_per_day.mean():.2f}")
            _axes2[0].axvline(_entries_per_day.median(), color="blue", linestyle="--", linewidth=2,
                            label=f"Median: {_entries_per_day.median():.2f}")
            _axes2[0].set_title("Entry Signal Count Distribution (All Period)", fontweight="bold")
            _axes2[0].set_xlabel("Number of Entry Signals")
            _axes2[0].set_ylabel("Frequency")
            _axes2[0].legend()
            _axes2[0].grid(True, alpha=0.3, axis="y")

            # Signal days only
            _entries_per_day_nonzero.plot.hist(bins=30, ax=_axes2[1], color="darkgreen", alpha=0.7, edgecolor="black")
            _axes2[1].axvline(_entries_per_day_nonzero.mean(), color="red", linestyle="--", linewidth=2,
                            label=f"Mean: {_entries_per_day_nonzero.mean():.2f}")
            _axes2[1].axvline(_entries_per_day_nonzero.median(), color="blue", linestyle="--", linewidth=2,
                            label=f"Median: {_entries_per_day_nonzero.median():.2f}")
            _axes2[1].set_title("Entry Signal Count Distribution (Signal Days Only)", fontweight="bold")
            _axes2[1].set_xlabel("Number of Entry Signals")
            _axes2[1].set_ylabel("Frequency")
            _axes2[1].legend()
            _axes2[1].grid(True, alpha=0.3, axis="y")

            plt.tight_layout()
    _fig2


@app.cell
def trade_analysis_initial(mo, pd, initial_portfolio):
    _output = [mo.md("## Trade Analysis: Initial Portfolio (Equal Weight)")]

    if initial_portfolio is not None and hasattr(initial_portfolio, "trades"):
        _trades_df = initial_portfolio.trades.records_readable

        if len(_trades_df) > 0:
            _analysis_df = _trades_df[["Column", "Entry Timestamp", "Exit Timestamp", "Return", "PnL"]].copy()
            _analysis_df.columns = ["Symbol", "Entry", "Exit", "Return (%)", "PnL"]

            _output.append(mo.md("### Top 20 Profit Trades"))
            _top_profit = _analysis_df.nlargest(20, "Return (%)")
            _output.append(mo.Html(_top_profit.to_html(index=False)))

            _output.append(mo.md("### Top 20 Loss Trades"))
            _top_loss = _analysis_df.nsmallest(20, "Return (%)")
            _output.append(mo.Html(_top_loss.to_html(index=False)))
        else:
            _output.append(mo.md("No trades recorded"))
    else:
        _output.append(mo.md("No trade data available"))

    mo.vstack(_output)


@app.cell
def trade_analysis_kelly(mo, pd, kelly_portfolio):
    _output = [mo.md("## Trade Analysis: Kelly Portfolio")]

    if kelly_portfolio is not None and hasattr(kelly_portfolio, "trades"):
        _trades_df = kelly_portfolio.trades.records_readable

        if len(_trades_df) > 0:
            _analysis_df = _trades_df[["Column", "Entry Timestamp", "Exit Timestamp", "Return", "PnL"]].copy()
            _analysis_df.columns = ["Symbol", "Entry", "Exit", "Return (%)", "PnL"]

            _output.append(mo.md("### Top 20 Profit Trades (Kelly)"))
            _top_profit = _analysis_df.nlargest(20, "Return (%)")
            _output.append(mo.Html(_top_profit.to_html(index=False)))

            _output.append(mo.md("### Top 20 Loss Trades (Kelly)"))
            _top_loss = _analysis_df.nsmallest(20, "Return (%)")
            _output.append(mo.Html(_top_loss.to_html(index=False)))
        else:
            _output.append(mo.md("No trades recorded"))
    else:
        _output.append(mo.md("No trade data available"))

    mo.vstack(_output)


@app.cell
def initial_portfolio_charts(mo, plt, initial_portfolio):
    _output = [mo.md("## Initial Portfolio (Equal Weight) Analysis")]

    if initial_portfolio is not None:
        _fig3, _axes3 = plt.subplots(2, 1, figsize=(12, 10))

        # Portfolio Value
        initial_portfolio.value().plot(ax=_axes3[0], title="Portfolio Value")
        _axes3[0].set_xlabel("Date")
        _axes3[0].set_ylabel("Value")
        _axes3[0].grid(True, alpha=0.3)

        # Drawdown
        initial_portfolio.drawdown().plot(ax=_axes3[1], title="Drawdown", color="red")
        _axes3[1].set_xlabel("Date")
        _axes3[1].set_ylabel("Drawdown")
        _axes3[1].grid(True, alpha=0.3)

        plt.tight_layout()
        _output.append(_fig3)

    mo.vstack(_output)


@app.cell
def initial_portfolio_stats(mo, pd, initial_portfolio):
    _output = []

    if initial_portfolio is not None:
        def _format_metric(metric, percentage=False):
            if isinstance(metric, pd.Series):
                value = metric.iloc[0] if len(metric) == 1 else metric.mean()
            else:
                value = metric
            return f"{value:.2%}" if percentage else f"{value:.2f}"

        _output.append(mo.md(f"""
### Risk Metrics (Initial Portfolio)

| Metric | Value |
|--------|-------|
| Annualized Volatility | {_format_metric(initial_portfolio.annualized_volatility(), percentage=True)} |
| Sharpe Ratio | {_format_metric(initial_portfolio.sharpe_ratio())} |
| Sortino Ratio | {_format_metric(initial_portfolio.sortino_ratio())} |
| Calmar Ratio | {_format_metric(initial_portfolio.calmar_ratio())} |
| Omega Ratio | {_format_metric(initial_portfolio.omega_ratio())} |
"""))

        # Trade stats
        _trade_stats = initial_portfolio.trades.stats()
        _output.append(mo.md("### Trade Statistics (Initial)"))
        _output.append(mo.Html(pd.DataFrame(_trade_stats).to_html()))

    mo.vstack(_output)


@app.cell
def initial_returns_distribution(plt, initial_portfolio):
    _fig4 = None
    if initial_portfolio is not None:
        _returns = initial_portfolio.returns()

        _fig4, _ax4 = plt.subplots(figsize=(10, 6))
        _returns.plot.hist(bins=50, ax=_ax4, alpha=0.7, edgecolor="black")
        _ax4.set_title("Returns Distribution (Initial Portfolio)")
        _ax4.set_xlabel("Returns")
        _ax4.set_ylabel("Frequency")
        _ax4.grid(True, alpha=0.3, axis="y")

        plt.tight_layout()
    _fig4


@app.cell
def kelly_portfolio_charts(mo, plt, kelly_portfolio):
    _output = [mo.md("## Kelly Portfolio Analysis")]

    if kelly_portfolio is not None:
        _fig5, _axes5 = plt.subplots(2, 1, figsize=(12, 10))

        # Portfolio Value
        kelly_portfolio.value().plot(ax=_axes5[0], title="Portfolio Value (Kelly Allocation)")
        _axes5[0].set_xlabel("Date")
        _axes5[0].set_ylabel("Value")
        _axes5[0].grid(True, alpha=0.3)

        # Drawdown
        kelly_portfolio.drawdown().plot(ax=_axes5[1], title="Drawdown (Kelly Allocation)", color="red")
        _axes5[1].set_xlabel("Date")
        _axes5[1].set_ylabel("Drawdown")
        _axes5[1].grid(True, alpha=0.3)

        plt.tight_layout()
        _output.append(_fig5)

    mo.vstack(_output)


@app.cell
def kelly_portfolio_stats(mo, pd, kelly_portfolio):
    _output = []

    if kelly_portfolio is not None:
        def _format_metric_kelly(metric, percentage=False):
            if isinstance(metric, pd.Series):
                value = metric.iloc[0] if len(metric) == 1 else metric.mean()
            else:
                value = metric
            return f"{value:.2%}" if percentage else f"{value:.2f}"

        _output.append(mo.md(f"""
### Risk Metrics (Kelly Portfolio)

| Metric | Value |
|--------|-------|
| Annualized Volatility | {_format_metric_kelly(kelly_portfolio.annualized_volatility(), percentage=True)} |
| Sharpe Ratio | {_format_metric_kelly(kelly_portfolio.sharpe_ratio())} |
| Sortino Ratio | {_format_metric_kelly(kelly_portfolio.sortino_ratio())} |
| Calmar Ratio | {_format_metric_kelly(kelly_portfolio.calmar_ratio())} |
| Omega Ratio | {_format_metric_kelly(kelly_portfolio.omega_ratio())} |
"""))

        # Trade stats
        _trade_stats_kelly = kelly_portfolio.trades.stats()
        _output.append(mo.md("### Trade Statistics (Kelly)"))
        _output.append(mo.Html(pd.DataFrame(_trade_stats_kelly).to_html()))

    mo.vstack(_output)


@app.cell
def kelly_returns_distribution(plt, kelly_portfolio):
    _fig6 = None
    if kelly_portfolio is not None:
        _returns_kelly = kelly_portfolio.returns()

        _fig6, _ax6 = plt.subplots(figsize=(10, 6))
        _returns_kelly.plot.hist(bins=50, ax=_ax6, alpha=0.7, edgecolor="black", color="orange")
        _ax6.set_title("Returns Distribution (Kelly Portfolio)")
        _ax6.set_xlabel("Returns")
        _ax6.set_ylabel("Frequency")
        _ax6.grid(True, alpha=0.3, axis="y")

        plt.tight_layout()
    _fig6


@app.cell
def final_stats(mo, pd, kelly_portfolio):
    _output = []
    if kelly_portfolio is not None:
        _output.append(mo.md("## Final Portfolio Statistics"))
        _final_stats = kelly_portfolio.stats()
        _output.append(mo.Html(pd.DataFrame(_final_stats).to_html()))
    mo.vstack(_output) if _output else None


@app.cell
def export_metrics_json(json, Path, pd, kelly_portfolio, allocation_info, output_html_path):
    import math

    def _coerce_float(value):
        """Convert value to float, returning None for non-finite or invalid values."""
        if value is None:
            return None
        try:
            f = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(f):
            return None
        return f

    def _extract_stat(stats, key):
        """Extract a single stat value from vectorbt stats (Series or DataFrame)."""
        if stats is None:
            return None
        if isinstance(stats, pd.Series):
            return _coerce_float(stats.get(key))
        if isinstance(stats, pd.DataFrame) and key in stats.index:
            row = stats.loc[key]
            return _coerce_float(row.mean() if hasattr(row, "mean") else row.iloc[0])
        return None

    if output_html_path:
        metrics = {}

        if kelly_portfolio is not None:
            stats = kelly_portfolio.stats()
            metrics["total_return"] = _extract_stat(stats, "Total Return [%]")
            metrics["max_drawdown"] = _extract_stat(stats, "Max Drawdown [%]")
            metrics["sharpe_ratio"] = _extract_stat(stats, "Sharpe Ratio")
            metrics["sortino_ratio"] = _extract_stat(stats, "Sortino Ratio")
            metrics["calmar_ratio"] = _extract_stat(stats, "Calmar Ratio")
            metrics["win_rate"] = _extract_stat(stats, "Win Rate [%]")
            metrics["profit_factor"] = _extract_stat(stats, "Profit Factor")
            total_trades = _extract_stat(stats, "Total Trades")
            metrics["total_trades"] = int(total_trades) if total_trades is not None else None

        if hasattr(allocation_info, "allocation"):
            metrics["optimal_allocation"] = _coerce_float(allocation_info.allocation)
        elif isinstance(allocation_info, (int, float)):
            metrics["optimal_allocation"] = _coerce_float(allocation_info)

        metrics_path = Path(output_html_path).with_suffix(".metrics.json")
        metrics_path.write_text(
            json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8"
        )


if __name__ == "__main__":
    app.run()
