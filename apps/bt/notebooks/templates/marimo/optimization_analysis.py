# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "pandas>=2.0.0",
#     "numpy>=1.20.0",
#     "matplotlib>=3.0.0",
# ]
# ///

"""
Optimization Analysis Template (Marimo版)

パラメータ最適化結果の可視化・分析テンプレート
CLI引数経由でパラメータを受け取り、静的HTMLとして出力
"""

import marimo

app = marimo.App(width="full", app_title="Optimization Analysis")


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
    _project_root = Path.cwd()
    if _project_root.name == "marimo":
        _project_root = _project_root.parent.parent.parent
    elif _project_root.name == "templates":
        _project_root = _project_root.parent.parent
    elif _project_root.name == "notebooks":
        _project_root = _project_root.parent

    if str(_project_root) not in sys.path:
        sys.path.insert(0, str(_project_root))

    # CLI引数からパラメータを取得
    _cli_args = mo.cli_args()
    _params_json_path = _cli_args.get("params-json", "")

    if _params_json_path and Path(_params_json_path).exists():
        with open(_params_json_path, "r", encoding="utf-8") as _f:
            _params = json.load(_f)
    else:
        _params = {
            "results_json_path": "",
            "strategy_name": "",
            "parameter_ranges": {},
            "scoring_weights": {},
            "n_combinations": 0,
        }

    results_json_path = _params.get("results_json_path", "")
    strategy_name = _params.get("strategy_name", "")
    parameter_ranges = _params.get("parameter_ranges", {})
    scoring_weights = _params.get("scoring_weights", {})
    n_combinations = _params.get("n_combinations", 0)

    mo.md(f"""
# Parameter Optimization Analysis

**Strategy**: {strategy_name}
**Combinations Tested**: {n_combinations}
""")

    return results_json_path, strategy_name, parameter_ranges, scoring_weights, n_combinations


@app.cell
def load_results(mo, json, results_json_path, Path):
    if not results_json_path or not Path(results_json_path).exists():
        mo.md("**Error**: Results JSON file not found")
        optimization_results = []
    else:
        with open(results_json_path, "r", encoding="utf-8") as _f:
            optimization_results = json.load(_f)
        mo.md(f"Loaded {len(optimization_results)} optimization results")

    return optimization_results,


@app.cell
def show_optimization_settings(mo, strategy_name, n_combinations, scoring_weights, parameter_ranges):
    _weights_table = "\n".join([f"| {_metric} | {_weight} |" for _metric, _weight in scoring_weights.items()])

    _ranges_text = ""
    for _section, _signals in parameter_ranges.items():
        if _signals:
            _ranges_text += f"\n**{_section}**:\n"
            for _signal, _params in _signals.items():
                if _params:
                    _ranges_text += f"- {_signal}:\n"
                    for _param, _values in _params.items():
                        _ranges_text += f"  - {_param}: {_values}\n"

    mo.md(f"""
## Optimization Settings

**Strategy Name**: {strategy_name}
**Total Combinations**: {n_combinations}

### Scoring Weights

| Metric | Weight |
|--------|--------|
{_weights_table}

### Parameter Ranges
{_ranges_text}
""")


@app.cell
def ranking_top20(mo, pd, optimization_results):
    _output = [mo.md("## Composite Score Ranking (Top 20)")]

    if optimization_results:
        _ranking_data = []
        for _i, _r in enumerate(optimization_results[:20], 1):
            _row = {"Rank": _i, "Score": _r["score"]}

            for _key, _value in _r["params"].items():
                _parts = _key.split(".")
                if _parts[0] == "entry_filter_params":
                    _short_key = f"entry_{_parts[-1]}"
                elif _parts[0] == "exit_trigger_params":
                    _short_key = f"exit_{_parts[-1]}"
                else:
                    _short_key = _parts[-1]
                _row[_short_key] = _value

            _row["Sharpe"] = _r["metric_values"].get("sharpe_ratio", 0)
            _row["Calmar"] = _r["metric_values"].get("calmar_ratio", 0)
            _row["Return"] = _r["metric_values"].get("total_return", 0)
            try:
                _row["Trades"] = int(float(_r["metric_values"].get("trade_count", 0)))
            except (TypeError, ValueError):
                _row["Trades"] = 0

            _ranking_data.append(_row)

        _ranking_df = pd.DataFrame(_ranking_data)
        _output.append(mo.Html(_ranking_df.to_html(index=False)))

    mo.vstack(_output)


@app.cell
def ranking_bottom10(mo, pd, optimization_results):
    _output = [mo.md("## Composite Score Ranking (Bottom 10)")]

    if optimization_results:
        _n_results = len(optimization_results)
        _bottom_10 = optimization_results[-10:] if _n_results >= 10 else optimization_results

        _bottom_ranking_data = []
        for _i, _r in enumerate(_bottom_10, _n_results - len(_bottom_10) + 1):
            _row = {"Rank": _i, "Score": _r["score"]}

            for _key, _value in _r["params"].items():
                _parts = _key.split(".")
                if _parts[0] == "entry_filter_params":
                    _short_key = f"entry_{_parts[-1]}"
                elif _parts[0] == "exit_trigger_params":
                    _short_key = f"exit_{_parts[-1]}"
                else:
                    _short_key = _parts[-1]
                _row[_short_key] = _value

            _row["Sharpe"] = _r["metric_values"].get("sharpe_ratio", 0)
            _row["Calmar"] = _r["metric_values"].get("calmar_ratio", 0)
            _row["Return"] = _r["metric_values"].get("total_return", 0)
            try:
                _row["Trades"] = int(float(_r["metric_values"].get("trade_count", 0)))
            except (TypeError, ValueError):
                _row["Trades"] = 0

            _bottom_ranking_data.append(_row)

        _bottom_ranking_df = pd.DataFrame(_bottom_ranking_data)
        _output.append(mo.Html(_bottom_ranking_df.to_html(index=False)))

    mo.vstack(_output)


@app.cell
def best_parameters(mo, optimization_results):
    _output = [mo.md("## Best Parameters Detail")]

    if optimization_results:
        _best_result = optimization_results[0]

        _params_table = "\n".join([
            f"| {_key.replace('entry_filter_params.', '').replace('exit_trigger_params.', '')} | {_value} |"
            for _key, _value in _best_result["params"].items()
        ])

        _metrics = _best_result["metric_values"]
        try:
            _trade_count = int(float(_metrics.get("trade_count", 0)))
        except (TypeError, ValueError):
            _trade_count = 0

        _metrics_table = f"""
| Sharpe Ratio | {_metrics.get('sharpe_ratio', 0):.4f} |
| Calmar Ratio | {_metrics.get('calmar_ratio', 0):.4f} |
| Total Return | {_metrics.get('total_return', 0):.2%} |
| Trade Count | {_trade_count} |
"""

        _norm_metrics = _best_result.get("normalized_metrics", {})
        _norm_table = "\n".join([f"| {_metric} | {_value:.4f} |" for _metric, _value in _norm_metrics.items()])

        _output.append(mo.md(f"""
### Composite Score: {_best_result['score']:.4f}

### Parameters

| Parameter | Value |
|-----------|-------|
{_params_table}

### Performance Metrics

| Metric | Value |
|--------|-------|
{_metrics_table}

### Normalized Metrics (0-1)

| Metric | Value |
|--------|-------|
{_norm_table}
"""))

    mo.vstack(_output)


@app.cell
def parameter_sensitivity(mo, np, optimization_results):
    _output = [mo.md("## Parameter Sensitivity Analysis")]
    correlations = []

    if optimization_results:
        _param_keys = list(optimization_results[0]["params"].keys())

        if len(_param_keys) == 0:
            _output.append(mo.md("No parameters to analyze"))
        else:
            # Calculate correlations
            for _param_key in _param_keys:
                _param_values = [_r["params"][_param_key] for _r in optimization_results]
                _scores = [_r["score"] for _r in optimization_results]

                if len(set(_param_values)) > 1:
                    _corr = np.corrcoef(_param_values, _scores)[0, 1]
                else:
                    _corr = 0.0

                _parts = _param_key.split(".")
                if _parts[0] == "entry_filter_params":
                    _short_name = f"entry_{_parts[-1]}"
                elif _parts[0] == "exit_trigger_params":
                    _short_name = f"exit_{_parts[-1]}"
                else:
                    _short_name = _parts[-1]

                correlations.append({
                    "parameter": _short_name,
                    "full_key": _param_key,
                    "correlation": _corr,
                    "abs_correlation": abs(_corr),
                })

            correlations.sort(key=lambda x: x["abs_correlation"], reverse=True)

            # Display ranking
            _corr_table = "\n".join([
                f"| {_idx} | {_c['parameter']} | {_c['correlation']:.4f} | {_c['abs_correlation']:.4f} |"
                for _idx, _c in enumerate(correlations, 1)
            ])

            _output.append(mo.md(f"""
### Parameter-Score Correlation Ranking

| Rank | Parameter | Correlation | Abs Correlation |
|------|-----------|-------------|-----------------|
{_corr_table}
"""))

    mo.vstack(_output)
    return correlations,


@app.cell
def sensitivity_scatter_plots(plt, optimization_results, correlations):
    _fig1 = None
    if optimization_results and correlations:
        _top_n = min(4, len(correlations))
        _top_params = correlations[:_top_n]

        _n_cols = min(2, _top_n)
        _n_rows = (_top_n + _n_cols - 1) // _n_cols

        _fig1, _axes1 = plt.subplots(_n_rows, _n_cols, figsize=(12, _n_rows * 4))

        if _top_n == 1:
            _axes1 = [_axes1]
        elif _top_n > 1:
            _axes1 = _axes1.flatten()

        for _idx, _param_info in enumerate(_top_params):
            _ax = _axes1[_idx] if _top_n > 1 else _axes1[0]
            _param_key = _param_info["full_key"]

            _param_values = [_r["params"][_param_key] for _r in optimization_results]
            _scores = [_r["score"] for _r in optimization_results]

            _ax.scatter(_param_values, _scores, alpha=0.6, s=30, c=_scores, cmap="viridis")

            _ax.set_xlabel(_param_info["parameter"], fontsize=10)
            _ax.set_ylabel("Composite Score", fontsize=10)
            _ax.set_title(f"#{_idx+1}: {_param_info['parameter']} (r={_param_info['correlation']:.3f})",
                         fontsize=11, fontweight="bold")
            _ax.grid(True, alpha=0.3)

            # Highlight best point
            _best_param = optimization_results[0]["params"][_param_key]
            _best_score = optimization_results[0]["score"]
            _ax.scatter([_best_param], [_best_score], c="red", s=100, marker="*",
                       edgecolors="black", linewidths=1.5, label="Best")
            _ax.legend()

        # Hide unused axes
        if _top_n > 1:
            for _j in range(_top_n, len(_axes1)):
                _axes1[_j].axis("off")

        plt.tight_layout()
    _fig1


@app.cell
def metrics_distribution(mo, np, plt, optimization_results):
    _output = [mo.md("## Metrics Distribution")]

    if optimization_results:
        _sharpe_values = [_r["metric_values"].get("sharpe_ratio", 0) for _r in optimization_results]
        _calmar_values = [_r["metric_values"].get("calmar_ratio", 0) for _r in optimization_results]
        _return_values = [_r["metric_values"].get("total_return", 0) for _r in optimization_results]

        _fig2, _axes2 = plt.subplots(1, 3, figsize=(18, 5))

        # Sharpe Ratio
        _axes2[0].hist(_sharpe_values, bins=20, alpha=0.7, color="skyblue", edgecolor="black")
        _axes2[0].axvline(optimization_results[0]["metric_values"].get("sharpe_ratio", 0),
                        color="red", linestyle="--", linewidth=2, label="Best")
        _axes2[0].set_xlabel("Sharpe Ratio", fontsize=11)
        _axes2[0].set_ylabel("Frequency", fontsize=11)
        _axes2[0].set_title("Sharpe Ratio Distribution", fontsize=12, fontweight="bold")
        _axes2[0].legend()
        _axes2[0].grid(True, alpha=0.3)

        # Calmar Ratio
        _axes2[1].hist(_calmar_values, bins=20, alpha=0.7, color="lightgreen", edgecolor="black")
        _axes2[1].axvline(optimization_results[0]["metric_values"].get("calmar_ratio", 0),
                        color="red", linestyle="--", linewidth=2, label="Best")
        _axes2[1].set_xlabel("Calmar Ratio", fontsize=11)
        _axes2[1].set_ylabel("Frequency", fontsize=11)
        _axes2[1].set_title("Calmar Ratio Distribution", fontsize=12, fontweight="bold")
        _axes2[1].legend()
        _axes2[1].grid(True, alpha=0.3)

        # Total Return
        _axes2[2].hist(_return_values, bins=20, alpha=0.7, color="salmon", edgecolor="black")
        _axes2[2].axvline(optimization_results[0]["metric_values"].get("total_return", 0),
                        color="red", linestyle="--", linewidth=2, label="Best")
        _axes2[2].set_xlabel("Total Return", fontsize=11)
        _axes2[2].set_ylabel("Frequency", fontsize=11)
        _axes2[2].set_title("Total Return Distribution", fontsize=12, fontweight="bold")
        _axes2[2].legend()
        _axes2[2].grid(True, alpha=0.3)

        plt.tight_layout()
        _output.append(_fig2)

        _output.append(mo.md(f"""
### Statistics

| Metric | Mean | Std Dev |
|--------|------|---------|
| Sharpe Ratio | {np.mean(_sharpe_values):.4f} | {np.std(_sharpe_values):.4f} |
| Calmar Ratio | {np.mean(_calmar_values):.4f} | {np.std(_calmar_values):.4f} |
| Total Return | {np.mean(_return_values):.2%} | {np.std(_return_values):.2%} |
"""))

    mo.vstack(_output)


if __name__ == "__main__":
    app.run()
