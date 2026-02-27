# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "pandas>=2.0.0",
#     "numpy>=1.20.0",
#     "matplotlib>=3.0.0",
#     "vectorbt>=0.26.0",
# ]
# ///

"""
Risk Adjusted Return Playground (Marimo)

UI playground for domain function validation.
Computation logic must stay in src/domains, this notebook provides UI only.
"""

from __future__ import annotations

import marimo

app = marimo.App(width="full", app_title="Risk Adjusted Return Playground")


@app.cell
def imports():
    import marimo as mo
    import sys
    from pathlib import Path
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    return mo, sys, Path, np, pd, plt


@app.cell
def bootstrap_project_root(sys, Path):
    project_root = Path.cwd()
    if project_root.name == "playground":
        project_root = project_root.parent.parent
    elif project_root.name == "notebooks":
        project_root = project_root.parent

    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from src.domains.strategy.indicators.calculations import (
        compute_risk_adjusted_return,
    )

    return compute_risk_adjusted_return,


@app.cell
def controls(mo):
    lookback = mo.ui.slider(20, 180, value=60, step=5, label="Lookback Period")
    ratio_type = mo.ui.dropdown(
        options=["sortino", "sharpe"],
        value="sortino",
        label="Ratio Type",
    )
    seed = mo.ui.number(value=42, start=0, stop=999999, step=1, label="Seed")
    n_days = mo.ui.slider(252, 1260, value=504, step=21, label="Days")
    mo.vstack([lookback, ratio_type, seed, n_days])
    return lookback, ratio_type, seed, n_days


@app.cell
def build_sample_series(np, pd, seed, n_days):
    rng = np.random.default_rng(seed.value)
    returns = rng.normal(loc=0.0005, scale=0.018, size=n_days.value)
    dates = pd.bdate_range("2022-01-03", periods=n_days.value)
    close = pd.Series(100.0 * np.cumprod(1.0 + returns), index=dates, name="close")
    return close,


@app.cell
def calculate_ratio(compute_risk_adjusted_return, close, lookback, ratio_type):
    ratio = compute_risk_adjusted_return(
        close=close,
        lookback_period=lookback.value,
        ratio_type=ratio_type.value,
    )
    return ratio,


@app.cell
def render_summary(mo, close, ratio):
    latest = ratio.dropna().iloc[-1] if not ratio.dropna().empty else None
    latest_text = f"{latest:.4f}" if latest is not None else "N/A"
    mo.md(
        f"""
## Risk Adjusted Return Playground

- Input series length: **{len(close)}**
- Latest valid ratio: **{latest_text}**
"""
    )


@app.cell
def render_chart(plt, close, ratio):
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    axes[0].plot(close.index, close.values, color="#1f77b4")
    axes[0].set_title("Synthetic Close")
    axes[0].grid(alpha=0.2)

    axes[1].plot(ratio.index, ratio.values, color="#2ca02c")
    axes[1].set_title("Risk Adjusted Return")
    axes[1].axhline(0.0, color="#666666", linewidth=1, alpha=0.6)
    axes[1].grid(alpha=0.2)

    fig.tight_layout()
    fig


@app.cell
def render_table(mo, pd, close, ratio):
    df = pd.DataFrame({"close": close, "risk_adjusted_return": ratio}).tail(20)
    mo.Html(df.to_html(index=True))


if __name__ == "__main__":
    app.run()
