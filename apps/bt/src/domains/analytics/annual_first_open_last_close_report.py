"""Report payload builders for annual first-open last-close research."""

from __future__ import annotations

import math
from typing import Any, cast

import pandas as pd


def _fmt_num(value: float | int | None, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    if isinstance(value, int):
        return f"{value}"
    return f"{value:.{digits}f}"


def build_summary_markdown(result: Any) -> str:
    lines = [
        "# Annual First-Open Last-Close Fundamental Panel",
        "",
        "## Setup",
        "",
        f"- Scope: `{', '.join(result.selected_markets)}`",
        "- Event: buy each stock at the first trading day's open and sell at the last trading day's close for each complete calendar year.",
        (
            "- Market classification uses `stock_master_daily` on each entry date; historical market membership is PIT-safe for the annual entry universe."
            if not result.current_market_snapshot_only
            else "- Market classification uses the current `stocks` snapshot fallback; historical market migrations are not reconstructed."
        ),
        "- Fundamental as-of: latest FY disclosure available on or before the entry date.",
        "- Per-share adjustment: EPS, BPS, forward EPS and dividend-per-share fields are adjusted to the latest share baseline available on or before entry, preferring quarterly shares, then onto the entry adjusted-price basis using stock split adjustment factors.",
        f"- Factor buckets: `{result.bucket_count}` within each year and market scope.",
        "",
        "## Portfolio Summary",
        "",
    ]
    all_years = (
        result.annual_portfolio_summary_df[
            result.annual_portfolio_summary_df["portfolio_scope"].astype(str)
            == "all_years"
        ]
        if not result.annual_portfolio_summary_df.empty
        else pd.DataFrame()
    )
    if all_years.empty:
        lines.append("- No realized annual portfolio could be built.")
    else:
        for row in all_years.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}`: events `{int(cast(int, row['realized_event_count']))}`, "
                f"total `{_fmt_num(cast(float | int | None, row['total_return_pct']))}%`, "
                f"CAGR `{_fmt_num(cast(float | int | None, row['cagr_pct']))}%`, "
                f"Sharpe `{_fmt_num(cast(float | int | None, row['sharpe_ratio']), 2)}`, "
                f"Sortino `{_fmt_num(cast(float | int | None, row['sortino_ratio']), 2)}`, "
                f"Calmar `{_fmt_num(cast(float | int | None, row['calmar_ratio']), 2)}`, "
                f"maxDD `{_fmt_num(cast(float | int | None, row['max_drawdown_pct']))}%`"
            )

    lines.extend(["", "## Strongest Factor Spreads", ""])
    preferred = result.factor_spread_summary_df.copy()
    if preferred.empty:
        lines.append("- No factor spread summary was available.")
    else:
        preferred["abs_spread"] = pd.to_numeric(
            preferred["preferred_minus_opposite_mean_return_pct"],
            errors="coerce",
        ).abs()
        preferred = preferred.sort_values("abs_spread", ascending=False).head(12)
        for row in preferred.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['feature_name']}`: "
                f"preferred spread `{_fmt_num(cast(float | int | None, row['preferred_minus_opposite_mean_return_pct']))}%`, "
                f"high-low `{_fmt_num(cast(float | int | None, row['high_minus_low_mean_return_pct']))}%`"
            )

    lines.extend(["", "## Diagnostics", ""])
    if result.event_ledger_df.empty:
        lines.append("- Event ledger is empty.")
    else:
        realized = result.event_ledger_df[
            result.event_ledger_df["status"] == "realized"
        ]
        adjusted = realized[realized["share_adjustment_applied"] == True]  # noqa: E712
        lines.append(f"- Realized events: `{len(realized)}`")
        lines.append(
            f"- Events with per-share split adjustment applied: `{len(adjusted)}`"
        )
    return "\n".join(lines)


def build_published_summary(result: Any) -> dict[str, Any]:
    return {
        "selectedMarkets": list(result.selected_markets),
        "bucketCount": result.bucket_count,
        "advWindow": result.adv_window,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "entryTiming": result.entry_timing,
        "exitTiming": result.exit_timing,
        "shareAdjustmentPolicy": result.share_adjustment_policy,
        "annualPortfolioSummary": result.annual_portfolio_summary_df.to_dict(
            orient="records"
        ),
        "factorSpreadSummary": result.factor_spread_summary_df.to_dict(
            orient="records"
        ),
        "featureCoverage": result.feature_coverage_df.to_dict(orient="records"),
    }
