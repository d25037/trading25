"""Portfolio table builders for annual first-open last-close research."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any

import numpy as np
import pandas as pd


_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth")


def _empty_result_df(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _to_nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(coerced):
        return None
    return coerced


def _series_stat(series: pd.Series, fn: str) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    if fn == "mean":
        return float(numeric.mean())
    raise ValueError(f"Unsupported stat: {fn}")


def _annualized_volatility_pct(daily_returns: pd.Series) -> float | None:
    numeric = pd.to_numeric(daily_returns, errors="coerce").dropna()
    if len(numeric) < 2:
        return None
    value = float(numeric.std(ddof=1) * math.sqrt(252.0) * 100.0)
    return value if math.isfinite(value) else None


def _annualized_sharpe(daily_returns: pd.Series) -> float | None:
    numeric = pd.to_numeric(daily_returns, errors="coerce").dropna()
    if len(numeric) < 2:
        return None
    std = float(numeric.std(ddof=1))
    if not math.isfinite(std) or math.isclose(std, 0.0, abs_tol=1e-12):
        return None
    value = float(numeric.mean()) / std * math.sqrt(252.0)
    return value if math.isfinite(value) else None


def _annualized_sortino(daily_returns: pd.Series) -> float | None:
    numeric = pd.to_numeric(daily_returns, errors="coerce").dropna()
    if len(numeric) < 2:
        return None
    downside = numeric[numeric < 0.0]
    if len(downside) < 2:
        return None
    downside_std = float(downside.std(ddof=1))
    if not math.isfinite(downside_std) or math.isclose(
        downside_std, 0.0, abs_tol=1e-12
    ):
        return None
    value = float(numeric.mean()) / downside_std * math.sqrt(252.0)
    return value if math.isfinite(value) else None


def build_annual_portfolio_daily_df(
    *,
    event_ledger_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "portfolio_scope",
        "date",
        "active_positions",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    if realized_df.empty or price_df.empty:
        return _empty_result_df(columns)
    price_by_code = {
        str(code): frame.sort_values("date", kind="stable").reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    aggregate: dict[tuple[str, str, str], list[float]] = defaultdict(lambda: [0.0, 0.0])
    for event in realized_df.to_dict(orient="records"):
        code = str(event["code"])
        price_frame = price_by_code.get(code)
        if price_frame is None:
            continue
        path_df = price_frame[
            (price_frame["date"] >= str(event["entry_date"]))
            & (price_frame["date"] <= str(event["exit_date"]))
        ].copy()
        if path_df.empty:
            continue
        entry_open = _to_nullable_float(event.get("entry_open"))
        if entry_open is None or entry_open <= 0:
            continue
        close_values = (
            pd.to_numeric(path_df["close"], errors="coerce").astype(float).to_numpy()
        )
        if not np.isfinite(close_values).all():
            continue
        previous_close = np.concatenate(([entry_open], close_values[:-1]))
        daily_returns = close_values / previous_close - 1.0
        scopes = ("all", str(event["market"]))
        portfolio_scopes = ("all_years", str(event["year"]))
        for market_scope in scopes:
            for portfolio_scope in portfolio_scopes:
                for date_value, daily_return in zip(
                    path_df["date"].astype(str),
                    daily_returns,
                    strict=True,
                ):
                    bucket = aggregate[(market_scope, portfolio_scope, str(date_value))]
                    bucket[0] += float(daily_return)
                    bucket[1] += 1.0
    if not aggregate:
        return _empty_result_df(columns)
    records = [
        {
            "market_scope": market_scope,
            "portfolio_scope": portfolio_scope,
            "date": date_value,
            "active_positions": int(values[1]),
            "mean_daily_return": float(values[0] / values[1]),
            "mean_daily_return_pct": float(values[0] / values[1] * 100.0),
        }
        for (market_scope, portfolio_scope, date_value), values in aggregate.items()
    ]
    daily_df = (
        pd.DataFrame(records)
        .sort_values(
            ["market_scope", "portfolio_scope", "date"],
            kind="stable",
        )
        .reset_index(drop=True)
    )
    daily_df["portfolio_value"] = np.nan
    daily_df["drawdown_pct"] = np.nan
    for _, group_df in daily_df.groupby(
        ["market_scope", "portfolio_scope"],
        observed=True,
        sort=False,
    ):
        idx = list(group_df.index)
        values = (1.0 + daily_df.loc[idx, "mean_daily_return"]).cumprod()
        peaks = values.cummax()
        daily_df.loc[idx, "portfolio_value"] = values.to_numpy()
        daily_df.loc[idx, "drawdown_pct"] = ((values / peaks - 1.0) * 100.0).to_numpy()
    return daily_df


def build_annual_portfolio_summary_df(
    *,
    annual_portfolio_daily_df: pd.DataFrame,
    event_ledger_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "portfolio_scope",
        "realized_event_count",
        "start_date",
        "end_date",
        "active_days",
        "avg_active_positions",
        "max_active_positions",
        "total_return_pct",
        "cagr_pct",
        "max_drawdown_pct",
        "annualized_volatility_pct",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
    ]
    if annual_portfolio_daily_df.empty:
        return _empty_result_df(columns)
    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    count_map: dict[tuple[str, str], int] = defaultdict(int)
    for event in realized_df.to_dict(orient="records"):
        for market_scope in ("all", str(event["market"])):
            count_map[(market_scope, "all_years")] += 1
            count_map[(market_scope, str(event["year"]))] += 1
    records: list[dict[str, Any]] = []
    for (market_scope, portfolio_scope), group_df in annual_portfolio_daily_df.groupby(
        ["market_scope", "portfolio_scope"],
        observed=True,
        sort=False,
    ):
        start_date = str(group_df["date"].iloc[0])
        end_date = str(group_df["date"].iloc[-1])
        total_return = float(group_df["portfolio_value"].iloc[-1] - 1.0)
        period_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
        cagr = None
        if period_days > 0 and total_return > -1.0:
            cagr_value = (1.0 + total_return) ** (365.25 / period_days) - 1.0
            cagr = float(cagr_value) if math.isfinite(cagr_value) else None
        max_drawdown_pct = _series_stat(group_df["drawdown_pct"], "mean")
        drawdown_min = pd.to_numeric(group_df["drawdown_pct"], errors="coerce").min()
        max_drawdown_pct = (
            float(drawdown_min) if pd.notna(drawdown_min) else max_drawdown_pct
        )
        records.append(
            {
                "market_scope": str(market_scope),
                "portfolio_scope": str(portfolio_scope),
                "realized_event_count": int(
                    count_map[(str(market_scope), str(portfolio_scope))]
                ),
                "start_date": start_date,
                "end_date": end_date,
                "active_days": int(len(group_df)),
                "avg_active_positions": _series_stat(
                    group_df["active_positions"], "mean"
                ),
                "max_active_positions": int(
                    pd.to_numeric(group_df["active_positions"]).max()
                ),
                "total_return_pct": total_return * 100.0,
                "cagr_pct": cagr * 100.0 if cagr is not None else None,
                "max_drawdown_pct": max_drawdown_pct,
                "annualized_volatility_pct": _annualized_volatility_pct(
                    group_df["mean_daily_return"]
                ),
                "sharpe_ratio": _annualized_sharpe(group_df["mean_daily_return"]),
                "sortino_ratio": _annualized_sortino(group_df["mean_daily_return"]),
                "calmar_ratio": (
                    cagr / abs(max_drawdown_pct / 100.0)
                    if cagr is not None
                    and max_drawdown_pct is not None
                    and max_drawdown_pct < -1e-12
                    else None
                ),
            }
        )
    result = pd.DataFrame(records)
    result["market_scope"] = pd.Categorical(
        result["market_scope"],
        categories=[
            scope
            for scope in _MARKET_SCOPE_ORDER
            if scope in set(result["market_scope"])
        ],
        ordered=True,
    )
    return result.sort_values(
        ["market_scope", "portfolio_scope"], kind="stable"
    ).reset_index(drop=True)
