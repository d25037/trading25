"""Shared regression-domain calculations for analytics services."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class OLSResult:
    """OLS regression output."""

    alpha: float
    beta: float
    r_squared: float
    residuals: list[float]


@dataclass(frozen=True)
class DailyReturn:
    """Daily log-return point."""

    date: str
    ret: float


@dataclass(frozen=True)
class RegressionMatch:
    """Residual-regression match summary."""

    code: str
    name: str
    category: str
    r_squared: float
    beta: float


def ols_regression(y: list[float], x: list[float]) -> OLSResult:
    """OLS regression: y = alpha + beta * x + residual."""
    n = len(y)
    if n != len(x):
        raise ValueError(f"Arrays must have same length: {n} != {len(x)}")
    if n < 2:
        raise ValueError(f"At least 2 data points required: {n}")

    mean_y = sum(y) / n
    mean_x = sum(x) / n
    var_x = sum((xi - mean_x) ** 2 for xi in x) / n

    if var_x == 0:
        residuals = [yi - mean_y for yi in y]
        return OLSResult(alpha=mean_y, beta=0.0, r_squared=0.0, residuals=residuals)

    cov_xy = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n)) / n
    beta = cov_xy / var_x
    alpha = mean_y - beta * mean_x

    residuals: list[float] = []
    ss_res = 0.0
    ss_tot = 0.0
    for i in range(n):
        predicted = alpha + beta * x[i]
        residual = y[i] - predicted
        residuals.append(residual)
        ss_res += residual**2
        ss_tot += (y[i] - mean_y) ** 2

    r_squared = 0.0 if ss_tot == 0 else max(0.0, min(1.0, 1 - ss_res / ss_tot))
    return OLSResult(alpha=alpha, beta=beta, r_squared=r_squared, residuals=residuals)


def calculate_daily_returns(prices: list[tuple[str, float]]) -> list[DailyReturn]:
    """Calculate daily log returns from (date, close) prices."""
    returns: list[DailyReturn] = []
    for i in range(1, len(prices)):
        prev_close = prices[i - 1][1]
        curr_close = prices[i][1]
        if prev_close > 0 and curr_close > 0:
            returns.append(DailyReturn(date=prices[i][0], ret=math.log(curr_close / prev_close)))
    return returns


def align_returns(
    left: list[DailyReturn],
    right: list[DailyReturn],
) -> tuple[list[str], list[float], list[float]]:
    """Align two return series by date."""
    right_map = {r.date: r.ret for r in right}
    dates: list[str] = []
    aligned_left: list[float] = []
    aligned_right: list[float] = []

    for point in left:
        rhs = right_map.get(point.date)
        if rhs is None:
            continue
        dates.append(point.date)
        aligned_left.append(point.ret)
        aligned_right.append(rhs)

    return dates, aligned_left, aligned_right


def find_best_matches(
    residuals: list[float],
    residual_dates: list[str],
    indices_returns: dict[str, list[DailyReturn]],
    category_codes: list[str],
    index_names: dict[str, tuple[str, str]],
    top_n: int = 3,
) -> list[RegressionMatch]:
    """Residual regression against index groups and return top-N by R-squared."""
    min_data_points = 30
    matches: list[RegressionMatch] = []

    residual_date_set = set(residual_dates)
    residual_map = dict(zip(residual_dates, residuals))

    for code in category_codes:
        index_rets = indices_returns.get(code)
        if not index_rets:
            continue

        aligned_res: list[float] = []
        aligned_idx: list[float] = []
        for ir in index_rets:
            if ir.date not in residual_date_set:
                continue
            residual_value = residual_map.get(ir.date)
            if residual_value is None:
                continue
            aligned_res.append(residual_value)
            aligned_idx.append(ir.ret)

        if len(aligned_res) < min_data_points:
            continue

        try:
            result = ols_regression(aligned_res, aligned_idx)
        except ValueError:
            continue

        name, category = index_names.get(code, (code, "unknown"))
        matches.append(
            RegressionMatch(
                code=code,
                name=name,
                category=category,
                r_squared=round(result.r_squared, 3),
                beta=round(result.beta, 3),
            )
        )

    matches.sort(key=lambda m: m.r_squared, reverse=True)
    return matches[:top_n]


def calculate_weighted_portfolio_returns(
    stock_returns_map: dict[str, list[DailyReturn]],
    weight_map: dict[str, float],
) -> list[DailyReturn]:
    """Calculate weighted portfolio return series from stock return series."""
    all_dates: set[str] = set()
    for returns in stock_returns_map.values():
        all_dates.update(point.date for point in returns)
    sorted_dates = sorted(all_dates)

    return_maps: dict[str, dict[str, float]] = {}
    for code, returns in stock_returns_map.items():
        return_maps[code] = {point.date: point.ret for point in returns}

    results: list[DailyReturn] = []
    for date in sorted_dates:
        weighted_ret = 0.0
        total_weight = 0.0
        for code, weight in weight_map.items():
            ret = return_maps.get(code, {}).get(date)
            if ret is None:
                continue
            weighted_ret += weight * ret
            total_weight += weight
        if total_weight > 0:
            results.append(DailyReturn(date=date, ret=weighted_ret / total_weight))

    return results
