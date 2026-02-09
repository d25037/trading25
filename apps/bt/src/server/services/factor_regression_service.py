"""
Factor Regression Service

2段階ファクター回帰分析サービス。
Stage 1: 市場回帰（銘柄リターン vs TOPIX）
Stage 2: 残差ファクターマッチング（残差 vs 各指数カテゴリ）

Hono performFactorRegression / FactorRegressionService 互換。
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import NamedTuple

from src.lib.market_db.query_helpers import stock_code_candidates
from src.lib.market_db.market_reader import MarketDbReader
from src.server.schemas.factor_regression import (
    DateRange,
    FactorRegressionResponse,
    IndexMatch,
)

# 指数カテゴリ定数
CATEGORY_SECTOR17 = "sector17"
CATEGORY_SECTOR33 = "sector33"
CATEGORY_TOPIX = "topix"
CATEGORY_MARKET = "market"
CATEGORY_STYLE = "style"

TOPIX_CODE = "0000"


class OLSResult(NamedTuple):
    """OLS 回帰結果"""

    alpha: float
    beta: float
    r_squared: float
    residuals: list[float]


class DailyReturn(NamedTuple):
    """日次リターン"""

    date: str
    ret: float


def _ols_regression(y: list[float], x: list[float]) -> OLSResult:
    """OLS 回帰 (y = alpha + beta * x + residual)"""
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


def _calculate_daily_returns(prices: list[tuple[str, float]]) -> list[DailyReturn]:
    """日次対数リターンを計算"""
    returns: list[DailyReturn] = []
    for i in range(1, len(prices)):
        prev_close = prices[i - 1][1]
        curr_close = prices[i][1]
        if prev_close > 0 and curr_close > 0:
            log_return = math.log(curr_close / prev_close)
            returns.append(DailyReturn(date=prices[i][0], ret=log_return))
    return returns


def _align_returns(
    stock_returns: list[DailyReturn], index_returns: list[DailyReturn]
) -> tuple[list[str], list[float], list[float]]:
    """2つのリターン系列を日付で整列"""
    index_map = {r.date: r.ret for r in index_returns}
    dates: list[str] = []
    aligned_stock: list[float] = []
    aligned_index: list[float] = []

    for sr in stock_returns:
        ir = index_map.get(sr.date)
        if ir is not None:
            dates.append(sr.date)
            aligned_stock.append(sr.ret)
            aligned_index.append(ir)

    return dates, aligned_stock, aligned_index


def _find_best_matches(
    residuals: list[float],
    residual_dates: list[str],
    indices_returns: dict[str, list[DailyReturn]],
    category_codes: list[str],
    index_names: dict[str, tuple[str, str]],
    top_n: int = 3,
) -> list[IndexMatch]:
    """カテゴリ内の指数に対して残差回帰し、R² 上位 N を返す"""
    min_data_points = 30
    matches: list[IndexMatch] = []

    residual_date_set = set(residual_dates)
    residual_map = dict(zip(residual_dates, residuals))

    for code in category_codes:
        index_rets = indices_returns.get(code)
        if not index_rets:
            continue

        # 残差と指数リターンを日付で整列
        aligned_res: list[float] = []
        aligned_idx: list[float] = []
        for ir in index_rets:
            if ir.date in residual_date_set:
                r = residual_map.get(ir.date)
                if r is not None:
                    aligned_res.append(r)
                    aligned_idx.append(ir.ret)

        if len(aligned_res) < min_data_points:
            continue

        try:
            result = _ols_regression(aligned_res, aligned_idx)
        except ValueError:
            continue

        name_info = index_names.get(code, (code, "unknown"))
        matches.append(
            IndexMatch(
                indexCode=code,
                indexName=name_info[0],
                category=name_info[1],
                rSquared=round(result.r_squared, 3),
                beta=round(result.beta, 3),
            )
        )

    matches.sort(key=lambda m: m.rSquared, reverse=True)
    return matches[:top_n]


class FactorRegressionService:
    """ファクター回帰分析サービス"""

    def __init__(self, reader: MarketDbReader) -> None:
        self._reader = reader

    def analyze_stock(
        self,
        symbol: str,
        lookback_days: int = 252,
    ) -> FactorRegressionResponse:
        """銘柄のファクター回帰分析を実行"""
        min_data_points = 60
        codes = stock_code_candidates(symbol)
        placeholders = ",".join("?" for _ in codes)

        # 銘柄情報
        stock = self._reader.query_one(
            f"SELECT code, company_name FROM stocks WHERE code IN ({placeholders}) "
            "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
            tuple(codes),
        )
        if stock is None:
            raise ValueError(f"Stock not found: {symbol}")
        db_code = stock["code"]

        company_name = stock["company_name"]

        # 銘柄価格データ
        stock_prices = [
            (row["date"], row["close"])
            for row in self._reader.query(
                "SELECT date, close FROM stock_data WHERE code = ? ORDER BY date", (db_code,)
            )
        ]
        if len(stock_prices) < min_data_points + 1:
            raise ValueError(
                f"Insufficient data: {len(stock_prices)} points (minimum {min_data_points + 1})"
            )

        stock_returns = _calculate_daily_returns(stock_prices)

        # TOPIX データ
        topix_prices = [
            (row["date"], row["close"])
            for row in self._reader.query(
                "SELECT date, close FROM topix_data ORDER BY date"
            )
        ]
        topix_returns = _calculate_daily_returns(topix_prices)

        # アライメント
        dates, aligned_stock, aligned_topix = _align_returns(stock_returns, topix_returns)

        if len(dates) < min_data_points:
            raise ValueError(
                f"Insufficient aligned data: {len(dates)} points (minimum {min_data_points})"
            )

        # lookback 制限
        if len(dates) > lookback_days:
            start = len(dates) - lookback_days
            dates = dates[start:]
            aligned_stock = aligned_stock[start:]
            aligned_topix = aligned_topix[start:]

        # Stage 1: 市場回帰
        market_reg = _ols_regression(aligned_stock, aligned_topix)

        # 全指数リターンを取得
        indices_returns, index_names = self._load_indices_returns()

        # カテゴリ別のコードリスト
        category_codes = self._get_category_codes()

        # Stage 2: 残差ファクターマッチング
        sector17_matches = _find_best_matches(
            market_reg.residuals, dates, indices_returns, category_codes[CATEGORY_SECTOR17], index_names
        )
        sector33_matches = _find_best_matches(
            market_reg.residuals, dates, indices_returns, category_codes[CATEGORY_SECTOR33], index_names
        )

        # TOPIX size + MARKET + STYLE (TOPIX 0000 除外)
        topix_style_codes = [
            c for c in category_codes.get(CATEGORY_TOPIX, []) if c != TOPIX_CODE
        ] + category_codes.get(CATEGORY_MARKET, []) + category_codes.get(CATEGORY_STYLE, [])
        topix_style_matches = _find_best_matches(
            market_reg.residuals, dates, indices_returns, topix_style_codes, index_names
        )

        sorted_dates = sorted(dates)

        return FactorRegressionResponse(
            stockCode=symbol,
            companyName=company_name,
            marketBeta=round(market_reg.beta, 3),
            marketRSquared=round(market_reg.r_squared, 3),
            sector17Matches=sector17_matches,
            sector33Matches=sector33_matches,
            topixStyleMatches=topix_style_matches,
            analysisDate=datetime.now(UTC).strftime("%Y-%m-%d"),
            dataPoints=len(dates),
            dateRange=DateRange(**{"from": sorted_dates[0], "to": sorted_dates[-1]}),
        )

    def _load_indices_returns(self) -> tuple[dict[str, list[DailyReturn]], dict[str, tuple[str, str]]]:
        """全指数のリターンと名前を取得"""
        # 指数マスターから名前とカテゴリを取得
        index_names: dict[str, tuple[str, str]] = {}
        for row in self._reader.query("SELECT code, name, category FROM index_master"):
            index_names[row["code"]] = (row["name"], row["category"])

        # 指数データからリターンを計算
        indices_returns: dict[str, list[DailyReturn]] = {}
        for code in index_names:
            prices = [
                (row["date"], row["close"])
                for row in self._reader.query(
                    "SELECT date, close FROM indices_data WHERE code = ? AND close IS NOT NULL ORDER BY date",
                    (code,),
                )
            ]
            if len(prices) >= 2:
                indices_returns[code] = _calculate_daily_returns(prices)

        return indices_returns, index_names

    def _get_category_codes(self) -> dict[str, list[str]]:
        """カテゴリ別の指数コードマップを取得"""
        categories: dict[str, list[str]] = {}
        for row in self._reader.query("SELECT code, category FROM index_master"):
            cat = row["category"]
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(row["code"])
        return categories
