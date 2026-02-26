"""
Factor Regression Service

2段階ファクター回帰分析サービス。
Stage 1: 市場回帰（銘柄リターン vs TOPIX）
Stage 2: 残差ファクターマッチング（残差 vs 各指数カテゴリ）

Hono performFactorRegression / FactorRegressionService 互換。
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.infrastructure.db.market.query_helpers import stock_code_candidates
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.domains.analytics.regression_core import (
    DailyReturn,
    RegressionMatch,
    align_returns,
    calculate_daily_returns,
    find_best_matches,
    ols_regression,
)
from src.entrypoints.http.schemas.factor_regression import (
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

        stock_returns = calculate_daily_returns(stock_prices)

        # TOPIX データ
        topix_prices = [
            (row["date"], row["close"])
            for row in self._reader.query(
                "SELECT date, close FROM topix_data ORDER BY date"
            )
        ]
        topix_returns = calculate_daily_returns(topix_prices)

        # アライメント
        dates, aligned_stock, aligned_topix = align_returns(stock_returns, topix_returns)

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
        market_reg = ols_regression(aligned_stock, aligned_topix)

        # 全指数リターンを取得
        indices_returns, index_names = self._load_indices_returns()

        # カテゴリ別のコードリスト
        category_codes = self._get_category_codes()

        # Stage 2: 残差ファクターマッチング
        sector17_matches = self._to_index_matches(
            find_best_matches(
            market_reg.residuals, dates, indices_returns, category_codes[CATEGORY_SECTOR17], index_names
            )
        )
        sector33_matches = self._to_index_matches(
            find_best_matches(
            market_reg.residuals, dates, indices_returns, category_codes[CATEGORY_SECTOR33], index_names
            )
        )

        # TOPIX size + MARKET + STYLE (TOPIX 0000 除外)
        topix_style_codes = [
            c for c in category_codes.get(CATEGORY_TOPIX, []) if c != TOPIX_CODE
        ] + category_codes.get(CATEGORY_MARKET, []) + category_codes.get(CATEGORY_STYLE, [])
        topix_style_matches = self._to_index_matches(
            find_best_matches(
            market_reg.residuals, dates, indices_returns, topix_style_codes, index_names
            )
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
                indices_returns[code] = calculate_daily_returns(prices)

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

    def _to_index_matches(self, matches: list[RegressionMatch]) -> list[IndexMatch]:
        return [
            IndexMatch(
                indexCode=match.code,
                indexName=match.name,
                category=match.category,
                rSquared=match.r_squared,
                beta=match.beta,
            )
            for match in matches
        ]
