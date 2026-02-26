"""
Portfolio Factor Regression Service

ポートフォリオ全体のファクター回帰分析。
加重平均ポートフォリオリターンを算出し、市場回帰 → 残差ファクターマッチングを実行。

Hono portfolio-factor-regression EP 互換。
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.portfolio_db import PortfolioDb
from src.domains.analytics.regression_core import (
    DailyReturn,
    align_returns,
    calculate_daily_returns,
    calculate_weighted_portfolio_returns,
    find_best_matches,
    ols_regression,
)
from src.entrypoints.http.schemas.portfolio_factor_regression import (
    DateRange,
    ExcludedStock,
    IndexMatch,
    PortfolioFactorRegressionResponse,
    StockWeight,
)
from src.application.services.factor_regression_service import (
    CATEGORY_MARKET,
    CATEGORY_SECTOR17,
    CATEGORY_SECTOR33,
    CATEGORY_STYLE,
    CATEGORY_TOPIX,
    TOPIX_CODE,
)


class PortfolioFactorRegressionService:
    """ポートフォリオファクター回帰分析"""

    def __init__(self, reader: MarketDbReader, portfolio_db: PortfolioDb) -> None:
        self._reader = reader
        self._pdb = portfolio_db

    def analyze(
        self,
        portfolio_id: int,
        lookback_days: int = 252,
    ) -> PortfolioFactorRegressionResponse:
        """ポートフォリオのファクター回帰分析を実行"""
        min_data_points = 60

        portfolio = self._pdb.get_portfolio(portfolio_id)
        if portfolio is None:
            raise ValueError(f"Portfolio {portfolio_id} not found")

        items = self._pdb.list_items(portfolio_id)
        if not items:
            raise ValueError("No stocks in portfolio")

        # 各銘柄の最新価格を取得して時価ウェイト計算
        weights: list[StockWeight] = []
        excluded: list[ExcludedStock] = []
        total_value = 0.0

        for item in items:
            price = self._reader.get_latest_price(item.code)
            if price is None:
                excluded.append(
                    ExcludedStock(
                        code=item.code,
                        companyName=item.company_name,
                        reason="No price data available",
                    )
                )
                continue
            market_value = price * item.quantity
            total_value += market_value
            weights.append(
                StockWeight(
                    code=item.code,
                    companyName=item.company_name,
                    weight=0.0,  # filled below
                    latestPrice=round(price, 1),
                    marketValue=round(market_value, 1),
                    quantity=item.quantity,
                )
            )

        if total_value <= 0:
            raise ValueError("Zero portfolio value: no valid stocks with price data")

        # Weight 確定
        for w in weights:
            w.weight = round(w.marketValue / total_value, 4)

        # 各銘柄の日次リターンを取得
        stock_returns_map: dict[str, list[DailyReturn]] = {}
        for sw in weights:
            prices = self._reader.get_stock_prices_by_date(sw.code)
            if len(prices) >= 2:
                stock_returns_map[sw.code] = calculate_daily_returns(prices)
            else:
                excluded.append(
                    ExcludedStock(
                        code=sw.code,
                        companyName=sw.companyName,
                        reason="Insufficient price history",
                    )
                )

        included_codes = set(stock_returns_map.keys())
        if not included_codes:
            raise ValueError("No valid stocks with sufficient data for analysis")

        # 加重平均ポートフォリオリターンを計算
        portfolio_returns = calculate_weighted_portfolio_returns(
            stock_returns_map, {w.code: w.weight for w in weights if w.code in included_codes}
        )

        # TOPIX データ
        topix_prices = [
            (r["date"], r["close"])
            for r in self._reader.query(
                "SELECT date, close FROM topix_data ORDER BY date"
            )
        ]
        topix_returns = calculate_daily_returns(topix_prices)

        # アライメント
        dates, aligned_port, aligned_topix = align_returns(portfolio_returns, topix_returns)

        if len(dates) < min_data_points:
            raise ValueError(
                f"Insufficient aligned data: {len(dates)} points (minimum {min_data_points})"
            )

        # lookback 制限
        if len(dates) > lookback_days:
            start = len(dates) - lookback_days
            dates = dates[start:]
            aligned_port = aligned_port[start:]
            aligned_topix = aligned_topix[start:]

        # Stage 1: 市場回帰
        market_reg = ols_regression(aligned_port, aligned_topix)

        # 全指数リターン・カテゴリを一括取得
        indices_returns, index_names = self._load_indices_returns()
        category_codes = self._get_category_codes()

        # Stage 2: 残差ファクターマッチング
        sector17_matches = self._find_matches(
            market_reg.residuals, dates, indices_returns, category_codes.get(CATEGORY_SECTOR17, []), index_names
        )
        sector33_matches = self._find_matches(
            market_reg.residuals, dates, indices_returns, category_codes.get(CATEGORY_SECTOR33, []), index_names
        )
        topix_style_codes = [
            c for c in category_codes.get(CATEGORY_TOPIX, []) if c != TOPIX_CODE
        ] + category_codes.get(CATEGORY_MARKET, []) + category_codes.get(CATEGORY_STYLE, [])
        topix_style_matches = self._find_matches(
            market_reg.residuals, dates, indices_returns, topix_style_codes, index_names
        )

        sorted_dates = sorted(dates)

        return PortfolioFactorRegressionResponse(
            portfolioId=portfolio.id,
            portfolioName=portfolio.name,
            weights=weights,
            totalValue=round(total_value, 1),
            stockCount=len(items),
            includedStockCount=len(included_codes),
            marketBeta=round(market_reg.beta, 3),
            marketRSquared=round(market_reg.r_squared, 3),
            sector17Matches=sector17_matches,
            sector33Matches=sector33_matches,
            topixStyleMatches=topix_style_matches,
            analysisDate=datetime.now(UTC).strftime("%Y-%m-%d"),
            dataPoints=len(dates),
            dateRange=DateRange(**{"from": sorted_dates[0], "to": sorted_dates[-1]}),
            excludedStocks=excluded,
        )

    def _load_indices_returns(
        self,
    ) -> tuple[dict[str, list[DailyReturn]], dict[str, tuple[str, str]]]:
        """全指数のリターンと名前を一括取得（N+1 回避版）"""
        # 指数マスター
        index_names: dict[str, tuple[str, str]] = {}
        for row in self._reader.query("SELECT code, name, category FROM index_master"):
            index_names[row["code"]] = (row["name"], row["category"])

        # 全指数データを一括取得
        all_data = self._reader.query(
            "SELECT code, date, close FROM indices_data WHERE close IS NOT NULL ORDER BY code, date"
        )

        # code ごとにグルーピング
        prices_by_code: dict[str, list[tuple[str, float]]] = {}
        for row in all_data:
            code = row["code"]
            if code not in prices_by_code:
                prices_by_code[code] = []
            prices_by_code[code].append((row["date"], row["close"]))

        indices_returns: dict[str, list[DailyReturn]] = {}
        for code, prices in prices_by_code.items():
            if len(prices) >= 2:
                indices_returns[code] = calculate_daily_returns(prices)

        return indices_returns, index_names

    def _get_category_codes(self) -> dict[str, list[str]]:
        """カテゴリ別の指数コードマップを一括取得"""
        categories: dict[str, list[str]] = {}
        for row in self._reader.query("SELECT code, category FROM index_master"):
            cat = row["category"]
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(row["code"])
        return categories

    def _find_matches(
        self,
        residuals: list[float],
        dates: list[str],
        indices_returns: dict[str, list[DailyReturn]],
        category_codes: list[str],
        index_names: dict[str, tuple[str, str]],
    ) -> list[IndexMatch]:
        """残差ファクターマッチング → IndexMatch（簡易形式）"""
        raw_matches = find_best_matches(
            residuals, dates, indices_returns, category_codes, index_names
        )
        return [
            IndexMatch(
                code=m.code,
                name=m.name,
                rSquared=m.r_squared,
            )
            for m in raw_matches
        ]
