"""
Portfolio Performance Service

ポートフォリオの P&L 計算、ベンチマーク比較、時系列リターンを算出する。
Hono portfolio/performance EP 互換。
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Row

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.portfolio_db import PortfolioDb
from src.entrypoints.http.schemas.portfolio_performance import (
    BenchmarkResult,
    BenchmarkTimeSeriesPoint,
    DateRange,
    HoldingDetail,
    PerformanceSummary,
    PortfolioPerformanceResponse,
    TimeSeriesPoint,
)
from src.application.services.factor_regression_service import (
    _align_returns,
    _calculate_daily_returns,
    _ols_regression,
)


class PortfolioPerformanceService:
    """ポートフォリオパフォーマンス分析"""

    def __init__(self, reader: MarketDbReader, portfolio_db: PortfolioDb) -> None:
        self._reader = reader
        self._pdb = portfolio_db

    def analyze(
        self,
        portfolio_id: int,
        benchmark_code: str = "0000",
        lookback_days: int = 252,
    ) -> PortfolioPerformanceResponse:
        """ポートフォリオのパフォーマンス分析を実行"""
        portfolio = self._pdb.get_portfolio(portfolio_id)
        if portfolio is None:
            raise ValueError(f"Portfolio {portfolio_id} not found")

        items = self._pdb.list_items(portfolio_id)
        warnings: list[str] = []

        if not items:
            return self._empty_response(portfolio, warnings)

        # Holdings 計算
        holdings: list[HoldingDetail] = []
        total_cost = 0.0
        current_value = 0.0

        for item in items:
            current_price = self._reader.get_latest_price(item.code)
            if current_price is None:
                warnings.append(f"No price data for {item.code}")
                current_price = item.purchase_price  # fallback

            cost = item.purchase_price * item.quantity
            market_value = current_price * item.quantity
            pnl = market_value - cost
            return_rate = pnl / cost if cost > 0 else 0.0

            total_cost += cost
            current_value += market_value

            holdings.append(
                HoldingDetail(
                    code=item.code,
                    companyName=item.company_name,
                    quantity=item.quantity,
                    purchasePrice=item.purchase_price,
                    currentPrice=round(current_price, 1),
                    cost=round(cost, 1),
                    marketValue=round(market_value, 1),
                    pnl=round(pnl, 1),
                    returnRate=round(return_rate, 4),
                    weight=0.0,  # filled below
                    purchaseDate=item.purchase_date,
                    account=item.account,
                )
            )

        # Weight 計算
        if current_value > 0:
            for h in holdings:
                h.weight = round(h.marketValue / current_value, 4)

        total_pnl = current_value - total_cost
        summary = PerformanceSummary(
            totalCost=round(total_cost, 1),
            currentValue=round(current_value, 1),
            totalPnL=round(total_pnl, 1),
            returnRate=round(total_pnl / total_cost, 4) if total_cost > 0 else 0.0,
        )

        # 時系列リターン計算
        time_series, portfolio_daily = self._calculate_portfolio_timeseries(
            items, holdings, lookback_days
        )

        # ベンチマーク分析
        benchmark: BenchmarkResult | None = None
        benchmark_ts: list[BenchmarkTimeSeriesPoint] | None = None
        date_range: DateRange | None = None

        if time_series and len(time_series) >= 30:
            benchmark_result = self._analyze_benchmark(
                portfolio_daily, benchmark_code, lookback_days
            )
            if benchmark_result is not None:
                benchmark, benchmark_ts = benchmark_result

            sorted_dates = [ts.date for ts in time_series]
            date_range = DateRange(**{"from": sorted_dates[0], "to": sorted_dates[-1]})

        return PortfolioPerformanceResponse(
            portfolioId=portfolio.id,
            portfolioName=portfolio.name,
            portfolioDescription=portfolio.description,
            summary=summary,
            holdings=holdings,
            timeSeries=time_series,
            benchmark=benchmark,
            benchmarkTimeSeries=benchmark_ts,
            analysisDate=datetime.now(UTC).strftime("%Y-%m-%d"),
            dateRange=date_range,
            dataPoints=len(time_series),
            warnings=warnings,
        )

    def _empty_response(self, portfolio: object, warnings: list[str]) -> PortfolioPerformanceResponse:
        """空のポートフォリオ用レスポンス"""
        return PortfolioPerformanceResponse(
            portfolioId=portfolio.id,  # type: ignore[union-attr]
            portfolioName=portfolio.name,  # type: ignore[union-attr]
            portfolioDescription=portfolio.description,  # type: ignore[union-attr]
            summary=PerformanceSummary(
                totalCost=0.0, currentValue=0.0, totalPnL=0.0, returnRate=0.0
            ),
            holdings=[],
            timeSeries=[],
            benchmark=None,
            benchmarkTimeSeries=None,
            analysisDate=datetime.now(UTC).strftime("%Y-%m-%d"),
            dateRange=None,
            dataPoints=0,
            warnings=warnings,
        )

    def _calculate_portfolio_timeseries(
        self,
        items: Sequence[Row[Any]],
        holdings: list[HoldingDetail],
        lookback_days: int,
    ) -> tuple[list[TimeSeriesPoint], list[tuple[str, float]]]:
        """ポートフォリオの日次リターン時系列を計算"""
        # 各銘柄の日次価格データを取得
        weight_map = {h.code: h.weight for h in holdings}
        daily_returns_by_code: dict[str, list[tuple[str, float]]] = {}

        for item in items:
            code4 = item.code  # type: ignore[union-attr]
            prices = self._reader.get_stock_prices_by_date(code4)
            if len(prices) >= 2:
                returns = _calculate_daily_returns(prices)
                daily_returns_by_code[code4] = [(dr.date, dr.ret) for dr in returns]

        if not daily_returns_by_code:
            return [], []

        # 全銘柄で共通の日付セットを求める
        all_dates: set[str] = set()
        for rets in daily_returns_by_code.values():
            all_dates.update(d for d, _ in rets)
        sorted_dates = sorted(all_dates)

        # lookback 制限
        if len(sorted_dates) > lookback_days:
            sorted_dates = sorted_dates[-lookback_days:]

        date_set = set(sorted_dates)

        # 各銘柄の return map
        return_maps: dict[str, dict[str, float]] = {}
        for code, rets in daily_returns_by_code.items():
            return_maps[code] = {d: r for d, r in rets if d in date_set}

        # 加重平均ポートフォリオリターン
        portfolio_daily: list[tuple[str, float]] = []
        time_series: list[TimeSeriesPoint] = []
        cumulative = 0.0

        for date in sorted_dates:
            daily_ret = 0.0
            total_weight = 0.0
            for code, w in weight_map.items():
                r = return_maps.get(code, {}).get(date)
                if r is not None:
                    daily_ret += w * r
                    total_weight += w
            if total_weight > 0:
                daily_ret = daily_ret / total_weight * sum(weight_map.values())

            cumulative += daily_ret
            portfolio_daily.append((date, daily_ret))
            time_series.append(
                TimeSeriesPoint(
                    date=date,
                    dailyReturn=round(daily_ret, 6),
                    cumulativeReturn=round(cumulative, 6),
                )
            )

        return time_series, portfolio_daily

    def _analyze_benchmark(
        self,
        portfolio_daily: list[tuple[str, float]],
        benchmark_code: str,
        lookback_days: int,
    ) -> tuple[BenchmarkResult, list[BenchmarkTimeSeriesPoint]] | None:
        """ベンチマーク比較分析"""
        # TOPIX データを取得（benchmark_code == "0000"）
        if benchmark_code == "0000":
            bench_prices = [
                (r["date"], r["close"])
                for r in self._reader.query(
                    "SELECT date, close FROM topix_data ORDER BY date"
                )
            ]
            bench_name = "TOPIX"
        else:
            bench_prices = [
                (r["date"], r["close"])
                for r in self._reader.query(
                    "SELECT date, close FROM indices_data WHERE code = ? AND close IS NOT NULL ORDER BY date",
                    (benchmark_code,),
                )
            ]
            name_row = self._reader.query_one(
                "SELECT name FROM index_master WHERE code = ?", (benchmark_code,)
            )
            bench_name = name_row["name"] if name_row else benchmark_code

        if len(bench_prices) < 2:
            return None

        bench_returns = _calculate_daily_returns(bench_prices)
        port_returns_dr = [
            type("DR", (), {"date": d, "ret": r})()
            for d, r in portfolio_daily
        ]

        dates, aligned_port, aligned_bench = _align_returns(port_returns_dr, bench_returns)  # type: ignore[arg-type]

        if len(dates) < 30:
            return None

        # lookback 制限
        if len(dates) > lookback_days:
            start = len(dates) - lookback_days
            dates = dates[start:]
            aligned_port = aligned_port[start:]
            aligned_bench = aligned_bench[start:]

        # OLS 回帰
        reg = _ols_regression(aligned_port, aligned_bench)

        # Correlation
        n = len(aligned_port)
        mean_p = sum(aligned_port) / n
        mean_b = sum(aligned_bench) / n
        cov = sum((aligned_port[i] - mean_p) * (aligned_bench[i] - mean_b) for i in range(n)) / n
        std_p = math.sqrt(sum((p - mean_p) ** 2 for p in aligned_port) / n)
        std_b = math.sqrt(sum((b - mean_b) ** 2 for b in aligned_bench) / n)
        correlation = cov / (std_p * std_b) if std_p > 0 and std_b > 0 else 0.0

        # 累積リターン
        bench_cum = sum(aligned_bench)
        port_cum = sum(aligned_port)

        benchmark_result = BenchmarkResult(
            code=benchmark_code,
            name=bench_name,
            beta=round(reg.beta, 3),
            alpha=round(reg.alpha, 6),
            correlation=round(correlation, 3),
            rSquared=round(reg.r_squared, 3),
            benchmarkReturn=round(bench_cum, 6),
            relativeReturn=round(port_cum - bench_cum, 6),
        )

        # ベンチマーク時系列
        benchmark_ts: list[BenchmarkTimeSeriesPoint] = []
        port_accum = 0.0
        bench_accum = 0.0
        for i, date in enumerate(dates):
            port_accum += aligned_port[i]
            bench_accum += aligned_bench[i]
            benchmark_ts.append(
                BenchmarkTimeSeriesPoint(
                    date=date,
                    portfolioReturn=round(port_accum, 6),
                    benchmarkReturn=round(bench_accum, 6),
                )
            )

        return benchmark_result, benchmark_ts
