"""
Screening Service

レンジブレイク検出サービス。
Hono ScreeningEngine / MarketScreeningService 互換。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from src.server.db.market_reader import MarketDbReader
from src.server.schemas.screening import (
    MarketScreeningResponse,
    RangeBreakDetails,
    ScreeningDetails,
    ScreeningResultItem,
    ScreeningSummary,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class StockDataPoint:
    """OHLCV データポイント"""

    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class RangeBreakParams:
    """レンジブレイクパラメータ"""

    period: int = 200
    lookback_days: int = 10
    volume_ratio_threshold: float = 1.7
    volume_short_period: int = 30
    volume_long_period: int = 120
    volume_type: str = "ema"  # "sma" or "ema"


# デフォルト設定
DEFAULT_FAST_PARAMS = RangeBreakParams(
    period=200, lookback_days=10,
    volume_ratio_threshold=1.7, volume_short_period=30, volume_long_period=120,
    volume_type="ema",
)

DEFAULT_SLOW_PARAMS = RangeBreakParams(
    period=200, lookback_days=10,
    volume_ratio_threshold=1.7, volume_short_period=50, volume_long_period=150,
    volume_type="sma",
)


# --- Volume utilities ---


def _sma(values: list[float], period: int) -> list[float]:
    """Simple Moving Average"""
    if period <= 0 or len(values) < period:
        return []
    result: list[float] = []
    s = sum(values[:period])
    result.append(s / period)
    for i in range(1, len(values) - period + 1):
        s = s - values[i - 1] + values[i + period - 1]
        result.append(s / period)
    return result


def _ema(values: list[float], period: int) -> list[float]:
    """Exponential Moving Average"""
    if period <= 0 or len(values) < period:
        return []
    multiplier = 2.0 / (period + 1)
    s = sum(values[:period]) / period
    result = [s]
    for i in range(period, len(values)):
        s = (values[i] - s) * multiplier + s
        result.append(s)
    return result


def _get_volume_avg(
    data: list[StockDataPoint], period: int, end_index: int, vol_type: str
) -> float | None:
    """指定インデックスにおけるボリュームMA値を取得"""
    if end_index < period - 1 or end_index >= len(data):
        return None

    volumes = [d.volume for d in data]
    ma = _ema(volumes, period) if vol_type == "ema" else _sma(volumes, period)

    # ma のインデックス = end_index - period + 1
    ma_index = end_index - period + 1
    if ma_index < 0 or ma_index >= len(ma):
        return None
    return ma[ma_index]


def _check_volume_condition(
    data: list[StockDataPoint],
    params: RangeBreakParams,
    index: int,
) -> tuple[bool, float, float, float]:
    """ボリューム条件チェック。(matched, ratio, short_avg, long_avg) を返す"""
    short_avg = _get_volume_avg(data, params.volume_short_period, index, params.volume_type)
    long_avg = _get_volume_avg(data, params.volume_long_period, index, params.volume_type)

    if short_avg is None or long_avg is None or long_avg <= 0:
        return False, 0.0, 0.0, 0.0

    matched = short_avg > long_avg * params.volume_ratio_threshold
    ratio = short_avg / long_avg if long_avg > 0 else 0.0
    return matched, ratio, short_avg, long_avg


# --- Range break detection ---


def _find_max_high(data: list[StockDataPoint], start: int, end: int) -> float:
    """範囲内の最大高値"""
    if start < 0 or end >= len(data) or start > end:
        return 0.0
    return max(data[i].high for i in range(start, end + 1))


def _detect_range_break(
    data: list[StockDataPoint],
    params: RangeBreakParams,
    recent_days: int,
) -> RangeBreakDetails | None:
    """レンジブレイクを検出"""
    if len(data) < params.period + recent_days:
        return None

    end_index = len(data) - 1
    start_index = max(params.period, end_index - recent_days + 1)

    for i in range(end_index, start_index - 1, -1):
        if i < params.period:
            continue

        # Recent max high
        recent_start = i - params.lookback_days + 1
        recent_max = _find_max_high(data, recent_start, i)

        # Period max high (before lookback)
        period_start = i - params.period
        period_end = i - params.lookback_days
        period_max = _find_max_high(data, period_start, period_end)

        if period_max <= 0 or recent_max <= 0:
            continue

        if recent_max >= period_max:
            # ボリューム条件チェック
            matched, ratio, short_avg, long_avg = _check_volume_condition(data, params, i)
            if matched:
                break_pct = ((recent_max - period_max) / period_max) * 100
                return RangeBreakDetails(
                    breakDate=data[i].date,
                    currentHigh=recent_max,
                    maxHighInLookback=period_max,
                    breakPercentage=break_pct,
                    volumeRatio=ratio,
                    avgVolume20Days=short_avg,
                    avgVolume100Days=long_avg,
                )

    return None


class ScreeningService:
    """スクリーニングサービス"""

    def __init__(self, reader: MarketDbReader) -> None:
        self._reader = reader

    def run_screening(
        self,
        markets: str = "prime",
        range_break_fast: bool = True,
        range_break_slow: bool = True,
        recent_days: int = 10,
        reference_date: str | None = None,
        min_break_percentage: float | None = None,
        min_volume_ratio: float | None = None,
        sort_by: str = "date",
        order: str = "desc",
        limit: int | None = None,
    ) -> MarketScreeningResponse:
        """スクリーニングを実行"""
        market_codes = [m.strip() for m in markets.split(",")]

        # 銘柄データをロード
        stocks = self._load_stocks(market_codes)

        results: list[ScreeningResultItem] = []
        skipped = 0
        type_counts: dict[str, int] = {"rangeBreakFast": 0, "rangeBreakSlow": 0}

        for stock_info, data in stocks:
            # reference_date でデータを切り詰め
            if reference_date:
                data = [d for d in data if d.date <= reference_date]

            if len(data) < 220:  # period(200) + some margin
                skipped += 1
                continue

            # Range Break Fast
            if range_break_fast:
                details = _detect_range_break(data, DEFAULT_FAST_PARAMS, recent_days)
                if details:
                    self._maybe_add_result(
                        results, stock_info, "rangeBreakFast", details,
                        min_break_percentage, min_volume_ratio,
                    )
                    type_counts["rangeBreakFast"] += 1

            # Range Break Slow
            if range_break_slow:
                details = _detect_range_break(data, DEFAULT_SLOW_PARAMS, recent_days)
                if details:
                    self._maybe_add_result(
                        results, stock_info, "rangeBreakSlow", details,
                        min_break_percentage, min_volume_ratio,
                    )
                    type_counts["rangeBreakSlow"] += 1

        # ソート
        results = self._sort_results(results, sort_by, order)

        # リミット
        if limit is not None and limit > 0:
            results = results[:limit]

        total_screened = len(stocks)
        match_count = len(results)

        return MarketScreeningResponse(
            results=results,
            summary=ScreeningSummary(
                totalStocksScreened=total_screened,
                matchCount=match_count,
                skippedCount=skipped,
                byScreeningType=type_counts,
            ),
            markets=market_codes,
            recentDays=recent_days,
            referenceDate=reference_date,
            lastUpdated=_now_iso(),
        )

    def _load_stocks(
        self, market_codes: list[str]
    ) -> list[tuple[dict, list[StockDataPoint]]]:
        """銘柄とOHLCVデータをロード"""
        # マーケットフィルタ
        placeholders = ",".join("?" for _ in market_codes)
        stocks_rows = self._reader.query(
            f"""SELECT code, company_name, scale_category, sector_33_name
            FROM stocks WHERE market_code IN ({placeholders})""",
            tuple(market_codes),
        )

        result: list[tuple[dict, list[StockDataPoint]]] = []
        for stock in stocks_rows:
            ohlcv_rows = self._reader.query(
                "SELECT date, open, high, low, close, volume FROM stock_data WHERE code = ? ORDER BY date",
                (stock["code"],),
            )
            data = [
                StockDataPoint(
                    date=r["date"],
                    open=r["open"],
                    high=r["high"],
                    low=r["low"],
                    close=r["close"],
                    volume=r["volume"],
                )
                for r in ohlcv_rows
            ]
            result.append((dict(stock), data))

        return result

    def _maybe_add_result(
        self,
        results: list[ScreeningResultItem],
        stock_info: dict,
        screening_type: str,
        details: RangeBreakDetails,
        min_break_pct: float | None,
        min_vol_ratio: float | None,
    ) -> None:
        """フィルタ適用してresultsに追加"""
        if min_break_pct is not None and details.breakPercentage < min_break_pct:
            return
        if min_vol_ratio is not None and details.volumeRatio < min_vol_ratio:
            return

        results.append(
            ScreeningResultItem(
                stockCode=stock_info["code"][:4],
                companyName=stock_info["company_name"],
                scaleCategory=stock_info.get("scale_category"),
                sector33Name=stock_info.get("sector_33_name"),
                screeningType=screening_type,
                matchedDate=details.breakDate,
                details=ScreeningDetails(rangeBreak=details),
            )
        )

    def _sort_results(
        self, results: list[ScreeningResultItem], sort_by: str, order: str
    ) -> list[ScreeningResultItem]:
        """ソート"""
        reverse = order == "desc"
        if sort_by == "date":
            results.sort(key=lambda r: r.matchedDate, reverse=reverse)
        elif sort_by == "stockCode":
            results.sort(key=lambda r: r.stockCode, reverse=reverse)
        elif sort_by == "volumeRatio":
            results.sort(
                key=lambda r: r.details.rangeBreak.volumeRatio if r.details.rangeBreak else 0,
                reverse=reverse,
            )
        elif sort_by == "breakPercentage":
            results.sort(
                key=lambda r: r.details.rangeBreak.breakPercentage if r.details.rangeBreak else 0,
                reverse=reverse,
            )
        return results
