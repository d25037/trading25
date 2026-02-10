"""
Screening helper function tests.
"""

from src.server.schemas.screening import RangeBreakDetails, ScreeningDetails, ScreeningResultItem
from src.server.services.screening_service import (
    RangeBreakParams,
    ScreeningService,
    StockDataPoint,
    _check_volume_condition,
    _detect_range_break,
    _ema,
    _find_max_high,
    _get_volume_avg,
    _sma,
)


class DummyReader:
    def query(self, sql, params=()):  # noqa: ANN001, ANN201
        return []


def make_point(day: int, high: float, volume: float) -> StockDataPoint:
    return StockDataPoint(
        date=f"2026-01-{day:02d}",
        open=high - 1.0,
        high=high,
        low=high - 2.0,
        close=high - 0.5,
        volume=volume,
    )


class TestVolumeHelpers:
    def test_sma_and_ema_return_empty_for_invalid_period(self):
        assert _sma([1, 2, 3], 0) == []
        assert _ema([1, 2, 3], 5) == []

    def test_get_volume_avg_returns_none_for_out_of_range(self):
        data = [make_point(1, 10, 100), make_point(2, 11, 120)]
        assert _get_volume_avg(data, period=3, end_index=1, vol_type="sma") is None

    def test_check_volume_condition_handles_zero_long_average(self):
        params = RangeBreakParams(volume_short_period=1, volume_long_period=1, volume_ratio_threshold=1.1)
        data = [make_point(1, 10, 0), make_point(2, 11, 0)]
        matched, ratio, short_avg, long_avg = _check_volume_condition(data, params, 1)
        assert (matched, ratio, short_avg, long_avg) == (False, 0.0, 0.0, 0.0)


class TestRangeBreakHelpers:
    def test_find_max_high_invalid_range_returns_zero(self):
        data = [make_point(1, 10, 100), make_point(2, 11, 100)]
        assert _find_max_high(data, start=-1, end=1) == 0.0
        assert _find_max_high(data, start=1, end=0) == 0.0

    def test_detect_range_break_returns_none_when_insufficient_data(self):
        params = RangeBreakParams(period=5, lookback_days=2)
        data = [make_point(1, 10, 100), make_point(2, 11, 100), make_point(3, 12, 100)]
        assert _detect_range_break(data, params=params, recent_days=2) is None

    def test_detect_range_break_returns_details_when_conditions_match(self):
        params = RangeBreakParams(
            period=3,
            lookback_days=1,
            volume_ratio_threshold=1.0,
            volume_short_period=1,
            volume_long_period=2,
            volume_type="sma",
        )
        data = [
            make_point(1, 10, 100),
            make_point(2, 10, 100),
            make_point(3, 10, 100),
            make_point(4, 11, 300),
            make_point(5, 12, 400),
        ]
        details = _detect_range_break(data, params=params, recent_days=2)
        assert details is not None
        assert details.breakDate == "2026-01-05"
        assert details.breakPercentage > 0


class TestServiceHelpers:
    def test_maybe_add_result_respects_filters(self):
        service = ScreeningService(DummyReader())
        results: list[ScreeningResultItem] = []
        details = RangeBreakDetails(
            breakDate="2026-01-05",
            currentHigh=12.0,
            maxHighInLookback=11.0,
            breakPercentage=1.0,
            volumeRatio=1.2,
            avgVolume20Days=300.0,
            avgVolume100Days=200.0,
        )

        service._maybe_add_result(  # noqa: SLF001
            results,
            {"code": "10010", "company_name": "A"},
            "rangeBreakFast",
            details,
            min_break_pct=2.0,
            min_vol_ratio=None,
        )
        assert results == []

        service._maybe_add_result(  # noqa: SLF001
            results,
            {"code": "10010", "company_name": "A"},
            "rangeBreakFast",
            details,
            min_break_pct=None,
            min_vol_ratio=1.5,
        )
        assert results == []

        service._maybe_add_result(  # noqa: SLF001
            results,
            {"code": "10010", "company_name": "A"},
            "rangeBreakFast",
            details,
            min_break_pct=0.5,
            min_vol_ratio=1.1,
        )
        assert len(results) == 1

    def test_sort_results_handles_all_sort_types(self):
        service = ScreeningService(DummyReader())
        item_a = ScreeningResultItem(
            stockCode="1001",
            companyName="A",
            screeningType="rangeBreakFast",
            matchedDate="2026-01-04",
            details=ScreeningDetails(
                rangeBreak=RangeBreakDetails(
                    breakDate="2026-01-04",
                    currentHigh=11.0,
                    maxHighInLookback=10.0,
                    breakPercentage=10.0,
                    volumeRatio=1.5,
                    avgVolume20Days=200.0,
                    avgVolume100Days=130.0,
                )
            ),
        )
        item_b = ScreeningResultItem(
            stockCode="1002",
            companyName="B",
            screeningType="rangeBreakSlow",
            matchedDate="2026-01-05",
            details=ScreeningDetails(
                rangeBreak=RangeBreakDetails(
                    breakDate="2026-01-05",
                    currentHigh=12.0,
                    maxHighInLookback=10.0,
                    breakPercentage=20.0,
                    volumeRatio=2.0,
                    avgVolume20Days=300.0,
                    avgVolume100Days=150.0,
                )
            ),
        )
        base = [item_a, item_b]

        assert service._sort_results(base.copy(), "date", "desc")[0].stockCode == "1002"  # noqa: SLF001
        assert service._sort_results(base.copy(), "stockCode", "asc")[0].stockCode == "1001"  # noqa: SLF001
        assert service._sort_results(base.copy(), "volumeRatio", "desc")[0].stockCode == "1002"  # noqa: SLF001
        assert service._sort_results(base.copy(), "breakPercentage", "desc")[0].stockCode == "1002"  # noqa: SLF001
