"""
Indicator Service ユニットテスト

relative OHLC計算とmargin_volume_ratio計算のテスト。
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.server.services.indicator_service import (
    IndicatorService,
    calculate_relative_ohlcv,
    compute_margin_volume_ratio,
)


# ===== Fixtures =====


def _make_ohlcv(
    dates: list[str],
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float] | None = None,
) -> pd.DataFrame:
    """テスト用OHLCVデータ作成"""
    idx = pd.DatetimeIndex(dates)
    data = {"Open": opens, "High": highs, "Low": lows, "Close": closes}
    if volumes is not None:
        data["Volume"] = volumes
    else:
        data["Volume"] = [1000.0] * len(dates)
    return pd.DataFrame(data, index=idx)


def _make_benchmark(
    dates: list[str],
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
) -> pd.DataFrame:
    """テスト用ベンチマークOHLCデータ作成（Volumeなし）"""
    return _make_ohlcv(dates, opens, highs, lows, closes).drop(columns=["Volume"])


# ===== calculate_relative_ohlcv Tests =====


class TestCalculateRelativeOHLCV:
    """相対OHLCV計算テスト"""

    def test_basic_division(self) -> None:
        """基本的な除算が正しく行われる"""
        dates = ["2024-01-01", "2024-01-02", "2024-01-03"]
        stock = _make_ohlcv(
            dates,
            opens=[100, 200, 300],
            highs=[110, 210, 310],
            lows=[90, 190, 290],
            closes=[105, 205, 305],
            volumes=[1000, 2000, 3000],
        )
        bench = _make_benchmark(
            dates,
            opens=[50, 100, 150],
            highs=[55, 105, 155],
            lows=[45, 95, 145],
            closes=[52, 102, 152],
        )

        result = calculate_relative_ohlcv(stock, bench)

        assert len(result) == 3
        np.testing.assert_almost_equal(result.iloc[0]["Open"], 100 / 50)
        np.testing.assert_almost_equal(result.iloc[0]["High"], 110 / 55)
        np.testing.assert_almost_equal(result.iloc[0]["Low"], 90 / 45)
        np.testing.assert_almost_equal(result.iloc[0]["Close"], 105 / 52)
        # Volumeはそのまま
        assert result.iloc[0]["Volume"] == 1000

    def test_date_alignment(self) -> None:
        """日付が異なるデータが正しくalignされる"""
        stock = _make_ohlcv(
            ["2024-01-01", "2024-01-02", "2024-01-03"],
            opens=[100, 200, 300],
            highs=[110, 210, 310],
            lows=[90, 190, 290],
            closes=[105, 205, 305],
        )
        # ベンチマークは01-02のみ欠損
        bench = _make_benchmark(
            ["2024-01-01", "2024-01-03"],
            opens=[50, 150],
            highs=[55, 155],
            lows=[45, 145],
            closes=[52, 152],
        )

        result = calculate_relative_ohlcv(stock, bench)

        # 共通日付の2日分のみ
        assert len(result) == 2
        assert result.index[0] == pd.Timestamp("2024-01-01")
        assert result.index[1] == pd.Timestamp("2024-01-03")

    def test_zero_division_skip(self) -> None:
        """ベンチマークにゼロがある場合、skipモードでその日を除外"""
        dates = ["2024-01-01", "2024-01-02"]
        stock = _make_ohlcv(
            dates,
            opens=[100, 200],
            highs=[110, 210],
            lows=[90, 190],
            closes=[105, 205],
        )
        bench = _make_benchmark(
            dates,
            opens=[50, 0],  # 2日目はゼロ
            highs=[55, 0],
            lows=[45, 0],
            closes=[52, 0],
        )

        result = calculate_relative_ohlcv(stock, bench, handle_zero_division="skip")
        assert len(result) == 1

    def test_zero_division_zero(self) -> None:
        """ベンチマークにゼロがある場合、zeroモードで0を返す"""
        dates = ["2024-01-01", "2024-01-02"]
        stock = _make_ohlcv(
            dates,
            opens=[100, 200],
            highs=[110, 210],
            lows=[90, 190],
            closes=[105, 205],
        )
        bench = _make_benchmark(
            dates,
            opens=[50, 0],
            highs=[55, 0],
            lows=[45, 0],
            closes=[52, 0],
        )

        result = calculate_relative_ohlcv(stock, bench, handle_zero_division="zero")
        assert len(result) == 2
        assert result.iloc[1]["Open"] == 0.0
        assert result.iloc[1]["Close"] == 0.0

    def test_no_common_dates_raises(self) -> None:
        """共通日付がない場合はValueError"""
        stock = _make_ohlcv(
            ["2024-01-01"],
            opens=[100], highs=[110], lows=[90], closes=[105],
        )
        bench = _make_benchmark(
            ["2024-02-01"],
            opens=[50], highs=[55], lows=[45], closes=[52],
        )

        with pytest.raises(ValueError, match="共通する日付がありません"):
            calculate_relative_ohlcv(stock, bench)

    def test_zero_division_null(self) -> None:
        """ベンチマークにゼロがある場合、nullモードでNaNを返す"""
        dates = ["2024-01-01", "2024-01-02"]
        stock = _make_ohlcv(
            dates,
            opens=[100, 200],
            highs=[110, 210],
            lows=[90, 190],
            closes=[105, 205],
        )
        bench = _make_benchmark(
            dates,
            opens=[50, 0],  # 2日目はゼロ
            highs=[55, 0],
            lows=[45, 0],
            closes=[52, 0],
        )

        result = calculate_relative_ohlcv(stock, bench, handle_zero_division="null")
        assert len(result) == 2
        # 2日目はNaN
        assert np.isnan(result.iloc[1]["Open"])
        assert np.isnan(result.iloc[1]["Close"])
        # 1日目は正常値
        np.testing.assert_almost_equal(result.iloc[0]["Open"], 100 / 50)

    def test_all_zero_benchmark_skip_raises(self) -> None:
        """ベンチマークが全てゼロの場合、skipモードでValueError"""
        dates = ["2024-01-01", "2024-01-02"]
        stock = _make_ohlcv(
            dates,
            opens=[100, 200],
            highs=[110, 210],
            lows=[90, 190],
            closes=[105, 205],
        )
        bench = _make_benchmark(
            dates,
            opens=[0, 0],
            highs=[0, 0],
            lows=[0, 0],
            closes=[0, 0],
        )

        with pytest.raises(ValueError, match="相対計算可能なデータがありません"):
            calculate_relative_ohlcv(stock, bench, handle_zero_division="skip")


# ===== compute_margin_volume_ratio Tests =====


class TestComputeMarginVolumeRatio:
    """信用残高/出来高比率テスト"""

    def test_basic_calculation(self) -> None:
        """基本的な比率計算が正しい"""
        # 1週間分の日次出来高
        vol_dates = pd.date_range("2024-01-08", periods=5, freq="B")
        volume = pd.Series(
            [1000.0, 1200.0, 800.0, 1100.0, 900.0],
            index=vol_dates,
        )

        # 信用残高データ（同じ週の金曜日）
        margin_dates = pd.DatetimeIndex(["2024-01-12"])
        margin_df = pd.DataFrame(
            {"longMarginVolume": [5000], "shortMarginVolume": [2000]},
            index=margin_dates,
        )

        records = compute_margin_volume_ratio(margin_df, volume)

        assert len(records) == 1
        record = records[0]
        assert "longRatio" in record
        assert "shortRatio" in record
        assert record["longVol"] == 5000
        assert record["shortVol"] == 2000

    def test_empty_data(self) -> None:
        """空データの場合は空リスト"""
        margin_df = pd.DataFrame(
            columns=["longMarginVolume", "shortMarginVolume"],
        )
        margin_df.index = pd.DatetimeIndex([])
        volume = pd.Series([], dtype=float, index=pd.DatetimeIndex([]))

        records = compute_margin_volume_ratio(margin_df, volume)
        assert records == []

    def test_zero_weekly_avg_volume_skipped(self) -> None:
        """週間平均出来高がゼロの場合、その週の信用データはスキップされる"""
        # 全て出来高ゼロの週
        vol_dates = pd.date_range("2024-01-08", periods=5, freq="B")
        volume = pd.Series(
            [0.0, 0.0, 0.0, 0.0, 0.0],
            index=vol_dates,
        )

        margin_dates = pd.DatetimeIndex(["2024-01-12"])
        margin_df = pd.DataFrame(
            {"longMarginVolume": [5000], "shortMarginVolume": [2000]},
            index=margin_dates,
        )

        records = compute_margin_volume_ratio(margin_df, volume)
        # ゼロ出来高の週はスキップされる（positive_vol.empty → []）
        assert records == []

    def test_nan_margin_volumes_skipped(self) -> None:
        """信用残高がNaNの場合、そのレコードはスキップされる"""
        vol_dates = pd.date_range("2024-01-08", periods=5, freq="B")
        volume = pd.Series(
            [1000.0, 1200.0, 800.0, 1100.0, 900.0],
            index=vol_dates,
        )

        margin_dates = pd.DatetimeIndex(["2024-01-12", "2024-01-19"])
        margin_df = pd.DataFrame(
            {
                "longMarginVolume": [np.nan, 5000],  # 最初はNaN
                "shortMarginVolume": [2000, np.nan],  # 2つ目はshortがNaN
            },
            index=margin_dates,
        )

        records = compute_margin_volume_ratio(margin_df, volume)
        # NaNを含む行はスキップされる
        assert len(records) == 0

    def test_multiple_weeks(self) -> None:
        """複数週にまたがるデータの正しい処理"""
        # 2週間分のデータ
        vol_dates = pd.date_range("2024-01-08", periods=10, freq="B")
        volume = pd.Series(
            [1000.0] * 10,  # 平均出来高は1000
            index=vol_dates,
        )

        # 各週の金曜日に信用データ
        margin_dates = pd.DatetimeIndex(["2024-01-12", "2024-01-19"])
        margin_df = pd.DataFrame(
            {
                "longMarginVolume": [5000, 10000],
                "shortMarginVolume": [2000, 4000],
            },
            index=margin_dates,
        )

        records = compute_margin_volume_ratio(margin_df, volume)
        assert len(records) == 2
        # 週ごとの平均出来高で計算される
        assert records[0]["longRatio"] == 5.0  # 5000 / 1000
        assert records[0]["shortRatio"] == 2.0  # 2000 / 1000
        assert records[1]["longRatio"] == 10.0  # 10000 / 1000
        assert records[1]["shortRatio"] == 4.0  # 4000 / 1000


# ===== IndicatorService Integration Tests =====


class TestIndicatorServiceRelativeMode:
    """IndicatorService relative mode統合テスト"""

    def test_compute_indicators_with_benchmark(self) -> None:
        """benchmark_code指定時にrelative OHLCで計算される"""
        service = IndicatorService()

        # load_ohlcvとload_benchmark_ohlcvをモック
        dates = pd.date_range("2024-01-01", periods=50, freq="B")
        stock_df = pd.DataFrame(
            {
                "Open": np.random.uniform(100, 200, 50),
                "High": np.random.uniform(200, 300, 50),
                "Low": np.random.uniform(50, 100, 50),
                "Close": np.random.uniform(100, 200, 50),
                "Volume": np.random.uniform(1000, 5000, 50),
            },
            index=dates,
        )
        bench_df = pd.DataFrame(
            {
                "Open": np.random.uniform(1000, 2000, 50),
                "High": np.random.uniform(2000, 3000, 50),
                "Low": np.random.uniform(500, 1000, 50),
                "Close": np.random.uniform(1000, 2000, 50),
            },
            index=dates,
        )

        with (
            patch.object(service, "load_ohlcv", return_value=stock_df),
            patch.object(service, "load_benchmark_ohlcv", return_value=bench_df),
        ):
            result = service.compute_indicators(
                stock_code="7203",
                source="market",
                timeframe="daily",
                indicators=[{"type": "sma", "params": {"period": 5}}],
                benchmark_code="topix",
                relative_options={"handle_zero_division": "skip"},
            )

        assert result["stock_code"] == "7203"
        assert "sma_5" in result["indicators"]
        # SMA値は相対値なので1.0前後になるはず（stock/benchmark）
        sma_records = result["indicators"]["sma_5"]
        assert len(sma_records) > 0
        for r in sma_records:
            if r["value"] is not None:
                assert 0 < r["value"] < 10  # 相対値として妥当な範囲

    def test_compute_indicators_without_benchmark(self) -> None:
        """benchmark_code未指定時は通常計算"""
        service = IndicatorService()

        dates = pd.date_range("2024-01-01", periods=30, freq="B")
        stock_df = pd.DataFrame(
            {
                "Open": np.linspace(100, 130, 30),
                "High": np.linspace(110, 140, 30),
                "Low": np.linspace(90, 120, 30),
                "Close": np.linspace(105, 135, 30),
                "Volume": [1000.0] * 30,
            },
            index=dates,
        )

        with patch.object(service, "load_ohlcv", return_value=stock_df):
            result = service.compute_indicators(
                stock_code="7203",
                source="market",
                timeframe="daily",
                indicators=[{"type": "sma", "params": {"period": 5}}],
            )

        assert "sma_5" in result["indicators"]
        sma_records = result["indicators"]["sma_5"]
        # 通常モードなので値は100台
        for r in sma_records:
            if r["value"] is not None:
                assert r["value"] > 50
