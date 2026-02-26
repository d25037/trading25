"""
Indicator Service ユニットテスト
"""

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.application.services.indicator_service import IndicatorService
from src.domains.analytics.margin_metrics import (
    compute_margin_flow_pressure,
    compute_margin_long_pressure,
    compute_margin_turnover_days,
    compute_margin_volume_ratio,
)
from src.domains.strategy.indicators.indicator_registry import (
    INDICATOR_REGISTRY,
    _make_key,
    _multi_series_to_records,
    _series_to_records,
)
from src.domains.strategy.indicators.relative_ohlcv import (
    _compute_relative_ohlc_column,
    calculate_relative_ohlcv,
)


# ===== テストデータ =====


def _make_ohlcv(n: int = 200) -> pd.DataFrame:
    """テスト用OHLCVデータ"""
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=n)
    base = np.linspace(100, 120, n) + np.random.randn(n) * 2
    return pd.DataFrame(
        {
            "Open": base - 0.5,
            "High": base + np.abs(np.random.randn(n)) * 2,
            "Low": base - np.abs(np.random.randn(n)) * 2,
            "Close": base,
            "Volume": np.random.randint(10000, 100000, n).astype(float),
        },
        index=dates,
    )


# ===== Helper Tests =====


class TestSeriesToRecords:
    """_series_to_records() テスト"""

    def test_basic(self):
        s = pd.Series(
            [1.0, 2.0, 3.0],
            index=pd.date_range("2024-01-01", periods=3),
        )
        records = _series_to_records(s, "include")
        assert len(records) == 3
        assert records[0]["date"] == "2024-01-01"
        assert records[0]["value"] == 1.0

    def test_nan_include(self):
        s = pd.Series(
            [1.0, np.nan, 3.0],
            index=pd.date_range("2024-01-01", periods=3),
        )
        records = _series_to_records(s, "include")
        assert len(records) == 3
        assert records[1]["value"] is None

    def test_nan_omit(self):
        s = pd.Series(
            [1.0, np.nan, 3.0],
            index=pd.date_range("2024-01-01", periods=3),
        )
        records = _series_to_records(s, "omit")
        assert len(records) == 2

    def test_inf_replaced_with_null(self):
        s = pd.Series(
            [1.0, np.inf, -np.inf],
            index=pd.date_range("2024-01-01", periods=3),
        )
        records = _series_to_records(s, "include")
        assert records[1]["value"] is None
        assert records[2]["value"] is None

    def test_rounding(self):
        s = pd.Series(
            [1.23456789],
            index=pd.date_range("2024-01-01", periods=1),
        )
        records = _series_to_records(s, "include")
        assert records[0]["value"] == 1.2346

    def test_custom_value_name(self):
        s = pd.Series([1.0], index=pd.date_range("2024-01-01", periods=1))
        records = _series_to_records(s, "include", value_name="rsi")
        assert "rsi" in records[0]


class TestMultiSeriesToRecords:
    """_multi_series_to_records() テスト"""

    def test_basic(self):
        idx = pd.date_range("2024-01-01", periods=3)
        records = _multi_series_to_records(
            {"a": pd.Series([1.0, 2.0, 3.0], index=idx),
             "b": pd.Series([4.0, 5.0, 6.0], index=idx)},
            "include",
        )
        assert len(records) == 3
        assert records[0]["a"] == 1.0
        assert records[0]["b"] == 4.0

    def test_all_null_omit(self):
        idx = pd.date_range("2024-01-01", periods=3)
        records = _multi_series_to_records(
            {"a": pd.Series([np.nan, 1.0, np.nan], index=idx),
             "b": pd.Series([np.nan, 2.0, np.nan], index=idx)},
            "omit",
        )
        assert len(records) == 1


class TestMakeKey:
    """_make_key() テスト"""

    def test_sma(self):
        assert _make_key("sma", period=20) == "sma_20"

    def test_macd(self):
        assert _make_key("macd", fast=12, slow=26, signal=9) == "macd_12_26_9"

    def test_bollinger(self):
        assert _make_key("bollinger", period=20, std=2.0) == "bollinger_20_2.0"


# ===== 12 Indicator Compute Function Tests =====


class TestIndicatorRegistry:
    """全12インジケーターのレジストリテスト"""

    def test_registry_has_12_entries(self):
        assert len(INDICATOR_REGISTRY) == 12

    def test_all_types_registered(self):
        expected = {
            "sma", "ema", "rsi", "macd", "ppo", "bollinger",
            "atr", "atr_support", "nbar_support", "volume_comparison",
            "trading_value_ma", "risk_adjusted_return",
        }
        assert set(INDICATOR_REGISTRY.keys()) == expected


class TestComputeSMA:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["sma"](ohlcv, {"period": 20}, "include")
        assert key == "sma_20"
        assert len(records) == 200
        assert "date" in records[0]
        assert "value" in records[0]


class TestComputeEMA:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["ema"](ohlcv, {"period": 12}, "include")
        assert key == "ema_12"
        assert len(records) == 200


class TestComputeRSI:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["rsi"](ohlcv, {"period": 14}, "include")
        assert key == "rsi_14"
        assert len(records) == 200

    def test_values_in_range(self):
        ohlcv = _make_ohlcv()
        _, records = INDICATOR_REGISTRY["rsi"](ohlcv, {"period": 14}, "omit")
        for r in records:
            if r["value"] is not None:
                assert 0 <= r["value"] <= 100


class TestComputeMACD:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["macd"](ohlcv, {}, "include")
        assert key == "macd_12_26_9"
        assert "macd" in records[-1]
        assert "signal" in records[-1]
        assert "histogram" in records[-1]


class TestComputePPO:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["ppo"](ohlcv, {}, "include")
        assert key == "ppo_12_26_9"
        assert "ppo" in records[-1]
        assert "signal" in records[-1]
        assert "histogram" in records[-1]


class TestComputeBollinger:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["bollinger"](ohlcv, {}, "include")
        assert key == "bollinger_20_2.0"
        assert "upper" in records[-1]
        assert "middle" in records[-1]
        assert "lower" in records[-1]

    def test_band_order(self):
        ohlcv = _make_ohlcv()
        _, records = INDICATOR_REGISTRY["bollinger"](ohlcv, {}, "omit")
        for r in records:
            if r["upper"] is not None and r["lower"] is not None:
                assert r["upper"] >= r["lower"]


class TestComputeATR:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["atr"](ohlcv, {"period": 14}, "include")
        assert key == "atr_14"

    def test_positive_values(self):
        ohlcv = _make_ohlcv()
        _, records = INDICATOR_REGISTRY["atr"](ohlcv, {"period": 14}, "omit")
        for r in records:
            if r["value"] is not None:
                assert r["value"] >= 0


class TestComputeATRSupport:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["atr_support"](ohlcv, {}, "include")
        assert key == "atr_support_20_2.0"
        assert len(records) == 200


class TestComputeNBarSupport:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["nbar_support"](ohlcv, {"period": 20}, "include")
        assert key == "nbar_support_20"


class TestComputeVolumeComparison:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["volume_comparison"](ohlcv, {}, "include")
        assert "volume_comparison" in key
        assert "shortMA" in records[-1]
        assert "longThresholdLower" in records[-1]
        assert "longThresholdHigher" in records[-1]

    def test_key_uses_lower_higher(self):
        ohlcv = _make_ohlcv()
        key, _ = INDICATOR_REGISTRY["volume_comparison"](
            ohlcv, {"lower_multiplier": 0.8, "higher_multiplier": 2.0}, "include"
        )
        assert "0.8" in key
        assert "2.0" in key

    def test_lower_higher_multiplier_values(self):
        ohlcv = _make_ohlcv()
        _, records_default = INDICATOR_REGISTRY["volume_comparison"](ohlcv, {}, "omit")
        _, records_custom = INDICATOR_REGISTRY["volume_comparison"](
            ohlcv,
            {"lower_multiplier": 0.5, "higher_multiplier": 3.0},
            "omit",
        )
        # カスタム倍率ではlowerが小さく、higherが大きくなる
        if records_default and records_custom:
            assert records_custom[-1]["longThresholdLower"] < records_default[-1]["longThresholdLower"]
            assert records_custom[-1]["longThresholdHigher"] > records_default[-1]["longThresholdHigher"]


class TestComputeTradingValueMA:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["trading_value_ma"](ohlcv, {"period": 20}, "include")
        assert key == "trading_value_ma_20"


class TestComputeRiskAdjustedReturn:
    def test_basic(self):
        ohlcv = _make_ohlcv()
        key, records = INDICATOR_REGISTRY["risk_adjusted_return"](
            ohlcv,
            {"lookback_period": 60, "ratio_type": "sortino"},
            "include",
        )
        assert key == "risk_adjusted_return_60_sortino"
        assert len(records) == 200
        assert "value" in records[-1]


# ===== Margin Indicators =====


class TestMarginIndicators:
    """信用指標テスト"""

    def setup_method(self):
        dates = pd.date_range("2024-01-01", periods=50)
        self.margin_df = pd.DataFrame(
            {
                "longMarginVolume": np.random.randint(100000, 500000, 50).astype(float),
                "shortMarginVolume": np.random.randint(10000, 100000, 50).astype(float),
            },
            index=dates,
        )
        self.volume = pd.Series(
            np.random.randint(50000, 200000, 50).astype(float),
            index=dates,
        )

    def test_long_pressure(self):
        records = compute_margin_long_pressure(self.margin_df, self.volume, 15)
        assert len(records) > 0
        assert "pressure" in records[0]
        assert "longVol" in records[0]
        assert "shortVol" in records[0]
        assert "avgVolume" in records[0]

    def test_flow_pressure(self):
        records = compute_margin_flow_pressure(self.margin_df, self.volume, 15)
        assert len(records) > 0
        assert "flowPressure" in records[0]

    def test_turnover_days(self):
        records = compute_margin_turnover_days(self.margin_df, self.volume, 15)
        assert len(records) > 0
        assert "turnoverDays" in records[0]

    def test_zero_volume_skipped(self):
        self.volume.iloc[:] = 0.0
        records = compute_margin_long_pressure(self.margin_df, self.volume, 15)
        assert len(records) == 0


# ===== IndicatorService =====


class TestIndicatorServiceResample:
    """resample_timeframe テスト"""

    def test_daily_no_change(self):
        df = _make_ohlcv(100)
        result = IndicatorService.resample_timeframe(df, "daily")
        assert len(result) == 100

    def test_weekly_resample(self):
        df = _make_ohlcv(100)
        result = IndicatorService.resample_timeframe(df, "weekly")
        assert len(result) < 100

    def test_monthly_resample(self):
        df = _make_ohlcv(365)
        result = IndicatorService.resample_timeframe(df, "monthly")
        assert len(result) <= 13


class TestIndicatorServiceLoadOHLCV:
    """load_ohlcv テスト"""

    @patch("src.infrastructure.external_api.dataset.DatasetAPIClient")
    def test_load_dataset_source(self, MockClient):
        service = IndicatorService()
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = _make_ohlcv(50)
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client

        df = service.load_ohlcv("7203", "topix500")
        assert len(df) == 50
        mock_client.get_stock_ohlcv.assert_called_once_with("7203", None, None)

    def test_load_market_source(self):
        service = IndicatorService()
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = _make_ohlcv(50)
        service._market_client = mock_client

        df = service.load_ohlcv("7203", "market")
        assert len(df) == 50
        mock_client.get_stock_ohlcv.assert_called_once()

    @patch("src.infrastructure.external_api.market_client.MarketAPIClient")
    def test_load_market_source_prefers_market_reader(self, MockMarketClient, market_db_path):
        reader = MarketDbReader(market_db_path)
        try:
            service = IndicatorService(market_reader=reader)
            df = service.load_ohlcv("7203", "market")
            assert len(df) == 3
            assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]
            MockMarketClient.assert_not_called()
        finally:
            reader.close()

    @patch("src.infrastructure.external_api.dataset.DatasetAPIClient")
    def test_load_with_dates(self, MockClient):
        service = IndicatorService()
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = _make_ohlcv(50)
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client

        service.load_ohlcv("7203", "topix500", date(2024, 1, 1), date(2024, 6, 30))
        mock_client.get_stock_ohlcv.assert_called_once_with("7203", "2024-01-01", "2024-06-30")

    @patch("src.infrastructure.external_api.dataset.DatasetAPIClient")
    def test_load_empty_raises(self, MockClient):
        service = IndicatorService()
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = pd.DataFrame()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client

        with pytest.raises(ValueError, match="取得できません"):
            service.load_ohlcv("9999", "topix500")


class TestIndicatorServiceComputeIndicators:
    """compute_indicators テスト"""

    @patch("src.infrastructure.external_api.dataset.DatasetAPIClient")
    def test_compute_single(self, MockClient):
        service = IndicatorService()
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = _make_ohlcv(100)
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client

        result = service.compute_indicators(
            "7203", "topix500", "daily",
            [{"type": "sma", "params": {"period": 20}}],
        )
        assert result["stock_code"] == "7203"
        assert result["timeframe"] == "daily"
        assert "sma_20" in result["indicators"]
        assert result["meta"]["bars"] == 100

    @patch("src.infrastructure.external_api.dataset.DatasetAPIClient")
    def test_compute_multiple(self, MockClient):
        service = IndicatorService()
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = _make_ohlcv(100)
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client

        result = service.compute_indicators(
            "7203", "topix500", "daily",
            [
                {"type": "sma", "params": {"period": 20}},
                {"type": "rsi", "params": {"period": 14}},
                {"type": "macd", "params": {}},
            ],
        )
        assert len(result["indicators"]) == 3

    @patch("src.infrastructure.external_api.dataset.DatasetAPIClient")
    def test_compute_unknown_type_skipped(self, MockClient):
        service = IndicatorService()
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = _make_ohlcv(100)
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client

        result = service.compute_indicators(
            "7203", "topix500", "daily",
            [{"type": "unknown_indicator", "params": {}}],
        )
        assert len(result["indicators"]) == 0

    @patch("src.infrastructure.external_api.dataset.DatasetAPIClient")
    def test_compute_with_weekly_resample(self, MockClient):
        service = IndicatorService()
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = _make_ohlcv(100)
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client

        result = service.compute_indicators(
            "7203", "topix500", "weekly",
            [{"type": "sma", "params": {"period": 5}}],
        )
        assert result["timeframe"] == "weekly"
        assert result["meta"]["bars"] < 100

    @patch("src.infrastructure.external_api.dataset.DatasetAPIClient")
    def test_compute_with_nan_omit(self, MockClient):
        service = IndicatorService()
        mock_client = MagicMock()
        mock_client.get_stock_ohlcv.return_value = _make_ohlcv(100)
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client

        result = service.compute_indicators(
            "7203", "topix500", "daily",
            [{"type": "sma", "params": {"period": 20}}],
            nan_handling="omit",
        )
        records = result["indicators"]["sma_20"]
        # omitモードではNaN行がスキップされる → recordsが100より少ない
        assert len(records) < 100


class TestIndicatorServiceComputeMarginIndicators:
    """compute_margin_indicators テスト"""

    def _make_margin_df(self, n: int = 50) -> pd.DataFrame:
        dates = pd.date_range("2024-01-01", periods=n)
        return pd.DataFrame({
            "longMarginVolume": np.random.randint(100000, 500000, n).astype(float),
            "shortMarginVolume": np.random.randint(10000, 100000, n).astype(float),
        }, index=dates)

    @patch("src.infrastructure.external_api.market_client.MarketAPIClient")
    @patch("src.infrastructure.external_api.jquants_client.JQuantsAPIClient")
    def test_compute_single_margin(self, MockJQuantsClient, MockMarketClient):
        service = IndicatorService()

        # JQuantsAPIClient mock
        mock_jquants = MagicMock()
        mock_jquants.get_margin_interest.return_value = self._make_margin_df()
        mock_jquants.__enter__ = MagicMock(return_value=mock_jquants)
        mock_jquants.__exit__ = MagicMock(return_value=False)
        MockJQuantsClient.return_value = mock_jquants

        # MarketAPIClient mock
        mock_market = MagicMock()
        mock_market.get_stock_ohlcv.return_value = _make_ohlcv(50)
        mock_market.__enter__ = MagicMock(return_value=mock_market)
        mock_market.__exit__ = MagicMock(return_value=False)
        MockMarketClient.return_value = mock_market

        result = service.compute_margin_indicators(
            "7203", "topix500", ["margin_long_pressure"],
        )
        assert result["stock_code"] == "7203"
        assert "margin_long_pressure" in result["indicators"]

    @patch("src.infrastructure.external_api.market_client.MarketAPIClient")
    @patch("src.infrastructure.external_api.jquants_client.JQuantsAPIClient")
    def test_compute_all_margin(self, MockJQuantsClient, MockMarketClient):
        service = IndicatorService()

        mock_jquants = MagicMock()
        mock_jquants.get_margin_interest.return_value = self._make_margin_df()
        mock_jquants.__enter__ = MagicMock(return_value=mock_jquants)
        mock_jquants.__exit__ = MagicMock(return_value=False)
        MockJQuantsClient.return_value = mock_jquants

        mock_market = MagicMock()
        mock_market.get_stock_ohlcv.return_value = _make_ohlcv(50)
        mock_market.__enter__ = MagicMock(return_value=mock_market)
        mock_market.__exit__ = MagicMock(return_value=False)
        MockMarketClient.return_value = mock_market

        result = service.compute_margin_indicators(
            "7203", "topix500",
            ["margin_long_pressure", "margin_flow_pressure", "margin_turnover_days"],
        )
        assert len(result["indicators"]) == 3

    @patch("src.infrastructure.external_api.jquants_client.JQuantsAPIClient")
    def test_margin_empty_data_raises(self, MockJQuantsClient):
        service = IndicatorService()
        mock_jquants = MagicMock()
        mock_jquants.get_margin_interest.return_value = pd.DataFrame()
        mock_jquants.__enter__ = MagicMock(return_value=mock_jquants)
        mock_jquants.__exit__ = MagicMock(return_value=False)
        MockJQuantsClient.return_value = mock_jquants

        with pytest.raises(ValueError, match="信用データが取得できません"):
            service.compute_margin_indicators("9999", "topix500", ["margin_long_pressure"])

    @patch("src.infrastructure.external_api.market_client.MarketAPIClient")
    @patch("src.infrastructure.external_api.jquants_client.JQuantsAPIClient")
    def test_margin_empty_ohlcv_raises(self, MockJQuantsClient, MockMarketClient):
        service = IndicatorService()

        mock_jquants = MagicMock()
        mock_jquants.get_margin_interest.return_value = self._make_margin_df()
        mock_jquants.__enter__ = MagicMock(return_value=mock_jquants)
        mock_jquants.__exit__ = MagicMock(return_value=False)
        MockJQuantsClient.return_value = mock_jquants

        mock_market = MagicMock()
        mock_market.get_stock_ohlcv.return_value = pd.DataFrame()
        mock_market.__enter__ = MagicMock(return_value=mock_market)
        mock_market.__exit__ = MagicMock(return_value=False)
        MockMarketClient.return_value = mock_market

        with pytest.raises(ValueError, match="OHLCVデータが取得できません"):
            service.compute_margin_indicators("7203", "topix500", ["margin_long_pressure"])

    @patch("src.infrastructure.external_api.market_client.MarketAPIClient")
    @patch("src.infrastructure.external_api.jquants_client.JQuantsAPIClient")
    def test_margin_unknown_type_skipped(self, MockJQuantsClient, MockMarketClient):
        service = IndicatorService()

        mock_jquants = MagicMock()
        mock_jquants.get_margin_interest.return_value = self._make_margin_df()
        mock_jquants.__enter__ = MagicMock(return_value=mock_jquants)
        mock_jquants.__exit__ = MagicMock(return_value=False)
        MockJQuantsClient.return_value = mock_jquants

        mock_market = MagicMock()
        mock_market.get_stock_ohlcv.return_value = _make_ohlcv(50)
        mock_market.__enter__ = MagicMock(return_value=mock_market)
        mock_market.__exit__ = MagicMock(return_value=False)
        MockMarketClient.return_value = mock_market

        result = service.compute_margin_indicators(
            "7203", "topix500", ["unknown_margin"],
        )
        assert len(result["indicators"]) == 0

    @patch("src.infrastructure.external_api.market_client.MarketAPIClient")
    @patch("src.infrastructure.external_api.jquants_client.JQuantsAPIClient")
    def test_margin_with_dates(self, MockJQuantsClient, MockMarketClient):
        service = IndicatorService()

        mock_jquants = MagicMock()
        mock_jquants.get_margin_interest.return_value = self._make_margin_df()
        mock_jquants.__enter__ = MagicMock(return_value=mock_jquants)
        mock_jquants.__exit__ = MagicMock(return_value=False)
        MockJQuantsClient.return_value = mock_jquants

        mock_market = MagicMock()
        mock_market.get_stock_ohlcv.return_value = _make_ohlcv(50)
        mock_market.__enter__ = MagicMock(return_value=mock_market)
        mock_market.__exit__ = MagicMock(return_value=False)
        MockMarketClient.return_value = mock_market

        service.compute_margin_indicators(
            "7203", "topix500", ["margin_long_pressure"],
            start_date=date(2024, 1, 1), end_date=date(2024, 6, 30),
        )
        mock_jquants.get_margin_interest.assert_called_once_with("7203", "2024-01-01", "2024-06-30")

    @patch("src.infrastructure.external_api.market_client.MarketAPIClient")
    @patch("src.infrastructure.external_api.jquants_client.JQuantsAPIClient")
    def test_margin_prefers_market_reader(self, MockJQuantsClient, MockMarketClient, market_db_path):
        reader = MarketDbReader(market_db_path)
        try:
            service = IndicatorService(market_reader=reader)

            mock_jquants = MagicMock()
            mock_jquants.get_margin_interest.return_value = self._make_margin_df()
            mock_jquants.__enter__ = MagicMock(return_value=mock_jquants)
            mock_jquants.__exit__ = MagicMock(return_value=False)
            MockJQuantsClient.return_value = mock_jquants

            result = service.compute_margin_indicators(
                "7203", "topix500", ["margin_long_pressure"],
            )

            assert "margin_long_pressure" in result["indicators"]
            MockMarketClient.assert_not_called()
        finally:
            reader.close()


class TestMarginEdgeCases:
    """信用指標のエッジケーステスト"""

    def test_margin_index_mismatch(self):
        """margin_dfとvolumeのインデックスが異なる場合"""
        margin_dates = pd.date_range("2024-01-01", periods=30)
        vol_dates = pd.date_range("2024-01-15", periods=30)

        margin_df = pd.DataFrame({
            "longMarginVolume": np.ones(30) * 200000,
            "shortMarginVolume": np.ones(30) * 50000,
        }, index=margin_dates)
        volume = pd.Series(np.ones(30) * 100000, index=vol_dates)

        records = compute_margin_long_pressure(margin_df, volume, 5)
        # 重複するインデックスのみ計算される
        assert isinstance(records, list)

    def test_flow_pressure_previous_net_nan(self):
        """flow_pressureで最初のdelta=NaNがスキップされること"""
        dates = pd.date_range("2024-01-01", periods=20)
        margin_df = pd.DataFrame({
            "longMarginVolume": np.ones(20) * 200000,
            "shortMarginVolume": np.ones(20) * 50000,
        }, index=dates)
        volume = pd.Series(np.ones(20) * 100000, index=dates)

        records = compute_margin_flow_pressure(margin_df, volume, 5)
        # 最初のdelta=NaNなのでスキップされる
        assert all(r.get("flowPressure") is not None for r in records)


class TestPPOZeroDivision:
    """PPOゼロ除算テスト"""

    def test_ppo_with_zero_close(self):
        """Close=0を含むデータでPPOがinfにならないこと"""
        dates = pd.date_range("2024-01-01", periods=50)
        ohlcv = pd.DataFrame({
            "Open": np.ones(50) * 100,
            "High": np.ones(50) * 110,
            "Low": np.ones(50) * 90,
            "Close": np.concatenate([np.zeros(5), np.ones(45) * 100]),
            "Volume": np.ones(50) * 100000,
        }, index=dates)

        key, records = INDICATOR_REGISTRY["ppo"](ohlcv, {}, "include")
        # infがNoneに変換されていることを確認
        for r in records:
            if r.get("ppo") is not None:
                assert not np.isinf(r["ppo"])


class TestComputeIndicatorsOutputOHLCV:
    """output='ohlcv'オプションのテスト"""

    def test_output_ohlcv_returns_ohlcv_data(self):
        """output='ohlcv'でOHLCVデータが返却されること"""
        ohlcv = _make_ohlcv(20)
        service = IndicatorService()

        with patch.object(service, "load_ohlcv", return_value=ohlcv):
            result = service.compute_indicators(
                stock_code="7203",
                source="market",
                timeframe="daily",
                indicators=[],
                output="ohlcv",
            )

        assert "ohlcv" in result
        assert result["ohlcv"] is not None
        assert len(result["ohlcv"]) == 20
        assert result["indicators"] == {}

        # OHLCVレコードの構造確認
        record = result["ohlcv"][0]
        assert "date" in record
        assert "open" in record
        assert "high" in record
        assert "low" in record
        assert "close" in record
        assert "volume" in record

    def test_output_ohlcv_with_resample(self):
        """output='ohlcv' + timeframe='weekly'でリサンプルされること"""
        ohlcv = _make_ohlcv(20)
        service = IndicatorService()

        with patch.object(service, "load_ohlcv", return_value=ohlcv):
            result = service.compute_indicators(
                stock_code="7203",
                source="market",
                timeframe="weekly",
                indicators=[],
                output="ohlcv",
            )

        assert result["ohlcv"] is not None
        # 20日分のデータは4週分程度になる
        assert len(result["ohlcv"]) < 20
        assert result["meta"]["source_bars"] == 20

    def test_output_ohlcv_with_benchmark(self):
        """output='ohlcv' + benchmark_codeで相対OHLCが返却されること"""
        stock_ohlcv = _make_ohlcv(20)
        benchmark_ohlcv = _make_ohlcv(20)

        service = IndicatorService()

        with patch.object(service, "load_ohlcv", return_value=stock_ohlcv):
            with patch.object(service, "load_benchmark_ohlcv", return_value=benchmark_ohlcv):
                result = service.compute_indicators(
                    stock_code="7203",
                    source="market",
                    timeframe="daily",
                    indicators=[],
                    benchmark_code="topix",
                    output="ohlcv",
                )

        assert result["ohlcv"] is not None
        # 相対OHLC値は1.0近辺になるはず
        for record in result["ohlcv"]:
            if record["close"] is not None:
                assert 0 < record["close"] < 10  # 極端な値でないことを確認

    def test_output_indicators_requires_indicators(self):
        """output='indicators'ではindicatorsが必須"""
        service = IndicatorService()

        with patch.object(service, "load_ohlcv", return_value=_make_ohlcv(20)):
            result = service.compute_indicators(
                stock_code="7203",
                source="market",
                timeframe="daily",
                indicators=[{"type": "sma", "params": {"period": 5}}],
                output="indicators",
            )

        assert "indicators" in result
        assert "sma_5" in result["indicators"]
        assert "ohlcv" not in result or result.get("ohlcv") is None


# ===== Relative OHLC =====


class TestComputeRelativeOHLCColumn:
    """_compute_relative_ohlc_column() テスト"""

    def test_skip_mode(self):
        """skipモードでゼロ除算はinfになること"""
        stock = pd.Series([100.0, 200.0, 300.0], index=pd.date_range("2024-01-01", periods=3))
        bench = pd.Series([50.0, 0.0, 100.0], index=pd.date_range("2024-01-01", periods=3))
        result = _compute_relative_ohlc_column(stock, bench, "skip")
        assert result.iloc[0] == 2.0
        assert np.isinf(result.iloc[1])  # ゼロ除算はinf
        assert result.iloc[2] == 3.0

    def test_zero_mode(self):
        """zeroモードでゼロ除算は0.0になること"""
        stock = pd.Series([100.0, 200.0], index=pd.date_range("2024-01-01", periods=2))
        bench = pd.Series([50.0, 0.0], index=pd.date_range("2024-01-01", periods=2))
        result = _compute_relative_ohlc_column(stock, bench, "zero")
        assert result.iloc[0] == 2.0
        assert result.iloc[1] == 0.0

    def test_null_mode(self):
        """nullモードでゼロ除算はNaNになること"""
        stock = pd.Series([100.0, 200.0], index=pd.date_range("2024-01-01", periods=2))
        bench = pd.Series([50.0, 0.0], index=pd.date_range("2024-01-01", periods=2))
        result = _compute_relative_ohlc_column(stock, bench, "null")
        assert result.iloc[0] == 2.0
        assert pd.isna(result.iloc[1])


class TestCalculateRelativeOHLCV:
    """calculate_relative_ohlcv() テスト"""

    def _make_stock_df(self, n: int = 5) -> pd.DataFrame:
        dates = pd.date_range("2024-01-08", periods=n, freq="B")
        return pd.DataFrame({
            "Open": [100.0 + i for i in range(n)],
            "High": [105.0 + i for i in range(n)],
            "Low": [95.0 + i for i in range(n)],
            "Close": [102.0 + i for i in range(n)],
            "Volume": [1000.0 + i*100 for i in range(n)],
        }, index=dates)

    def _make_benchmark_df(self, n: int = 5) -> pd.DataFrame:
        dates = pd.date_range("2024-01-08", periods=n, freq="B")
        return pd.DataFrame({
            "Open": [2000.0 + i*10 for i in range(n)],
            "High": [2050.0 + i*10 for i in range(n)],
            "Low": [1950.0 + i*10 for i in range(n)],
            "Close": [2010.0 + i*10 for i in range(n)],
            "Volume": [100000.0 + i*1000 for i in range(n)],
        }, index=dates)

    def test_basic_calculation(self):
        """基本的な相対OHLC計算"""
        stock = self._make_stock_df()
        bench = self._make_benchmark_df()
        result = calculate_relative_ohlcv(stock, bench, "skip")

        assert len(result) == 5
        # 相対値の確認（stock/bench）
        assert abs(result.iloc[0]["Open"] - 100.0 / 2000.0) < 0.0001
        # Volumeはそのまま保持
        assert result.iloc[0]["Volume"] == 1000.0

    def test_skip_mode_filters_zero(self):
        """skipモードでゼロを含む行が除外されること"""
        stock = self._make_stock_df()
        bench = self._make_benchmark_df()
        bench.iloc[2, bench.columns.get_loc("Open")] = 0.0

        result = calculate_relative_ohlcv(stock, bench, "skip")
        assert len(result) == 4  # 1行除外

    def test_no_common_dates_raises(self):
        """共通日付がない場合にエラー"""
        stock = self._make_stock_df()
        bench_dates = pd.date_range("2025-01-08", periods=5, freq="B")
        bench = pd.DataFrame({
            "Open": [2000.0] * 5,
            "High": [2050.0] * 5,
            "Low": [1950.0] * 5,
            "Close": [2010.0] * 5,
            "Volume": [100000.0] * 5,
        }, index=bench_dates)

        with pytest.raises(ValueError, match="共通する日付がありません"):
            calculate_relative_ohlcv(stock, bench, "skip")

    def test_all_zero_division_raises(self):
        """全日がゼロ除算でエラー"""
        stock = self._make_stock_df()
        bench = pd.DataFrame({
            "Open": [0.0] * 5,
            "High": [0.0] * 5,
            "Low": [0.0] * 5,
            "Close": [0.0] * 5,
            "Volume": [100000.0] * 5,
        }, index=stock.index)

        with pytest.raises(ValueError, match="相対計算可能なデータがありません"):
            calculate_relative_ohlcv(stock, bench, "skip")


class TestResampleTimeframeIndexAdjustment:
    """resample_timeframeのインデックス調整テスト"""

    def test_weekly_index_is_monday(self):
        """週足インデックスが月曜日であること"""
        dates = pd.date_range("2024-01-08", periods=10, freq="B")
        df = pd.DataFrame({
            "Open": [100.0] * 10,
            "High": [105.0] * 10,
            "Low": [95.0] * 10,
            "Close": [102.0] * 10,
            "Volume": [1000.0] * 10,
        }, index=dates)

        service = IndicatorService()
        result = service.resample_timeframe(df, "weekly")

        for idx in result.index:
            assert idx.weekday() == 0, f"Expected Monday (0), got {idx.weekday()}"

    def test_monthly_index_is_first_day(self):
        """月足インデックスが月初日であること"""
        dates = pd.date_range("2024-01-02", periods=40, freq="B")
        df = pd.DataFrame({
            "Open": [100.0] * 40,
            "High": [105.0] * 40,
            "Low": [95.0] * 40,
            "Close": [102.0] * 40,
            "Volume": [1000.0] * 40,
        }, index=dates)

        service = IndicatorService()
        result = service.resample_timeframe(df, "monthly")

        for idx in result.index:
            assert idx.day == 1, f"Expected day 1, got {idx.day}"


class TestMarginVolumeRatio:
    """compute_margin_volume_ratio テスト"""

    def test_basic(self):
        """基本的な信用残高/出来高比率計算"""
        dates = pd.date_range("2024-01-08", periods=20, freq="B")
        margin_df = pd.DataFrame({
            "longMarginVolume": np.ones(20) * 200000,
            "shortMarginVolume": np.ones(20) * 50000,
        }, index=dates)
        volume = pd.Series(np.ones(20) * 100000, index=dates)

        records = compute_margin_volume_ratio(margin_df, volume, 15)
        assert len(records) > 0
        assert "longRatio" in records[0]
        assert "shortRatio" in records[0]
        assert "weeklyAvgVolume" in records[0]

    def test_zero_volume_skipped(self):
        """ゼロ出来高の週はスキップ"""
        dates = pd.date_range("2024-01-08", periods=5, freq="B")
        margin_df = pd.DataFrame({
            "longMarginVolume": np.ones(5) * 200000,
            "shortMarginVolume": np.ones(5) * 50000,
        }, index=dates)
        volume = pd.Series(np.zeros(5), index=dates)

        records = compute_margin_volume_ratio(margin_df, volume, 15)
        assert len(records) == 0
