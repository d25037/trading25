"""
Indicator Schemas ユニットテスト
"""

import pytest
from pydantic import ValidationError

from src.entrypoints.http.schemas.indicators import (
    ATRParams,
    ATRSupportParams,
    BollingerParams,
    EMAParams,
    IndicatorComputeRequest,
    IndicatorComputeResponse,
    IndicatorSpec,
    MACDParams,
    MarginIndicatorRequest,
    NBarSupportParams,
    PPOParams,
    RSIParams,
    RiskAdjustedReturnParams,
    SMAParams,
    TradingValueMAParams,
    VolumeComparisonParams,
)


class TestParamsModels:
    """個別パラメータモデルのテスト"""

    def test_sma_params_valid(self):
        p = SMAParams(period=20)
        assert p.period == 20

    def test_sma_params_boundary(self):
        SMAParams(period=1)
        SMAParams(period=500)
        with pytest.raises(ValidationError):
            SMAParams(period=0)
        with pytest.raises(ValidationError):
            SMAParams(period=501)

    def test_ema_params_valid(self):
        p = EMAParams(period=12)
        assert p.period == 12

    def test_rsi_params_default(self):
        p = RSIParams()
        assert p.period == 14

    def test_macd_params_default(self):
        p = MACDParams()
        assert p.fast_period == 12
        assert p.slow_period == 26
        assert p.signal_period == 9

    def test_ppo_params_default(self):
        p = PPOParams()
        assert p.fast_period == 12

    def test_bollinger_params_default(self):
        p = BollingerParams()
        assert p.period == 20
        assert p.std_dev == 2.0

    def test_bollinger_params_invalid_std(self):
        with pytest.raises(ValidationError):
            BollingerParams(std_dev=0)
        with pytest.raises(ValidationError):
            BollingerParams(std_dev=6.0)

    def test_atr_params_default(self):
        p = ATRParams()
        assert p.period == 14

    def test_atr_support_params_default(self):
        p = ATRSupportParams()
        assert p.lookback_period == 20
        assert p.atr_multiplier == 2.0

    def test_nbar_support_params(self):
        p = NBarSupportParams(period=60)
        assert p.period == 60

    def test_volume_comparison_params_default(self):
        p = VolumeComparisonParams()
        assert p.short_period == 20
        assert p.long_period == 100
        assert p.lower_multiplier == 1.0
        assert p.higher_multiplier == 1.5
        assert p.ma_type == "sma"

    def test_volume_comparison_params_custom(self):
        p = VolumeComparisonParams(lower_multiplier=0.8, higher_multiplier=2.0)
        assert p.lower_multiplier == 0.8
        assert p.higher_multiplier == 2.0

    def test_volume_comparison_params_invalid(self):
        with pytest.raises(ValidationError):
            VolumeComparisonParams(lower_multiplier=0)
        with pytest.raises(ValidationError):
            VolumeComparisonParams(higher_multiplier=-1)

    def test_trading_value_ma_params(self):
        p = TradingValueMAParams(period=50)
        assert p.period == 50

    def test_risk_adjusted_return_params_default(self):
        p = RiskAdjustedReturnParams()
        assert p.lookback_period == 60
        assert p.ratio_type == "sortino"

    def test_risk_adjusted_return_params_invalid_ratio_type(self):
        with pytest.raises(ValidationError):
            RiskAdjustedReturnParams(ratio_type="invalid")


class TestIndicatorSpec:
    """IndicatorSpec バリデーションテスト"""

    def test_sma_spec_valid(self):
        spec = IndicatorSpec(type="sma", params={"period": 20})
        assert spec.type == "sma"

    def test_sma_spec_invalid_period(self):
        with pytest.raises(ValidationError):
            IndicatorSpec(type="sma", params={"period": 0})

    def test_macd_spec_defaults(self):
        spec = IndicatorSpec(type="macd", params={})
        assert spec.type == "macd"

    def test_bollinger_spec_custom(self):
        spec = IndicatorSpec(type="bollinger", params={"period": 30, "std_dev": 2.5})
        assert spec.type == "bollinger"

    def test_invalid_type(self):
        with pytest.raises(ValidationError):
            IndicatorSpec(type="unknown", params={})

    def test_all_12_types(self):
        types = [
            "sma", "ema", "rsi", "macd", "ppo", "bollinger",
            "atr", "atr_support", "nbar_support", "volume_comparison",
            "trading_value_ma", "risk_adjusted_return",
        ]
        for t in types:
            spec = IndicatorSpec(type=t, params={} if t not in ("sma", "ema") else {"period": 20})
            assert spec.type == t


class TestIndicatorComputeRequest:
    """リクエストモデルのテスト"""

    def test_valid_request(self):
        req = IndicatorComputeRequest(
            stock_code="7203",
            indicators=[
                IndicatorSpec(type="sma", params={"period": 20}),
                IndicatorSpec(type="rsi", params={}),
            ],
        )
        assert req.stock_code == "7203"
        assert len(req.indicators) == 2
        assert req.source == "dataset"
        assert req.timeframe == "daily"
        assert req.nan_handling == "include"

    def test_empty_indicators_rejected_for_output_indicators(self):
        """output='indicators'の場合、空indicatorsはエラー"""
        with pytest.raises(ValidationError):
            IndicatorComputeRequest(
                stock_code="7203",
                indicators=[],
                output="indicators",
            )

    def test_empty_indicators_allowed_for_output_ohlcv(self):
        """output='ohlcv'の場合、空indicatorsが許可される"""
        req = IndicatorComputeRequest(
            stock_code="7203",
            indicators=[],
            output="ohlcv",
        )
        assert len(req.indicators) == 0
        assert req.output == "ohlcv"

    def test_many_indicators(self):
        """多数のインジケーターが許可されること（上限なし）"""
        specs = [IndicatorSpec(type="sma", params={"period": i}) for i in range(1, 21)]
        req = IndicatorComputeRequest(stock_code="7203", indicators=specs)
        assert len(req.indicators) == 20

    def test_empty_stock_code_rejected(self):
        with pytest.raises(ValidationError):
            IndicatorComputeRequest(
                stock_code="",
                indicators=[IndicatorSpec(type="sma", params={"period": 20})],
            )


class TestIndicatorComputeResponse:
    """レスポンスモデルのテスト"""

    def test_valid_response(self):
        resp = IndicatorComputeResponse(
            stock_code="7203",
            timeframe="daily",
            meta={"bars": 500},
            indicators={
                "sma_20": [{"date": "2024-01-01", "value": 100.5}],
            },
        )
        assert resp.stock_code == "7203"
        assert "sma_20" in resp.indicators


class TestMarginIndicatorRequest:
    """信用指標リクエストのテスト"""

    def test_valid_request(self):
        req = MarginIndicatorRequest(
            stock_code="7203",
            indicators=["margin_long_pressure", "margin_turnover_days"],
        )
        assert len(req.indicators) == 2
        assert req.average_period == 15

    def test_invalid_indicator_type(self):
        with pytest.raises(ValidationError):
            MarginIndicatorRequest(
                stock_code="7203",
                indicators=["unknown"],
            )
