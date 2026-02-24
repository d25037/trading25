"""
Pydanticバリデーションモデルのユニットテスト（最新アーキテクチャ対応版）

SharedConfig + SignalParams（統一Signalsシステム）対応
"""

import pytest
from pydantic import ValidationError

from src.shared.models.config import SharedConfig
from src.shared.models.signals import (
    SignalParams,
    VolumeSignalParams,
    CrossoverSignalParams,
    MeanReversionSignalParams,
    BollingerBandsSignalParams,
    PeriodBreakoutParams,
    FundamentalSignalParams,
    BetaSignalParams,
    ATRSupportBreakParams,
)


class TestSharedConfig:
    """SharedConfigのテスト（最新アーキテクチャ対応）"""

    def test_shared_config_defaults_align(self):
        """SharedConfigのデフォルト値がdefault.yamlと整合していることを確認"""
        config = SharedConfig(stock_codes=["dummy"])

        assert config.initial_cash == 10000000
        assert config.fees == 0.001
        assert config.slippage == 0.0
        assert config.spread == 0.0
        assert config.borrow_fee == 0.0
        assert config.max_concurrent_positions is None
        assert config.max_exposure is None
        assert config.dataset == "primeExTopix500"
        assert config.include_margin_data is True
        assert config.include_statements_data is True
        assert config.group_by is True
        assert config.cash_sharing is True
        assert config.printlog is False
        assert config.kelly_fraction == 1.0
        assert config.min_allocation == 0.01
        assert config.max_allocation == 0.5
        assert config.timeframe == "daily"
        assert config.parameter_optimization is not None
        assert config.parameter_optimization.n_jobs == -1
        assert config.parameter_optimization.scoring_weights == {
            "sharpe_ratio": 0.5,
            "calmar_ratio": 0.3,
            "total_return": 0.2,
        }

    def test_valid_shared_config(self):
        """有効なSharedConfig作成"""
        config = SharedConfig(
            initial_cash=200000,
            fees=0.002,
            dataset="sampleA",
            stock_codes=["17190"],
        )
        assert config.initial_cash == 200000
        assert config.fees == 0.002
        assert config.stock_codes == ["17190"]

    def test_invalid_initial_cash(self):
        """無効な初期資金でValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            SharedConfig(initial_cash=-1000, dataset="sampleA")
        assert "初期資金は正の値である必要があります" in str(exc_info.value)

    def test_invalid_fees(self):
        """無効な手数料でValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            SharedConfig(
                fees=1.5,  # 1以上は無効
                dataset="sampleA",
            )
        assert "手数料は0以上1未満である必要があります" in str(exc_info.value)

    def test_multi_stock_codes(self):
        """複数銘柄コード設定テスト"""
        config = SharedConfig(
            initial_cash=100000,
            dataset="sampleA",
            stock_codes=["17190", "23010", "98030"],
        )
        assert len(config.stock_codes) == 3


class TestSignalParams:
    """SignalParams統合テスト（統一Signalsシステム）"""

    def test_valid_signal_params_initialization(self):
        """SignalParams正常初期化テスト"""
        params = SignalParams()

        # デフォルトで全シグナル無効
        assert params.volume.enabled is False
        assert params.crossover.enabled is False
        assert params.mean_reversion.enabled is False
        assert params.bollinger_bands.enabled is False
        assert params.period_breakout.enabled is False

    def test_signal_params_has_any_enabled(self):
        """has_any_enabled()メソッドテスト"""
        params = SignalParams()

        # 全無効時
        assert params.has_any_enabled() is False

        # 1つ有効化
        params.volume.enabled = True
        assert params.has_any_enabled() is True

    def test_signal_params_entry_exit_alias(self):
        """エントリー・エグジットエイリアスメソッドテスト"""
        params = SignalParams()
        params.crossover.enabled = True

        assert params.has_any_entry_enabled() is True
        assert params.has_any_exit_enabled() is True


class TestVolumeSignalParams:
    """VolumeSignalParamsのテスト"""

    def test_valid_volume_params(self):
        """有効なVolumeSignalParams作成"""
        params = VolumeSignalParams(
            enabled=True,
            direction="surge",
            threshold=1.5,
            short_period=20,
            long_period=100,
        )
        assert params.enabled is True
        assert params.direction == "surge"
        assert params.threshold == 1.5

    def test_invalid_period_order(self):
        """期間順序が無効でValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            VolumeSignalParams(
                enabled=True,
                short_period=100,
                long_period=20,  # short_period以下は無効
            )
        assert "出来高長期期間は出来高短期期間より大きい必要があります" in str(
            exc_info.value
        )

    def test_valid_ma_type_sma(self):
        """有効なma_type=smaでVolumeSignalParams作成"""
        params = VolumeSignalParams(enabled=True, ma_type="sma")
        assert params.ma_type == "sma"

    def test_valid_ma_type_ema(self):
        """有効なma_type=emaでVolumeSignalParams作成"""
        params = VolumeSignalParams(enabled=True, ma_type="ema")
        assert params.ma_type == "ema"

    def test_invalid_ma_type(self):
        """無効なma_typeでValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            VolumeSignalParams(
                enabled=True,
                ma_type="wma",  # wmaは未対応
            )
        assert "ma_typeは'sma'または'ema'のみ指定可能です" in str(exc_info.value)

    def test_default_ma_type(self):
        """デフォルトma_typeはsma"""
        params = VolumeSignalParams(enabled=True)
        assert params.ma_type == "sma"


class TestCrossoverSignalParams:
    """CrossoverSignalParamsのテスト"""

    def test_valid_crossover_params(self):
        """有効なCrossoverSignalParams作成"""
        params = CrossoverSignalParams(
            enabled=True, type="sma", direction="golden", fast_period=10, slow_period=30
        )
        assert params.enabled is True
        assert params.type == "sma"
        assert params.fast_period == 10
        assert params.slow_period == 30

    def test_invalid_period_order(self):
        """期間順序が無効でValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            CrossoverSignalParams(
                enabled=True,
                fast_period=30,
                slow_period=20,  # fast_period以下は無効
            )
        assert "slow_periodはfast_periodより大きい必要があります" in str(exc_info.value)

    def test_macd_crossover(self):
        """MACDクロスオーバーパラメータテスト"""
        params = CrossoverSignalParams(
            enabled=True,
            type="macd",
            direction="golden",
            fast_period=12,
            slow_period=26,
            signal_period=9,
        )
        assert params.type == "macd"
        assert params.signal_period == 9


class TestMeanReversionSignalParams:
    """MeanReversionSignalParamsのテスト"""

    def test_valid_mean_reversion_params(self):
        """有効なMeanReversionSignalParams作成"""
        params = MeanReversionSignalParams(
            enabled=True,
            baseline_type="sma",
            baseline_period=25,
            deviation_threshold=0.2,
            deviation_direction="below",
        )
        assert params.enabled is True
        assert params.baseline_period == 25
        assert params.deviation_threshold == 0.2

    def test_invalid_deviation_threshold(self):
        """無効な乖離閾値でValidationError"""
        with pytest.raises(ValidationError):
            MeanReversionSignalParams(
                enabled=True,
                deviation_threshold=1.5,  # >1.0は無効
            )


class TestBollingerBandsSignalParams:
    """BollingerBandsSignalParamsのテスト"""

    def test_valid_bollinger_params(self):
        """有効なBollingerBandsSignalParams作成"""
        params = BollingerBandsSignalParams(
            enabled=True, window=20, alpha=2.0, position="below_upper"
        )
        assert params.enabled is True
        assert params.window == 20
        assert params.alpha == 2.0

    def test_boundary_alpha_values(self):
        """α値境界値テスト"""
        # 正常な境界値
        params = BollingerBandsSignalParams(enabled=True, alpha=0.5)
        assert params.alpha == 0.5

        # 無効な境界値
        with pytest.raises(ValidationError):
            BollingerBandsSignalParams(
                enabled=True,
                alpha=0.0,  # <=0は無効
            )


class TestPeriodBreakoutParams:
    """PeriodBreakoutParamsのテスト"""

    def test_valid_period_breakout_params(self):
        """有効なPeriodBreakoutParams作成"""
        params = PeriodBreakoutParams(
            enabled=True,
            direction="high",
            condition="break",
            period=20,
            lookback_days=1,
        )
        assert params.enabled is True
        assert params.direction == "high"
        assert params.period == 20


class TestFundamentalSignalParams:
    """FundamentalSignalParamsのテスト"""

    def test_valid_fundamental_params(self):
        """有効なFundamentalSignalParams作成"""
        params = FundamentalSignalParams()
        params.per.enabled = True
        params.per.threshold = 15.0
        params.roe.enabled = True
        params.roe.threshold = 10.0

        assert params.per.enabled is True
        assert params.per.threshold == 15.0
        assert params.roe.enabled is True

    def test_peg_ratio_params(self):
        """PEG Ratioパラメータテスト"""
        params = FundamentalSignalParams()
        params.peg_ratio.enabled = True
        params.peg_ratio.threshold = 1.0

        assert params.peg_ratio.enabled is True
        assert params.peg_ratio.threshold == 1.0


class TestBetaSignalParams:
    """BetaSignalParamsのテスト"""

    def test_valid_beta_params(self):
        """有効なBetaSignalParams作成"""
        params = BetaSignalParams(
            enabled=True, min_beta=0.5, max_beta=1.5, lookback_period=200
        )
        assert params.enabled is True
        assert params.min_beta == 0.5
        assert params.max_beta == 1.5

    def test_invalid_beta_range(self):
        """無効なβ値範囲でValidationError"""
        with pytest.raises(ValidationError) as exc_info:
            BetaSignalParams(
                enabled=True,
                min_beta=1.5,
                max_beta=0.5,  # min >= maxは無効
            )
        assert "β値上限は下限より大きい必要があります" in str(exc_info.value)


class TestATRSupportBreakParams:
    """ATRSupportBreakParamsのテスト"""

    def test_valid_atr_support_params(self):
        """有効なATRSupportBreakParams作成"""
        params = ATRSupportBreakParams(
            enabled=True,
            direction="break",
            lookback_period=20,
            atr_multiplier=2.0,
            price_column="close",
        )
        assert params.enabled is True
        assert params.atr_multiplier == 2.0

    def test_boundary_atr_multiplier(self):
        """ATR倍率境界値テスト"""
        # 正常な境界値
        params = ATRSupportBreakParams(enabled=True, atr_multiplier=0.1)
        assert params.atr_multiplier == 0.1

        # 無効な境界値
        with pytest.raises(ValidationError):
            ATRSupportBreakParams(
                enabled=True,
                atr_multiplier=0.0,  # <=0は無効
            )


class TestIntegratedSignalParams:
    """統合シグナルパラメータテスト（エントリー・エグジット両用設計）"""

    def test_entry_filter_params_setup(self):
        """エントリーフィルターパラメータ設定テスト"""
        entry_params = SignalParams()

        # 複数フィルター有効化
        entry_params.volume.enabled = True
        entry_params.volume.direction = "surge"
        entry_params.crossover.enabled = True
        entry_params.crossover.type = "sma"
        entry_params.crossover.direction = "golden"

        assert entry_params.has_any_entry_enabled() is True
        assert entry_params.volume.enabled is True
        assert entry_params.crossover.enabled is True

    def test_exit_trigger_params_setup(self):
        """エグジットトリガーパラメータ設定テスト"""
        exit_params = SignalParams()

        # 複数トリガー有効化
        exit_params.atr_support_break.enabled = True
        exit_params.atr_support_break.direction = "break"
        exit_params.bollinger_bands.enabled = True
        exit_params.bollinger_bands.position = "below_lower"

        assert exit_params.has_any_exit_enabled() is True
        assert exit_params.atr_support_break.enabled is True
        assert exit_params.bollinger_bands.enabled is True


if __name__ == "__main__":
    pytest.main([__file__])
