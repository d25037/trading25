"""
ボラティリティシグナルユニットテスト

volatility.pyのボラティリティシグナル関数をテスト
"""

import pytest
import pandas as pd
import numpy as np

from src.domains.strategy.signals.volatility import (
    bollinger_bands_signal,
    bollinger_cross_signal,
    bollinger_position_signal,
    volatility_relative_signal,
    rolling_volatility_signal,
    volatility_percentile_signal,
    low_volatility_stock_screen_signal,
)


class TestVolatilityRelativeSignal:
    """volatility_relative_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=300)
        # 銘柄価格（高ボラティリティ）
        self.stock_price = pd.Series(
            np.random.randn(300).cumsum() + 100, index=self.dates
        )
        # ベンチマーク価格（低ボラティリティ）
        self.benchmark_price = pd.Series(
            np.random.randn(300).cumsum() * 0.5 + 1000, index=self.dates
        )

    def test_volatility_relative_basic(self):
        """相対ボラティリティシグナル基本テスト"""
        signal = volatility_relative_signal(
            self.stock_price,
            self.benchmark_price,
            lookback_period=200,
            threshold_multiplier=1.0,
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.stock_price)

    def test_threshold_multiplier_effect(self):
        """閾値倍率の効果テスト"""
        signal_strict = volatility_relative_signal(
            self.stock_price,
            self.benchmark_price,
            lookback_period=200,
            threshold_multiplier=0.5,
        )
        signal_loose = volatility_relative_signal(
            self.stock_price,
            self.benchmark_price,
            lookback_period=200,
            threshold_multiplier=2.0,
        )

        assert isinstance(signal_strict, pd.Series)
        assert isinstance(signal_loose, pd.Series)
        # 緩い閾値の方がTrue数が多い
        assert signal_loose.sum() >= signal_strict.sum()

    def test_lookback_period_effect(self):
        """ルックバック期間の効果テスト"""
        signal_short = volatility_relative_signal(
            self.stock_price,
            self.benchmark_price,
            lookback_period=100,
            threshold_multiplier=1.0,
        )
        signal_long = volatility_relative_signal(
            self.stock_price,
            self.benchmark_price,
            lookback_period=250,
            threshold_multiplier=1.0,
        )

        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)


class TestRollingVolatilitySignal:
    """rolling_volatility_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)
        # 低ボラティリティ価格データ
        self.price = pd.Series(
            np.random.randn(200).cumsum() * 0.2 + 100, index=self.dates
        )

    def test_rolling_volatility_basic(self):
        """ローリングボラティリティシグナル基本テスト"""
        signal = rolling_volatility_signal(self.price, window=20, threshold=0.3)

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price)
        # 低ボラティリティなのでTrueが発生
        assert signal.iloc[30:].sum() > 0

    def test_threshold_effect(self):
        """閾値の効果テスト"""
        signal_strict = rolling_volatility_signal(self.price, window=20, threshold=0.1)
        signal_loose = rolling_volatility_signal(self.price, window=20, threshold=0.5)

        assert isinstance(signal_strict, pd.Series)
        assert isinstance(signal_loose, pd.Series)
        # 緩い閾値の方がTrue数が多い
        assert signal_loose.sum() >= signal_strict.sum()

    def test_window_effect(self):
        """ウィンドウサイズの効果テスト"""
        signal_short = rolling_volatility_signal(self.price, window=10, threshold=0.3)
        signal_long = rolling_volatility_signal(self.price, window=50, threshold=0.3)

        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)


class TestVolatilityPercentileSignal:
    """volatility_percentile_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2022-01-01", periods=300)
        self.price = pd.Series(np.random.randn(300).cumsum() + 100, index=self.dates)

    def test_volatility_percentile_basic(self):
        """ボラティリティパーセンタイルシグナル基本テスト"""
        signal = volatility_percentile_signal(
            self.price, window=20, lookback=252, percentile=50.0
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price)

    def test_percentile_effect(self):
        """パーセンタイル閾値の効果テスト"""
        signal_low = volatility_percentile_signal(
            self.price, window=20, lookback=200, percentile=20.0
        )
        signal_high = volatility_percentile_signal(
            self.price, window=20, lookback=200, percentile=80.0
        )

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 高いパーセンタイルの方がTrue数が多い
        assert signal_high.sum() >= signal_low.sum()


class TestLowVolatilityStockScreenSignal:
    """low_volatility_stock_screen_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)
        # 価格が100円以上で低ボラティリティ
        self.price = pd.Series(
            np.random.randn(200).cumsum() * 0.2 + 150, index=self.dates
        )

    def test_low_volatility_screen_basic(self):
        """低ボラティリティスクリーニング基本テスト"""
        signal = low_volatility_stock_screen_signal(
            self.price, min_price=100.0, max_volatility=0.25, window=60
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price)
        # 条件を満たすのでTrueが発生
        assert signal.iloc[70:].sum() > 0

    def test_min_price_effect(self):
        """最低価格条件の効果テスト"""
        signal_low_price = low_volatility_stock_screen_signal(
            self.price, min_price=100.0, max_volatility=0.25, window=60
        )
        signal_high_price = low_volatility_stock_screen_signal(
            self.price, min_price=200.0, max_volatility=0.25, window=60
        )

        assert isinstance(signal_low_price, pd.Series)
        assert isinstance(signal_high_price, pd.Series)
        # 低い価格条件の方がTrue数が多い
        assert signal_low_price.sum() >= signal_high_price.sum()

    def test_max_volatility_effect(self):
        """最大ボラティリティ条件の効果テスト"""
        signal_strict = low_volatility_stock_screen_signal(
            self.price, min_price=100.0, max_volatility=0.1, window=60
        )
        signal_loose = low_volatility_stock_screen_signal(
            self.price, min_price=100.0, max_volatility=0.5, window=60
        )

        assert isinstance(signal_strict, pd.Series)
        assert isinstance(signal_loose, pd.Series)
        # 緩い条件の方がTrue数が多い
        assert signal_loose.sum() >= signal_strict.sum()


class TestBollingerPositionSignal:
    """bollinger_position_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)
        # トレンド変化を含む価格データ
        close = np.concatenate(
            [
                np.linspace(100, 120, 100),  # 上昇トレンド
                np.linspace(120, 100, 100),  # 下降トレンド
            ]
        )
        self.ohlc_data = pd.DataFrame(
            {
                "Open": close - 1,
                "High": close + 2,
                "Low": close - 2,
                "Close": close,
                "Volume": np.random.randint(1000, 10000, 200),
            },
            index=self.dates,
        )

    def test_bollinger_below_upper(self):
        """BB上限以下シグナルテスト"""
        signal = bollinger_position_signal(
            self.ohlc_data, window=20, alpha=2.0, level="upper", direction="below"
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.ohlc_data)
        # ほとんどの期間でTrue（上限以下）
        assert signal.sum() > len(signal) * 0.8

    def test_bollinger_above_lower(self):
        """BB下限以上シグナルテスト"""
        signal = bollinger_position_signal(
            self.ohlc_data, window=20, alpha=2.0, level="lower", direction="above"
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # ほとんどの期間でTrue（下限以上）
        assert signal.sum() > len(signal) * 0.8

    def test_bollinger_above_middle(self):
        """BB中央線以上シグナルテスト"""
        signal = bollinger_position_signal(
            self.ohlc_data, window=20, alpha=2.0, level="middle", direction="above"
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_bollinger_below_middle(self):
        """BB中央線以下シグナルテスト"""
        signal = bollinger_position_signal(
            self.ohlc_data, window=20, alpha=2.0, level="middle", direction="below"
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_bollinger_above_upper(self):
        """BB上限以上シグナルテスト（過熱）"""
        signal = bollinger_position_signal(
            self.ohlc_data, window=20, alpha=2.0, level="upper", direction="above"
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 上限以上はまれ
        assert signal.sum() >= 0

    def test_bollinger_below_lower(self):
        """BB下限以下シグナルテスト（売られすぎ）"""
        signal = bollinger_position_signal(
            self.ohlc_data, window=20, alpha=2.0, level="lower", direction="below"
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 下限以下はまれ
        assert signal.sum() >= 0

    def test_invalid_level(self):
        """不正なlevelでエラー"""
        with pytest.raises(ValueError, match="不正なlevel"):
            bollinger_position_signal(
                self.ohlc_data, window=20, alpha=2.0, level="invalid"
            )

    def test_alpha_effect(self):
        """α値の効果テスト"""
        signal_narrow = bollinger_position_signal(
            self.ohlc_data, window=20, alpha=1.0, level="upper", direction="below"
        )
        signal_wide = bollinger_position_signal(
            self.ohlc_data, window=20, alpha=3.0, level="upper", direction="below"
        )

        assert isinstance(signal_narrow, pd.Series)
        assert isinstance(signal_wide, pd.Series)
        # 広いバンドの方がTrue数が多い
        assert signal_wide.sum() >= signal_narrow.sum()


class TestBollingerCrossSignal:
    """bollinger_cross_signal() のテスト"""

    def setup_method(self):
        self.dates = pd.date_range("2023-01-01", periods=200)
        close = np.concatenate(
            [
                np.linspace(100, 120, 100),
                np.linspace(120, 95, 100),
            ]
        )
        self.ohlc_data = pd.DataFrame(
            {
                "Open": close - 1,
                "High": close + 2,
                "Low": close - 2,
                "Close": close,
                "Volume": np.random.randint(1000, 10000, 200),
            },
            index=self.dates,
        )

    def test_cross_below_upper_returns_bool_series(self):
        signal = bollinger_cross_signal(
            self.ohlc_data,
            window=20,
            alpha=2.0,
            level="upper",
            direction="below",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.ohlc_data)

    def test_lookback_extends_cross_event(self):
        signal_short = bollinger_cross_signal(
            self.ohlc_data,
            window=20,
            alpha=2.0,
            level="middle",
            direction="below",
            lookback_days=1,
        )
        signal_long = bollinger_cross_signal(
            self.ohlc_data,
            window=20,
            alpha=2.0,
            level="middle",
            direction="below",
            lookback_days=5,
        )

        assert signal_long.sum() >= signal_short.sum()

    def test_invalid_direction_raises(self):
        with pytest.raises(ValueError, match="不正なdirection"):
            bollinger_cross_signal(
                self.ohlc_data,
                window=20,
                alpha=2.0,
                level="upper",
                direction="invalid",
            )


class TestBollingerBandsSignalAlias:
    """後方互換 alias のテスト"""

    def setup_method(self):
        self.dates = pd.date_range("2023-01-01", periods=120)
        close = np.linspace(100, 110, 120)
        self.ohlc_data = pd.DataFrame(
            {
                "Open": close - 1,
                "High": close + 1,
                "Low": close - 1,
                "Close": close,
                "Volume": np.random.randint(1000, 10000, 120),
            },
            index=self.dates,
        )

    def test_alias_maps_to_position_signal(self):
        alias_signal = bollinger_bands_signal(
            self.ohlc_data,
            window=20,
            alpha=2.0,
            position="below_upper",
        )
        direct_signal = bollinger_position_signal(
            self.ohlc_data,
            window=20,
            alpha=2.0,
            level="upper",
            direction="below",
        )

        pd.testing.assert_series_equal(alias_signal, direct_signal)

    def test_invalid_position_raises(self):
        with pytest.raises(ValueError, match="不正なposition"):
            bollinger_bands_signal(self.ohlc_data, position="invalid")


class TestVolatilitySignalIntegration:
    """SignalProcessorとの統合テスト"""

    def test_volatility_percentile_with_signal_processor(self):
        """SignalProcessorでボラティリティパーセンタイルシグナルを使用"""
        from src.domains.strategy.signals.processor import SignalProcessor
        from src.shared.models.signals import SignalParams

        dates = pd.date_range("2023-01-01", periods=320)
        close = pd.Series(np.random.randn(320).cumsum() + 100, index=dates)
        ohlc_data = pd.DataFrame(
            {
                "Open": close - 0.5,
                "High": close + 1.5,
                "Low": close - 1.5,
                "Close": close,
                "Volume": np.random.randint(1000, 10000, 320),
            },
            index=dates,
        )

        base_signal = pd.Series([True] * len(dates), index=dates)

        params = SignalParams()
        params.volatility_percentile.enabled = True
        params.volatility_percentile.window = 20
        params.volatility_percentile.lookback = 252
        params.volatility_percentile.percentile = 80.0

        processor = SignalProcessor()
        result = processor.apply_signals(
            base_signal=base_signal,
            signal_type="entry",
            ohlc_data=ohlc_data,
            signal_params=params,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool
        assert len(result) == len(base_signal)

    def test_bollinger_signal_with_signal_processor(self):
        """SignalProcessorでボリンジャーバンドシグナルを使用"""
        from src.domains.strategy.signals.processor import SignalProcessor
        from src.shared.models.signals import SignalParams

        dates = pd.date_range("2023-01-01", periods=200)
        ohlc_data = pd.DataFrame(
            {
                "Open": np.random.randn(200).cumsum() + 100,
                "High": np.random.randn(200).cumsum() + 105,
                "Low": np.random.randn(200).cumsum() + 95,
                "Close": np.random.randn(200).cumsum() + 100,
                "Volume": np.random.randint(1000, 10000, 200),
            },
            index=dates,
        )

        base_signal = pd.Series([True] * 200, index=dates)

        # ボリンジャーバンドシグナルを有効化
        params = SignalParams()
        params.bollinger_position.enabled = True
        params.bollinger_position.window = 20
        params.bollinger_position.alpha = 2.0
        params.bollinger_position.level = "upper"
        params.bollinger_position.direction = "below"

        processor = SignalProcessor()
        result = processor.apply_signals(
            base_signal=base_signal,
            signal_type="entry",
            ohlc_data=ohlc_data,
            signal_params=params,
        )

        assert isinstance(result, pd.Series)
        assert result.dtype == bool
        assert len(result) == len(base_signal)


if __name__ == "__main__":
    pytest.main([__file__])
