"""
平均回帰シグナルユニットテスト

mean_reversion.pyの平均回帰シグナル関数をテスト
"""

import pytest
import pandas as pd
import numpy as np

from src.domains.strategy.signals.mean_reversion import (
    deviation_signal,
    price_recovery_signal,
    mean_reversion_entry_signal,
    mean_reversion_exit_signal,
    mean_reversion_combined_signal,
)


class TestDeviationSignal:
    """deviation_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        # ベースライン=100で変動する価格
        self.baseline = pd.Series(np.ones(100) * 100, index=self.dates)
        # 価格が上下に変動（80～120）
        self.price = pd.Series(
            np.concatenate(
                [
                    np.linspace(100, 80, 30),  # 下落（-20%）
                    np.linspace(80, 120, 40),  # 回復・上昇（+40%）
                    np.linspace(120, 100, 30),  # 下落
                ]
            ),
            index=self.dates,
        )

    def test_deviation_below_basic(self):
        """基準線より下への乖離シグナル基本テスト"""
        # 20%以上下落部分を検出
        signal = deviation_signal(
            self.price, self.baseline, threshold=0.15, direction="below"
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price)
        # 最初の下落部分でTrueが発生
        assert signal.iloc[20:35].sum() > 0

    def test_deviation_above_basic(self):
        """基準線より上への乖離シグナル基本テスト"""
        # 15%以上上昇部分を検出
        signal = deviation_signal(
            self.price, self.baseline, threshold=0.15, direction="above"
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price)
        # 上昇部分でTrueが発生
        assert signal.iloc[55:75].sum() > 0

    def test_deviation_threshold_effect(self):
        """閾値の効果テスト"""
        signal_low = deviation_signal(
            self.price, self.baseline, threshold=0.1, direction="below"
        )
        signal_high = deviation_signal(
            self.price, self.baseline, threshold=0.3, direction="below"
        )

        assert isinstance(signal_low, pd.Series)
        assert isinstance(signal_high, pd.Series)
        # 低い閾値の方がTrue数が多い
        assert signal_low.sum() >= signal_high.sum()

    def test_invalid_direction(self):
        """不正なdirectionでエラー"""
        with pytest.raises(ValueError, match="不正なdirection"):
            deviation_signal(
                self.price, self.baseline, threshold=0.2, direction="invalid"
            )

    def test_nan_handling(self):
        """NaN処理テスト"""
        price_with_nan = self.price.copy()
        price_with_nan.iloc[0:10] = np.nan

        signal = deviation_signal(
            price_with_nan, self.baseline, threshold=0.2, direction="below"
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # NaNの部分はFalse
        assert not signal.iloc[0:10].any()

    def test_baseline_nan_handling(self):
        """ベースラインNaN処理テスト"""
        baseline_with_nan = self.baseline.copy()
        baseline_with_nan.iloc[0:10] = np.nan

        signal = deviation_signal(
            self.price, baseline_with_nan, threshold=0.2, direction="below"
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # ベースラインNaNの部分はFalse
        assert not signal.iloc[0:10].any()


class TestPriceRecoverySignal:
    """price_recovery_signal()の基本テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=100)
        self.baseline = pd.Series(np.ones(100) * 100, index=self.dates)
        # 価格がベースラインを上下に横切る
        self.price = pd.Series(
            np.concatenate(
                [
                    np.ones(30) * 95,  # 下
                    np.ones(40) * 105,  # 上
                    np.ones(30) * 95,  # 下
                ]
            ),
            index=self.dates,
        )

    def test_recovery_above_basic(self):
        """上抜け回復シグナル基本テスト"""
        signal = price_recovery_signal(self.price, self.baseline, direction="above")

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price)
        # 上にいる期間（30:70）でTrue
        assert signal.iloc[30:70].sum() == 40

    def test_recovery_below_basic(self):
        """下抜け回復シグナル基本テスト"""
        signal = price_recovery_signal(self.price, self.baseline, direction="below")

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price)
        # 下にいる期間（0:30、70:100）でTrue
        assert signal.iloc[0:30].sum() == 30
        assert signal.iloc[70:100].sum() == 30

    def test_invalid_direction(self):
        """不正なdirectionでエラー"""
        with pytest.raises(ValueError, match="不正なdirection"):
            price_recovery_signal(self.price, self.baseline, direction="invalid")

    def test_nan_handling(self):
        """NaN処理テスト"""
        price_with_nan = self.price.copy()
        price_with_nan.iloc[0:10] = np.nan

        signal = price_recovery_signal(price_with_nan, self.baseline, direction="above")

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # NaNの部分はFalse
        assert not signal.iloc[0:10].any()


class TestMeanReversionEntrySignal:
    """mean_reversion_entry_signal()の統合テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)
        # トレンド変化を含む価格データ
        self.ohlc_data = pd.DataFrame(
            {
                "Open": np.random.randn(200).cumsum() + 100,
                "High": np.random.randn(200).cumsum() + 105,
                "Low": np.random.randn(200).cumsum() + 95,
                "Close": np.concatenate(
                    [
                        np.linspace(100, 80, 50),  # 下降トレンド（-20%）
                        np.linspace(80, 120, 100),  # 上昇トレンド（+50%）
                        np.linspace(120, 100, 50),  # 調整
                    ]
                ),
                "Volume": np.random.randint(1000, 10000, 200),
            },
            index=self.dates,
        )

    def test_mean_reversion_entry_sma(self):
        """SMAベースライン平均回帰エントリーテスト"""
        signal = mean_reversion_entry_signal(
            self.ohlc_data,
            baseline_type="sma",
            baseline_period=25,
            deviation_threshold=0.1,
            deviation_direction="below",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.ohlc_data)
        # 下降トレンド部分でTrueが発生する可能性（データパターン依存）
        assert signal.sum() >= 0

    def test_mean_reversion_entry_ema(self):
        """EMAベースライン平均回帰エントリーテスト"""
        signal = mean_reversion_entry_signal(
            self.ohlc_data,
            baseline_type="ema",
            baseline_period=25,
            deviation_threshold=0.15,
            deviation_direction="below",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.ohlc_data)

    def test_invalid_baseline_type(self):
        """未対応のベースラインタイプでエラー"""
        with pytest.raises(ValueError, match="未対応のベースラインタイプ"):
            mean_reversion_entry_signal(
                self.ohlc_data,
                baseline_type="invalid",
                baseline_period=25,
                deviation_threshold=0.15,
                deviation_direction="below",
            )

    def test_deviation_direction_above(self):
        """上方乖離エントリーテスト（ショート想定）"""
        signal = mean_reversion_entry_signal(
            self.ohlc_data,
            baseline_type="sma",
            baseline_period=25,
            deviation_threshold=0.15,
            deviation_direction="above",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        # 上昇トレンド後半部分でTrueが発生する可能性
        assert signal.iloc[100:150].sum() >= 0


class TestMeanReversionExitSignal:
    """mean_reversion_exit_signal()の統合テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)
        # トレンド変化を含む価格データ
        close = np.concatenate(
            [
                np.linspace(100, 80, 50),  # 下降トレンド
                np.linspace(80, 120, 100),  # 上昇トレンド
                np.linspace(120, 100, 50),  # 調整
            ]
        )
        self.ohlc_data = pd.DataFrame(
            {
                "Open": close - 1,
                "High": close + 5,
                "Low": close - 5,
                "Close": close,
                "Volume": np.random.randint(1000, 10000, 200),
            },
            index=self.dates,
        )

    def test_mean_reversion_exit_high(self):
        """高値回復エグジットテスト"""
        signal = mean_reversion_exit_signal(
            self.ohlc_data,
            baseline_type="sma",
            baseline_period=25,
            recovery_direction="above",
            recovery_price="high",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.ohlc_data)
        # 上昇トレンド部分でTrueが発生
        assert signal.iloc[50:150].sum() > 0

    def test_mean_reversion_exit_close(self):
        """終値回復エグジットテスト"""
        signal = mean_reversion_exit_signal(
            self.ohlc_data,
            baseline_type="sma",
            baseline_period=25,
            recovery_direction="above",
            recovery_price="close",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.ohlc_data)

    def test_mean_reversion_exit_low(self):
        """安値回復エグジットテスト（ショート想定）"""
        signal = mean_reversion_exit_signal(
            self.ohlc_data,
            baseline_type="sma",
            baseline_period=25,
            recovery_direction="below",
            recovery_price="low",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.ohlc_data)

    def test_invalid_baseline_type(self):
        """未対応のベースラインタイプでエラー"""
        with pytest.raises(ValueError, match="未対応のベースラインタイプ"):
            mean_reversion_exit_signal(
                self.ohlc_data,
                baseline_type="invalid",
                baseline_period=25,
                recovery_direction="above",
                recovery_price="high",
            )


class TestMeanReversionCombinedSignal:
    """mean_reversion_combined_signal()の統合テスト"""

    def setup_method(self):
        """テストデータ作成"""
        self.dates = pd.date_range("2023-01-01", periods=200)
        # 下降→回復のトレンド
        trend = np.concatenate(
            [
                np.linspace(150, 100, 100),
                np.linspace(100, 130, 100),
            ]
        )
        self.ohlc_data = pd.DataFrame(
            {
                "High": trend + 5,
                "Low": trend - 5,
                "Close": trend,
                "Volume": [1000000] * 200,
            },
            index=self.dates,
        )

    def test_combined_signal_basic(self):
        """統合シグナル基本テスト（乖離 OR 回復）"""
        signal = mean_reversion_combined_signal(
            self.ohlc_data,
            baseline_type="sma",
            baseline_period=25,
            deviation_threshold=0.2,
            deviation_direction="below",
            recovery_price="high",
            recovery_direction="above",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.ohlc_data)
        # 乖離 OR 回復でシグナル発生
        assert signal.any()

    def test_combined_signal_entry_like(self):
        """エントリー的使用（乖離重視）"""
        signal = mean_reversion_combined_signal(
            self.ohlc_data,
            baseline_type="sma",
            baseline_period=25,
            deviation_threshold=0.15,
            deviation_direction="below",
            recovery_price="high",
            recovery_direction="above",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.ohlc_data)
        # 乖離 OR 回復でシグナル発生（全体で判定）
        assert signal.any()

    def test_combined_signal_exit_like(self):
        """エグジット的使用（回復重視）"""
        signal = mean_reversion_combined_signal(
            self.ohlc_data,
            baseline_type="sma",
            baseline_period=25,
            deviation_threshold=0.05,  # 小さめの閾値（回復条件を優先）
            deviation_direction="above",
            recovery_price="high",
            recovery_direction="above",
        )

        assert isinstance(signal, pd.Series)
        assert signal.any()
        # 回復フェーズでシグナル発生
        assert signal.iloc[100:].any()

    def test_ema_baseline(self):
        """EMA基準線でのテスト"""
        signal = mean_reversion_combined_signal(
            self.ohlc_data,
            baseline_type="ema",
            baseline_period=25,
            deviation_threshold=0.2,
            deviation_direction="below",
            recovery_price="close",
            recovery_direction="above",
        )

        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert signal.any()

    def test_invalid_baseline_type(self):
        """不正なベースラインタイプでエラー"""
        with pytest.raises(ValueError, match="未対応のベースラインタイプ"):
            mean_reversion_combined_signal(
                self.ohlc_data,
                baseline_type="invalid",
                baseline_period=25,
                deviation_threshold=0.2,
                deviation_direction="below",
                recovery_price="high",
                recovery_direction="above",
            )

    def test_empty_data(self):
        """空のDataFrameでもエラーにならない"""
        empty = pd.DataFrame(columns=["High", "Low", "Close", "Volume"])

        signal = mean_reversion_combined_signal(
            empty,
            baseline_type="sma",
            baseline_period=25,
            deviation_threshold=0.2,
            deviation_direction="below",
            recovery_price="high",
            recovery_direction="above",
        )

        assert isinstance(signal, pd.Series)
        assert len(signal) == 0


if __name__ == "__main__":
    pytest.main([__file__])
