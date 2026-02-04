"""
リスク調整リターンシグナル ユニットテスト
"""

import numpy as np
import pandas as pd
import pytest

from src.strategies.signals.risk_adjusted import risk_adjusted_return_signal


class TestRiskAdjustedReturnSignal:
    """risk_adjusted_return_signal() テスト"""

    def setup_method(self):
        """テストデータ作成"""
        np.random.seed(42)
        self.dates = pd.date_range("2024-01-01", periods=100)
        # 上昇トレンド（高リスク調整リターン）
        self.price_up = pd.Series(
            100 * np.cumprod(1 + np.random.normal(0.005, 0.01, 100)), index=self.dates
        )
        # 下降トレンド（低リスク調整リターン）
        self.price_down = pd.Series(
            100 * np.cumprod(1 + np.random.normal(-0.005, 0.01, 100)), index=self.dates
        )
        # 高ボラティリティ（低シャープレシオ）
        self.price_volatile = pd.Series(
            100 * np.cumprod(1 + np.random.normal(0.002, 0.05, 100)), index=self.dates
        )
        # 低ボラティリティ上昇（高シャープレシオ）
        self.price_steady_up = pd.Series(
            100 * np.cumprod(1 + np.random.normal(0.003, 0.003, 100)), index=self.dates
        )

    def test_sharpe_above_condition_basic(self):
        """シャープレシオ above条件基本テスト"""
        signal = risk_adjusted_return_signal(
            close=self.price_up,
            lookback_period=20,
            threshold=0.5,
            ratio_type="sharpe",
            condition="above",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price_up)

    def test_sharpe_below_condition_basic(self):
        """シャープレシオ below条件基本テスト"""
        signal = risk_adjusted_return_signal(
            close=self.price_down,
            lookback_period=20,
            threshold=0.0,
            ratio_type="sharpe",
            condition="below",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_sortino_above_condition_basic(self):
        """ソルティノレシオ above条件基本テスト"""
        signal = risk_adjusted_return_signal(
            close=self.price_up,
            lookback_period=20,
            threshold=0.5,
            ratio_type="sortino",
            condition="above",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool
        assert len(signal) == len(self.price_up)

    def test_sortino_below_condition_basic(self):
        """ソルティノレシオ below条件基本テスト"""
        signal = risk_adjusted_return_signal(
            close=self.price_down,
            lookback_period=20,
            threshold=0.0,
            ratio_type="sortino",
            condition="below",
        )
        assert isinstance(signal, pd.Series)
        assert signal.dtype == bool

    def test_threshold_sensitivity(self):
        """閾値感度テスト"""
        signal_low = risk_adjusted_return_signal(
            close=self.price_up,
            lookback_period=30,
            threshold=0.5,
            ratio_type="sharpe",
            condition="above",
        )
        signal_high = risk_adjusted_return_signal(
            close=self.price_up,
            lookback_period=30,
            threshold=2.0,
            ratio_type="sharpe",
            condition="above",
        )
        # 低い閾値の方が多くのシグナルが出る
        assert signal_low.sum() >= signal_high.sum()

    def test_lookback_period_effect(self):
        """期間効果テスト"""
        signal_short = risk_adjusted_return_signal(
            close=self.price_up,
            lookback_period=10,
            threshold=0.5,
            ratio_type="sharpe",
            condition="above",
        )
        signal_long = risk_adjusted_return_signal(
            close=self.price_up,
            lookback_period=40,
            threshold=0.5,
            ratio_type="sharpe",
            condition="above",
        )
        # 両方ともSeriesを返す
        assert isinstance(signal_short, pd.Series)
        assert isinstance(signal_long, pd.Series)

    def test_volatility_impact_on_sharpe(self):
        """ボラティリティがシャープレシオに与える影響"""
        # 安定上昇 vs 高ボラティリティ
        signal_steady = risk_adjusted_return_signal(
            close=self.price_steady_up,
            lookback_period=30,
            threshold=1.0,
            ratio_type="sharpe",
            condition="above",
        )
        signal_volatile = risk_adjusted_return_signal(
            close=self.price_volatile,
            lookback_period=30,
            threshold=1.0,
            ratio_type="sharpe",
            condition="above",
        )
        # 安定上昇の方がシャープレシオが高い傾向
        assert signal_steady.sum() >= signal_volatile.sum()

    def test_sortino_vs_sharpe(self):
        """ソルティノとシャープの比較"""
        signal_sharpe = risk_adjusted_return_signal(
            close=self.price_up,
            lookback_period=30,
            threshold=1.0,
            ratio_type="sharpe",
            condition="above",
        )
        signal_sortino = risk_adjusted_return_signal(
            close=self.price_up,
            lookback_period=30,
            threshold=1.0,
            ratio_type="sortino",
            condition="above",
        )
        # 両方ともSeriesを返す（比較は実データ依存）
        assert isinstance(signal_sharpe, pd.Series)
        assert isinstance(signal_sortino, pd.Series)

    def test_nan_handling(self):
        """NaN処理テスト"""
        price_with_nan = self.price_up.copy()
        price_with_nan.iloc[10:15] = np.nan
        signal = risk_adjusted_return_signal(
            close=price_with_nan,
            lookback_period=20,
            threshold=0.5,
            ratio_type="sharpe",
            condition="above",
        )
        assert isinstance(signal, pd.Series)
        # NaNを含む期間はFalseになる
        assert not signal.isna().any()

    def test_empty_series(self):
        """空Series処理テスト"""
        empty = pd.Series(dtype=float)
        signal = risk_adjusted_return_signal(
            close=empty,
            lookback_period=20,
            threshold=0.5,
            ratio_type="sharpe",
            condition="above",
        )
        assert len(signal) == 0

    def test_invalid_ratio_type(self):
        """不正なratio_typeテスト"""
        with pytest.raises(ValueError):
            risk_adjusted_return_signal(
                close=self.price_up,
                lookback_period=20,
                threshold=0.5,
                ratio_type="invalid",
                condition="above",
            )

    def test_invalid_condition(self):
        """不正なconditionテスト"""
        with pytest.raises(ValueError):
            risk_adjusted_return_signal(
                close=self.price_up,
                lookback_period=20,
                threshold=0.5,
                ratio_type="sharpe",
                condition="invalid",
            )

    def test_zero_std_handling(self):
        """標準偏差ゼロ（完全フラット価格）処理テスト"""
        flat_price = pd.Series([100.0] * 50, index=pd.date_range("2024-01-01", periods=50))
        signal = risk_adjusted_return_signal(
            close=flat_price,
            lookback_period=20,
            threshold=0.5,
            ratio_type="sharpe",
            condition="above",
        )
        assert isinstance(signal, pd.Series)
        # 標準偏差0の場合はFalse
        assert not signal.any()

    def test_all_negative_returns(self):
        """全て負のリターン（一貫した下落）テスト"""
        signal = risk_adjusted_return_signal(
            close=self.price_down,
            lookback_period=30,
            threshold=0.0,
            ratio_type="sortino",
            condition="below",
        )
        # 下落トレンドではソルティノ < 0
        assert isinstance(signal, pd.Series)
        # 閾値0未満のシグナルが多いはず
        assert signal.sum() > 0

    def test_short_series(self):
        """短いSeries（lookback_periodより短い）テスト"""
        short_price = pd.Series([100, 101, 102], index=pd.date_range("2024-01-01", periods=3))
        signal = risk_adjusted_return_signal(
            close=short_price,
            lookback_period=20,
            threshold=0.5,
            ratio_type="sharpe",
            condition="above",
        )
        assert isinstance(signal, pd.Series)
        assert len(signal) == 3
        # lookback期間に満たないのでFalse
        assert not signal.any()

    def test_inf_handling_sharpe(self):
        """Inf値混入テスト（シャープレシオ）"""
        price_with_inf = self.price_up.copy()
        price_with_inf.iloc[20] = np.inf
        signal = risk_adjusted_return_signal(
            close=price_with_inf,
            lookback_period=20,
            threshold=0.5,
            ratio_type="sharpe",
            condition="above",
        )
        assert isinstance(signal, pd.Series)
        # Inf処理後もNaNにならない（boolに変換済み）
        assert not signal.isna().any()
        assert signal.dtype == bool

    def test_inf_handling_sortino(self):
        """Inf値混入テスト（ソルティノレシオ）"""
        price_with_inf = self.price_up.copy()
        price_with_inf.iloc[30] = -np.inf
        signal = risk_adjusted_return_signal(
            close=price_with_inf,
            lookback_period=20,
            threshold=0.5,
            ratio_type="sortino",
            condition="above",
        )
        assert isinstance(signal, pd.Series)
        assert not signal.isna().any()
        assert signal.dtype == bool
