"""
共通財務計算関数のユニットテスト
"""

import numpy as np
import pandas as pd
import pytest

from src.shared.utils.financial import calc_market_cap, calc_market_cap_scalar


class TestCalcMarketCap:
    """calc_market_cap() ベクトル化版テスト"""

    def setup_method(self):
        self.dates = pd.date_range("2024-01-01", periods=10)
        self.close = pd.Series(np.ones(10) * 1000.0, index=self.dates)
        self.shares = pd.Series(np.ones(10, dtype=int) * 1_000_000, index=self.dates)
        self.treasury = pd.Series(np.ones(10, dtype=int) * 100_000, index=self.dates)

    def test_basic_floating_shares(self):
        """基本計算: 流通株式ベース"""
        result = calc_market_cap(self.close, self.shares, self.treasury, use_floating_shares=True)
        # 1000 * (1_000_000 - 100_000) = 900_000_000
        assert isinstance(result, pd.Series)
        assert len(result) == 10
        np.testing.assert_allclose(result.values, 900_000_000.0)

    def test_basic_total_shares(self):
        """基本計算: 発行済み全体"""
        result = calc_market_cap(self.close, self.shares, self.treasury, use_floating_shares=False)
        # 1000 * 1_000_000 = 1_000_000_000
        np.testing.assert_allclose(result.values, 1_000_000_000.0)

    def test_zero_shares_returns_nan(self):
        """株式数0の場合はNaN"""
        shares_zero = pd.Series(np.zeros(10, dtype=int), index=self.dates)
        result = calc_market_cap(self.close, shares_zero, self.treasury, use_floating_shares=False)
        assert result.isna().all()

    def test_negative_floating_shares_returns_nan(self):
        """流通株式が負の場合はNaN（自己株式 > 発行済み）"""
        treasury_large = pd.Series(np.ones(10, dtype=int) * 2_000_000, index=self.dates)
        result = calc_market_cap(self.close, self.shares, treasury_large, use_floating_shares=True)
        assert result.isna().all()

    def test_treasury_shares_nan_treated_as_zero(self):
        """自己株式NaNは0扱い"""
        treasury_nan = pd.Series([np.nan] * 10, index=self.dates)
        result = calc_market_cap(self.close, self.shares, treasury_nan, use_floating_shares=True)
        # 1000 * 1_000_000 = 1_000_000_000
        np.testing.assert_allclose(result.values, 1_000_000_000.0)

    def test_close_nan_propagates(self):
        """Close=NaN → NaN"""
        close_nan = self.close.copy()
        close_nan.iloc[0:3] = np.nan
        result = calc_market_cap(close_nan, self.shares, self.treasury, use_floating_shares=True)
        assert result.iloc[0:3].isna().all()
        assert result.iloc[3:].notna().all()


class TestCalcMarketCapScalar:
    """calc_market_cap_scalar() スカラー版テスト"""

    def test_basic(self):
        """基本計算"""
        result = calc_market_cap_scalar(1000.0, 1_000_000.0, 100_000.0)
        assert result == pytest.approx(900_000_000.0)

    def test_treasury_shares_none(self):
        """自己株式None → 0扱い"""
        result = calc_market_cap_scalar(1000.0, 1_000_000.0, None)
        assert result == pytest.approx(1_000_000_000.0)

    def test_zero_price(self):
        """株価0 → None"""
        assert calc_market_cap_scalar(0.0, 1_000_000.0, 0.0) is None

    def test_negative_price(self):
        """株価負 → None"""
        assert calc_market_cap_scalar(-100.0, 1_000_000.0, 0.0) is None

    def test_shares_outstanding_none(self):
        """発行済み株式数None → None"""
        assert calc_market_cap_scalar(1000.0, None, 0.0) is None  # type: ignore[arg-type]

    def test_zero_actual_shares(self):
        """実質株式数0（全て自己株式） → None"""
        assert calc_market_cap_scalar(1000.0, 100_000.0, 100_000.0) is None

    def test_negative_actual_shares(self):
        """実質株式数負 → None"""
        assert calc_market_cap_scalar(1000.0, 100_000.0, 200_000.0) is None
