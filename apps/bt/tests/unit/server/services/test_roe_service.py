"""
ROE Service Unit Tests
"""

import pytest

from src.server.services.roe_service import (
    ROEService,
    _calculate_single_roe,
    _normalize_period_type,
    _is_quarterly,
    _should_prefer,
)


class TestNormalizePeriodType:
    def test_fy(self):
        assert _normalize_period_type("FY") == "FY"

    def test_1q(self):
        assert _normalize_period_type("1Q") == "1Q"

    def test_2q(self):
        assert _normalize_period_type("2Q") == "2Q"

    def test_3q(self):
        assert _normalize_period_type("3Q") == "3Q"

    def test_empty(self):
        assert _normalize_period_type("") == "FY"

    def test_q1_format(self):
        assert _normalize_period_type("Q1") == "1Q"

    def test_half(self):
        assert _normalize_period_type("HALF") == "2Q"


class TestIsQuarterly:
    def test_quarterly(self):
        assert _is_quarterly("1Q") is True
        assert _is_quarterly("2Q") is True
        assert _is_quarterly("3Q") is True

    def test_full_year(self):
        assert _is_quarterly("FY") is False


class TestCalculateSingleROE:
    def test_basic_roe(self):
        """基本的な ROE 計算"""
        stmt = {
            "LocalCode": "72030",
            "TypeOfCurrentPeriod": "FY",
            "CurrentPeriodEndDate": "2024-03-31",
            "TypeOfDocument": "ConsolidatedAnnual",
            "Profit": 250000,
            "Equity": 1500000,
        }
        result = _calculate_single_roe(stmt)
        assert result is not None
        assert abs(result.roe - 16.6667) < 0.01
        assert result.metadata.code == "7203"
        assert result.metadata.periodType == "FY"
        assert result.metadata.isConsolidated is True

    def test_annualize_1q(self):
        """1Q の年換算"""
        stmt = {
            "LocalCode": "72030",
            "TypeOfCurrentPeriod": "1Q",
            "CurrentPeriodEndDate": "2024-06-30",
            "TypeOfDocument": "Consolidated1Q",
            "Profit": 50000,
            "Equity": 1500000,
        }
        result = _calculate_single_roe(stmt, annualize=True)
        assert result is not None
        # 50000 * 4 / 1500000 * 100 = 13.33%
        assert abs(result.roe - 13.3333) < 0.01
        assert result.metadata.isAnnualized is True

    def test_annualize_2q(self):
        """2Q の年換算"""
        stmt = {
            "LocalCode": "72030",
            "TypeOfCurrentPeriod": "2Q",
            "CurrentPeriodEndDate": "2024-09-30",
            "TypeOfDocument": "Consolidated2Q",
            "Profit": 100000,
            "Equity": 1500000,
        }
        result = _calculate_single_roe(stmt, annualize=True)
        assert result is not None
        # 100000 * 2 / 1500000 * 100 = 13.33%
        assert abs(result.roe - 13.3333) < 0.01

    def test_no_annualize(self):
        """年換算なし"""
        stmt = {
            "LocalCode": "72030",
            "TypeOfCurrentPeriod": "1Q",
            "CurrentPeriodEndDate": "2024-06-30",
            "TypeOfDocument": "Consolidated",
            "Profit": 50000,
            "Equity": 1500000,
        }
        result = _calculate_single_roe(stmt, annualize=False)
        assert result is not None
        # 50000 / 1500000 * 100 = 3.33%
        assert abs(result.roe - 3.3333) < 0.01

    def test_non_consolidated_fallback(self):
        """連結がない場合の非連結フォールバック"""
        stmt = {
            "LocalCode": "72030",
            "TypeOfCurrentPeriod": "FY",
            "CurrentPeriodEndDate": "2024-03-31",
            "TypeOfDocument": "NonConsolidated",
            "Profit": None,
            "Equity": None,
            "NonConsolidatedProfit": 50000,
            "NonConsolidatedEquity": 500000,
        }
        result = _calculate_single_roe(stmt, prefer_consolidated=True)
        assert result is not None
        assert abs(result.roe - 10.0) < 0.01

    def test_below_min_equity(self):
        """最低自己資本未満"""
        stmt = {
            "LocalCode": "72030",
            "TypeOfCurrentPeriod": "FY",
            "CurrentPeriodEndDate": "2024-03-31",
            "TypeOfDocument": "Consolidated",
            "Profit": 500,
            "Equity": 100,
        }
        result = _calculate_single_roe(stmt, min_equity=1000)
        assert result is None

    def test_zero_equity(self):
        """自己資本ゼロ"""
        stmt = {
            "LocalCode": "72030",
            "TypeOfCurrentPeriod": "FY",
            "CurrentPeriodEndDate": "2024-03-31",
            "TypeOfDocument": "Consolidated",
            "Profit": 50000,
            "Equity": 0,
        }
        result = _calculate_single_roe(stmt)
        assert result is None

    def test_missing_profit(self):
        """利益なし"""
        stmt = {
            "LocalCode": "72030",
            "TypeOfCurrentPeriod": "FY",
            "CurrentPeriodEndDate": "2024-03-31",
            "TypeOfDocument": "Consolidated",
            "Equity": 1500000,
        }
        result = _calculate_single_roe(stmt)
        assert result is None

    def test_accounting_standard_ifrs(self):
        """IFRS 会計基準の検出"""
        stmt = {
            "LocalCode": "72030",
            "TypeOfCurrentPeriod": "FY",
            "CurrentPeriodEndDate": "2024-03-31",
            "TypeOfDocument": "ConsolidatedIFRS",
            "Profit": 250000,
            "Equity": 1500000,
        }
        result = _calculate_single_roe(stmt)
        assert result is not None
        assert result.metadata.accountingStandard == "IFRS"


class TestShouldPrefer:
    def test_prefer_fy_over_quarterly(self):
        """FY を Q より優先"""
        fy = {"TypeOfCurrentPeriod": "FY", "CurrentPeriodEndDate": "2024-03-31"}
        q1 = {"TypeOfCurrentPeriod": "1Q", "CurrentPeriodEndDate": "2024-06-30"}
        assert _should_prefer(fy, q1) is True
        assert _should_prefer(q1, fy) is False

    def test_prefer_newer(self):
        """同期間タイプなら新しい方を優先"""
        old = {"TypeOfCurrentPeriod": "FY", "CurrentPeriodEndDate": "2023-03-31"}
        new = {"TypeOfCurrentPeriod": "FY", "CurrentPeriodEndDate": "2024-03-31"}
        assert _should_prefer(new, old) is True
        assert _should_prefer(old, new) is False
