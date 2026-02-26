"""
ROE Service Unit Tests
"""

from src.domains.fundamentals.roe import (
    calculate_single_roe as _calculate_single_roe,
    is_quarterly as _is_quarterly,
    normalize_period_type as _normalize_period_type,
    should_prefer as _should_prefer,
)
from src.application.services.roe_service import (
    _to_response_item,
)


def _as_response(stmt, annualize=True, prefer_consolidated=True, min_equity=1000):
    result = _calculate_single_roe(
        stmt,
        annualize=annualize,
        prefer_consolidated=prefer_consolidated,
        min_equity=min_equity,
    )
    return None if result is None else _to_response_item(result)




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
            "Code": "72030",
            "CurPerType": "FY",
            "CurPerEn": "2024-03-31",
            "DocType": "ConsolidatedAnnual",
            "NP": 250000,
            "Eq": 1500000,
        }
        result = _as_response(stmt)
        assert result is not None
        assert abs(result.roe - 16.6667) < 0.01
        assert result.metadata.code == "7203"
        assert result.metadata.periodType == "FY"
        assert result.metadata.isConsolidated is True

    def test_annualize_1q(self):
        """1Q の年換算"""
        stmt = {
            "Code": "72030",
            "CurPerType": "1Q",
            "CurPerEn": "2024-06-30",
            "DocType": "Consolidated1Q",
            "NP": 50000,
            "Eq": 1500000,
        }
        result = _as_response(stmt, annualize=True)
        assert result is not None
        # 50000 * 4 / 1500000 * 100 = 13.33%
        assert abs(result.roe - 13.3333) < 0.01
        assert result.metadata.isAnnualized is True

    def test_annualize_2q(self):
        """2Q の年換算"""
        stmt = {
            "Code": "72030",
            "CurPerType": "2Q",
            "CurPerEn": "2024-09-30",
            "DocType": "Consolidated2Q",
            "NP": 100000,
            "Eq": 1500000,
        }
        result = _as_response(stmt, annualize=True)
        assert result is not None
        # 100000 * 2 / 1500000 * 100 = 13.33%
        assert abs(result.roe - 13.3333) < 0.01

    def test_no_annualize(self):
        """年換算なし"""
        stmt = {
            "Code": "72030",
            "CurPerType": "1Q",
            "CurPerEn": "2024-06-30",
            "DocType": "Consolidated",
            "NP": 50000,
            "Eq": 1500000,
        }
        result = _as_response(stmt, annualize=False)
        assert result is not None
        # 50000 / 1500000 * 100 = 3.33%
        assert abs(result.roe - 3.3333) < 0.01

    def test_non_consolidated_fallback(self):
        """連結がない場合の非連結フォールバック"""
        stmt = {
            "Code": "72030",
            "CurPerType": "FY",
            "CurPerEn": "2024-03-31",
            "DocType": "NonConsolidated",
            "NP": None,
            "Eq": None,
            "NCNP": 50000,
            "NCEq": 500000,
        }
        result = _as_response(stmt, prefer_consolidated=True)
        assert result is not None
        assert abs(result.roe - 10.0) < 0.01

    def test_below_min_equity(self):
        """最低自己資本未満"""
        stmt = {
            "Code": "72030",
            "CurPerType": "FY",
            "CurPerEn": "2024-03-31",
            "DocType": "Consolidated",
            "NP": 500,
            "Eq": 100,
        }
        result = _as_response(stmt, min_equity=1000)
        assert result is None

    def test_zero_equity(self):
        """自己資本ゼロ"""
        stmt = {
            "Code": "72030",
            "CurPerType": "FY",
            "CurPerEn": "2024-03-31",
            "DocType": "Consolidated",
            "NP": 50000,
            "Eq": 0,
        }
        result = _as_response(stmt)
        assert result is None

    def test_missing_profit(self):
        """利益なし"""
        stmt = {
            "Code": "72030",
            "CurPerType": "FY",
            "CurPerEn": "2024-03-31",
            "DocType": "Consolidated",
            "Eq": 1500000,
        }
        result = _as_response(stmt)
        assert result is None

    def test_accounting_standard_ifrs(self):
        """IFRS 会計基準の検出"""
        stmt = {
            "Code": "72030",
            "CurPerType": "FY",
            "CurPerEn": "2024-03-31",
            "DocType": "ConsolidatedIFRS",
            "NP": 250000,
            "Eq": 1500000,
        }
        result = _as_response(stmt)
        assert result is not None
        assert result.metadata.accountingStandard == "IFRS"


class TestShouldPrefer:
    def test_prefer_fy_over_quarterly(self):
        """FY を Q より優先"""
        fy = {"CurPerType": "FY", "CurPerEn": "2024-03-31"}
        q1 = {"CurPerType": "1Q", "CurPerEn": "2024-06-30"}
        assert _should_prefer(fy, q1) is True
        assert _should_prefer(q1, fy) is False

    def test_prefer_newer(self):
        """同期間タイプなら新しい方を優先"""
        old = {"CurPerType": "FY", "CurPerEn": "2023-03-31"}
        new = {"CurPerType": "FY", "CurPerEn": "2024-03-31"}
        assert _should_prefer(new, old) is True
        assert _should_prefer(old, new) is False
