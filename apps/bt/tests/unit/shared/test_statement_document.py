from __future__ import annotations

from src.shared.models.types import normalize_period_type
from src.shared.utils.statement_document import (
    classify_statement_document,
    is_actual_fy_financial_statement,
    is_earn_forecast_revision_document,
    is_statement_period_financial_document,
)


def test_classify_statement_document_kinds() -> None:
    assert (
        classify_statement_document("FYFinancialStatements_Consolidated_IFRS")
        == "financial_statement"
    )
    assert (
        classify_statement_document("EarnForecastRevision") == "earn_forecast_revision"
    )
    assert (
        classify_statement_document("REITDividendForecastRevision")
        == "dividend_forecast_revision"
    )
    assert classify_statement_document(None) == "unknown"


def test_actual_fy_financial_statement_requires_period_and_document_kind() -> None:
    assert is_actual_fy_financial_statement(
        "FY",
        "FYFinancialStatements_Consolidated_JP",
    )
    assert not is_actual_fy_financial_statement("FY", "EarnForecastRevision")
    assert not is_actual_fy_financial_statement(
        "3Q",
        "3QFinancialStatements_Consolidated_JP",
    )
    assert is_actual_fy_financial_statement(
        "FY",
        None,
        allow_unknown_document=True,
    )


def test_period_normalization_accepts_jquants_4q_5q() -> None:
    assert normalize_period_type("4Q") == "4Q"
    assert normalize_period_type("5Q") == "5Q"
    assert is_earn_forecast_revision_document("REITEarnForecastRevision")
    assert is_statement_period_financial_document(
        "2Q",
        "2QFinancialStatements_Consolidated_JP",
    )
    assert not is_statement_period_financial_document("FY", "EarnForecastRevision")
