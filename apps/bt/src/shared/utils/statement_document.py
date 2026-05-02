"""Helpers for J-Quants statement document semantics."""

from __future__ import annotations

from typing import Literal

from src.shared.models.types import normalize_period_type

StatementDocumentKind = Literal[
    "financial_statement",
    "earn_forecast_revision",
    "dividend_forecast_revision",
    "unknown",
]


def classify_statement_document(type_of_document: str | None) -> StatementDocumentKind:
    value = str(type_of_document or "")
    if "FinancialStatements" in value:
        return "financial_statement"
    if "EarnForecastRevision" in value:
        return "earn_forecast_revision"
    if "DividendForecastRevision" in value:
        return "dividend_forecast_revision"
    return "unknown"


def is_actual_fy_financial_statement(
    period_type: str | None,
    type_of_document: str | None,
    *,
    allow_unknown_document: bool = False,
) -> bool:
    if normalize_period_type(period_type) != "FY":
        return False
    document_kind = classify_statement_document(type_of_document)
    if document_kind == "financial_statement":
        return True
    return allow_unknown_document and document_kind == "unknown"


def is_earn_forecast_revision_document(type_of_document: str | None) -> bool:
    return classify_statement_document(type_of_document) == "earn_forecast_revision"


def is_statement_period_financial_document(
    period_type: str | None,
    type_of_document: str | None,
    *,
    allow_unknown_document: bool = False,
) -> bool:
    normalized_period = normalize_period_type(period_type)
    if normalized_period is None:
        return False
    document_kind = classify_statement_document(type_of_document)
    if document_kind == "financial_statement":
        return True
    return allow_unknown_document and document_kind == "unknown"
