"""Adapters from local market DB statement rows to fundamentals domain inputs."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.infrastructure.external_api.jquants_client import JQuantsStatement


def _get(row: Any, *keys: str) -> Any:
    for key in keys:
        if isinstance(row, dict) and key in row:
            return row[key]
        getter = getattr(row, "get", None)
        if callable(getter):
            value = getter(key)
            if value is not None:
                return value
        try:
            value = row[key]
        except (KeyError, IndexError, TypeError):
            continue
        if value is not None:
            return value
    return None


def _normalize_optional_scalar(value: Any) -> Any | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if pd.isna(value):
        return None
    return value


def _normalize_optional_float(value: Any) -> float | None:
    normalized = _normalize_optional_scalar(value)
    if normalized is None:
        return None
    return float(normalized)


def _normalize_optional_text(value: Any) -> str | None:
    normalized = _normalize_optional_scalar(value)
    if normalized is None:
        return None
    return str(normalized)


def _normalize_required_text(value: Any, fallback: str = "") -> str:
    normalized = _normalize_optional_text(value)
    return normalized if normalized is not None else fallback


def market_statement_row_to_jquants_statement(
    row: Any,
    *,
    code_fallback: str = "",
    disclosed_at: Any | None = None,
) -> JQuantsStatement:
    disclosed_date = _normalize_required_text(
        disclosed_at if disclosed_at is not None else _get(row, "disclosed_date", "disclosedDate")
    )
    period_end = (
        _normalize_optional_text(_get(row, "periodEnd", "period_end", "curPerEn"))
        or disclosed_date
    )
    code = _normalize_required_text(_get(row, "code"), fallback=code_fallback)
    doc_type = _normalize_required_text(_get(row, "type_of_document", "typeOfDocument"))
    period_type = _normalize_required_text(
        _get(row, "type_of_current_period", "typeOfCurrentPeriod")
    )
    dividend_fy = _normalize_optional_float(_get(row, "dividend_fy", "dividendFY"))
    forecast_dividend_fy = _normalize_optional_float(
        _get(row, "forecast_dividend_fy", "forecastDividendFY")
    )
    next_year_forecast_dividend_fy = _normalize_optional_float(
        _get(row, "next_year_forecast_dividend_fy", "nextYearForecastDividendFY")
    )

    return JQuantsStatement(
        DiscDate=disclosed_date,
        Code=code,
        DocType=doc_type,
        CurPerType=period_type,
        CurPerSt=period_end,
        CurPerEn=period_end,
        CurFYSt=period_end,
        CurFYEn=period_end,
        NxtFYSt=None,
        NxtFYEn=None,
        Sales=_normalize_optional_float(_get(row, "sales")),
        OP=_normalize_optional_float(_get(row, "operating_profit", "operatingProfit")),
        OdP=_normalize_optional_float(_get(row, "ordinary_profit", "ordinaryProfit")),
        NP=_normalize_optional_float(_get(row, "profit")),
        EPS=_normalize_optional_float(_get(row, "earnings_per_share", "earningsPerShare")),
        DEPS=None,
        TA=_normalize_optional_float(_get(row, "total_assets", "totalAssets")),
        Eq=_normalize_optional_float(_get(row, "equity")),
        EqAR=None,
        BPS=_normalize_optional_float(_get(row, "bps")),
        CFO=_normalize_optional_float(_get(row, "operating_cash_flow", "operatingCashFlow")),
        CFI=_normalize_optional_float(_get(row, "investing_cash_flow", "investingCashFlow")),
        CFF=_normalize_optional_float(_get(row, "financing_cash_flow", "financingCashFlow")),
        CashEq=_normalize_optional_float(_get(row, "cash_and_equivalents", "cashAndEquivalents")),
        ShOutFY=_normalize_optional_float(_get(row, "shares_outstanding", "sharesOutstanding")),
        TrShFY=_normalize_optional_float(_get(row, "treasury_shares", "treasuryShares")),
        AvgSh=_normalize_optional_float(_get(row, "shares_outstanding", "sharesOutstanding")),
        FEPS=_normalize_optional_float(_get(row, "forecast_eps", "forecastEps")),
        NxFEPS=_normalize_optional_float(
            _get(row, "next_year_forecast_earnings_per_share", "nextYearForecastEarningsPerShare")
        ),
        DivFY=dividend_fy,
        DivAnn=dividend_fy,
        PayoutRatioAnn=_normalize_optional_float(_get(row, "payout_ratio", "payoutRatio")),
        FDivFY=forecast_dividend_fy,
        FDivAnn=forecast_dividend_fy,
        FPayoutRatioAnn=_normalize_optional_float(
            _get(row, "forecast_payout_ratio", "forecastPayoutRatio")
        ),
        NxFDivFY=next_year_forecast_dividend_fy,
        NxFDivAnn=next_year_forecast_dividend_fy,
        NxFPayoutRatioAnn=_normalize_optional_float(
            _get(row, "next_year_forecast_payout_ratio", "nextYearForecastPayoutRatio")
        ),
        NCSales=None,
        NCOP=None,
        NCOdP=None,
        NCNP=None,
        NCEPS=None,
        NCTA=None,
        NCEq=None,
        NCEqAR=None,
        NCBPS=None,
        FNCEPS=None,
        NxFNCEPS=None,
    )
