"""
JQuants Proxy Service

JQuants API 生データの取得・変換ロジック。
Hono の Layer 1 JQuants Proxy と同等の機能を提供する。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.server.clients.jquants_client import JQuantsAsyncClient
from src.server.schemas.jquants import (
    ApiIndex,
    ApiIndicesResponse,
    ApiListedInfo,
    ApiListedInfoResponse,
    ApiMarginInterest,
    ApiMarginInterestResponse,
    AuthStatusResponse,
    DailyQuoteItem,
    DailyQuotesResponse,
    RawStatementItem,
    RawStatementsResponse,
    StatementItem,
    StatementsResponse,
    TopixRawItem,
    TopixRawResponse,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class JQuantsProxyService:
    """JQuants API プロキシサービス"""

    def __init__(self, client: JQuantsAsyncClient) -> None:
        self._client = client

    # --- Auth Status ---

    def get_auth_status(self) -> AuthStatusResponse:
        has_key = self._client.has_api_key
        return AuthStatusResponse(authenticated=has_key, hasApiKey=has_key)

    # --- Daily Quotes ---

    async def get_daily_quotes(
        self,
        code: str,
        date_from: str | None = None,
        date_to: str | None = None,
        date: str | None = None,
    ) -> DailyQuotesResponse:
        params: dict[str, Any] = {"code": code}
        if date_from:
            params["from"] = date_from
        if date_to:
            params["to"] = date_to
        if date:
            params["date"] = date

        body = await self._client.get("/equities/bars/daily", params)
        raw_data = body.get("daily_quotes", [])
        data = [DailyQuoteItem.model_validate(item) for item in raw_data]
        pagination_key = body.get("pagination_key")
        return DailyQuotesResponse(data=data, pagination_key=pagination_key)

    # --- Indices ---

    async def get_indices(
        self,
        code: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        date: str | None = None,
    ) -> ApiIndicesResponse:
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date_from:
            params["from"] = date_from
        if date_to:
            params["to"] = date_to
        if date:
            params["date"] = date

        body = await self._client.get("/indices/bars/daily", params)
        raw_data = body.get("indices", [])

        indices = []
        for item in raw_data:
            indices.append(
                ApiIndex(
                    date=item.get("Date", ""),
                    code=item.get("Code"),
                    open=item.get("Open", 0),
                    high=item.get("High", 0),
                    low=item.get("Low", 0),
                    close=item.get("Close", 0),
                )
            )
        return ApiIndicesResponse(indices=indices, lastUpdated=_now_iso())

    # --- Listed Info ---

    async def get_listed_info(
        self,
        code: str | None = None,
        date: str | None = None,
    ) -> ApiListedInfoResponse:
        params: dict[str, Any] = {}
        if code:
            params["code"] = code
        if date:
            params["date"] = date

        body = await self._client.get("/equities/master", params)
        raw_data = body.get("info", [])

        info = []
        for item in raw_data:
            info.append(
                ApiListedInfo(
                    code=str(item.get("Code", ""))[:4],
                    companyName=item.get("CompanyName", ""),
                    companyNameEnglish=item.get("CompanyNameEnglish"),
                    marketCode=item.get("MarketCode"),
                    marketCodeName=item.get("MarketCodeName"),
                    sector33Code=item.get("Sector33Code"),
                    sector33CodeName=item.get("Sector33CodeName"),
                    scaleCategory=item.get("ScaleCategory"),
                )
            )
        return ApiListedInfoResponse(info=info, lastUpdated=_now_iso())

    # --- Margin Interest ---

    async def get_margin_interest(
        self,
        symbol: str,
        date_from: str | None = None,
        date_to: str | None = None,
        date: str | None = None,
    ) -> ApiMarginInterestResponse:
        params: dict[str, Any] = {"code": f"{symbol}0"}
        if date_from:
            params["from"] = date_from
        if date_to:
            params["to"] = date_to
        if date:
            params["date"] = date

        body = await self._client.get("/markets/margin-interest", params)
        raw_data = body.get("weekly_margin_interest", [])

        margin_interest = []
        for item in raw_data:
            margin_interest.append(
                ApiMarginInterest(
                    date=item.get("Date", ""),
                    code=str(item.get("Code", ""))[:4],
                    shortMarginTradeVolume=item.get("ShortSellingWithRestrictions", 0),
                    longMarginTradeVolume=item.get("MarginBuyingNew", 0),
                    shortMarginOutstandingBalance=item.get("ShortSellingOutstandingBalance"),
                    longMarginOutstandingBalance=item.get("MarginBuyingOutstandingBalance"),
                )
            )
        return ApiMarginInterestResponse(
            marginInterest=margin_interest,
            symbol=symbol,
            lastUpdated=_now_iso(),
        )

    # --- Statements (EPS subset) ---

    async def get_statements(self, code: str) -> StatementsResponse:
        body = await self._client.get("/fins/statements", {"code": code})
        raw_data = body.get("statements", [])

        data = []
        for stmt in raw_data:
            data.append(
                StatementItem(
                    DiscDate=stmt.get("DisclosedDate", ""),
                    Code=str(stmt.get("LocalCode", ""))[:4],
                    CurPerType=stmt.get("CurrentPeriodEndDate", "")[-4:] if stmt.get("TypeOfCurrentPeriod") else stmt.get("TypeOfCurrentPeriod", ""),
                    CurPerSt=stmt.get("CurrentPeriodStartDate", ""),
                    CurPerEn=stmt.get("CurrentPeriodEndDate", ""),
                    EPS=stmt.get("EarningsPerShare"),
                    FEPS=stmt.get("ForecastEarningsPerShare"),
                    NxFEPS=stmt.get("NextYearForecastEarningsPerShare"),
                    NCEPS=stmt.get("NonConsolidatedEarningsPerShare"),
                    FNCEPS=stmt.get("ForecastNonConsolidatedEarningsPerShare"),
                    NxFNCEPS=stmt.get("NextYearForecastNonConsolidatedEarningsPerShare"),
                )
            )
        return StatementsResponse(data=data)

    # --- Statements Raw (complete) ---

    async def get_statements_raw(self, code: str) -> RawStatementsResponse:
        body = await self._client.get("/fins/statements", {"code": code})
        raw_data = body.get("statements", [])

        data = []
        for stmt in raw_data:
            data.append(
                RawStatementItem(
                    DiscDate=stmt.get("DisclosedDate", ""),
                    Code=str(stmt.get("LocalCode", ""))[:4],
                    DocType=stmt.get("TypeOfDocument"),
                    CurPerType=stmt.get("TypeOfCurrentPeriod", ""),
                    CurPerSt=stmt.get("CurrentPeriodStartDate", ""),
                    CurPerEn=stmt.get("CurrentPeriodEndDate", ""),
                    CurFYSt=stmt.get("CurrentFiscalYearStartDate"),
                    CurFYEn=stmt.get("CurrentFiscalYearEndDate"),
                    NxtFYSt=stmt.get("NextFiscalYearStartDate"),
                    NxtFYEn=stmt.get("NextFiscalYearEndDate"),
                    Sales=stmt.get("NetSales"),
                    OP=stmt.get("OperatingProfit"),
                    OdP=stmt.get("OrdinaryProfit"),
                    NP=stmt.get("Profit"),
                    EPS=stmt.get("EarningsPerShare"),
                    DEPS=stmt.get("DilutedEarningsPerShare"),
                    TA=stmt.get("TotalAssets"),
                    Eq=stmt.get("Equity"),
                    EqAR=stmt.get("EquityToAssetRatio"),
                    BPS=stmt.get("BookValuePerShare"),
                    CFO=stmt.get("CashFlowsFromOperatingActivities"),
                    CFI=stmt.get("CashFlowsFromInvestingActivities"),
                    CFF=stmt.get("CashFlowsFromFinancingActivities"),
                    CashEq=stmt.get("CashAndEquivalents"),
                    ShOutFY=stmt.get("NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock"),
                    TrShFY=stmt.get("NumberOfTreasuryStockAtTheEndOfFiscalYear"),
                    AvgSh=stmt.get("AverageNumberOfShares"),
                    FEPS=stmt.get("ForecastEarningsPerShare"),
                    NxFEPS=stmt.get("NextYearForecastEarningsPerShare"),
                    NCSales=stmt.get("NonConsolidatedNetSales"),
                    NCOP=stmt.get("NonConsolidatedOperatingProfit"),
                    NCOdP=stmt.get("NonConsolidatedOrdinaryProfit"),
                    NCNP=stmt.get("NonConsolidatedProfit"),
                    NCEPS=stmt.get("NonConsolidatedEarningsPerShare"),
                    NCTA=stmt.get("NonConsolidatedTotalAssets"),
                    NCEq=stmt.get("NonConsolidatedEquity"),
                    NCEqAR=stmt.get("NonConsolidatedEquityToAssetRatio"),
                    NCBPS=stmt.get("NonConsolidatedBookValuePerShare"),
                    FNCEPS=stmt.get("ForecastNonConsolidatedEarningsPerShare"),
                    NxFNCEPS=stmt.get("NextYearForecastNonConsolidatedEarningsPerShare"),
                )
            )
        return RawStatementsResponse(data=data)

    # --- TOPIX Raw ---

    async def get_topix(
        self,
        date_from: str | None = None,
        date_to: str | None = None,
        date: str | None = None,
    ) -> TopixRawResponse:
        params: dict[str, Any] = {"code": "0000"}  # TOPIX code
        if date_from:
            params["from"] = date_from
        if date_to:
            params["to"] = date_to
        if date:
            params["date"] = date

        body = await self._client.get("/indices/bars/daily", params)
        raw_data = body.get("indices", [])

        topix = []
        for item in raw_data:
            topix.append(
                TopixRawItem(
                    Date=item.get("Date", ""),
                    Open=item.get("Open"),
                    High=item.get("High"),
                    Low=item.get("Low"),
                    Close=item.get("Close"),
                )
            )
        return TopixRawResponse(topix=topix)
