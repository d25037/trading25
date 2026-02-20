"""
JQuants Proxy Service

JQuants API 生データの取得・変換ロジック。
Hono の Layer 1 JQuants Proxy と同等の機能を提供する。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from loguru import logger

from src.server.clients.jquants_client import JQuantsAsyncClient
from src.server.middleware.correlation import get_correlation_id
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
from src.server.services.expiring_singleflight_cache import CacheState, ExpiringSingleFlightCache


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


MARGIN_INTEREST_TTL_SECONDS = 5 * 60
FINS_SUMMARY_TTL_SECONDS = 15 * 60


def _build_cache_key(path: str, params: dict[str, Any]) -> str:
    serialized = json.dumps(params, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"{path}:{serialized}"


class JQuantsProxyService:
    """JQuants API プロキシサービス"""

    def __init__(self, client: JQuantsAsyncClient) -> None:
        self._client = client
        self._cache = ExpiringSingleFlightCache[dict[str, Any]]()

    async def _get_cached(
        self,
        path: str,
        params: dict[str, Any],
        ttl_seconds: int,
    ) -> dict[str, Any]:
        key = _build_cache_key(path, params)
        body, state = await self._cache.get_or_set(
            key=key,
            ttl_seconds=ttl_seconds,
            fetcher=lambda: self._client.get(path, params),
        )
        self._log_cache_state(path, state, key)
        return body

    @staticmethod
    def _log_cache_state(path: str, state: CacheState, key: str) -> None:
        logger.info(
            f"JQuants cache {state}: {path}",
            event="jquants_proxy_cache",
            cacheState=state,
            endpoint=path,
            cacheKey=key,
            correlationId=get_correlation_id(),
        )

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
        raw_data = body.get("data", [])
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
        raw_data = body.get("data", [])

        indices = []
        for item in raw_data:
            indices.append(
                ApiIndex(
                    date=item.get("Date", ""),
                    code=item.get("Code"),
                    open=item.get("O", 0),
                    high=item.get("H", 0),
                    low=item.get("L", 0),
                    close=item.get("C", 0),
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
        raw_data = body.get("data", [])

        info = []
        for item in raw_data:
            info.append(
                ApiListedInfo(
                    code=str(item.get("Code", ""))[:4],
                    companyName=item.get("CoName", ""),
                    companyNameEnglish=item.get("CoNameEn"),
                    marketCode=item.get("Mkt"),
                    marketCodeName=item.get("MktNm"),
                    sector17Code=item.get("S17"),
                    sector17CodeName=item.get("S17Nm"),
                    sector33Code=item.get("S33"),
                    sector33CodeName=item.get("S33Nm"),
                    scaleCategory=item.get("ScaleCat"),
                    date=item.get("Date"),
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

        body = await self._get_cached(
            "/markets/margin-interest",
            params,
            ttl_seconds=MARGIN_INTEREST_TTL_SECONDS,
        )
        raw_data = body.get("data", [])

        margin_interest = []
        for item in raw_data:
            margin_interest.append(
                ApiMarginInterest(
                    date=item.get("Date", ""),
                    code=str(item.get("Code", ""))[:4],
                    shortMarginTradeVolume=item.get("ShrtStdVol", 0),
                    longMarginTradeVolume=item.get("LongStdVol", 0),
                    shortMarginOutstandingBalance=item.get("ShrtVol"),
                    longMarginOutstandingBalance=item.get("LongVol"),
                )
            )
        return ApiMarginInterestResponse(
            marginInterest=margin_interest,
            symbol=symbol,
            lastUpdated=_now_iso(),
        )

    # --- Statements (EPS subset) ---

    async def get_statements(self, code: str) -> StatementsResponse:
        body = await self._get_cached(
            "/fins/summary",
            {"code": code},
            ttl_seconds=FINS_SUMMARY_TTL_SECONDS,
        )
        raw_data = body.get("data", [])

        data = []
        for stmt in raw_data:
            data.append(
                StatementItem(
                    DiscDate=stmt.get("DiscDate", ""),
                    Code=str(stmt.get("Code", ""))[:4],
                    CurPerType=stmt.get("CurPerType", ""),
                    CurPerSt=stmt.get("CurPerSt", ""),
                    CurPerEn=stmt.get("CurPerEn", ""),
                    EPS=stmt.get("EPS"),
                    FEPS=stmt.get("FEPS"),
                    NxFEPS=stmt.get("NxFEPS"),
                    NCEPS=stmt.get("NCEPS"),
                    FNCEPS=stmt.get("FNCEPS"),
                    NxFNCEPS=stmt.get("NxFNCEPS"),
                )
            )
        return StatementsResponse(data=data)

    # --- Statements Raw (complete) ---

    async def get_statements_raw(self, code: str) -> RawStatementsResponse:
        body = await self._get_cached(
            "/fins/summary",
            {"code": code},
            ttl_seconds=FINS_SUMMARY_TTL_SECONDS,
        )
        raw_data = body.get("data", [])

        data = []
        for stmt in raw_data:
            data.append(
                RawStatementItem(
                    DiscDate=stmt.get("DiscDate", ""),
                    Code=str(stmt.get("Code", ""))[:4],
                    DocType=stmt.get("DocType"),
                    CurPerType=stmt.get("CurPerType", ""),
                    CurPerSt=stmt.get("CurPerSt", ""),
                    CurPerEn=stmt.get("CurPerEn", ""),
                    CurFYSt=stmt.get("CurFYSt"),
                    CurFYEn=stmt.get("CurFYEn"),
                    NxtFYSt=stmt.get("NxtFYSt"),
                    NxtFYEn=stmt.get("NxtFYEn"),
                    Sales=stmt.get("Sales"),
                    OP=stmt.get("OP"),
                    OdP=stmt.get("OdP"),
                    NP=stmt.get("NP"),
                    EPS=stmt.get("EPS"),
                    DEPS=stmt.get("DEPS"),
                    TA=stmt.get("TA"),
                    Eq=stmt.get("Eq"),
                    EqAR=stmt.get("EqAR"),
                    BPS=stmt.get("BPS"),
                    CFO=stmt.get("CFO"),
                    CFI=stmt.get("CFI"),
                    CFF=stmt.get("CFF"),
                    CashEq=stmt.get("CashEq"),
                    ShOutFY=stmt.get("ShOutFY"),
                    TrShFY=stmt.get("TrShFY"),
                    AvgSh=stmt.get("AvgSh"),
                    FEPS=stmt.get("FEPS"),
                    NxFEPS=stmt.get("NxFEPS"),
                    DivFY=stmt.get("DivFY"),
                    DivAnn=stmt.get("DivAnn"),
                    PayoutRatioAnn=stmt.get("PayoutRatioAnn"),
                    FDivFY=stmt.get("FDivFY"),
                    FDivAnn=stmt.get("FDivAnn"),
                    FPayoutRatioAnn=stmt.get("FPayoutRatioAnn"),
                    NxFDivFY=stmt.get("NxFDivFY"),
                    NxFDivAnn=stmt.get("NxFDivAnn"),
                    NxFPayoutRatioAnn=stmt.get("NxFPayoutRatioAnn"),
                    NCSales=stmt.get("NCSales"),
                    NCOP=stmt.get("NCOP"),
                    NCOdP=stmt.get("NCOdP"),
                    NCNP=stmt.get("NCNP"),
                    NCEPS=stmt.get("NCEPS"),
                    NCTA=stmt.get("NCTA"),
                    NCEq=stmt.get("NCEq"),
                    NCEqAR=stmt.get("NCEqAR"),
                    NCBPS=stmt.get("NCBPS"),
                    FNCEPS=stmt.get("FNCEPS"),
                    NxFNCEPS=stmt.get("NxFNCEPS"),
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
        raw_data = body.get("data", [])

        topix = []
        for item in raw_data:
            topix.append(
                TopixRawItem(
                    Date=item.get("Date", ""),
                    Open=item.get("O"),
                    High=item.get("H"),
                    Low=item.get("L"),
                    Close=item.get("C"),
                )
            )
        return TopixRawResponse(topix=topix)
