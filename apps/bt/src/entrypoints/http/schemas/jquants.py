"""
JQuants Proxy Response Schemas

Hono hono-openapi-baseline.json と完全互換のレスポンススキーマ。
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, Field


def _empty_str_to_none(v: Any) -> Any:
    """JQuantsの空文字数値をNoneに変換"""
    if v == "":
        return None
    return v


NullableFloat = Annotated[float | None, BeforeValidator(_empty_str_to_none)]


# --- Auth Status ---


class AuthStatusResponse(BaseModel):
    """JQuants API v2 認証ステータス"""

    authenticated: bool = Field(description="API キーが設定されているか")
    hasApiKey: bool = Field(description="API キーが設定されているか")


# --- Daily Quotes ---


class DailyQuoteItem(BaseModel):
    """日足クォートデータ（JQuants 生フォーマット）"""

    Date: str
    Code: str
    O: float | None = None  # noqa: E741
    H: float | None = None
    L: float | None = None
    C: float | None = None
    UL: float | None = None
    LL: float | None = None
    Vo: float | None = None
    Va: float | None = None
    AdjFactor: float
    AdjO: float | None = None
    AdjH: float | None = None
    AdjL: float | None = None
    AdjC: float | None = None
    AdjVo: float | None = None


class DailyQuotesResponse(BaseModel):
    """日足クォートレスポンス"""

    data: list[DailyQuoteItem]
    pagination_key: str | None = None


# --- Minute Bars ---


class MinuteBarItem(BaseModel):
    """分足データ（JQuants 生フォーマット）"""

    Date: str
    Time: str
    Code: str
    O: NullableFloat = None  # noqa: E741
    H: NullableFloat = None
    L: NullableFloat = None
    C: NullableFloat = None
    Vo: NullableFloat = None
    Va: NullableFloat = None


class MinuteBarsResponse(BaseModel):
    """分足レスポンス"""

    data: list[MinuteBarItem]
    pagination_key: str | None = None


# --- Indices ---


class ApiIndex(BaseModel):
    """指数データポイント"""

    date: str = Field(description="日付 (YYYY-MM-DD)")
    code: str | None = None
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)


class ApiIndicesResponse(BaseModel):
    """指数データレスポンス"""

    indices: list[ApiIndex]
    lastUpdated: str = Field(description="最終更新日時 (ISO 8601)")


# --- Listed Info ---


class ApiListedInfo(BaseModel):
    """上場銘柄情報"""

    code: str = Field(min_length=4, max_length=4)
    companyName: str
    companyNameEnglish: str | None = None
    marketCode: str | None = None
    marketCodeName: str | None = None
    sector17Code: str | None = None
    sector17CodeName: str | None = None
    sector33Code: str | None = None
    sector33CodeName: str | None = None
    scaleCategory: str | None = None
    date: str | None = None


class ApiListedInfoResponse(BaseModel):
    """上場銘柄情報レスポンス"""

    info: list[ApiListedInfo]
    lastUpdated: str = Field(description="最終更新日時 (ISO 8601)")


# --- Margin Interest ---


class ApiMarginInterest(BaseModel):
    """週次信用取引データポイント"""

    date: str = Field(description="日付 (YYYY-MM-DD)")
    code: str = Field(min_length=4, max_length=4)
    shortMarginTradeVolume: float = Field(ge=0)
    longMarginTradeVolume: float = Field(ge=0)
    shortMarginOutstandingBalance: float | None = Field(default=None, ge=0)
    longMarginOutstandingBalance: float | None = Field(default=None, ge=0)


class ApiMarginInterestResponse(BaseModel):
    """週次信用取引データレスポンス"""

    marginInterest: list[ApiMarginInterest]
    symbol: str = Field(min_length=4, max_length=4)
    lastUpdated: str = Field(description="最終更新日時 (ISO 8601)")


# --- Statements (EPS subset) ---


class StatementItem(BaseModel):
    """財務諸表データ（EPS サブセット）"""

    DiscDate: str
    Code: str
    CurPerType: str
    CurPerSt: str
    CurPerEn: str
    EPS: NullableFloat = None
    FEPS: NullableFloat = None
    NxFEPS: NullableFloat = None
    NCEPS: NullableFloat = None
    FNCEPS: NullableFloat = None
    NxFNCEPS: NullableFloat = None


class StatementsResponse(BaseModel):
    """財務諸表レスポンス（EPS サブセット）"""

    data: list[StatementItem]
    pagination_key: str | None = None


# --- Statements Raw (complete) ---


class RawStatementItem(BaseModel):
    """財務諸表データ（完全版）"""

    # Identification
    DiscDate: str
    Code: str
    DocType: str | None = None
    # Period Information
    CurPerType: str
    CurPerSt: str
    CurPerEn: str
    CurFYSt: str | None = None
    CurFYEn: str | None = None
    NxtFYSt: str | None = None
    NxtFYEn: str | None = None
    # Financial Performance (Consolidated)
    Sales: NullableFloat = None
    OP: NullableFloat = None
    OdP: NullableFloat = None
    NP: NullableFloat = None
    EPS: NullableFloat = None
    DEPS: NullableFloat = None
    # Financial Position (Consolidated)
    TA: NullableFloat = None
    Eq: NullableFloat = None
    EqAR: NullableFloat = None
    BPS: NullableFloat = None
    # Cash Flow
    CFO: NullableFloat = None
    CFI: NullableFloat = None
    CFF: NullableFloat = None
    CashEq: NullableFloat = None
    # Share Information
    ShOutFY: NullableFloat = None
    TrShFY: NullableFloat = None
    AvgSh: NullableFloat = None
    # Forecast EPS
    FEPS: NullableFloat = None
    NxFEPS: NullableFloat = None
    # Dividend
    DivFY: NullableFloat = None
    DivAnn: NullableFloat = None
    PayoutRatioAnn: NullableFloat = None
    # Forecast Dividend / Payout Ratio
    FDivFY: NullableFloat = None
    FDivAnn: NullableFloat = None
    FPayoutRatioAnn: NullableFloat = None
    NxFDivFY: NullableFloat = None
    NxFDivAnn: NullableFloat = None
    NxFPayoutRatioAnn: NullableFloat = None
    # Non-Consolidated Financial Performance
    NCSales: NullableFloat = None
    NCOP: NullableFloat = None
    NCOdP: NullableFloat = None
    NCNP: NullableFloat = None
    NCEPS: NullableFloat = None
    # Non-Consolidated Financial Position
    NCTA: NullableFloat = None
    NCEq: NullableFloat = None
    NCEqAR: NullableFloat = None
    NCBPS: NullableFloat = None
    # Non-Consolidated Forecast EPS
    FNCEPS: NullableFloat = None
    NxFNCEPS: NullableFloat = None


class RawStatementsResponse(BaseModel):
    """財務諸表レスポンス（完全版）"""

    data: list[RawStatementItem]
    pagination_key: str | None = None


# --- TOPIX Raw ---


class TopixRawItem(BaseModel):
    """TOPIX 生データポイント"""

    Date: str
    Open: float | None = None
    High: float | None = None
    Low: float | None = None
    Close: float | None = None


class TopixRawResponse(BaseModel):
    """TOPIX 生データレスポンス"""

    topix: list[TopixRawItem]


# --- N225 Options Explorer ---


class N225OptionsNumericRange(BaseModel):
    """N225 options numeric range summary."""

    min: float | None = None
    max: float | None = None


class N225OptionItem(BaseModel):
    """Normalized Nikkei 225 option daily bar item."""

    date: str = Field(description="Trade date (YYYY-MM-DD)")
    code: str = Field(description="Option code")
    wholeDayOpen: NullableFloat = None
    wholeDayHigh: NullableFloat = None
    wholeDayLow: NullableFloat = None
    wholeDayClose: NullableFloat = None
    nightSessionOpen: NullableFloat = None
    nightSessionHigh: NullableFloat = None
    nightSessionLow: NullableFloat = None
    nightSessionClose: NullableFloat = None
    daySessionOpen: NullableFloat = None
    daySessionHigh: NullableFloat = None
    daySessionLow: NullableFloat = None
    daySessionClose: NullableFloat = None
    volume: NullableFloat = None
    openInterest: NullableFloat = None
    turnoverValue: NullableFloat = None
    contractMonth: str | None = None
    strikePrice: NullableFloat = None
    onlyAuctionVolume: NullableFloat = None
    emergencyMarginTriggerDivision: str | None = None
    emergencyMarginTriggerLabel: str | None = None
    putCallDivision: str | None = None
    putCallLabel: str | None = None
    lastTradingDay: str | None = None
    specialQuotationDay: str | None = None
    settlementPrice: NullableFloat = None
    theoreticalPrice: NullableFloat = None
    baseVolatility: NullableFloat = None
    underlyingPrice: NullableFloat = None
    impliedVolatility: NullableFloat = None
    interestRate: NullableFloat = None


class N225OptionsSummary(BaseModel):
    """N225 options summary for the resolved date."""

    totalCount: int = Field(ge=0)
    putCount: int = Field(ge=0)
    callCount: int = Field(ge=0)
    totalVolume: float = Field(ge=0)
    totalOpenInterest: float = Field(ge=0)
    strikePriceRange: N225OptionsNumericRange
    underlyingPriceRange: N225OptionsNumericRange
    settlementPriceRange: N225OptionsNumericRange


class N225OptionsExplorerResponse(BaseModel):
    """Normalized N225 options explorer response."""

    requestedDate: str | None = Field(default=None, description="Requested date if explicitly provided")
    resolvedDate: str = Field(description="Resolved trade date (YYYY-MM-DD)")
    lastUpdated: str = Field(description="Last updated timestamp (ISO 8601)")
    sourceCallCount: int = Field(ge=0, description="Number of external API calls used to fetch the resolved date")
    availableContractMonths: list[str]
    items: list[N225OptionItem]
    summary: N225OptionsSummary
