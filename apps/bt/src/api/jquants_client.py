"""API client for JQuants data via apps/ts/api proxy."""

from __future__ import annotations

from typing import Annotated, Any, Optional

import pandas as pd
from pydantic import BaseModel, BeforeValidator

from src.api.client import BaseAPIClient
from src.api.exceptions import APINotFoundError


def _empty_str_to_none(v: Any) -> Any:
    """Convert empty string to None for numeric fields.

    JQuants API sometimes returns empty strings instead of null for missing values.
    """
    if v == "":
        return None
    return v


# Type alias for nullable float fields that may receive empty strings from API
NullableFloat = Annotated[float | None, BeforeValidator(_empty_str_to_none)]


class JQuantsStatement(BaseModel):
    """Raw JQuants statement data model."""

    # Identification
    DiscDate: str
    Code: str
    DocType: str
    # Period Information
    CurPerType: str
    CurPerSt: str
    CurPerEn: str
    CurFYSt: str
    CurFYEn: str
    NxtFYSt: str | None
    NxtFYEn: str | None
    # Financial Performance (Consolidated)
    Sales: NullableFloat
    OP: NullableFloat
    OdP: NullableFloat
    NP: NullableFloat
    EPS: NullableFloat
    DEPS: NullableFloat
    # Financial Position (Consolidated)
    TA: NullableFloat
    Eq: NullableFloat
    EqAR: NullableFloat
    BPS: NullableFloat
    # Cash Flow
    CFO: NullableFloat
    CFI: NullableFloat
    CFF: NullableFloat
    CashEq: NullableFloat
    # Share Information
    ShOutFY: NullableFloat
    TrShFY: NullableFloat
    AvgSh: NullableFloat
    # Forecast EPS
    FEPS: NullableFloat
    NxFEPS: NullableFloat
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
    NCSales: NullableFloat
    NCOP: NullableFloat
    NCOdP: NullableFloat
    NCNP: NullableFloat
    NCEPS: NullableFloat
    # Non-Consolidated Financial Position
    NCTA: NullableFloat
    NCEq: NullableFloat
    NCEqAR: NullableFloat
    NCBPS: NullableFloat
    # Non-Consolidated Forecast EPS
    FNCEPS: NullableFloat
    NxFNCEPS: NullableFloat


class StockInfo(BaseModel):
    """Stock information from market.db."""

    code: str
    companyName: str
    companyNameEnglish: str
    marketCode: str
    marketName: str
    sector17Code: str
    sector17Name: str
    sector33Code: str
    sector33Name: str
    scaleCategory: str
    listedDate: str


class JQuantsAPIClient(BaseAPIClient):
    """API client for JQuants data via apps/ts/api proxy.

    This client provides access to JQuants financial statements and margin data
    through the apps/ts/api proxy endpoints.

    Endpoints used:
        - GET /api/jquants/statements/raw - Raw financial statements
        - GET /api/jquants/stocks/{code}/margin-interest - Margin interest data
        - GET /api/market/stocks/{code} - Stock information

    Usage:
        ```python
        with JQuantsAPIClient() as client:
            statements = client.get_statements("7203")
            margin_df = client.get_margin_interest("7203")
            stock_info = client.get_stock_info("7203")
        ```
    """

    def get_margin_interest(
        self,
        code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get margin interest data from JQuants via apps/ts/api proxy.

        Args:
            code: Stock code (e.g., "7203")
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)

        Returns:
            DataFrame with columns [longMarginVolume, shortMarginVolume]
            and DatetimeIndex
        """
        params: dict[str, str] = {}
        if start_date:
            params["from"] = start_date
        if end_date:
            params["to"] = end_date

        data = self._get(
            f"/api/jquants/stocks/{code}/margin-interest",
            params=params if params else None,
        )

        if not isinstance(data, dict) or "marginInterest" not in data:
            return pd.DataFrame()

        margin_data: list[dict[str, Any]] = data.get("marginInterest", [])
        if not margin_data:
            return pd.DataFrame()

        df = pd.DataFrame(margin_data)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)

        # Rename columns to match expected format for margin indicators
        df = df.rename(columns={
            "longMarginTradeVolume": "longMarginVolume",
            "shortMarginTradeVolume": "shortMarginVolume",
        })

        # Keep only required columns
        return df[["longMarginVolume", "shortMarginVolume"]]

    def get_statements(self, code: str) -> list[JQuantsStatement]:
        """Get raw financial statements from JQuants via apps/ts/api proxy.

        Args:
            code: Stock code (e.g., "7203")

        Returns:
            List of JQuantsStatement objects
        """
        data = self._get("/api/jquants/statements/raw", params={"code": code})

        if not isinstance(data, dict) or "data" not in data:
            return []

        statements_data: list[dict[str, Any]] = data.get("data", [])
        return [JQuantsStatement.model_validate(stmt) for stmt in statements_data]

    def get_stock_info(self, code: str) -> StockInfo | None:
        """Get stock information from market.db.

        Args:
            code: Stock code (e.g., "7203")

        Returns:
            StockInfo object or None if not found
        """
        try:
            data = self._get(f"/api/market/stocks/{code}")

            if not isinstance(data, dict):
                return None

            return StockInfo.model_validate(data)
        except APINotFoundError:
            # Stock not found is expected case
            return None
        # Let other exceptions (APIConnectionError, APITimeoutError) propagate
