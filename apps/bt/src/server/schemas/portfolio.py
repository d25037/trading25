"""
Portfolio Schemas

Hono Portfolio API 互換のリクエスト/レスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# ===== Request Schemas =====


class PortfolioCreateRequest(BaseModel):
    name: str = Field(min_length=1)
    description: str | None = None


class PortfolioUpdateRequest(BaseModel):
    name: str | None = Field(None, min_length=1)
    description: str | None = None


class PortfolioItemCreateRequest(BaseModel):
    code: str = Field(pattern=r"^\d[0-9A-Z]\d[0-9A-Z]$")
    companyName: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    purchasePrice: float = Field(gt=0)
    purchaseDate: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    account: str | None = None
    notes: str | None = None


class PortfolioItemUpdateRequest(BaseModel):
    quantity: int | None = Field(None, gt=0)
    purchasePrice: float | None = Field(None, gt=0)
    purchaseDate: str | None = Field(None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    account: str | None = None
    notes: str | None = None


class StockUpdateRequest(BaseModel):
    """PUT /{portfolioName}/stocks/{code}"""

    companyName: str | None = Field(None, min_length=1)
    quantity: int | None = Field(None, gt=0)
    purchasePrice: float | None = Field(None, gt=0)
    purchaseDate: str | None = Field(None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    account: str | None = None
    notes: str | None = None


# ===== Response Schemas =====


class PortfolioSummaryResponse(BaseModel):
    """Portfolio list item (stockCount/totalShares 付き)"""

    id: int
    name: str
    description: str | None = None
    stockCount: int
    totalShares: int
    createdAt: str
    updatedAt: str


class PortfolioResponse(BaseModel):
    """Portfolio 単体レスポンス (create/update)"""

    id: int
    name: str
    description: str | None = None
    createdAt: str
    updatedAt: str


class PortfolioItemResponse(BaseModel):
    """Portfolio item レスポンス"""

    id: int
    portfolioId: int
    code: str
    companyName: str
    quantity: int
    purchasePrice: float
    purchaseDate: str
    account: str | None = None
    notes: str | None = None
    createdAt: str
    updatedAt: str


class PortfolioDetailResponse(BaseModel):
    """GET /api/portfolio/{id} — portfolio + items"""

    id: int
    name: str
    description: str | None = None
    createdAt: str
    updatedAt: str
    items: list[PortfolioItemResponse]


class PortfolioCodesResponse(BaseModel):
    """GET /api/portfolio/{name}/codes"""

    name: str
    codes: list[str]


class DeleteResponse(BaseModel):
    """DELETE 成功レスポンス"""

    success: bool = True
    message: str


class StockDeleteResponse(BaseModel):
    """DELETE stock with deletedItem"""

    success: bool = True
    message: str
    deletedItem: PortfolioItemResponse
