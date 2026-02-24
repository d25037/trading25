"""
ポートフォリオモデル定義

Pydanticによるポートフォリオデータの型安全な管理
"""

from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class PortfolioMetadata(BaseModel):
    """ポートフォリオメタデータ"""

    key: str
    value: str
    updated_at: datetime


class Portfolio(BaseModel):
    """ポートフォリオ基本情報"""

    id: int
    name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class PortfolioItem(BaseModel):
    """ポートフォリオ保有銘柄"""

    id: int
    portfolio_id: int
    code: str = Field(..., description="銘柄コード")
    company_name: str = Field(..., description="会社名")
    quantity: int = Field(..., gt=0, description="保有数量")
    purchase_price: float = Field(..., gt=0, description="購入単価")
    purchase_date: date = Field(..., description="購入日")
    account: Optional[str] = None
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    @field_validator("code")
    @classmethod
    def validate_code(cls, v: str) -> str:
        """銘柄コードの検証"""
        if not v or not v.strip():
            raise ValueError("銘柄コードは必須です")
        return v.strip()

    @property
    def total_cost(self) -> float:
        """取得原価"""
        return self.quantity * self.purchase_price


class PortfolioSummary(BaseModel):
    """ポートフォリオサマリー"""

    portfolio: Portfolio
    items: list[PortfolioItem]
    total_stocks: int = Field(..., description="保有銘柄数")
    total_cost: float = Field(..., description="合計取得原価")
