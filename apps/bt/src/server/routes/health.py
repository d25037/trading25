"""
Health Check Endpoint
"""

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(tags=["Health"])


class HealthResponse(BaseModel):
    """ヘルスチェックレスポンス"""

    status: str
    service: str
    version: str


@router.get("/api/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    ヘルスチェック

    サーバーの状態を確認
    """
    return HealthResponse(
        status="healthy",
        service="trading25-bt",
        version="0.1.0",
    )
