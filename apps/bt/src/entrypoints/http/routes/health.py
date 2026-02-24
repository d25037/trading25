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


_health_response = HealthResponse(
    status="healthy",
    service="trading25-bt",
    version="0.1.0",
)


@router.get("/api/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    ヘルスチェック

    サーバーの状態を確認
    """
    return _health_response


@router.get("/health", response_model=HealthResponse)
async def health_check_alias() -> HealthResponse:
    """
    ヘルスチェック（Hono 互換エイリアス）

    `/api/health` と同一レスポンスを返す
    """
    return _health_response
