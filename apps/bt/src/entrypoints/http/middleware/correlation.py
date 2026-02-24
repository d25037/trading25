"""Correlation ID middleware."""

from uuid import uuid4

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from src.shared.observability.correlation import (
    CORRELATION_ID_HEADER,
    reset_correlation_id,
    set_correlation_id,
)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """correlation ID をリクエスト/レスポンスに付与するミドルウェア"""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # ヘッダから取得、なければ自動生成
        cid = request.headers.get(CORRELATION_ID_HEADER) or str(uuid4())
        token = set_correlation_id(cid)

        try:
            response = await call_next(request)
            response.headers[CORRELATION_ID_HEADER] = cid
            return response
        finally:
            reset_correlation_id(token)
