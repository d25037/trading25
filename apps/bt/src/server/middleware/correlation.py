"""
Correlation ID Middleware

リクエストごとに correlation ID を管理し、レスポンスヘッダに付与する。
x-correlation-id ヘッダがあればそれを使用し、なければ uuid4 を自動生成する。
"""

from contextvars import ContextVar
from uuid import uuid4

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

CORRELATION_ID_HEADER = "x-correlation-id"

# Context variable for the current request's correlation ID
correlation_id_ctx: ContextVar[str] = ContextVar("correlation_id", default="")


def get_correlation_id() -> str:
    """現在のリクエストの correlation ID を取得"""
    return correlation_id_ctx.get()


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """correlation ID をリクエスト/レスポンスに付与するミドルウェア"""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # ヘッダから取得、なければ自動生成
        cid = request.headers.get(CORRELATION_ID_HEADER) or str(uuid4())
        token = correlation_id_ctx.set(cid)

        try:
            response = await call_next(request)
            response.headers[CORRELATION_ID_HEADER] = cid
            return response
        finally:
            correlation_id_ctx.reset(token)
