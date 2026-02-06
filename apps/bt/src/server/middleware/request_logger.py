"""
Request Logger Middleware

Hono の httpLogger と同等のリクエストロギングを提供する。
ログフォーマット: {method} {path} {status} {elapsed}ms
構造化フィールド: correlationId, method, path, status, elapsed
"""

import time

from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from src.server.middleware.correlation import get_correlation_id


class RequestLoggerMiddleware(BaseHTTPMiddleware):
    """リクエスト/レスポンスをログに記録するミドルウェア

    CorrelationIdMiddleware の外側（先に実行）で動作する。
    correlation ID は call_next 後に ContextVar から取得する
    （inner の CorrelationIdMiddleware がセット済み、finally リセット前）。
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        method = request.method
        path = request.url.path
        start = time.monotonic()

        response = await call_next(request)

        elapsed_ms = round((time.monotonic() - start) * 1000, 1)
        status = response.status_code
        correlation_id = get_correlation_id()

        log_kwargs = {
            "correlationId": correlation_id,
            "method": method,
            "path": path,
            "status": status,
            "elapsed": elapsed_ms,
        }

        if status >= 500:
            logger.error(f"{method} {path} {status} {elapsed_ms}ms", **log_kwargs)
        else:
            logger.info(f"{method} {path} {status} {elapsed_ms}ms", **log_kwargs)

        return response
