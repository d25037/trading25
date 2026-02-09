"""
Request Logger Middleware

Hono の httpLogger と同等のリクエストロギングを提供する。
ログフォーマット: {method} {path} {status} {elapsed}ms
構造化フィールド: correlationId, method, path, status, elapsed
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from sqlalchemy.exc import SQLAlchemyError

from src.server.clients.jquants_client import JQuantsApiError
from src.server.middleware.correlation import CORRELATION_ID_HEADER, get_correlation_id


class RequestLoggerMiddleware(BaseHTTPMiddleware):
    """リクエスト/レスポンスをログに記録するミドルウェア

    CorrelationIdMiddleware の外側（先に実行）で動作する。
    correlation ID は call_next 後に ContextVar から取得する
    （inner の CorrelationIdMiddleware がセット済み、finally リセット前）。

    未処理例外もここでキャッチし、統一エラーフォーマットの 500 を返す
    （BaseHTTPMiddleware の制約により app.exception_handler(Exception) に
    到達しないため）。
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        method = request.method
        path = request.url.path
        start = time.monotonic()

        try:
            response = await call_next(request)
        except JQuantsApiError as exc:
            status_code = exc.status_code
            error_text = {502: "Bad Gateway", 504: "Gateway Timeout"}.get(
                status_code, "Bad Gateway"
            )
            return self._build_error_response(
                method, path, start, status_code,
                error_text, exc.message, log_level="warning",
            )
        except SQLAlchemyError as exc:
            return self._build_error_response(
                method, path, start, 500,
                "Internal Server Error", "Database error",
                log_suffix=f"Database error: {exc}",
            )
        except Exception as exc:
            return self._build_error_response(
                method, path, start, 500,
                "Internal Server Error", "Internal server error",
                log_suffix=f"Unhandled exception: {exc}",
            )

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

    def _build_error_response(
        self,
        method: str,
        path: str,
        start: float,
        status_code: int,
        error: str,
        message: str,
        *,
        log_level: str = "exception",
        log_suffix: str | None = None,
    ) -> JSONResponse:
        """統一エラーフォーマットの JSONResponse を構築しログ出力する"""
        elapsed_ms = round((time.monotonic() - start) * 1000, 1)
        correlation_id = get_correlation_id()

        suffix = f" - {log_suffix}" if log_suffix else f" - {message}"
        log_msg = f"{method} {path} {status_code} {elapsed_ms}ms{suffix}"
        log_kwargs = {
            "correlationId": correlation_id,
            "method": method,
            "path": path,
            "status": status_code,
            "elapsed": elapsed_ms,
        }
        getattr(logger, log_level)(log_msg, **log_kwargs)

        body = {
            "status": "error",
            "error": error,
            "message": message,
            "timestamp": datetime.now(UTC).isoformat(),
            "correlationId": correlation_id,
        }
        response = JSONResponse(status_code=status_code, content=body)
        if correlation_id:
            response.headers[CORRELATION_ID_HEADER] = correlation_id
        return response
