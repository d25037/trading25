"""Correlation ID context for cross-layer observability."""

from __future__ import annotations

from contextvars import ContextVar, Token

CORRELATION_ID_HEADER = "x-correlation-id"

_correlation_id_ctx: ContextVar[str] = ContextVar("correlation_id", default="")


def get_correlation_id() -> str:
    """Get current request correlation ID."""
    return _correlation_id_ctx.get()


def set_correlation_id(correlation_id: str) -> Token[str]:
    """Set current request correlation ID and return reset token."""
    return _correlation_id_ctx.set(correlation_id)


def reset_correlation_id(token: Token[str]) -> None:
    """Reset correlation ID context with token from `set_correlation_id`."""
    _correlation_id_ctx.reset(token)
