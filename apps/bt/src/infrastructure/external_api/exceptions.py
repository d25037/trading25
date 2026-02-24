"""Custom exceptions for API clients."""

from __future__ import annotations


class APIError(Exception):
    """Base exception for API-related errors."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class APIConnectionError(APIError):
    """Raised when connection to the API server fails."""

    def __init__(self, message: str = "Failed to connect to API server") -> None:
        super().__init__(message)


class APITimeoutError(APIError):
    """Raised when API request times out."""

    def __init__(self, message: str = "API request timed out") -> None:
        super().__init__(message)


class APINotFoundError(APIError):
    """Raised when requested resource is not found (404)."""

    def __init__(self, message: str = "Resource not found") -> None:
        super().__init__(message, status_code=404)


class APIValidationError(APIError):
    """Raised when request validation fails (400)."""

    def __init__(self, message: str = "Request validation failed") -> None:
        super().__init__(message, status_code=400)


class APIServerError(APIError):
    """Raised when server returns 5xx error."""

    def __init__(self, message: str = "API server error") -> None:
        super().__init__(message, status_code=500)
