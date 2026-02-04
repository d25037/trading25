"""Base API client with httpx synchronous implementation."""

from __future__ import annotations

from typing import Any, TypeVar

import httpx
from pydantic import BaseModel

from src.api.exceptions import (
    APIConnectionError,
    APIError,
    APINotFoundError,
    APIServerError,
    APITimeoutError,
    APIValidationError,
)
from src.config.settings import get_settings

T = TypeVar("T", bound=BaseModel)

_settings = get_settings()
DEFAULT_BASE_URL = _settings.api_base_url
DEFAULT_TIMEOUT = _settings.api_timeout


class BaseAPIClient:
    """Base API client with httpx synchronous implementation.

    Features:
        - Lazy initialization of httpx.Client
        - Context manager support for proper resource cleanup
        - Unified error handling with custom exceptions
        - Support for Pydantic response model validation

    Usage:
        ```python
        with BaseAPIClient() as client:
            data = client._get("/api/endpoint")
        ```

        Or without context manager:
        ```python
        client = BaseAPIClient()
        try:
            data = client._get("/api/endpoint")
        finally:
            client.close()
        ```
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        """Initialize the API client.

        Args:
            base_url: Base URL of the API server (default: http://localhost:3001)
            timeout: Request timeout in seconds (default: 30.0)
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Lazy-initialized httpx client."""
        if self._client is None:
            self._client = httpx.Client(
                base_url=self.base_url,
                timeout=self.timeout,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
        return self._client

    def close(self) -> None:
        """Close the httpx client and release resources."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> BaseAPIClient:
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Context manager exit with proper cleanup."""
        self.close()

    def _handle_response_error(self, response: httpx.Response) -> None:
        """Handle HTTP error responses with appropriate exceptions.

        Args:
            response: The httpx Response object

        Raises:
            APINotFoundError: For 404 responses
            APIValidationError: For 400 responses
            APIServerError: For 5xx responses
            APIError: For other error responses
        """
        if response.is_success:
            return

        status_code = response.status_code
        try:
            error_detail = response.json().get("message", response.text)
        except Exception:
            error_detail = response.text

        if status_code == 404:
            raise APINotFoundError(f"Resource not found: {error_detail}")
        elif status_code == 400:
            raise APIValidationError(f"Validation error: {error_detail}")
        elif status_code >= 500:
            raise APIServerError(f"Server error ({status_code}): {error_detail}")
        else:
            raise APIError(f"API error ({status_code}): {error_detail}", status_code)

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Execute an HTTP request with error handling.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            path: API endpoint path (e.g., "/api/dataset/sampleA/stocks")
            params: Query parameters
            json: JSON body for POST/PUT requests

        Returns:
            Parsed JSON response (dict or list)

        Raises:
            APIConnectionError: When connection fails
            APITimeoutError: When request times out
            APIError: For other API errors
        """
        try:
            response = self.client.request(
                method=method,
                url=path,
                params=params,
                json=json,
            )
            self._handle_response_error(response)
            return response.json()  # type: ignore[no-any-return]
        except httpx.TimeoutException as e:
            raise APITimeoutError(f"Request timeout: {path}") from e
        except httpx.ConnectError as e:
            raise APIConnectionError(
                f"Connection failed: {self.base_url}{path}"
            ) from e
        except (APIError, APINotFoundError, APIValidationError, APIServerError):
            raise
        except httpx.HTTPError as e:
            raise APIError(f"HTTP error: {e}") from e

    def _get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        response_model: type[T] | None = None,
    ) -> T | dict[str, Any] | list[dict[str, Any]]:
        """Execute a GET request.

        Args:
            path: API endpoint path
            params: Query parameters
            response_model: Optional Pydantic model for response validation

        Returns:
            Parsed response (validated model if response_model provided)
        """
        data = self._request("GET", path, params=params)
        if response_model is not None and isinstance(data, dict):
            return response_model.model_validate(data)
        return data

    def _post(
        self,
        path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        response_model: type[T] | None = None,
    ) -> T | dict[str, Any] | list[dict[str, Any]]:
        """Execute a POST request.

        Args:
            path: API endpoint path
            json: JSON body
            params: Query parameters
            response_model: Optional Pydantic model for response validation

        Returns:
            Parsed response (validated model if response_model provided)
        """
        data = self._request("POST", path, params=params, json=json)
        if response_model is not None and isinstance(data, dict):
            return response_model.model_validate(data)
        return data

    def _put(
        self,
        path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        response_model: type[T] | None = None,
    ) -> T | dict[str, Any] | list[dict[str, Any]]:
        """Execute a PUT request.

        Args:
            path: API endpoint path
            json: JSON body
            params: Query parameters
            response_model: Optional Pydantic model for response validation

        Returns:
            Parsed response (validated model if response_model provided)
        """
        data = self._request("PUT", path, params=params, json=json)
        if response_model is not None and isinstance(data, dict):
            return response_model.model_validate(data)
        return data

    def _delete(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Execute a DELETE request.

        Args:
            path: API endpoint path
            params: Query parameters

        Returns:
            Parsed response
        """
        return self._request("DELETE", path, params=params)

    def health_check(self) -> bool:
        """Check if the API server is healthy.

        Returns:
            True if the server is healthy, False otherwise
        """
        try:
            self._get("/health")
            return True
        except APIError:
            return False
