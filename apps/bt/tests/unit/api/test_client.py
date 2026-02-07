"""Unit tests for BaseAPIClient."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.api.client import BaseAPIClient
from src.api.exceptions import (
    APIConnectionError,
    APIError,
    APINotFoundError,
    APIServerError,
    APITimeoutError,
    APIValidationError,
)


class TestBaseAPIClient:
    """Tests for BaseAPIClient."""

    def test_init_default_values(self) -> None:
        """Test default initialization values."""
        client = BaseAPIClient()
        assert client.base_url == "http://localhost:3002"
        assert client.timeout == 30.0
        assert client._client is None

    def test_init_custom_values(self) -> None:
        """Test initialization with custom values."""
        client = BaseAPIClient(base_url="http://example.com:8080", timeout=60.0)
        assert client.base_url == "http://example.com:8080"
        assert client.timeout == 60.0

    def test_base_url_trailing_slash_removed(self) -> None:
        """Test that trailing slash is removed from base_url."""
        client = BaseAPIClient(base_url="http://localhost:3002/")
        assert client.base_url == "http://localhost:3002"

    def test_lazy_client_initialization(self) -> None:
        """Test that httpx.Client is lazily initialized."""
        client = BaseAPIClient()
        assert client._client is None

        # Access client property to trigger initialization
        with patch("httpx.Client") as mock_client_class:
            mock_client_class.return_value = MagicMock()
            _ = client.client
            mock_client_class.assert_called_once()

    def test_close_releases_client(self) -> None:
        """Test that close() releases the client."""
        client = BaseAPIClient()
        mock_httpx_client = MagicMock()
        client._client = mock_httpx_client

        client.close()

        mock_httpx_client.close.assert_called_once()
        assert client._client is None

    def test_close_when_not_initialized(self) -> None:
        """Test that close() does nothing when client is not initialized."""
        client = BaseAPIClient()
        client.close()  # Should not raise

    def test_context_manager(self) -> None:
        """Test context manager protocol."""
        with patch("httpx.Client") as mock_client_class:
            mock_httpx_client = MagicMock()
            mock_client_class.return_value = mock_httpx_client

            with BaseAPIClient() as client:
                _ = client.client  # Trigger initialization

            mock_httpx_client.close.assert_called_once()

    def test_handle_response_error_success(self) -> None:
        """Test that successful responses don't raise."""
        client = BaseAPIClient()
        response = MagicMock()
        response.is_success = True

        # Should not raise
        client._handle_response_error(response)

    def test_handle_response_error_404(self) -> None:
        """Test 404 response handling."""
        client = BaseAPIClient()
        response = MagicMock()
        response.is_success = False
        response.status_code = 404
        response.json.return_value = {"message": "Not found"}

        with pytest.raises(APINotFoundError) as exc_info:
            client._handle_response_error(response)
        assert "Not found" in str(exc_info.value)

    def test_handle_response_error_400(self) -> None:
        """Test 400 response handling."""
        client = BaseAPIClient()
        response = MagicMock()
        response.is_success = False
        response.status_code = 400
        response.json.return_value = {"message": "Invalid request"}

        with pytest.raises(APIValidationError) as exc_info:
            client._handle_response_error(response)
        assert "Invalid request" in str(exc_info.value)

    def test_handle_response_error_500(self) -> None:
        """Test 500 response handling."""
        client = BaseAPIClient()
        response = MagicMock()
        response.is_success = False
        response.status_code = 500
        response.json.return_value = {"message": "Internal error"}

        with pytest.raises(APIServerError) as exc_info:
            client._handle_response_error(response)
        assert "Internal error" in str(exc_info.value)

    def test_handle_response_error_other(self) -> None:
        """Test other error status codes."""
        client = BaseAPIClient()
        response = MagicMock()
        response.is_success = False
        response.status_code = 403
        response.json.return_value = {"message": "Forbidden"}

        with pytest.raises(APIError) as exc_info:
            client._handle_response_error(response)
        assert "Forbidden" in str(exc_info.value)
        assert exc_info.value.status_code == 403

    @patch("httpx.Client")
    def test_request_success(self, mock_client_class: MagicMock) -> None:
        """Test successful request."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {"data": "test"}

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = BaseAPIClient()
        result = client._request("GET", "/api/test")

        assert result == {"data": "test"}
        mock_httpx_client.request.assert_called_once_with(
            method="GET",
            url="/api/test",
            params=None,
            json=None,
        )

    @patch("httpx.Client")
    def test_request_timeout(self, mock_client_class: MagicMock) -> None:
        """Test request timeout handling."""
        mock_httpx_client = MagicMock()
        mock_httpx_client.request.side_effect = httpx.TimeoutException("Timeout")
        mock_client_class.return_value = mock_httpx_client

        client = BaseAPIClient()

        with pytest.raises(APITimeoutError) as exc_info:
            client._request("GET", "/api/test")
        assert "timeout" in str(exc_info.value).lower()

    @patch("httpx.Client")
    def test_request_connection_error(self, mock_client_class: MagicMock) -> None:
        """Test connection error handling."""
        mock_httpx_client = MagicMock()
        mock_httpx_client.request.side_effect = httpx.ConnectError("Connection refused")
        mock_client_class.return_value = mock_httpx_client

        client = BaseAPIClient()

        with pytest.raises(APIConnectionError) as exc_info:
            client._request("GET", "/api/test")
        assert "Connection failed" in str(exc_info.value)

    @patch("httpx.Client")
    def test_get_request(self, mock_client_class: MagicMock) -> None:
        """Test GET request helper."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = [{"id": 1}, {"id": 2}]

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = BaseAPIClient()
        result = client._get("/api/items", params={"limit": 10})

        assert result == [{"id": 1}, {"id": 2}]

    @patch("httpx.Client")
    def test_post_request(self, mock_client_class: MagicMock) -> None:
        """Test POST request helper."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {"id": 1, "created": True}

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = BaseAPIClient()
        result = client._post("/api/items", json={"name": "test"})

        assert result == {"id": 1, "created": True}

    @patch("httpx.Client")
    def test_health_check_success(self, mock_client_class: MagicMock) -> None:
        """Test health check returns True on success."""
        mock_response = MagicMock()
        mock_response.is_success = True
        mock_response.json.return_value = {"status": "ok"}

        mock_httpx_client = MagicMock()
        mock_httpx_client.request.return_value = mock_response
        mock_client_class.return_value = mock_httpx_client

        client = BaseAPIClient()
        assert client.health_check() is True

    @patch("httpx.Client")
    def test_health_check_failure(self, mock_client_class: MagicMock) -> None:
        """Test health check returns False on failure."""
        mock_httpx_client = MagicMock()
        mock_httpx_client.request.side_effect = httpx.ConnectError("Connection refused")
        mock_client_class.return_value = mock_httpx_client

        client = BaseAPIClient()
        assert client.health_check() is False
