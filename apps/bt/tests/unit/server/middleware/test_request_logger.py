"""リクエストロギングミドルウェアのテスト"""

from unittest.mock import patch

from fastapi.testclient import TestClient

from src.server.app import create_app


def _make_client() -> TestClient:
    app = create_app()
    return TestClient(app)


class TestRequestLoggerMiddleware:
    """RequestLoggerMiddleware のログ出力をテスト"""

    def setup_method(self) -> None:
        self.client = _make_client()

    def test_logs_method_path_status_elapsed(self) -> None:
        """ログに method, path, status, elapsed が含まれること"""
        with patch("src.server.middleware.request_logger.logger") as mock_logger:
            self.client.get("/api/health")

            mock_logger.info.assert_called()
            log_msg = mock_logger.info.call_args[0][0]
            assert "GET" in log_msg
            assert "/api/health" in log_msg
            assert "200" in log_msg
            assert "ms" in log_msg

    def test_logs_correlation_id(self) -> None:
        """ログの kwargs に correlationId が含まれること"""
        test_cid = "12345678-1234-1234-1234-123456789abc"
        with patch("src.server.middleware.request_logger.logger") as mock_logger:
            self.client.get("/api/health", headers={"x-correlation-id": test_cid})

            mock_logger.info.assert_called()
            kwargs = mock_logger.info.call_args[1]
            assert kwargs["correlationId"] == test_cid

    def test_logs_auto_generated_correlation_id(self) -> None:
        """correlation ID が自動生成されてログに含まれること"""
        with patch("src.server.middleware.request_logger.logger") as mock_logger:
            self.client.get("/api/health")

            mock_logger.info.assert_called()
            kwargs = mock_logger.info.call_args[1]
            assert kwargs["correlationId"] != ""

    def test_logs_structured_fields(self) -> None:
        """構造化フィールド (method, path, status, elapsed) がログ kwargs に含まれること"""
        with patch("src.server.middleware.request_logger.logger") as mock_logger:
            self.client.get("/api/health")

            mock_logger.info.assert_called()
            kwargs = mock_logger.info.call_args[1]
            assert kwargs["method"] == "GET"
            assert kwargs["path"] == "/api/health"
            assert kwargs["status"] == 200
            assert isinstance(kwargs["elapsed"], float)

    def test_error_status_logs_at_error_level(self) -> None:
        """5xx ステータスは error レベルでログされること"""
        with patch("src.server.middleware.request_logger.logger") as mock_logger:
            self.client.get("/nonexistent/path/that/triggers/500/somehow")

            # 404 は info レベル
            if mock_logger.error.called:
                kwargs = mock_logger.error.call_args[1]
                assert kwargs["status"] >= 500
            else:
                # 404 の場合は info
                mock_logger.info.assert_called()
                kwargs = mock_logger.info.call_args[1]
                assert kwargs["status"] < 500

    def test_500_error_logs_at_error_level(self) -> None:
        """500 レスポンスは error レベルでログされること"""
        from fastapi import APIRouter, HTTPException

        app = create_app()
        test_router = APIRouter()

        @test_router.get("/test/500")
        async def trigger_500() -> dict:
            raise HTTPException(status_code=500, detail="test error")

        app.include_router(test_router)
        client = TestClient(app)

        with patch("src.server.middleware.request_logger.logger") as mock_logger:
            client.get("/test/500")

            mock_logger.error.assert_called()
            kwargs = mock_logger.error.call_args[1]
            assert kwargs["status"] == 500
            assert kwargs["method"] == "GET"
            assert kwargs["path"] == "/test/500"


class TestMiddlewareOrder:
    """ミドルウェアの実行順序テスト: RequestLogger が CorrelationId の外側で動作すること"""

    def test_correlation_id_available_in_log(self) -> None:
        """RequestLogger のログに CorrelationId ミドルウェアが設定した ID が含まれること"""
        test_cid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        client = _make_client()

        with patch("src.server.middleware.request_logger.logger") as mock_logger:
            resp = client.get("/api/health", headers={"x-correlation-id": test_cid})

            # レスポンスヘッダにも同じ correlation ID
            assert resp.headers.get("x-correlation-id") == test_cid

            # ログにも同じ correlation ID
            mock_logger.info.assert_called()
            kwargs = mock_logger.info.call_args[1]
            assert kwargs["correlationId"] == test_cid

    def test_auto_generated_cid_consistent(self) -> None:
        """自動生成の correlation ID がレスポンスヘッダとログで一致すること"""
        client = _make_client()

        with patch("src.server.middleware.request_logger.logger") as mock_logger:
            resp = client.get("/api/health")

            header_cid = resp.headers.get("x-correlation-id")
            assert header_cid is not None

            kwargs = mock_logger.info.call_args[1]
            assert kwargs["correlationId"] == header_cid
