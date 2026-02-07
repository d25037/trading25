"""CORS 設定のテスト"""

from fastapi.testclient import TestClient

from src.server.app import create_app

ALLOWED_ORIGINS = [
    "http://localhost:5173",
    "http://localhost:4173",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:4173",
]


def _make_client() -> TestClient:
    return TestClient(create_app())


class TestCORSPreflight:
    """OPTIONS preflight リクエストのテスト"""

    def setup_method(self) -> None:
        self.client = _make_client()

    def test_preflight_returns_cors_headers(self) -> None:
        """preflight が正しい CORS ヘッダを返すこと"""
        resp = self.client.options(
            "/api/health",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "x-correlation-id",
            },
        )
        assert resp.status_code == 200
        assert resp.headers.get("access-control-allow-origin") == "http://localhost:5173"
        assert "GET" in resp.headers.get("access-control-allow-methods", "")

    def test_preflight_allows_correlation_id_header(self) -> None:
        """x-correlation-id が allow-headers に含まれること"""
        resp = self.client.options(
            "/api/health",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "x-correlation-id",
            },
        )
        allowed = resp.headers.get("access-control-allow-headers", "").lower()
        assert "x-correlation-id" in allowed


class TestCORSAllowedOrigins:
    """許可されたオリジンのテスト"""

    def setup_method(self) -> None:
        self.client = _make_client()

    def test_all_allowed_origins(self) -> None:
        """全ての許可オリジンが正しく動作すること"""
        for origin in ALLOWED_ORIGINS:
            resp = self.client.get("/api/health", headers={"Origin": origin})
            assert resp.headers.get("access-control-allow-origin") == origin, f"Origin {origin} not allowed"

    def test_disallowed_origin_rejected(self) -> None:
        """非許可オリジンが拒否されること"""
        resp = self.client.get(
            "/api/health",
            headers={"Origin": "http://evil.example.com"},
        )
        assert resp.headers.get("access-control-allow-origin") is None


class TestCORSExposeHeaders:
    """expose-headers のテスト"""

    def setup_method(self) -> None:
        self.client = _make_client()

    def test_correlation_id_exposed(self) -> None:
        """x-correlation-id が expose-headers に含まれること"""
        resp = self.client.get(
            "/api/health",
            headers={"Origin": "http://localhost:5173"},
        )
        exposed = resp.headers.get("access-control-expose-headers", "").lower()
        assert "x-correlation-id" in exposed


class TestCORSMethods:
    """許可メソッドのテスト"""

    def setup_method(self) -> None:
        self.client = _make_client()

    def test_allowed_methods(self) -> None:
        """GET/POST/PUT/DELETE/OPTIONS が許可されていること"""
        for method in ["GET", "POST", "PUT", "DELETE", "OPTIONS"]:
            resp = self.client.options(
                "/api/health",
                headers={
                    "Origin": "http://localhost:5173",
                    "Access-Control-Request-Method": method,
                },
            )
            assert resp.status_code == 200, f"Method {method} not allowed"
