"""統一エラーレスポンスフォーマットのテスト"""

from uuid import UUID

import pytest
from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field
from sqlalchemy.exc import OperationalError

from src.entrypoints.http.app import _configure_http_app
from src.entrypoints.http.openapi_config import get_openapi_config
from src.entrypoints.http.routes import chart, health, market_data, strategies
from src.infrastructure.external_api.clients.jquants_client import JQuantsApiError


def _make_test_app() -> TestClient:
    """テスト用のエンドポイントを追加した TestClient を作成"""
    app = FastAPI(**get_openapi_config())
    _configure_http_app(app)
    app.include_router(health.router)
    app.include_router(strategies.router)
    app.include_router(market_data.router)
    app.include_router(chart.router)

    test_router = APIRouter()

    class TestBody(BaseModel):
        name: str = Field(min_length=1)
        age: int = Field(gt=0)

    @test_router.get("/test/not-found")
    async def trigger_not_found() -> dict:
        raise HTTPException(status_code=404, detail="リソースが見つかりません")

    @test_router.get("/test/bad-request")
    async def trigger_bad_request() -> dict:
        raise HTTPException(status_code=400, detail="無効なリクエスト")

    @test_router.get("/test/server-error")
    async def trigger_server_error() -> dict:
        raise HTTPException(status_code=500, detail="内部エラー")

    @test_router.get("/test/conflict")
    async def trigger_conflict() -> dict:
        raise HTTPException(status_code=409, detail="リソースが競合しています")

    @test_router.get("/test/structured-not-found")
    async def trigger_structured_not_found() -> dict:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "Structured not found",
                "details": [
                    {"field": "reason", "message": "stock_not_found"},
                    {"field": "recovery", "message": "stock_refresh"},
                ],
            },
        )

    @test_router.post("/test/validation")
    async def trigger_validation(body: TestBody) -> dict:
        return {"name": body.name, "age": body.age}

    @test_router.get("/test/runtime-error")
    async def trigger_runtime_error() -> dict:
        raise RuntimeError("unexpected failure")

    @test_router.get("/test/attribute-error")
    async def trigger_attribute_error() -> dict:
        raise AttributeError("'NoneType' object has no attribute 'get_all_stocks'")

    @test_router.get("/test/sqlalchemy-error")
    async def trigger_sqlalchemy_error() -> dict:
        raise OperationalError("SELECT 1", {}, Exception("database is locked"))

    @test_router.get("/test/jquants-502")
    async def trigger_jquants_502() -> dict:
        raise JQuantsApiError(502, "JQuants API error (403): /markets/margin-interest")

    @test_router.get("/test/jquants-504")
    async def trigger_jquants_504() -> dict:
        raise JQuantsApiError(504, "JQuants API timeout: /equities/master")

    _registered_handlers = (
        trigger_not_found,
        trigger_bad_request,
        trigger_server_error,
        trigger_conflict,
        trigger_structured_not_found,
        trigger_validation,
        trigger_runtime_error,
        trigger_attribute_error,
        trigger_sqlalchemy_error,
        trigger_jquants_502,
        trigger_jquants_504,
    )
    _ = tuple(handler.__name__ for handler in _registered_handlers)

    app.include_router(test_router)
    return TestClient(app)


@pytest.fixture(scope="module")
def shared_client():
    client = _make_test_app()
    try:
        yield client
    finally:
        client.close()


@pytest.fixture(scope="module")
def no_db_client():
    app = FastAPI(**get_openapi_config())
    _configure_http_app(app)
    app.include_router(market_data.router)
    app.include_router(chart.router)
    app.state.market_data_service = None
    app.state.chart_service = None
    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()


class UsesSharedClient:
    @pytest.fixture(autouse=True)
    def _inject_client(self, shared_client: TestClient) -> None:
        self.client = shared_client


class UsesNoDbClient:
    @pytest.fixture(autouse=True)
    def _inject_client(self, no_db_client: TestClient) -> None:
        self.client = no_db_client


class TestHttpExceptionFormat(UsesSharedClient):
    """HTTPException が統一エラーフォーマットに変換されることをテスト"""

    def test_404_error_format(self) -> None:
        resp = self.client.get("/test/not-found")
        assert resp.status_code == 404
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == "Not Found"
        assert body["message"] == "リソースが見つかりません"
        assert "timestamp" in body
        assert "correlationId" in body
        # correlationId は有効な UUID であること
        UUID(body["correlationId"])

    def test_400_error_format(self) -> None:
        resp = self.client.get("/test/bad-request")
        assert resp.status_code == 400
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == "Bad Request"
        assert body["message"] == "無効なリクエスト"

    def test_500_error_format(self) -> None:
        resp = self.client.get("/test/server-error")
        assert resp.status_code == 500
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == "Internal Server Error"
        assert body["message"] == "内部エラー"

    def test_409_error_format(self) -> None:
        resp = self.client.get("/test/conflict")
        assert resp.status_code == 409
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == "Conflict"

    def test_no_details_field_for_http_exception(self) -> None:
        """HTTPException の場合、details フィールドは含まれない"""
        resp = self.client.get("/test/not-found")
        body = resp.json()
        assert "details" not in body

    def test_structured_http_exception_preserves_details(self) -> None:
        resp = self.client.get("/test/structured-not-found")
        assert resp.status_code == 404
        body = resp.json()
        assert body["message"] == "Structured not found"
        assert body["details"] == [
            {"field": "reason", "message": "stock_not_found"},
            {"field": "recovery", "message": "stock_refresh"},
        ]


class TestValidationErrorFormat(UsesSharedClient):
    """RequestValidationError が統一エラーフォーマットに変換されることをテスト"""

    def test_validation_error_format(self) -> None:
        resp = self.client.post("/test/validation", json={"name": "", "age": -1})
        assert resp.status_code == 422
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == "Unprocessable Entity"
        assert body["message"] == "Validation failed"
        assert "details" in body
        assert isinstance(body["details"], list)
        assert len(body["details"]) > 0

    def test_validation_error_has_field_level_details(self) -> None:
        resp = self.client.post("/test/validation", json={"name": "", "age": -1})
        body = resp.json()
        fields = [d["field"] for d in body["details"]]
        # name と age の両方にエラーがあること
        assert any("name" in f for f in fields)
        assert any("age" in f for f in fields)

    def test_validation_error_missing_body(self) -> None:
        resp = self.client.post("/test/validation", content=b"invalid json", headers={"content-type": "application/json"})
        assert resp.status_code == 422
        body = resp.json()
        assert body["status"] == "error"
        assert "details" in body


class TestCorrelationId(UsesSharedClient):
    """Correlation ID の伝播と自動生成をテスト"""

    def test_auto_generated_correlation_id(self) -> None:
        """correlation ID が自動生成されること"""
        resp = self.client.get("/api/health")
        assert resp.status_code == 200
        cid = resp.headers.get("x-correlation-id")
        assert cid is not None
        UUID(cid)  # 有効な UUID

    def test_propagated_correlation_id(self) -> None:
        """リクエストヘッダの correlation ID が伝播されること"""
        test_cid = "12345678-1234-1234-1234-123456789abc"
        resp = self.client.get("/api/health", headers={"x-correlation-id": test_cid})
        assert resp.headers.get("x-correlation-id") == test_cid

    def test_error_response_contains_correlation_id(self) -> None:
        """エラーレスポンスの body にも correlation ID が含まれること"""
        test_cid = "abcdef01-2345-6789-abcd-ef0123456789"
        resp = self.client.get("/test/not-found", headers={"x-correlation-id": test_cid})
        body = resp.json()
        assert body["correlationId"] == test_cid
        # ヘッダにも同じ値
        assert resp.headers.get("x-correlation-id") == test_cid

    def test_correlation_id_in_validation_error(self) -> None:
        """バリデーションエラーにも correlation ID が含まれること"""
        test_cid = "11111111-2222-3333-4444-555555555555"
        resp = self.client.post(
            "/test/validation",
            json={"name": "", "age": -1},
            headers={"x-correlation-id": test_cid},
        )
        body = resp.json()
        assert body["correlationId"] == test_cid


class TestExistingEndpointErrors(UsesSharedClient):
    """既存エンドポイントのエラーも統一フォーマットであることを確認"""

    def test_nonexistent_strategy_returns_unified_format(self) -> None:
        """存在しない戦略の取得が統一フォーマットで返ること"""
        resp = self.client.get("/api/strategies/nonexistent_strategy_xyz")
        assert resp.status_code == 404
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == "Not Found"
        assert "correlationId" in body
        assert "timestamp" in body


class TestGenericExceptionHandler(UsesSharedClient):
    """汎用例外ハンドラが未処理例外を統一フォーマットでキャッチすることをテスト"""

    def test_runtime_error_returns_500_unified_format(self) -> None:
        """RuntimeError が統一フォーマット 500 で返ること"""
        resp = self.client.get("/test/runtime-error")
        assert resp.status_code == 500
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == "Internal Server Error"
        assert body["message"] == "Internal server error"
        assert "correlationId" in body
        assert "timestamp" in body

    def test_attribute_error_returns_500_unified_format(self) -> None:
        """AttributeError が統一フォーマット 500 で返ること"""
        resp = self.client.get("/test/attribute-error")
        assert resp.status_code == 500
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == "Internal Server Error"
        assert body["message"] == "Internal server error"

    def test_generic_error_does_not_leak_details(self) -> None:
        """汎用例外のエラー詳細がレスポンスに漏れないこと"""
        resp = self.client.get("/test/runtime-error")
        body = resp.json()
        assert "unexpected failure" not in body.get("message", "")
        assert "details" not in body

    def test_generic_error_has_correlation_id(self) -> None:
        """汎用例外レスポンスに correlationId が含まれること"""
        test_cid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        resp = self.client.get("/test/runtime-error", headers={"x-correlation-id": test_cid})
        body = resp.json()
        assert body["correlationId"] == test_cid


class TestSQLAlchemyExceptionHandler(UsesSharedClient):
    """SQLAlchemy 例外ハンドラが DB エラーを統一フォーマットでキャッチすることをテスト"""

    def test_sqlalchemy_error_returns_500_with_db_message(self) -> None:
        """SQLAlchemy OperationalError が統一フォーマット 500 + "Database error" で返ること"""
        resp = self.client.get("/test/sqlalchemy-error")
        assert resp.status_code == 500
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == "Internal Server Error"
        assert body["message"] == "Database error"
        assert "correlationId" in body
        assert "timestamp" in body

    def test_sqlalchemy_error_does_not_leak_sql(self) -> None:
        """SQLAlchemy エラーの SQL 詳細がレスポンスに漏れないこと"""
        resp = self.client.get("/test/sqlalchemy-error")
        body = resp.json()
        assert "SELECT" not in body.get("message", "")
        assert "database is locked" not in body.get("message", "")


class TestNoDatabaseGracefulError(UsesNoDbClient):
    """DB 未初期化時に 500 ではなく 422 が返ることを確認する回帰テスト"""

    def test_market_stocks_returns_422_not_500(self) -> None:
        resp = self.client.get("/api/market/stocks")
        assert resp.status_code == 422
        body = resp.json()
        assert body["status"] == "error"
        assert "not initialized" in body["message"].lower() or "not initialized" in body["message"]

    def test_market_topix_returns_422_not_500(self) -> None:
        resp = self.client.get("/api/market/topix")
        assert resp.status_code == 422
        body = resp.json()
        assert body["status"] == "error"

    def test_chart_indices_returns_422_not_500(self) -> None:
        resp = self.client.get("/api/chart/indices")
        assert resp.status_code == 422
        body = resp.json()
        assert body["status"] == "error"


class TestJQuantsApiErrorFormat(UsesSharedClient):
    """JQuants API エラーが統一フォーマット + 502/504 で返ること"""

    def test_502_jquants_error(self) -> None:
        """JQuants HTTP エラーが 502 Bad Gateway で返ること"""
        resp = self.client.get("/test/jquants-502")
        assert resp.status_code == 502
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == "Bad Gateway"
        assert "JQuants" in body["message"]
        assert "403" in body["message"]
        assert "correlationId" in body
        assert "timestamp" in body

    def test_504_jquants_timeout(self) -> None:
        """JQuants タイムアウトが 504 Gateway Timeout で返ること"""
        resp = self.client.get("/test/jquants-504")
        assert resp.status_code == 504
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == "Gateway Timeout"
        assert "JQuants" in body["message"]
        assert "correlationId" in body
        assert "timestamp" in body

    def test_jquants_error_has_correlation_id_header(self) -> None:
        """JQuants エラーレスポンスに correlation ID ヘッダが含まれること"""
        test_cid = "jquants-test-cid-1234-567890abcdef"
        resp = self.client.get(
            "/test/jquants-502", headers={"x-correlation-id": test_cid}
        )
        assert resp.headers.get("x-correlation-id") == test_cid
        body = resp.json()
        assert body["correlationId"] == test_cid
