"""OpenAPI スキーマ互換性テスト"""

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app

# Hono baseline に存在する 10 operation tags
HONO_OPERATION_TAGS = {
    "Analytics",
    "Chart",
    "Database",
    "Dataset",
    "Dataset Data",
    "Health",
    "JQuants Proxy",
    "Market Data",
    "Portfolio",
    "Watchlist",
}


@pytest.fixture(scope="module")
def openapi_app():
    return create_app()


@pytest.fixture(scope="module")
def openapi_client(openapi_app):
    client = TestClient(openapi_app)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture(scope="module")
def openapi_schema(openapi_app):
    return openapi_app.openapi()


class TestOpenAPISchema:
    """OpenAPI スキーマの基本検証"""

    def test_openapi_version(self, openapi_schema) -> None:
        """OpenAPI 3.1 であること"""
        assert openapi_schema["openapi"].startswith("3.1")

    def test_info_title(self, openapi_schema) -> None:
        """info.title = 'Trading25 API' であること"""
        assert openapi_schema["info"]["title"] == "Trading25 API"

    def test_info_version(self, openapi_schema) -> None:
        """info.version = '1.0.0' であること"""
        assert openapi_schema["info"]["version"] == "1.0.0"

    def test_info_contact(self, openapi_schema) -> None:
        """info.contact が設定されていること"""
        assert openapi_schema["info"]["contact"]["name"] == "Trading25 Team"

    def test_info_license(self, openapi_schema) -> None:
        """info.license が MIT であること"""
        assert openapi_schema["info"]["license"]["name"] == "MIT"

    def test_servers(self, openapi_schema) -> None:
        """servers に FastAPI (3002) エントリが存在すること"""
        urls = [s["url"] for s in openapi_schema.get("servers", [])]
        assert "http://localhost:3002" in urls

    def test_hono_operation_tags_defined(self, openapi_schema) -> None:
        """Hono baseline の 10 operation tags が top-level tags に定義されていること"""
        tag_names = {t["name"] for t in openapi_schema.get("tags", [])}
        missing = HONO_OPERATION_TAGS - tag_names
        assert not missing, f"Missing tags: {missing}"

    def test_ohlcv_refs_are_stable_and_legacy_compatible(self, openapi_schema) -> None:
        """OHLCV系の $ref が baseline 互換キーへ固定されること"""
        schemas = openapi_schema.get("components", {}).get("schemas", {})

        ohlcv_resample = schemas.get("OHLCVResampleResponse", {})
        resample_ref = (
            ohlcv_resample.get("properties", {})
            .get("data", {})
            .get("items", {})
            .get("$ref")
        )
        assert resample_ref == "#/components/schemas/src__server__schemas__indicators__OHLCVRecord"

        path_single_ref = (
            openapi_schema.get("paths", {})
            .get("/api/dataset/{name}/stocks/{code}/ohlcv", {})
            .get("get", {})
            .get("responses", {})
            .get("200", {})
            .get("content", {})
            .get("application/json", {})
            .get("schema", {})
            .get("items", {})
            .get("$ref")
        )
        assert path_single_ref == "#/components/schemas/OHLCVRecord"

        path_batch_ref = (
            openapi_schema.get("paths", {})
            .get("/api/dataset/{name}/stocks/ohlcv/batch", {})
            .get("get", {})
            .get("responses", {})
            .get("200", {})
            .get("content", {})
            .get("application/json", {})
            .get("schema", {})
            .get("additionalProperties", {})
            .get("items", {})
            .get("$ref")
        )
        assert path_batch_ref == "#/components/schemas/OHLCVRecord"

    @pytest.mark.parametrize(
        "path",
        [
            "/api/analytics/screening/jobs/{job_id}/stream",
            "/api/db/sync/jobs/{jobId}/stream",
        ],
    )
    def test_sse_endpoints_use_text_event_stream_for_200(self, openapi_schema, path: str) -> None:
        operation = openapi_schema["paths"][path]["get"]
        content = operation["responses"]["200"]["content"]
        assert list(content.keys()) == ["text/event-stream"]


class TestErrorResponseSchema:
    """ErrorResponse スキーマの OpenAPI 公開テスト"""

    def test_error_response_in_components(self, openapi_schema) -> None:
        """ErrorResponse が components/schemas に存在すること"""
        schemas = openapi_schema.get("components", {}).get("schemas", {})
        assert "ErrorResponse" in schemas

    def test_error_response_has_required_fields(self, openapi_schema) -> None:
        """ErrorResponse スキーマに必須フィールドが含まれること"""
        schemas = openapi_schema.get("components", {}).get("schemas", {})
        er = schemas.get("ErrorResponse", {})
        props = er.get("properties", {})
        for field in ["status", "error", "message", "timestamp", "correlationId"]:
            assert field in props, f"Missing field: {field}"

    def test_endpoints_have_error_responses(self, openapi_schema) -> None:
        """各エンドポイントに 400/500 レスポンスが定義されていること"""
        paths = openapi_schema.get("paths", {})
        assert len(paths) > 0, "No paths found"
        for path, methods in paths.items():
            for method, operation in methods.items():
                if not isinstance(operation, dict) or "responses" not in operation:
                    continue
                responses = operation["responses"]
                for code in ["400", "500"]:
                    assert code in responses, f"{method.upper()} {path} missing {code} response"


class TestDocUI:
    """/doc エンドポイントのテスト"""

    def test_doc_returns_200(self, openapi_client) -> None:
        """/doc が 200 HTML を返すこと"""
        resp = openapi_client.get("/doc")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    def test_docs_disabled(self, openapi_client) -> None:
        """デフォルトの /docs が無効であること"""
        resp = openapi_client.get("/docs")
        assert resp.status_code == 404

    def test_redoc_disabled(self, openapi_client) -> None:
        """/redoc が無効であること"""
        resp = openapi_client.get("/redoc")
        assert resp.status_code == 404
