"""OpenAPI スキーマ互換性テスト"""

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


def _make_client() -> TestClient:
    return TestClient(create_app())


class TestOpenAPISchema:
    """OpenAPI スキーマの基本検証"""

    def setup_method(self) -> None:
        self.client = _make_client()
        resp = self.client.get("/openapi.json")
        assert resp.status_code == 200
        self.schema = resp.json()

    def test_openapi_version(self) -> None:
        """OpenAPI 3.1 であること"""
        assert self.schema["openapi"].startswith("3.1")

    def test_info_title(self) -> None:
        """info.title = 'Trading25 API' であること"""
        assert self.schema["info"]["title"] == "Trading25 API"

    def test_info_version(self) -> None:
        """info.version = '1.0.0' であること"""
        assert self.schema["info"]["version"] == "1.0.0"

    def test_info_contact(self) -> None:
        """info.contact が設定されていること"""
        assert self.schema["info"]["contact"]["name"] == "Trading25 Team"

    def test_info_license(self) -> None:
        """info.license が MIT であること"""
        assert self.schema["info"]["license"]["name"] == "MIT"

    def test_servers(self) -> None:
        """servers に FastAPI (3002) エントリが存在すること"""
        urls = [s["url"] for s in self.schema.get("servers", [])]
        assert "http://localhost:3002" in urls

    def test_hono_operation_tags_defined(self) -> None:
        """Hono baseline の 10 operation tags が top-level tags に定義されていること"""
        tag_names = {t["name"] for t in self.schema.get("tags", [])}
        missing = HONO_OPERATION_TAGS - tag_names
        assert not missing, f"Missing tags: {missing}"

    def test_ohlcv_refs_are_stable_and_legacy_compatible(self) -> None:
        """OHLCV系の $ref が baseline 互換キーへ固定されること"""
        schemas = self.schema.get("components", {}).get("schemas", {})

        ohlcv_resample = schemas.get("OHLCVResampleResponse", {})
        resample_ref = (
            ohlcv_resample.get("properties", {})
            .get("data", {})
            .get("items", {})
            .get("$ref")
        )
        assert resample_ref == "#/components/schemas/src__server__schemas__indicators__OHLCVRecord"

        path_single_ref = (
            self.schema.get("paths", {})
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
            self.schema.get("paths", {})
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


class TestErrorResponseSchema:
    """ErrorResponse スキーマの OpenAPI 公開テスト"""

    def setup_method(self) -> None:
        self.client = _make_client()
        resp = self.client.get("/openapi.json")
        self.schema = resp.json()

    def test_error_response_in_components(self) -> None:
        """ErrorResponse が components/schemas に存在すること"""
        schemas = self.schema.get("components", {}).get("schemas", {})
        assert "ErrorResponse" in schemas

    def test_error_response_has_required_fields(self) -> None:
        """ErrorResponse スキーマに必須フィールドが含まれること"""
        schemas = self.schema.get("components", {}).get("schemas", {})
        er = schemas.get("ErrorResponse", {})
        props = er.get("properties", {})
        for field in ["status", "error", "message", "timestamp", "correlationId"]:
            assert field in props, f"Missing field: {field}"

    def test_endpoints_have_error_responses(self) -> None:
        """各エンドポイントに 400/500 レスポンスが定義されていること"""
        paths = self.schema.get("paths", {})
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

    def setup_method(self) -> None:
        self.client = _make_client()

    def test_doc_returns_200(self) -> None:
        """/doc が 200 HTML を返すこと"""
        resp = self.client.get("/doc")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    def test_docs_disabled(self) -> None:
        """デフォルトの /docs が無効であること"""
        resp = self.client.get("/docs")
        assert resp.status_code == 404

    def test_redoc_disabled(self) -> None:
        """/redoc が無効であること"""
        resp = self.client.get("/redoc")
        assert resp.status_code == 404
