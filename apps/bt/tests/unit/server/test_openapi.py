"""OpenAPI スキーマ契約テスト"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.entrypoints.http.app import _register_routes
from copy import deepcopy

from src.entrypoints.http.openapi_config import (
    _stabilize_date_range_refs,
    _stabilize_schema_refs,
    customize_openapi,
    get_openapi_config,
)

REQUIRED_OPERATION_TAGS = {
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
    app = FastAPI(**get_openapi_config())
    _register_routes(app)
    app.openapi = lambda: customize_openapi(app)  # type: ignore[method-assign]
    return app


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

    def test_required_operation_tags_defined(self, openapi_schema) -> None:
        """公開 operation tags が top-level tags に定義されていること"""
        tag_names = {t["name"] for t in openapi_schema.get("tags", [])}
        missing = REQUIRED_OPERATION_TAGS - tag_names
        assert not missing, f"Missing tags: {missing}"

    def test_portfolio_date_range_ref_is_stable(self, openapi_schema) -> None:
        schemas = openapi_schema["components"]["schemas"]
        assert "DateRange" in schemas
        reference = schemas["PortfolioPerformanceResponse"]["properties"]["dateRange"][
            "anyOf"
        ][0]
        assert reference["$ref"] == "#/components/schemas/DateRange"
        assert not any(
            key.startswith("src__application__contracts__portfolio_performance__")
            for key in schemas
        )

    def test_factor_regression_response_graph_is_stable(self, openapi_schema) -> None:
        schemas = openapi_schema["components"]["schemas"]
        factor_response = schemas["FactorRegressionResponse"]
        portfolio_response = schemas["PortfolioFactorRegressionResponse"]

        assert list(factor_response["properties"]) == [
            "stockCode",
            "companyName",
            "marketBeta",
            "marketRSquared",
            "sector17Matches",
            "sector33Matches",
            "topixStyleMatches",
            "analysisDate",
            "dataPoints",
            "dateRange",
        ]
        assert list(portfolio_response["properties"]) == [
            "portfolioId",
            "portfolioName",
            "weights",
            "totalValue",
            "stockCount",
            "includedStockCount",
            "marketBeta",
            "marketRSquared",
            "sector17Matches",
            "sector33Matches",
            "topixStyleMatches",
            "analysisDate",
            "dataPoints",
            "dateRange",
            "excludedStocks",
        ]
        assert list(schemas["IndexMatch"]["properties"]) == [
            "code",
            "name",
            "rSquared",
        ]
        assert list(
            schemas["src__server__schemas__factor_regression__IndexMatch"][
                "properties"
            ]
        ) == ["indexCode", "indexName", "category", "rSquared", "beta"]

    def test_component_stabilizer_rejects_occupied_unequal_target(self) -> None:
        source = "src__application__contracts__factor_regression__IndexMatch"
        target = "src__server__schemas__factor_regression__IndexMatch"
        schema = {
            "components": {
                "schemas": {
                    source: {"type": "object", "properties": {"source": {}}},
                    target: {"type": "object", "properties": {"target": {}}},
                }
            },
            "paths": {
                "/example": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "$ref": f"#/components/schemas/{source}"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
        }

        with pytest.raises(ValueError, match=f"{source}.*{target}"):
            _stabilize_schema_refs(schema)
        assert source in schema["components"]["schemas"]
        assert target in schema["components"]["schemas"]
        response_ref = schema["paths"]["/example"]["get"]["responses"]["200"][
            "content"
        ]["application/json"]["schema"]["$ref"]
        assert response_ref == f"#/components/schemas/{source}"

    def test_component_stabilizer_preflights_all_renames_before_mutating(self) -> None:
        equal_source = "src__application__contracts__portfolio_factor_regression__DateRange"
        equal_target = "src__server__schemas__portfolio_factor_regression__DateRange"
        collision_source = "src__application__contracts__factor_regression__IndexMatch"
        collision_target = "src__server__schemas__factor_regression__IndexMatch"
        equal_schema = {"type": "object", "properties": {"from": {}, "to": {}}}
        schema = {
            "components": {
                "schemas": {
                    equal_source: equal_schema,
                    equal_target: deepcopy(equal_schema),
                    collision_source: {"type": "object", "properties": {"source": {}}},
                    collision_target: {"type": "object", "properties": {"target": {}}},
                }
            },
            "paths": {
                "/example": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {
                                        "schema": {"$ref": f"#/components/schemas/{equal_source}"}
                                    }
                                }
                            }
                        }
                    }
                }
            },
        }
        original = deepcopy(schema)

        with pytest.raises(ValueError, match=f"{collision_source}.*{collision_target}"):
            _stabilize_schema_refs(schema)

        assert schema == original

    def test_ohlcv_refs_are_stable_and_legacy_compatible(self, openapi_schema) -> None:
        """OHLCV系の $ref が baseline 互換キーへ固定されること"""
        schemas = openapi_schema.get("components", {}).get("schemas", {})
        assert schemas["OHLCVRecord"]["properties"]["volume"]["type"] == "number"

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

    def test_ranking_parameters_exclude_deprecated_liquidity_state(
        self, openapi_schema
    ) -> None:
        parameters = openapi_schema["paths"]["/api/analytics/ranking"]["get"][
            "parameters"
        ]
        assert [parameter["name"] for parameter in parameters] == [
            "date",
            "limit",
            "markets",
            "lookbackDays",
            "periodDays",
            "scope",
            "sector33Name",
            "sector17Name",
            "includeValuation",
            "includeSectorStrength",
            "sectorStrengthFamily",
            "forwardEpsDisclosedWithinDays",
            "regimeState",
            "fundamentalState",
            "riskState",
            "technicalState",
        ]

    def test_removed_screening_endpoint_only_declares_gone_response(
        self, openapi_schema
    ) -> None:
        responses = openapi_schema["paths"]["/api/analytics/screening"]["get"][
            "responses"
        ]

        assert "200" not in responses
        assert responses["410"]["description"] == "Gone"
        assert responses["410"]["content"]["application/json"]["schema"] == {
            "$ref": "#/components/schemas/ErrorResponse"
        }


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

    @pytest.mark.parametrize(
        ("path", "method"),
        [
            ("/api/fundamentals/compute", "post"),
            ("/api/analytics/fundamentals/{symbol}", "get"),
        ],
    )
    def test_fundamentals_errors_use_unified_contract(
        self, openapi_schema, path: str, method: str
    ) -> None:
        responses = openapi_schema["paths"][path][method]["responses"]
        for status in ("404", "409", "422"):
            schema = responses[status]["content"]["application/json"]["schema"]
            assert schema == {"$ref": "#/components/schemas/ErrorResponse"}


def test_fundamentals_contracts_share_date_and_response_semantics(openapi_schema) -> None:
    post = openapi_schema["paths"]["/api/fundamentals/compute"]["post"]
    get = openapi_schema["paths"]["/api/analytics/fundamentals/{symbol}"]["get"]
    response_ref = {"$ref": "#/components/schemas/FundamentalsComputeResponse"}
    assert post["responses"]["200"]["content"]["application/json"]["schema"] == response_ref
    assert get["responses"]["200"]["content"]["application/json"]["schema"] == response_ref

    response_schema = openapi_schema["components"]["schemas"]["FundamentalsComputeResponse"]
    assert "asOfDate" in response_schema["required"]
    assert response_schema["properties"]["asOfDate"]["type"] == "string"

    request_ref = post["requestBody"]["content"]["application/json"]["schema"]["$ref"]
    request_schema = openapi_schema["components"]["schemas"][request_ref.rsplit("/", 1)[-1]]
    assert request_schema["properties"]["from_date"]["anyOf"][0]["format"] == "date"
    assert request_schema["properties"]["to_date"]["anyOf"][0]["format"] == "date"
    assert "display lower bound" in request_schema["properties"]["from_date"]["description"]
    assert "knowledge/event cutoff" in request_schema["properties"]["to_date"]["description"]

    query_parameters = {item["name"]: item for item in get["parameters"]}
    assert query_parameters["from"]["schema"]["anyOf"][0]["format"] == "date"
    assert query_parameters["to"]["schema"]["anyOf"][0]["format"] == "date"
    assert "display lower bound" in query_parameters["from"]["description"]
    assert "knowledge/event cutoff" in query_parameters["to"]["description"]
    assert "Provider vintage" in post["description"]
    assert "market_db_sync" in post["description"]


def test_stabilize_date_range_refs_discovers_portfolio_owner_from_response_ref() -> None:
    qualified_name = "src__application__contracts__portfolio_performance__DateRange"
    unrelated_name = "src__another__module__DateRange"
    candidate = {
        "properties": {
            "from": {"type": "string"},
            "to": {"type": "string"},
        }
    }
    unrelated = {"properties": {"min": {}, "max": {}}}
    reference = {"$ref": f"#/components/schemas/{qualified_name}"}
    schemas = {
        qualified_name: candidate,
        unrelated_name: unrelated,
        "PortfolioPerformanceResponse": {
            "properties": {"dateRange": {"anyOf": [reference, {"type": "null"}]}}
        },
    }

    _stabilize_date_range_refs(schemas)

    assert schemas["DateRange"] == candidate
    assert reference["$ref"] == "#/components/schemas/DateRange"
    assert qualified_name not in schemas
    assert schemas[unrelated_name] == unrelated


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
