"""
OpenAPI Configuration

Hono baseline (contracts/hono-openapi-baseline.json) と互換の OpenAPI 設定を集中管理する。
ErrorResponse スキーマを全エンドポイントに共通注入する customize_openapi() も提供。
"""

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from src.server.schemas.error import ErrorDetail, ErrorResponse

# Hono baseline の 10 operation tags + bt 固有 8 タグ
OPENAPI_TAGS: list[dict[str, str]] = [
    # Hono 互換（operation tags 基準）
    {"name": "Health", "description": "Health check endpoints for service monitoring"},
    {"name": "JQuants Proxy", "description": "Layer 1 raw JQuants data for debugging and development"},
    {"name": "Chart", "description": "Layer 2 optimized chart-ready data (production, caching enabled)"},
    {"name": "Analytics", "description": "Layer 2 computed metrics and analytics for production"},
    {"name": "Market Data", "description": "Market data sync, validation, refresh, and database maintenance"},
    {"name": "Portfolio", "description": "Portfolio CRUD operations and stock holdings management"},
    {"name": "Watchlist", "description": "Watchlist management endpoints"},
    {"name": "Database", "description": "Database maintenance and management"},
    {"name": "Dataset", "description": "Dataset management endpoints"},
    {"name": "Dataset Data", "description": "Dataset data retrieval endpoints"},
    # bt 固有
    {"name": "Backtest", "description": "Backtest execution and management"},
    {"name": "Strategies", "description": "Strategy configuration and management"},
    {"name": "Optimization", "description": "Strategy optimization endpoints"},
    {"name": "Lab", "description": "Experimental analysis lab"},
    {"name": "Signals", "description": "Signal reference and schema"},
    {"name": "Indicators", "description": "Technical indicator computation"},
    {"name": "OHLCV", "description": "OHLCV data retrieval and processing"},
    {"name": "Fundamentals", "description": "Fundamental data analysis"},
]

# ステータスコード → テキスト
STATUS_TEXT: dict[int, str] = {
    400: "Bad Request",
    404: "Not Found",
    500: "Internal Server Error",
}


def get_openapi_config() -> dict:
    """FastAPI コンストラクタに渡す OpenAPI 関連の設定を返す"""
    return {
        "title": "Trading25 API",
        "version": "1.0.0",
        "description": (
            "Financial data analysis API with JQuants integration, "
            "featuring a two-layer architecture "
            "(Layer 1: JQuants Proxy for development/debugging, "
            "Layer 2: Chart & Analytics for production)"
        ),
        "openapi_tags": OPENAPI_TAGS,
        "docs_url": "/doc",
        "redoc_url": None,
        "servers": [
            {"url": "http://localhost:3002", "description": "FastAPI server"},
            {"url": "http://localhost:3001", "description": "Hono server (legacy)"},
        ],
        "contact": {"name": "Trading25 Team"},
        "license_info": {"name": "MIT", "url": "https://opensource.org/licenses/MIT"},
    }


def customize_openapi(app: FastAPI) -> dict:
    """OpenAPI スキーマをカスタマイズ（ErrorResponse 共通注入）

    全エンドポイントの responses に共通の 400/404/500 → ErrorResponse を注入する。
    """
    if app.openapi_schema:
        return app.openapi_schema

    config = get_openapi_config()

    schema = get_openapi(
        title=config["title"],
        version=config["version"],
        description=config["description"],
        routes=app.routes,
        tags=config["openapi_tags"],
        servers=config["servers"],
        contact=config["contact"],
        license_info=config["license_info"],
    )

    # ErrorResponse / ErrorDetail スキーマを components/schemas に注入
    components = schema.setdefault("components", {})
    schemas = components.setdefault("schemas", {})

    for model in [ErrorResponse, ErrorDetail]:
        model_schema = model.model_json_schema(ref_template="#/components/schemas/{model}")
        # $defs から子スキーマを展開
        defs = model_schema.pop("$defs", {})
        for def_name, def_schema in defs.items():
            if def_name not in schemas:
                schemas[def_name] = def_schema
        schemas[model.__name__] = model_schema

    # 全パスの全操作に ErrorResponse を追加
    error_ref = {"$ref": "#/components/schemas/ErrorResponse"}

    paths = schema.get("paths", {})
    for path_data in paths.values():
        for _key, operation in path_data.items():
            if not isinstance(operation, dict) or "responses" not in operation:
                continue
            for code, desc in STATUS_TEXT.items():
                code_str = str(code)
                if code_str not in operation["responses"]:
                    operation["responses"][code_str] = {
                        "description": desc,
                        "content": {"application/json": {"schema": error_ref}},
                    }

    app.openapi_schema = schema
    return schema
