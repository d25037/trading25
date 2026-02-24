"""
OpenAPI Configuration

Hono baseline (contracts/hono-openapi-baseline.json) と互換の OpenAPI 設定を集中管理する。
ErrorResponse スキーマを全エンドポイントに共通注入する customize_openapi() も提供。
"""

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from src.entrypoints.http.schemas.error import ErrorDetail, ErrorResponse

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

_SCHEMA_PREFIX_NEW = "src__entrypoints__http__schemas__"
_SCHEMA_PREFIX_LEGACY = "src__server__schemas__"
_LEGACY_INDICATORS_OHLCV_SCHEMA = "src__server__schemas__indicators__OHLCVRecord"
_LEGACY_DATASET_DATA_OHLCV_SCHEMA = "src__server__schemas__dataset_data__OHLCVRecord"
_LEGACY_DATASET_DATE_RANGE = "src__server__schemas__dataset__DateRange"
_LEGACY_DB_DATE_RANGE = "src__server__schemas__db__DateRange"
_LEGACY_FACTOR_DATE_RANGE = "src__server__schemas__factor_regression__DateRange"
_LEGACY_PORTFOLIO_FACTOR_DATE_RANGE = "src__server__schemas__portfolio_factor_regression__DateRange"
_LEGACY_FACTOR_INDEX_MATCH = "src__server__schemas__factor_regression__IndexMatch"
_LEGACY_PORTFOLIO_FACTOR_INDEX_MATCH = "src__server__schemas__portfolio_factor_regression__IndexMatch"
_LEGACY_PORTFOLIO_PERF_DATE_RANGE = "src__server__schemas__portfolio_performance__DateRange"


def _normalize_integral_floats(node):
    """OpenAPI内の 0.0 / 1.0 などを 0 / 1 に正規化する。"""
    if isinstance(node, dict):
        for key, value in list(node.items()):
            node[key] = _normalize_integral_floats(value)
        return node
    if isinstance(node, list):
        for idx, value in enumerate(node):
            node[idx] = _normalize_integral_floats(value)
        return node
    if isinstance(node, float) and node.is_integer():
        return int(node)
    return node


def _stabilize_schema_refs(schema: dict) -> None:
    """ref名を legacy 互換へ寄せて、モジュール移設による差分ノイズを抑える。"""
    components = schema.get("components", {})
    schemas: dict[str, dict] = components.get("schemas", {})
    if not schemas:
        return

    rename_map: dict[str, str] = {}

    # 旧 server 系プレフィックスに寄せる
    for name in list(schemas.keys()):
        if name.startswith(_SCHEMA_PREFIX_NEW):
            rename_map[name] = name.replace(_SCHEMA_PREFIX_NEW, _SCHEMA_PREFIX_LEGACY, 1)

    # schema keys rename
    for old_name, new_name in sorted(rename_map.items(), key=lambda x: len(x[0]), reverse=True):
        if old_name not in schemas:
            continue
        value = schemas.pop(old_name)
        if new_name not in schemas:
            schemas[new_name] = value

    # $ref rename
    ref_map = {
        f"#/components/schemas/{old}": f"#/components/schemas/{new}"
        for old, new in rename_map.items()
    }

    def _rewrite_refs(node) -> None:
        if isinstance(node, dict):
            for key, value in list(node.items()):
                if key == "$ref" and isinstance(value, str):
                    node[key] = ref_map.get(value, value)
                else:
                    _rewrite_refs(value)
        elif isinstance(node, list):
            for value in node:
                _rewrite_refs(value)

    _rewrite_refs(schema)

    # DateRange 名称衝突を historical naming に合わせる
    def _is_min_max_date_range(candidate: dict | None) -> bool:
        if not isinstance(candidate, dict):
            return False
        props = candidate.get("properties", {})
        return isinstance(props, dict) and "min" in props and "max" in props

    def _is_from_to_date_range(candidate: dict | None) -> bool:
        if not isinstance(candidate, dict):
            return False
        props = candidate.get("properties", {})
        return isinstance(props, dict) and "from" in props and "to" in props

    plain_date_range = schemas.get("DateRange")
    dataset_date_range_schema: dict | None = None
    db_date_range_schema: dict | None = None
    if _is_min_max_date_range(plain_date_range):
        dataset_date_range_schema = plain_date_range
        db_date_range_schema = plain_date_range

    existing_dataset_date_range = schemas.get(_LEGACY_DATASET_DATE_RANGE)
    if _is_min_max_date_range(existing_dataset_date_range):
        dataset_date_range_schema = existing_dataset_date_range

    existing_db_date_range = schemas.get(_LEGACY_DB_DATE_RANGE)
    if _is_min_max_date_range(existing_db_date_range):
        db_date_range_schema = existing_db_date_range

    if dataset_date_range_schema is None:
        dataset_date_range_schema = db_date_range_schema
    if db_date_range_schema is None:
        db_date_range_schema = dataset_date_range_schema

    # 1) plain DateRange に factor-regression 用が入っている場合は legacy key に移す
    date_range_schema = schemas.get("DateRange")
    if isinstance(date_range_schema, dict) and date_range_schema.get("description") == "分析期間":
        schemas.setdefault(_LEGACY_FACTOR_DATE_RANGE, date_range_schema)

    # 2) portfolio_performance 用 DateRange を plain DateRange 側へ戻す
    portfolio_perf_date_range = schemas.get(_LEGACY_PORTFOLIO_PERF_DATE_RANGE)
    if isinstance(portfolio_perf_date_range, dict):
        schemas["DateRange"] = portfolio_perf_date_range
        schemas.pop(_LEGACY_PORTFOLIO_PERF_DATE_RANGE, None)

    portfolio_factor_date_range = schemas.get(_LEGACY_PORTFOLIO_FACTOR_DATE_RANGE)
    if not isinstance(portfolio_factor_date_range, dict):
        current_plain_date_range = schemas.get("DateRange")
        if isinstance(current_plain_date_range, dict) and _is_from_to_date_range(current_plain_date_range):
            schemas[_LEGACY_PORTFOLIO_FACTOR_DATE_RANGE] = current_plain_date_range

    # 3) FactorRegressionResponse の dateRange は factor-regression 専用 key を参照
    if _LEGACY_FACTOR_DATE_RANGE not in schemas:
        current_plain_date_range = schemas.get("DateRange")
        if isinstance(current_plain_date_range, dict) and _is_from_to_date_range(current_plain_date_range):
            schemas[_LEGACY_FACTOR_DATE_RANGE] = current_plain_date_range

    factor_response = schemas.get("FactorRegressionResponse")
    if isinstance(factor_response, dict):
        date_range_ref = (
            factor_response.get("properties", {})
            .get("dateRange", {})
        )
        if isinstance(date_range_ref, dict) and date_range_ref.get("$ref") == "#/components/schemas/DateRange":
            date_range_ref["$ref"] = f"#/components/schemas/{_LEGACY_FACTOR_DATE_RANGE}"

    # 4) PortfolioPerformanceResponse の dateRange は plain DateRange を参照
    portfolio_perf_response = schemas.get("PortfolioPerformanceResponse")
    if isinstance(portfolio_perf_response, dict):
        date_range_any_of = (
            portfolio_perf_response.get("properties", {})
            .get("dateRange", {})
            .get("anyOf", [])
        )
        if isinstance(date_range_any_of, list):
            for item in date_range_any_of:
                if isinstance(item, dict) and item.get("$ref") == f"#/components/schemas/{_LEGACY_PORTFOLIO_PERF_DATE_RANGE}":
                    item["$ref"] = "#/components/schemas/DateRange"

    # 5) DatasetSnapshot の dateRange は historical key を維持
    if isinstance(dataset_date_range_schema, dict):
        schemas.setdefault(_LEGACY_DATASET_DATE_RANGE, dataset_date_range_schema)
    if isinstance(db_date_range_schema, dict):
        schemas.setdefault(_LEGACY_DB_DATE_RANGE, db_date_range_schema)

    dataset_snapshot = schemas.get("DatasetSnapshot")
    if isinstance(dataset_snapshot, dict):
        date_range_any_of = (
            dataset_snapshot.get("properties", {})
            .get("dateRange", {})
            .get("anyOf", [])
        )
        if isinstance(date_range_any_of, list):
            for item in date_range_any_of:
                if isinstance(item, dict) and item.get("$ref") == "#/components/schemas/DateRange":
                    item["$ref"] = f"#/components/schemas/{_LEGACY_DATASET_DATE_RANGE}"

    # 6) db 系スキーマの dateRange は db 専用 key を参照
    for schema_name in ("IndicesStats", "StockDataStats", "StockDataValidation", "TopixStats"):
        target_schema = schemas.get(schema_name)
        if not isinstance(target_schema, dict):
            continue
        date_range_any_of = (
            target_schema.get("properties", {})
            .get("dateRange", {})
            .get("anyOf", [])
        )
        if isinstance(date_range_any_of, list):
            for item in date_range_any_of:
                if isinstance(item, dict) and item.get("$ref") == "#/components/schemas/DateRange":
                    item["$ref"] = f"#/components/schemas/{_LEGACY_DB_DATE_RANGE}"

    # 7) PortfolioFactorRegressionResponse の dateRange は専用 key を参照
    portfolio_factor_response = schemas.get("PortfolioFactorRegressionResponse")
    if isinstance(portfolio_factor_response, dict):
        date_range_ref = (
            portfolio_factor_response.get("properties", {})
            .get("dateRange", {})
        )
        if isinstance(date_range_ref, dict) and date_range_ref.get("$ref") == "#/components/schemas/DateRange":
            date_range_ref["$ref"] = f"#/components/schemas/{_LEGACY_PORTFOLIO_FACTOR_DATE_RANGE}"

    # IndexMatch 名称衝突を historical naming に合わせる
    # factor_regression 側は legacy key、portfolio_factor_regression 側は plain IndexMatch を維持する
    plain_index_match = schemas.get("IndexMatch")
    if isinstance(plain_index_match, dict):
        schemas.setdefault(_LEGACY_FACTOR_INDEX_MATCH, plain_index_match)

    portfolio_factor_index_match = schemas.get(_LEGACY_PORTFOLIO_FACTOR_INDEX_MATCH)
    if isinstance(portfolio_factor_index_match, dict):
        schemas["IndexMatch"] = portfolio_factor_index_match
        schemas.pop(_LEGACY_PORTFOLIO_FACTOR_INDEX_MATCH, None)

    factor_response = schemas.get("FactorRegressionResponse")
    if isinstance(factor_response, dict):
        for key in ("topixStyleMatches", "sector33Matches", "sector17Matches"):
            items = (
                factor_response.get("properties", {})
                .get(key, {})
                .get("items")
            )
            if isinstance(items, dict) and items.get("$ref") == "#/components/schemas/IndexMatch":
                items["$ref"] = f"#/components/schemas/{_LEGACY_FACTOR_INDEX_MATCH}"

    portfolio_factor_response = schemas.get("PortfolioFactorRegressionResponse")
    if isinstance(portfolio_factor_response, dict):
        for key in ("topixStyleMatches", "sector33Matches", "sector17Matches"):
            items = (
                portfolio_factor_response.get("properties", {})
                .get(key, {})
                .get("items")
            )
            if isinstance(items, dict) and items.get("$ref") == f"#/components/schemas/{_LEGACY_PORTFOLIO_FACTOR_INDEX_MATCH}":
                items["$ref"] = "#/components/schemas/IndexMatch"

    # OHLCVRecord 名称衝突を固定（dataset_data は plain、indicators は legacy key）
    def _is_dataset_ohlcv(candidate: dict | None) -> bool:
        if not isinstance(candidate, dict):
            return False
        props = candidate.get("properties", {})
        if not isinstance(props, dict):
            return False
        volume_type = props.get("volume", {}).get("type")
        return candidate.get("description") is None and volume_type == "integer"

    def _is_indicators_ohlcv(candidate: dict | None) -> bool:
        if not isinstance(candidate, dict):
            return False
        props = candidate.get("properties", {})
        if not isinstance(props, dict):
            return False
        volume_type = props.get("volume", {}).get("type")
        return candidate.get("description") == "OHLCVレコード" and volume_type == "number"

    dataset_ohlcv_schema: dict | None = None
    indicators_ohlcv_schema: dict | None = None

    for name, candidate in schemas.items():
        if "OHLCVRecord" not in name:
            continue
        if _is_dataset_ohlcv(candidate):
            dataset_ohlcv_schema = candidate
        if _is_indicators_ohlcv(candidate):
            indicators_ohlcv_schema = candidate

    if isinstance(dataset_ohlcv_schema, dict):
        schemas["OHLCVRecord"] = dataset_ohlcv_schema
    if isinstance(indicators_ohlcv_schema, dict):
        schemas[_LEGACY_INDICATORS_OHLCV_SCHEMA] = indicators_ohlcv_schema

    dataset_ohlcv_response = schemas.get("DatasetOHLCVResponse")
    if isinstance(dataset_ohlcv_response, dict):
        data_items = (
            dataset_ohlcv_response.get("properties", {})
            .get("data", {})
            .get("items")
        )
        if isinstance(data_items, dict):
            data_items["$ref"] = "#/components/schemas/OHLCVRecord"

    for response_name in ("OHLCVResponse", "OHLCVResampleResponse"):
        ohlcv_response = schemas.get(response_name)
        if isinstance(ohlcv_response, dict):
            data_items = (
                ohlcv_response.get("properties", {})
                .get("data", {})
                .get("items")
            )
            if isinstance(data_items, dict):
                data_items["$ref"] = f"#/components/schemas/{_LEGACY_INDICATORS_OHLCV_SCHEMA}"

    # Path-level dataset_data refs も plain OHLCVRecord に正規化する
    dataset_data_ref = f"#/components/schemas/{_LEGACY_DATASET_DATA_OHLCV_SCHEMA}"

    def _rewrite_dataset_data_ohlcv_refs(node) -> None:
        if isinstance(node, dict):
            for key, value in list(node.items()):
                if key == "$ref" and value == dataset_data_ref:
                    node[key] = "#/components/schemas/OHLCVRecord"
                else:
                    _rewrite_dataset_data_ohlcv_refs(value)
        elif isinstance(node, list):
            for value in node:
                _rewrite_dataset_data_ohlcv_refs(value)

    _rewrite_dataset_data_ohlcv_refs(schema.get("paths", {}))

    schemas.pop(_LEGACY_DATASET_DATA_OHLCV_SCHEMA, None)


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

    _stabilize_schema_refs(schema)
    _normalize_integral_floats(schema)

    app.openapi_schema = schema
    return schema
