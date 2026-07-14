"""
OpenAPI Configuration

FastAPI OpenAPI 設定を集中管理する。
ErrorResponse スキーマを全エンドポイントに共通注入する customize_openapi() も提供。
"""

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from src.entrypoints.http.schemas.error import ErrorDetail, ErrorResponse

OPENAPI_TAGS: list[dict[str, str]] = [
    {"name": "Health", "description": "Health check endpoints for service monitoring"},
    {"name": "JQuants Proxy", "description": "Layer 1 raw JQuants data for debugging and development"},
    {"name": "Moomoo OpenD", "description": "Read-only moomoo OpenD US market data for research"},
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
_APPLICATION_FACTOR_DATE_RANGE = (
    "src__application__contracts__factor_regression__DateRange"
)
_APPLICATION_PORTFOLIO_FACTOR_DATE_RANGE = (
    "src__application__contracts__portfolio_factor_regression__DateRange"
)
_APPLICATION_FACTOR_INDEX_MATCH = (
    "src__application__contracts__factor_regression__IndexMatch"
)
_APPLICATION_PORTFOLIO_FACTOR_INDEX_MATCH = (
    "src__application__contracts__portfolio_factor_regression__IndexMatch"
)


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


def _rewrite_refs(node, ref_map: dict[str, str]) -> None:
    if isinstance(node, dict):
        for key, value in list(node.items()):
            if key == "$ref" and isinstance(value, str):
                node[key] = ref_map.get(value, value)
            else:
                _rewrite_refs(value, ref_map)
    elif isinstance(node, list):
        for value in node:
            _rewrite_refs(value, ref_map)


def _rename_openapi_component(
    schema: dict,
    schemas: dict[str, dict],
    source_name: str,
    target_name: str,
) -> None:
    if source_name == target_name or source_name not in schemas:
        return

    source_schema = schemas[source_name]
    target_schema = schemas.get(target_name)
    if target_schema is not None and target_schema != source_schema:
        raise ValueError(
            "Cannot rename OpenAPI component "
            f"{source_name!r} to occupied target {target_name!r}: schemas differ"
        )

    if target_schema is None:
        schemas[target_name] = source_schema
    schemas.pop(source_name)
    _rewrite_refs(
        schema,
        {
            f"#/components/schemas/{source_name}": (
                f"#/components/schemas/{target_name}"
            )
        },
    )


def _preflight_openapi_component_renames(
    schemas: dict[str, dict],
    rename_map: dict[str, str],
) -> None:
    active_renames = {
        source_name: target_name
        for source_name, target_name in rename_map.items()
        if source_name != target_name and source_name in schemas
    }
    target_sources: dict[str, str] = {}
    for source_name, target_name in active_renames.items():
        source_schema = schemas[source_name]
        prior_source = target_sources.get(target_name)
        if prior_source is not None and schemas[prior_source] != source_schema:
            raise ValueError(
                "Cannot rename OpenAPI components "
                f"{prior_source!r} and {source_name!r} to shared target "
                f"{target_name!r}: schemas differ"
            )
        target_sources[target_name] = source_name

        target_schema = schemas.get(target_name)
        if target_schema is not None and target_schema != source_schema:
            raise ValueError(
                "Cannot rename OpenAPI component "
                f"{source_name!r} to occupied target {target_name!r}: schemas differ"
            )


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


def _stabilize_date_range_refs(schemas: dict[str, dict]) -> None:
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

    # 2) PortfolioPerformanceResponse が参照する from/to DateRange を plain key に戻す
    portfolio_perf_response = schemas.get("PortfolioPerformanceResponse")
    if isinstance(portfolio_perf_response, dict):
        date_range_any_of = (
            portfolio_perf_response.get("properties", {})
            .get("dateRange", {})
            .get("anyOf", [])
        )
        if isinstance(date_range_any_of, list):
            component_prefix = "#/components/schemas/"
            for item in date_range_any_of:
                if not isinstance(item, dict):
                    continue
                ref = item.get("$ref")
                if not isinstance(ref, str) or not ref.startswith(component_prefix):
                    continue
                component_name = ref.removeprefix(component_prefix)
                candidate = schemas.get(component_name)
                if not isinstance(candidate, dict) or not _is_from_to_date_range(candidate):
                    continue
                schemas["DateRange"] = candidate
                item["$ref"] = "#/components/schemas/DateRange"
                if component_name != "DateRange":
                    schemas.pop(component_name, None)
                break

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
        date_range_ref = factor_response.get("properties", {}).get("dateRange", {})
        if isinstance(date_range_ref, dict) and date_range_ref.get("$ref") == "#/components/schemas/DateRange":
            date_range_ref["$ref"] = f"#/components/schemas/{_LEGACY_FACTOR_DATE_RANGE}"

    # 4) DatasetSnapshot の dateRange は historical key を維持
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

    # 5) db 系スキーマの dateRange は db 専用 key を参照
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

    # 6) PortfolioFactorRegressionResponse の dateRange は専用 key を参照
    portfolio_factor_response = schemas.get("PortfolioFactorRegressionResponse")
    if isinstance(portfolio_factor_response, dict):
        date_range_ref = portfolio_factor_response.get("properties", {}).get("dateRange", {})
        if isinstance(date_range_ref, dict) and date_range_ref.get("$ref") == "#/components/schemas/DateRange":
            date_range_ref["$ref"] = f"#/components/schemas/{_LEGACY_PORTFOLIO_FACTOR_DATE_RANGE}"


def _stabilize_index_match_refs(schemas: dict[str, dict]) -> None:
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
            items = factor_response.get("properties", {}).get(key, {}).get("items")
            if isinstance(items, dict) and items.get("$ref") == "#/components/schemas/IndexMatch":
                items["$ref"] = f"#/components/schemas/{_LEGACY_FACTOR_INDEX_MATCH}"

    portfolio_factor_response = schemas.get("PortfolioFactorRegressionResponse")
    if isinstance(portfolio_factor_response, dict):
        for key in ("topixStyleMatches", "sector33Matches", "sector17Matches"):
            items = portfolio_factor_response.get("properties", {}).get(key, {}).get("items")
            if isinstance(items, dict) and items.get("$ref") == f"#/components/schemas/{_LEGACY_PORTFOLIO_FACTOR_INDEX_MATCH}":
                items["$ref"] = "#/components/schemas/IndexMatch"


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


def _stabilize_ohlcv_refs(schema: dict, schemas: dict[str, dict]) -> None:
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
        data_items = dataset_ohlcv_response.get("properties", {}).get("data", {}).get("items")
        if isinstance(data_items, dict):
            data_items["$ref"] = "#/components/schemas/OHLCVRecord"

    for response_name in ("OHLCVResponse", "OHLCVResampleResponse"):
        ohlcv_response = schemas.get(response_name)
        if isinstance(ohlcv_response, dict):
            data_items = ohlcv_response.get("properties", {}).get("data", {}).get("items")
            if isinstance(data_items, dict):
                data_items["$ref"] = f"#/components/schemas/{_LEGACY_INDICATORS_OHLCV_SCHEMA}"

    # Path-level dataset_data refs も plain OHLCVRecord に正規化する
    dataset_data_ref = f"#/components/schemas/{_LEGACY_DATASET_DATA_OHLCV_SCHEMA}"
    _rewrite_refs(schema.get("paths", {}), {dataset_data_ref: "#/components/schemas/OHLCVRecord"})

    schemas.pop(_LEGACY_DATASET_DATA_OHLCV_SCHEMA, None)


def _stabilize_schema_refs(schema: dict) -> None:
    """ref名を安定化し、モジュール移設による差分ノイズを抑える。"""
    components = schema.get("components", {})
    schemas: dict[str, dict] = components.get("schemas", {})
    if not schemas:
        return

    rename_map: dict[str, str] = {}

    # application ownership への移設後も既存の公開 component keys を維持する
    rename_map.update(
        {
            _APPLICATION_FACTOR_DATE_RANGE: _LEGACY_FACTOR_DATE_RANGE,
            _APPLICATION_PORTFOLIO_FACTOR_DATE_RANGE: (
                _LEGACY_PORTFOLIO_FACTOR_DATE_RANGE
            ),
            _APPLICATION_FACTOR_INDEX_MATCH: _LEGACY_FACTOR_INDEX_MATCH,
            _APPLICATION_PORTFOLIO_FACTOR_INDEX_MATCH: "IndexMatch",
        }
    )

    # 過去に公開した server 系プレフィックスに寄せる
    for name in list(schemas.keys()):
        if name.startswith(_SCHEMA_PREFIX_NEW):
            rename_map[name] = name.replace(_SCHEMA_PREFIX_NEW, _SCHEMA_PREFIX_LEGACY, 1)

    # Validate the full transaction before changing any schema key or ref.
    _preflight_openapi_component_renames(schemas, rename_map)

    for old_name, new_name in sorted(rename_map.items(), key=lambda x: len(x[0]), reverse=True):
        _rename_openapi_component(schema, schemas, old_name, new_name)
    _stabilize_date_range_refs(schemas)
    _stabilize_index_match_refs(schemas)
    _stabilize_ohlcv_refs(schema, schemas)


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
