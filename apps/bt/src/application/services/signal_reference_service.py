"""
シグナルリファレンスサービス

SIGNAL_REGISTRYからシグナル定義を読み取り、
フロントエンド向けのリファレンスデータを構築する
"""

from __future__ import annotations

import re
from typing import Any, Union, get_args, get_origin

import yaml
from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from src.domains.strategy.runtime.compiler import (
    CompiledSignalScope,
    resolve_signal_availability,
)
from src.domains.strategy.signals.feature_registry import resolve_feature_requirement_spec
from src.shared.models.signals import SignalParams
from src.entrypoints.http.schemas.signal_reference import SignalFieldTypeValue
from src.domains.strategy.signals.registry import SIGNAL_REGISTRY
from src.shared.models.config import SharedConfig

# カテゴリ表示名マッピング
CATEGORY_LABELS: dict[str, str] = {
    "breakout": "ブレイクアウト",
    "trend": "トレンド",
    "volume": "出来高",
    "oscillator": "オシレーター",
    "volatility": "ボラティリティ",
    "macro": "マクロ",
    "fundamental": "ファンダメンタル",
    "sector": "セクター",
}

_SIGNAL_COPY_OVERRIDES: dict[str, dict[str, list[str] | str]] = {
    "fundamental.forward_eps_growth": {
        "summary": "Forecast-vs-actual EPS growth filter for pre-open fundamental selection.",
        "when_to_use": [
            "Use when the strategy prefers firms with clearly improving next-FY EPS guidance.",
            "Works well with liquidity or trend filters that avoid thinly traded forecast surprises.",
        ],
        "pitfalls": [
            "Forecast metrics require local statements coverage and can go stale if statements sync is behind.",
            "Thresholds are ratios (0.2 means 20%), not percent strings.",
        ],
    },
    "fundamental.forecast_eps_above_recent_fy_actuals": {
        "summary": "Checks whether the latest forecast EPS exceeds the strongest recent FY actual EPS.",
        "when_to_use": [
            "Use when you want breakout-style fundamental improvement rather than simple year-over-year growth.",
        ],
        "pitfalls": [
            "Large lookback windows reduce the eligible universe quickly.",
        ],
    },
    "index_open_gap_regime": {
        "summary": "Same-session open-gap regime filter for market-level timing strategies.",
        "when_to_use": [
            "Use for intraday/current-session strategies that act on the benchmark opening gap.",
        ],
        "pitfalls": [
            "This signal is only actionable under availability rules that support same-session observation.",
        ],
    },
    "margin": {
        "summary": "Uses local margin-interest data as a positioning / crowding proxy.",
        "when_to_use": [
            "Use when the strategy needs crowding or balance-sheet style sentiment input.",
        ],
        "pitfalls": [
            "The market DB must include local margin snapshots; otherwise validation may succeed but execution can lack data.",
        ],
    },
    "universe_rank_bucket": {
        "summary": "Ranks daily `close / SMA(period) - 1` divergence inside the configured universe, with SMA50 as the research default.",
        "when_to_use": [
            "Use when the strategy wants the strongest, weakest, or middle cohort relative to its own index or stock universe.",
            "Start from SMA50 when reading oversold / rebound setups, then widen to SMA100 or shorten to SMA20 as needed.",
            "Pair with a fixed stock universe in shared_config so the bucket semantics stay stable across runs.",
        ],
        "pitfalls": [
            "Single-stock or tiny universes will usually return no matches because decile buckets need enough constituents.",
            "Q10 is the below-SMA cohort and Q1 is the above-SMA cohort; validate which side matches the intended mean-reversion or momentum read.",
        ],
    },
}

_SIGNAL_FIELD_OVERRIDES: dict[str, dict[str, str]] = {
    "enabled": {
        "label": "Enabled",
    },
    "period": {
        "label": "Period",
        "unit": "bars",
    },
    "lookback_period": {
        "label": "Lookback Period",
        "unit": "bars",
    },
    "lookback_days": {
        "label": "Lookback Days",
        "unit": "days",
    },
    "short_period": {
        "label": "Short Period",
        "unit": "bars",
    },
    "ema_period": {
        "label": "EMA Period",
        "unit": "bars",
    },
    "long_period": {
        "label": "Long Period",
        "unit": "bars",
    },
    "window": {
        "label": "Window",
        "unit": "bars",
    },
    "baseline_period": {
        "label": "Baseline Period",
        "unit": "bars",
    },
    "threshold": {
        "label": "Threshold",
        "placeholder": "0.2",
    },
    "ratio_threshold": {
        "label": "Ratio Threshold",
        "placeholder": "1.5",
    },
    "min_threshold": {
        "label": "Min Threshold",
    },
    "max_threshold": {
        "label": "Max Threshold",
    },
    "threshold_value": {
        "label": "Threshold Value",
    },
    "period_type": {
        "label": "Period Type",
    },
    "price_sma_period": {
        "label": "Price SMA Window",
        "unit": "bars",
    },
    "price_bucket": {
        "label": "Price Bucket",
    },
    "min_constituents": {
        "label": "Min Constituents",
    },
    "use_adjusted": {
        "label": "Use Adjusted Values",
    },
    "condition": {
        "label": "Condition",
    },
    "ma_type": {
        "label": "MA Type",
    },
}

_RELATIVE_MODE_UNSUPPORTED_SIGNAL_TYPES = {
    "volume_ratio_above",
    "volume_ratio_below",
    "trading_value",
    "trading_value_ema_ratio_above",
    "trading_value_ema_ratio_below",
    "trading_value_range",
}


def _get_param_model(param_key: str) -> type[BaseModel] | None:
    """param_keyからPydanticモデルクラスを取得

    Args:
        param_key: SignalParams内のフィールドパス (例: 'volume', 'fundamental.per')

    Returns:
        対応するPydanticモデルクラス、または見つからない場合はNone
    """
    parts = param_key.split(".")
    model: type[BaseModel] = SignalParams

    for part in parts:
        if part not in model.model_fields:
            return None
        field_info = model.model_fields[part]
        annotation = field_info.annotation
        if annotation is None:
            return None
        # BaseModelのサブクラスかチェック
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            model = annotation
        else:
            return None

    return model


def _unwrap_optional(annotation: Any) -> Any:
    """Optional[X] / X | None からXを取り出す"""
    origin = get_origin(annotation)
    if origin is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _get_field_type(annotation: Any) -> SignalFieldTypeValue:
    """Pydanticフィールドのアノテーションから表示用型名を取得"""
    annotation = _unwrap_optional(annotation)
    if annotation is bool:
        return "boolean"
    if annotation in (int, float):
        return "number"
    if annotation is str:
        return "string"
    # Literal型のチェック
    args = get_args(annotation)
    if args and all(isinstance(a, str) for a in args):
        return "select"
    if args and all(isinstance(a, (int, float)) for a in args):
        return "number"
    return "string"


def _extract_parenthetical_segments(text: str) -> list[str]:
    """最外括弧の内容を順序通りに抽出（ネスト対応）"""
    close_to_open = {"）": "（", ")": "("}
    stack: list[tuple[str, int]] = []
    segments: list[str] = []

    for idx, char in enumerate(text):
        if char in ("（", "("):
            stack.append((char, idx))
            continue

        if char not in close_to_open or not stack:
            continue

        open_char, start_idx = stack[-1]
        if open_char != close_to_open[char]:
            continue

        stack.pop()
        if not stack:
            segments.append(text[start_idx + 1:idx])

    return segments


def _get_field_options(annotation: Any, field_info: FieldInfo) -> list[str] | None:
    """フィールドの選択肢を抽出

    Literal型の場合はその値を、str型+validatorの場合はdescriptionから抽出を試みる
    """
    annotation = _unwrap_optional(annotation)

    # Literal型: 直接値を取得
    args = get_args(annotation)
    if args and all(isinstance(a, str) for a in args):
        return list(args)

    # str型: descriptionから選択肢を抽出
    if annotation is str and field_info.description:
        for segment in _extract_parenthetical_segments(field_info.description):
            # パターン: "xxx（yyy=aaa、zzz=bbb）" から選択肢を抽出
            options = re.findall(r"([A-Za-z0-9_]+)\s*[=＝]", segment)
            if options:
                return list(dict.fromkeys(options))

            # "sma/ema" のようなスラッシュ区切り
            match = re.search(r"([A-Za-z0-9_]+(?:/[A-Za-z0-9_]+)+)", segment)
            if match:
                return match.group(1).split("/")

    return None


def _extract_constraints_from_json_schema(
    model_class: type[BaseModel],
) -> dict[str, dict[str, float]]:
    """model_json_schema() 経由でフィールドの制約を抽出

    Returns:
        {field_name: {"gt": ..., "ge": ..., "lt": ..., "le": ...}}
    """
    schema = model_class.model_json_schema()
    properties = schema.get("properties", {})
    result: dict[str, dict[str, float]] = {}

    for field_name, field_schema in properties.items():
        constraints: dict[str, float] = {}
        if "exclusiveMinimum" in field_schema:
            constraints["gt"] = field_schema["exclusiveMinimum"]
        if "minimum" in field_schema:
            constraints["ge"] = field_schema["minimum"]
        if "exclusiveMaximum" in field_schema:
            constraints["lt"] = field_schema["exclusiveMaximum"]
        if "maximum" in field_schema:
            constraints["le"] = field_schema["maximum"]
        if constraints:
            result[field_name] = constraints

    return result


def _resolve_default_value(field_info: FieldInfo) -> Any:
    """フィールドのdefault値を正規化

    required / default / default_factory の3パターンを統一的に処理。
    default_factoryは実行時生成のためNoneを返す。
    """
    if not field_info.is_required() and field_info.default is not PydanticUndefined:
        return field_info.default
    return None


def _humanize_field_name(name: str) -> str:
    return name.replace("_", " ").strip().title()


def _build_field_data(
    name: str,
    field_info: FieldInfo,
    constraints_map: dict[str, dict[str, float]],
) -> dict[str, Any] | None:
    """単一フィールドの情報dictを構築

    Returns:
        フィールド情報dict、またはannotationがNoneの場合はNone
    """
    annotation = field_info.annotation
    if annotation is None:
        return None

    field_type = _get_field_type(annotation)
    options = _get_field_options(annotation, field_info)

    if options and field_type == "string":
        field_type = "select"

    override = _SIGNAL_FIELD_OVERRIDES.get(name, {})

    field_data: dict[str, Any] = {
        "name": name,
        "label": override.get("label", _humanize_field_name(name)),
        "type": field_type,
        "description": field_info.description or "",
        "default": _resolve_default_value(field_info),
        "unit": override.get("unit"),
        "placeholder": override.get("placeholder"),
    }

    if options:
        field_data["options"] = options

    if name in constraints_map:
        field_data["constraints"] = constraints_map[name]

    return field_data


def _extract_parent_scalar_fields(parent_model: type[BaseModel]) -> list[dict[str, Any]]:
    """親モデルからスカラーフィールドを抽出（enabled・BaseModelサブクラスはスキップ）

    子シグナルのfield一覧に親レベルのパラメータ（例: period_type）を追加するために使用。
    """
    fields: list[dict[str, Any]] = []
    constraints_map = _extract_constraints_from_json_schema(parent_model)

    for name, field_info in parent_model.model_fields.items():
        annotation = field_info.annotation
        if annotation is None:
            continue
        if name == "enabled":
            continue
        # BaseModelサブクラス（子シグナルモデル）はスキップ
        unwrapped = _unwrap_optional(annotation)
        if isinstance(unwrapped, type) and issubclass(unwrapped, BaseModel):
            continue

        field_data = _build_field_data(name, field_info, constraints_map)
        if field_data is not None:
            fields.append(field_data)

    return fields


def _extract_fields(model_class: type[BaseModel]) -> list[dict[str, Any]]:
    """Pydanticモデルからフィールド情報を自動抽出"""
    fields: list[dict[str, Any]] = []
    constraints_map = _extract_constraints_from_json_schema(model_class)

    for name, field_info in model_class.model_fields.items():
        field_data = _build_field_data(name, field_info, constraints_map)
        if field_data is not None:
            fields.append(field_data)

    return fields


def _generate_yaml_snippet(param_key: str, model_class: type[BaseModel]) -> str:
    """デフォルト値からYAMLスニペットを自動生成

    - enabledは必ずtrueに上書き
    - ファンダメンタル個別シグナルは親enabled: trueを含める
    """
    # モデルのデフォルトインスタンスからdictを取得
    instance = model_class()
    data = instance.model_dump()

    # enabledをtrueに上書き
    if "enabled" in data:
        data["enabled"] = True

    parts = param_key.split(".")

    if len(parts) == 2 and parts[0] == "fundamental":
        # ファンダメンタル個別シグナル: 親構造を含める
        sub_key = parts[1]
        # 親スカラーフィールド（例: period_type）のデフォルト値を注入
        parent_model = _get_param_model("fundamental")
        parent_scalar_defaults: dict[str, Any] = {}
        if parent_model is not None:
            for pf in _extract_parent_scalar_fields(parent_model):
                if pf["default"] is not None:
                    parent_scalar_defaults[pf["name"]] = pf["default"]
        wrapper = {
            "fundamental": {
                "enabled": True,
                **parent_scalar_defaults,
                sub_key: data,
            }
        }
        return yaml.dump(wrapper, default_flow_style=False, allow_unicode=True).strip()
    else:
        # 通常のシグナル: param_keyをルートキーとして出力
        root_key = parts[0]
        wrapper = {root_key: data}
        return yaml.dump(wrapper, default_flow_style=False, allow_unicode=True).strip()


def _build_usage_hint(entry_purpose: str, exit_purpose: str) -> str:
    """entry_purposeとexit_purposeからusage_hintを自動合成"""
    return f"Entry: {entry_purpose} / Exit: {exit_purpose}"


def _build_signal_authoring_copy(
    signal_def: Any,
    yaml_snippet: str,
) -> dict[str, Any]:
    override = _SIGNAL_COPY_OVERRIDES.get(signal_def.param_key, {})
    summary = str(override.get("summary", signal_def.description))
    when_to_use = list(
        override.get(
            "when_to_use",
            [
                signal_def.entry_purpose,
                f"Exit-side behavior: {signal_def.exit_purpose}",
            ],
        )
    )
    pitfalls = list(
        override.get(
            "pitfalls",
            [
                "Confirm the required local data domains are synced before relying on this signal.",
            ],
        )
    )
    examples = list(override.get("examples", [yaml_snippet]))
    return {
        "summary": summary,
        "when_to_use": when_to_use,
        "pitfalls": pitfalls,
        "examples": examples,
    }


_REFERENCE_EXECUTION_SEMANTICS = (
    "standard",
    "next_session_round_trip",
    "current_session_round_trip",
    "overnight_round_trip",
)


def _build_shared_config_for_execution_semantics(
    execution_semantics: str,
) -> SharedConfig:
    return SharedConfig.model_validate(
        {
            "timeframe": "daily",
            "execution_policy": {"mode": execution_semantics},
        },
        context={"resolve_stock_codes": False},
    )


def _build_availability_profiles(signal_def: Any) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    for execution_semantics in _REFERENCE_EXECUTION_SEMANTICS:
        shared_config = _build_shared_config_for_execution_semantics(
            execution_semantics
        )
        supported_scopes = [CompiledSignalScope.ENTRY]
        if execution_semantics == "standard" and not signal_def.exit_disabled:
            supported_scopes.append(CompiledSignalScope.EXIT)
        for scope in supported_scopes:
            profiles.append(
                {
                    "scope": scope,
                    "execution_semantics": execution_semantics,
                    "availability": resolve_signal_availability(
                        scope=scope,
                        signal_def=signal_def,
                        shared_config=shared_config,
                    ).model_dump(mode="json"),
                }
            )
    return profiles


def _derive_signal_type(param_key: str) -> str:
    return param_key.split(".")[-1]


def _build_chart_capability(signal_def: Any) -> dict[str, Any]:
    signal_type = _derive_signal_type(signal_def.param_key)
    requirement_domains = {
        resolve_feature_requirement_spec(requirement).data_domain
        for requirement in signal_def.data_requirements
    }
    return {
        "supported": True,
        "supported_modes": ["entry"] if signal_def.exit_disabled else ["entry", "exit"],
        "supports_relative_mode": signal_type not in _RELATIVE_MODE_UNSUPPORTED_SIGNAL_TYPES,
        "requires_benchmark": "benchmark" in requirement_domains,
        "requires_sector_data": "sector" in requirement_domains,
        "requires_margin_data": "margin" in requirement_domains,
        "requires_statements_data": "statements" in requirement_domains,
    }


def build_signal_reference() -> dict[str, Any]:
    """全シグナルのリファレンスデータを構築

    Returns:
        dict with keys: signals, categories, total
    """
    signals: list[dict[str, Any]] = []

    for signal_def in SIGNAL_REGISTRY:
        model_class = _get_param_model(signal_def.param_key)

        if model_class is None:
            # モデルが見つからない場合はフィールドなしで登録
            fields: list[dict[str, Any]] = []
            yaml_snippet = ""
        else:
            fields = _extract_fields(model_class)

            # fundamental子シグナル: 親スカラーフィールド（例: period_type）を先頭に追加
            parts = signal_def.param_key.split(".")
            if len(parts) == 2 and parts[0] == "fundamental":
                parent_model = _get_param_model("fundamental")
                if parent_model is not None:
                    child_field_names = {f["name"] for f in fields}
                    parent_fields = [
                        f for f in _extract_parent_scalar_fields(parent_model)
                        if f["name"] not in child_field_names
                    ]
                    fields = parent_fields + fields

            yaml_snippet = _generate_yaml_snippet(signal_def.param_key, model_class)

        signal_data = {
            "key": signal_def.param_key.replace(".", "_"),  # param_keyベースの安定スラッグ
            "signal_type": _derive_signal_type(signal_def.param_key),
            "name": signal_def.name,
            "category": signal_def.category,
            "description": signal_def.description,
            **_build_signal_authoring_copy(signal_def, yaml_snippet),
            "usage_hint": _build_usage_hint(signal_def.entry_purpose, signal_def.exit_purpose),
            "fields": fields,
            "yaml_snippet": yaml_snippet,
            "exit_disabled": signal_def.exit_disabled,
            "data_requirements": signal_def.data_requirements,
            "availability_profiles": _build_availability_profiles(signal_def),
            "chart": _build_chart_capability(signal_def),
        }
        signals.append(signal_data)

    # カテゴリ一覧を構築（順序保持）
    seen_categories: set[str] = set()
    categories: list[dict[str, Any]] = []
    for signal in signals:
        cat = signal["category"]
        if cat not in seen_categories:
            seen_categories.add(cat)
            categories.append({
                "key": cat,
                "label": CATEGORY_LABELS.get(cat, cat),
            })

    return {
        "signals": signals,
        "categories": categories,
        "total": len(signals),
    }
