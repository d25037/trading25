"""
シグナルリファレンスAPIテスト
"""

from typing import Literal, Optional

import yaml
from pydantic import BaseModel, Field

from src.server.services.signal_reference_service import (
    build_signal_reference,
    _get_param_model,
    _extract_fields,
    _extract_parent_scalar_fields,
    _get_field_type,
    _get_field_options,
    _unwrap_optional,
    CATEGORY_LABELS,
)
from src.strategies.signals.registry import SIGNAL_REGISTRY


class TestBuildSignalReference:
    """build_signal_reference() のテスト"""

    def test_returns_all_signals(self):
        """全シグナルが返却されること"""
        result = build_signal_reference()
        assert result["total"] == len(SIGNAL_REGISTRY)
        assert len(result["signals"]) == len(SIGNAL_REGISTRY)

    def test_signal_has_required_keys(self):
        """各シグナルが必須キーを持つこと"""
        result = build_signal_reference()
        required_keys = {"key", "name", "category", "description", "usage_hint", "fields", "yaml_snippet", "exit_disabled", "data_requirements"}
        for signal in result["signals"]:
            assert required_keys.issubset(signal.keys()), f"Missing keys in signal '{signal.get('name', 'unknown')}'"

    def test_categories_are_valid(self):
        """全カテゴリが有効なカテゴリであること"""
        result = build_signal_reference()
        valid_categories = set(CATEGORY_LABELS.keys())
        for signal in result["signals"]:
            assert signal["category"] in valid_categories, (
                f"Invalid category '{signal['category']}' for signal '{signal['name']}'"
            )

    def test_categories_list_populated(self):
        """カテゴリ一覧が正しく構築されること"""
        result = build_signal_reference()
        categories = result["categories"]
        assert len(categories) > 0
        for cat in categories:
            assert "key" in cat
            assert "label" in cat
            assert cat["label"] != ""

    def test_sector_category_included(self):
        """sectorカテゴリが含まれること"""
        result = build_signal_reference()
        category_keys = [c["key"] for c in result["categories"]]
        assert "sector" in category_keys

    def test_sector_signals_count(self):
        """セクターシグナルが3つ含まれること"""
        result = build_signal_reference()
        sector_signals = [s for s in result["signals"] if s["category"] == "sector"]
        assert len(sector_signals) == 3

    def test_risk_adjusted_return_is_not_classified_as_fundamental(self):
        """risk_adjusted_return は fundamental 配下ではなく volatility として返ること"""
        result = build_signal_reference()
        signal = next(s for s in result["signals"] if s["key"] == "risk_adjusted_return")
        assert signal["category"] == "volatility"
        parsed = yaml.safe_load(signal["yaml_snippet"])
        assert "risk_adjusted_return" in parsed
        assert "fundamental" not in parsed

    def test_new_fundamental_growth_signals_are_included(self):
        result = build_signal_reference()
        keys = {s["key"] for s in result["signals"]}
        assert "fundamental_dividend_per_share_growth" in keys
        assert "fundamental_cfo_margin" in keys
        assert "fundamental_simple_fcf_margin" in keys
        assert "fundamental_cfo_to_net_profit_ratio" in keys
        assert "fundamental_cfo_yield_growth" in keys
        assert "fundamental_simple_fcf_yield_growth" in keys


class TestYAMLSnippets:
    """YAMLスニペットのテスト"""

    def test_all_snippets_are_valid_yaml(self):
        """全YAMLスニペットが有効なYAMLであること"""
        result = build_signal_reference()
        for signal in result["signals"]:
            snippet = signal["yaml_snippet"]
            if snippet:
                parsed = yaml.safe_load(snippet)
                assert isinstance(parsed, dict), (
                    f"YAML snippet for '{signal['name']}' is not a dict"
                )

    def test_snippets_have_enabled_true(self):
        """enabledがスニペットでtrueになっていること"""
        result = build_signal_reference()
        for signal in result["signals"]:
            snippet = signal["yaml_snippet"]
            if not snippet:
                continue
            parsed = yaml.safe_load(snippet)
            # トップレベルまたはネストされたenabledを探す
            found_enabled = False
            for _key, value in parsed.items():
                if isinstance(value, dict):
                    if "enabled" in value:
                        assert value["enabled"] is True, (
                            f"enabled should be true in snippet for '{signal['name']}'"
                        )
                        found_enabled = True
            assert found_enabled, f"No 'enabled' found in snippet for '{signal['name']}'"

    def test_fundamental_snippets_have_parent_enabled(self):
        """ファンダメンタル個別シグナルのスニペットに親enabled: trueが含まれること"""
        result = build_signal_reference()
        fundamental_individual_signals = [
            s for s in result["signals"]
            if s["category"] == "fundamental" and "fundamental" in (yaml.safe_load(s["yaml_snippet"]) or {})
        ]
        for signal in fundamental_individual_signals:
            parsed = yaml.safe_load(signal["yaml_snippet"])
            assert "fundamental" in parsed, (
                f"Missing 'fundamental' wrapper in snippet for '{signal['name']}'"
            )
            assert parsed["fundamental"]["enabled"] is True, (
                f"Parent 'enabled' should be true in snippet for '{signal['name']}'"
            )


class TestFieldExtraction:
    """フィールド情報抽出のテスト"""

    def test_fields_match_pydantic_model(self):
        """フィールド情報がPydanticモデルと一致すること（fundamental子は親スカラーフィールド含む）"""
        result = build_signal_reference()
        for signal in result["signals"]:
            # registry内のparam_keyと一致するものを探す
            matching_defs = [
                sd for sd in SIGNAL_REGISTRY
                if sd.param_key.replace(".", "_") == signal["key"]
            ]
            if not matching_defs:
                continue
            param_key = matching_defs[0].param_key
            model_class = _get_param_model(param_key)
            if model_class is None:
                continue
            expected_field_names = set(model_class.model_fields.keys())

            # fundamental子シグナル: 親スカラーフィールドも期待値に含める
            parts = param_key.split(".")
            if len(parts) == 2 and parts[0] == "fundamental":
                parent_model = _get_param_model("fundamental")
                if parent_model is not None:
                    parent_scalar_names = {f["name"] for f in _extract_parent_scalar_fields(parent_model)}
                    expected_field_names = expected_field_names | parent_scalar_names

            extracted_field_names = {f["name"] for f in signal["fields"]}
            assert extracted_field_names == expected_field_names, (
                f"Field mismatch for '{signal['name']}': "
                f"expected={expected_field_names}, extracted={extracted_field_names}"
            )

    def test_field_has_required_properties(self):
        """各フィールドが必須プロパティを持つこと"""
        result = build_signal_reference()
        for signal in result["signals"]:
            for field in signal["fields"]:
                assert "name" in field
                assert "type" in field
                assert field["type"] in ("boolean", "number", "string", "select")
                assert "description" in field


class TestGetParamModel:
    """_get_param_model() のテスト"""

    def test_simple_param_key(self):
        """単純なparam_keyでモデルが取得できること"""
        model = _get_param_model("volume")
        assert model is not None
        assert "enabled" in model.model_fields

    def test_nested_param_key(self):
        """ネストされたparam_keyでモデルが取得できること"""
        model = _get_param_model("fundamental.per")
        assert model is not None
        assert "threshold" in model.model_fields

    def test_invalid_param_key(self):
        """存在しないparam_keyでNoneが返ること"""
        model = _get_param_model("nonexistent")
        assert model is None


class TestSignalDefinitionIntegrity:
    """SignalDefinition の category/description/param_key 整合性テスト"""

    def test_all_signals_have_category(self):
        """全シグナルにcategoryが設定されていること"""
        for sd in SIGNAL_REGISTRY:
            assert sd.category, f"Signal '{sd.name}' has no category"
            assert sd.category in CATEGORY_LABELS, (
                f"Signal '{sd.name}' has invalid category '{sd.category}'"
            )

    def test_all_signals_have_description(self):
        """全シグナルにdescriptionが設定されていること"""
        for sd in SIGNAL_REGISTRY:
            assert sd.description, f"Signal '{sd.name}' has no description"

    def test_all_signals_have_param_key(self):
        """全シグナルにparam_keyが設定されていること"""
        for sd in SIGNAL_REGISTRY:
            assert sd.param_key, f"Signal '{sd.name}' has no param_key"

    def test_all_param_keys_resolve_to_models(self):
        """全param_keyが有効なPydanticモデルに解決されること"""
        for sd in SIGNAL_REGISTRY:
            model = _get_param_model(sd.param_key)
            assert model is not None, (
                f"Signal '{sd.name}' param_key '{sd.param_key}' does not resolve to a model"
            )


class TestUnwrapOptional:
    """_unwrap_optional() のテスト"""

    def test_unwrap_optional_int(self):
        """Optional[int]からintを取り出せること"""
        assert _unwrap_optional(Optional[int]) is int

    def test_unwrap_optional_str(self):
        """Optional[str]からstrを取り出せること"""
        assert _unwrap_optional(Optional[str]) is str

    def test_plain_type_unchanged(self):
        """非Optionalの型はそのまま返ること"""
        assert _unwrap_optional(int) is int
        assert _unwrap_optional(str) is str

    def test_optional_literal(self):
        """Optional[Literal[...]]からLiteralを取り出せること"""
        annotation = Optional[Literal["a", "b"]]
        unwrapped = _unwrap_optional(annotation)
        from typing import get_args
        assert get_args(unwrapped) == ("a", "b")


class TestFieldTypeOptional:
    """Optional型フィールドの型判定テスト"""

    def test_optional_literal_returns_select(self):
        """Optional[Literal[...]]がselectと判定されること"""
        annotation = Optional[Literal["sma", "ema"]]
        assert _get_field_type(annotation) == "select"

    def test_optional_int_returns_number(self):
        """Optional[int]がnumberと判定されること"""
        assert _get_field_type(Optional[int]) == "number"

    def test_optional_literal_options(self):
        """Optional[Literal[...]]から選択肢が抽出されること"""
        annotation = Optional[Literal["sma", "ema"]]
        # FieldInfoを手動構築
        from pydantic.fields import FieldInfo as FI
        field_info = FI(default=None)
        options = _get_field_options(annotation, field_info)
        assert options == ["sma", "ema"]


class TestDefaultFactoryField:
    """default_factory フィールドのテスト"""

    def test_default_factory_field_returns_none(self):
        """default_factoryフィールドのdefaultがNoneとなること"""

        class ModelWithFactory(BaseModel):
            items: list[str] = Field(default_factory=list, description="リスト")
            name: str = Field(default="test", description="名前")

        fields = _extract_fields(ModelWithFactory)
        items_field = next(f for f in fields if f["name"] == "items")
        name_field = next(f for f in fields if f["name"] == "name")
        assert items_field["default"] is None  # default_factory → None
        assert name_field["default"] == "test"

    def test_required_field_returns_none(self):
        """requiredフィールドのdefaultがNoneとなること"""

        class ModelWithRequired(BaseModel):
            value: int = Field(description="必須値")

        fields = _extract_fields(ModelWithRequired)
        value_field = next(f for f in fields if f["name"] == "value")
        assert value_field["default"] is None


class TestDescriptionOptionsExtraction:
    """descriptionからの選択肢抽出の安全性テスト"""

    def test_no_brackets_in_desc(self):
        """括弧なしのdescriptionで例外が発生しないこと"""
        from pydantic.fields import FieldInfo as FI
        field_info = FI(default="test", description="パターン abc=xxx def=yyy")
        # 括弧なしの場合、正規表現マッチはするが括弧抽出で空文字列
        result = _get_field_options(str, field_info)
        # マッチしても括弧が無ければNone
        assert result is None

    def test_ascii_brackets_in_desc(self):
        """半角括弧からも選択肢が抽出できること"""
        from pydantic.fields import FieldInfo as FI
        field_info = FI(default="surge", description="方向 (surge=急増, drop=減少)")
        result = _get_field_options(str, field_info)
        assert result == ["surge", "drop"]

    def test_nested_brackets_in_desc(self):
        """入れ子括弧のdescriptionでも選択肢が欠落しないこと"""
        from pydantic.fields import FieldInfo as FI
        field_info = FI(
            default="below",
            description="閾値条件（below=閾値より下（売られすぎ）、above=閾値より上（買われすぎ））",
        )
        result = _get_field_options(str, field_info)
        assert result == ["below", "above"]

    def test_multiple_parenthetical_segments(self):
        """複数括弧セグメントでも選択肢を含むセグメントを拾えること"""
        from pydantic.fields import FieldInfo as FI
        field_info = FI(
            default="below",
            description="補足（一般説明） 閾値条件（below=閾値より下、above=閾値より上）",
        )
        result = _get_field_options(str, field_info)
        assert result == ["below", "above"]

    def test_mismatched_brackets_do_not_raise(self):
        """括弧不整合でも例外なくNoneを返すこと"""
        from pydantic.fields import FieldInfo as FI
        field_info = FI(default="below", description="閾値条件（below=閾値より下, above=閾値より上)")
        result = _get_field_options(str, field_info)
        assert result is None

    def test_rsi_threshold_condition_options_in_reference(self):
        """signal referenceのrsi_threshold.conditionにbelow/aboveが出ること"""
        result = build_signal_reference()
        signal = next(s for s in result["signals"] if s["key"] == "rsi_threshold")
        condition_field = next(f for f in signal["fields"] if f["name"] == "condition")
        assert condition_field["type"] == "select"
        assert condition_field["options"] == ["below", "above"]


class TestFundamentalParentFieldPropagation:
    """ファンダメンタル子シグナルへの親フィールド伝搬テスト"""

    @staticmethod
    def _get_fundamental_child_signals() -> list[dict]:
        """fundamental.* パスの子シグナルのみ取得"""
        result = build_signal_reference()
        # param_keyが "fundamental.*" のシグナル（keyは "fundamental_*"）
        fundamental_child_keys = {
            sd.param_key.replace(".", "_")
            for sd in SIGNAL_REGISTRY
            if sd.param_key.startswith("fundamental.")
        }
        return [
            s for s in result["signals"]
            if s["key"] in fundamental_child_keys
        ]

    def test_fundamental_snippets_have_period_type(self):
        """ファンダメンタル子シグナルのYAMLスニペットにperiod_typeが含まれること"""
        signals = self._get_fundamental_child_signals()
        assert len(signals) > 0, "No fundamental child signals found"
        for signal in signals:
            parsed = yaml.safe_load(signal["yaml_snippet"])
            assert "fundamental" in parsed, (
                f"Missing 'fundamental' wrapper in snippet for '{signal['name']}'"
            )
            assert "period_type" in parsed["fundamental"], (
                f"Missing 'period_type' in snippet for '{signal['name']}'"
            )

    def test_fundamental_fields_include_period_type(self):
        """ファンダメンタル子シグナルのfieldsにperiod_typeが含まれること"""
        signals = self._get_fundamental_child_signals()
        assert len(signals) > 0, "No fundamental child signals found"
        for signal in signals:
            field_names = {f["name"] for f in signal["fields"]}
            assert "period_type" in field_names, (
                f"Missing 'period_type' in fields for '{signal['name']}'"
            )

    def test_period_type_has_select_options(self):
        """period_typeがselect型で正しい選択肢を持つこと"""
        signals = self._get_fundamental_child_signals()
        assert len(signals) > 0
        signal = signals[0]
        period_type_field = next(
            (f for f in signal["fields"] if f["name"] == "period_type"), None
        )
        assert period_type_field is not None
        assert period_type_field["type"] == "select"
        assert period_type_field["options"] == ["all", "FY", "1Q", "2Q", "3Q"]

    def test_child_field_takes_priority_over_parent(self):
        """子が同名フィールドを持つ場合に子優先であること（重複なし検証）"""
        parent_model = _get_param_model("fundamental")
        assert parent_model is not None
        _parent_scalar_names = {f["name"] for f in _extract_parent_scalar_fields(parent_model)}

        signals = self._get_fundamental_child_signals()
        for signal in signals:
            # 各フィールド名の出現が1回のみであること（重複なし）
            field_names = [f["name"] for f in signal["fields"]]
            assert len(field_names) == len(set(field_names)), (
                f"Duplicate field names in '{signal['name']}': {field_names}"
            )
