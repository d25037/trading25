"""
シグナルシステム同期テスト

SIGNAL_REGISTRY / build_signal_reference / SignalParams JSON Schema の
整合性をCIレベルで保証する。
"""

from src.shared.models.signals import SignalParams
from src.application.services.signal_reference_service import (
    _get_param_model,
    build_signal_reference,
)
from src.domains.strategy.signals.registry import SIGNAL_REGISTRY


class TestRegistryReferenceSync:
    """SIGNAL_REGISTRY と build_signal_reference() の同期テスト"""

    def test_all_registry_param_keys_in_reference(self):
        """全 param_key が build_signal_reference() の出力に含まれること"""
        result = build_signal_reference()
        reference_keys = {s["key"] for s in result["signals"]}
        for signal_def in SIGNAL_REGISTRY:
            expected_key = signal_def.param_key.replace(".", "_")
            assert expected_key in reference_keys, (
                f"Signal '{signal_def.name}' (param_key='{signal_def.param_key}') "
                f"is in SIGNAL_REGISTRY but not in build_signal_reference() output"
            )

    def test_reference_count_matches_registry(self):
        """build_signal_reference() のシグナル数が SIGNAL_REGISTRY と一致"""
        result = build_signal_reference()
        assert result["total"] == len(SIGNAL_REGISTRY)


class TestConstraintsSync:
    """API側 constraints と Pydanticモデル JSON Schema の整合性テスト"""

    def test_constraints_match_json_schema(self):
        """build_signal_reference() の constraints が
        Pydanticモデルの JSON Schema と一致すること"""
        result = build_signal_reference()

        for signal in result["signals"]:
            # registry から対応する signal_def を取得
            matching_defs = [
                sd for sd in SIGNAL_REGISTRY
                if sd.param_key.replace(".", "_") == signal["key"]
            ]
            if not matching_defs:
                continue

            model_class = _get_param_model(matching_defs[0].param_key)
            if model_class is None:
                continue

            schema = model_class.model_json_schema()
            properties = schema.get("properties", {})

            for field_data in signal["fields"]:
                field_name = field_data["name"]
                constraints = field_data.get("constraints")

                if field_name not in properties:
                    continue

                field_schema = properties[field_name]

                # constraints がある場合、JSON Schema と一致するか検証
                if constraints:
                    if "gt" in constraints:
                        assert field_schema.get("exclusiveMinimum") == constraints["gt"], (
                            f"Signal '{signal['name']}' field '{field_name}': "
                            f"gt mismatch"
                        )
                    if "ge" in constraints:
                        assert field_schema.get("minimum") == constraints["ge"], (
                            f"Signal '{signal['name']}' field '{field_name}': "
                            f"ge mismatch"
                        )
                    if "lt" in constraints:
                        assert field_schema.get("exclusiveMaximum") == constraints["lt"], (
                            f"Signal '{signal['name']}' field '{field_name}': "
                            f"lt mismatch"
                        )
                    if "le" in constraints:
                        assert field_schema.get("maximum") == constraints["le"], (
                            f"Signal '{signal['name']}' field '{field_name}': "
                            f"le mismatch"
                        )


class TestDataRequirementsSync:
    """data_requirements と data_checker の整合性テスト"""

    def test_data_checker_and_requirements_consistency(self):
        """data_requirements が設定されているシグナルは data_checker も持つべき（逆もまた然り）"""
        for signal_def in SIGNAL_REGISTRY:
            has_requirements = len(signal_def.data_requirements) > 0
            has_checker = signal_def.data_checker is not None
            assert has_requirements == has_checker, (
                f"Signal '{signal_def.name}' (param_key='{signal_def.param_key}'): "
                f"data_requirements={'set' if has_requirements else 'empty'} "
                f"but data_checker={'set' if has_checker else 'None'}"
            )

    def test_all_signals_have_data_requirements(self):
        """全シグナルに data_requirements が設定されていること"""
        for signal_def in SIGNAL_REGISTRY:
            assert len(signal_def.data_requirements) > 0, (
                f"Signal '{signal_def.name}' (param_key='{signal_def.param_key}') "
                f"has no data_requirements set"
            )

    def test_data_requirements_in_reference_output(self):
        """build_signal_reference() 出力の全シグナルに data_requirements が含まれること"""
        result = build_signal_reference()
        for signal in result["signals"]:
            assert "data_requirements" in signal, (
                f"Signal '{signal['name']}' missing 'data_requirements' in reference output"
            )
            assert isinstance(signal["data_requirements"], list), (
                f"Signal '{signal['name']}' data_requirements is not a list"
            )


class TestJsonSchemaEndpointConsistency:
    """SignalParams.model_json_schema() の整合性テスト"""

    def test_schema_contains_all_signal_fields(self):
        """JSON Schema が全シグナルフィールドを含むこと"""
        schema = SignalParams.model_json_schema()
        properties = schema.get("properties", {})

        # registry の top-level param_key が全てプロパティに含まれる
        top_level_keys = set()
        for signal_def in SIGNAL_REGISTRY:
            top_key = signal_def.param_key.split(".")[0]
            top_level_keys.add(top_key)

        for key in top_level_keys:
            assert key in properties, (
                f"Top-level param_key '{key}' not found in SignalParams JSON Schema"
            )

    def test_schema_is_valid_dict(self):
        """JSON Schema が有効な辞書であること"""
        schema = SignalParams.model_json_schema()
        assert isinstance(schema, dict)
        assert "properties" in schema
        assert "title" in schema
