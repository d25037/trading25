from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.application.services.strategy_dataset_metadata import (
    StrategyDatasetMetadata,
    _dataset_resolver,
    canonicalize_market_list,
    resolve_dataset_metadata,
    resolve_strategy_dataset_metadata,
    stringify_markets,
    union_market_lists,
)


def test_dataset_resolver_uses_settings_base_path(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, str] = {}

    class _StubDatasetResolver:
        def __init__(self, base_path: str) -> None:
            captured["base_path"] = base_path

    monkeypatch.setattr(
        "src.application.services.strategy_dataset_metadata.get_settings",
        lambda: SimpleNamespace(dataset_base_path="/tmp/datasets"),
    )
    monkeypatch.setattr(
        "src.application.services.strategy_dataset_metadata.DatasetResolver",
        _StubDatasetResolver,
    )

    _dataset_resolver()

    assert captured["base_path"] == "/tmp/datasets"


def test_canonicalize_market_list_normalizes_aliases_and_order() -> None:
    assert canonicalize_market_list(["0113", "prime", "0111", "custom"]) == [
        "prime",
        "growth",
        "custom",
    ]


def test_union_market_lists_and_stringify_markets() -> None:
    markets = union_market_lists([["0112", "prime"], ["growth", "0111"], ["custom"]])

    assert markets == ["prime", "standard", "growth", "custom"]
    assert stringify_markets(markets) == "prime,standard,growth,custom"


def test_resolve_dataset_metadata_success(monkeypatch: pytest.MonkeyPatch) -> None:
    class _StubResolver:
        def exists(self, dataset_name: str) -> bool:
            assert dataset_name == "primeExTopix500_20260316"
            return True

        def get_snapshot_dir(self, dataset_name: str) -> str:
            assert dataset_name == "primeExTopix500_20260316"
            return "/tmp/datasets/primeExTopix500_20260316"

    monkeypatch.setattr(
        "src.application.services.strategy_dataset_metadata._dataset_resolver",
        lambda dataset_base_path=None: _StubResolver(),
    )
    monkeypatch.setattr(
        "src.application.services.strategy_dataset_metadata.read_dataset_snapshot_manifest",
        lambda snapshot_dir: SimpleNamespace(
            dataset=SimpleNamespace(preset="primeExTopix500")
        ),
    )
    monkeypatch.setattr(
        "src.application.services.strategy_dataset_metadata.get_preset",
        lambda preset_name: SimpleNamespace(markets=["0112", "0111"]),
    )

    resolved = resolve_dataset_metadata("primeExTopix500_20260316")

    assert resolved == StrategyDatasetMetadata(
        dataset_name="primeExTopix500_20260316",
        dataset_preset="primeExTopix500",
        screening_default_markets=["prime", "standard"],
    )


def test_resolve_dataset_metadata_rejects_invalid_dataset_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.application.services.strategy_dataset_metadata._dataset_resolver",
        lambda dataset_base_path=None: SimpleNamespace(),
    )

    with pytest.raises(ValueError, match="Invalid dataset name"):
        resolve_dataset_metadata("   ")


def test_resolve_dataset_metadata_rejects_missing_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    class _StubResolver:
        def exists(self, dataset_name: str) -> bool:
            assert dataset_name == "missing"
            return False

    monkeypatch.setattr(
        "src.application.services.strategy_dataset_metadata._dataset_resolver",
        lambda dataset_base_path=None: _StubResolver(),
    )

    with pytest.raises(FileNotFoundError, match="Dataset not found: missing"):
        resolve_dataset_metadata("missing")


def test_resolve_dataset_metadata_rejects_unknown_preset(monkeypatch: pytest.MonkeyPatch) -> None:
    class _StubResolver:
        def exists(self, dataset_name: str) -> bool:
            return True

        def get_snapshot_dir(self, dataset_name: str) -> str:
            return f"/tmp/datasets/{dataset_name}"

    monkeypatch.setattr(
        "src.application.services.strategy_dataset_metadata._dataset_resolver",
        lambda dataset_base_path=None: _StubResolver(),
    )
    monkeypatch.setattr(
        "src.application.services.strategy_dataset_metadata.read_dataset_snapshot_manifest",
        lambda snapshot_dir: SimpleNamespace(
            dataset=SimpleNamespace(preset="unknownPreset")
        ),
    )
    monkeypatch.setattr(
        "src.application.services.strategy_dataset_metadata.get_preset",
        lambda preset_name: None,
    )

    with pytest.raises(ValueError, match="Unknown dataset preset in manifest: unknownPreset"):
        resolve_dataset_metadata("primeExTopix500_20260316")


def test_resolve_strategy_dataset_metadata_returns_none_without_dataset() -> None:
    class _StubLoader:
        def merge_shared_config(self, config: dict[str, object]) -> dict[str, object]:
            assert config == {"entry_filter_params": {}}
            return {"direction": "longonly"}

    resolved = resolve_strategy_dataset_metadata(
        "experimental/demo",
        config_loader=_StubLoader(),
        strategy_config={"entry_filter_params": {}},
    )

    assert resolved == StrategyDatasetMetadata(
        dataset_name=None,
        dataset_preset=None,
        screening_default_markets=None,
    )


def test_resolve_strategy_dataset_metadata_loads_strategy_when_config_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _StubLoader:
        def load_strategy_config(self, strategy_name: str) -> dict[str, object]:
            assert strategy_name == "production/demo"
            return {"shared_config": {"dataset": "primeExTopix500_20260316"}}

        def merge_shared_config(self, config: dict[str, object]) -> dict[str, object]:
            return dict(config["shared_config"])

    monkeypatch.setattr(
        "src.application.services.strategy_dataset_metadata.resolve_dataset_metadata",
        lambda dataset_name, dataset_base_path=None: StrategyDatasetMetadata(
            dataset_name=dataset_name,
            dataset_preset="primeExTopix500",
            screening_default_markets=["prime", "standard"],
        ),
    )

    resolved = resolve_strategy_dataset_metadata(
        "production/demo",
        config_loader=_StubLoader(),
    )

    assert resolved == StrategyDatasetMetadata(
        dataset_name="primeExTopix500_20260316",
        dataset_preset="primeExTopix500",
        screening_default_markets=["prime", "standard"],
    )
