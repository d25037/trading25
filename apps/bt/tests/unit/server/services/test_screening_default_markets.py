from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.application.services.screening_default_markets import (
    _build_scope_label,
    resolve_default_screening_markets,
    validate_selected_screening_strategy_datasets,
)
from src.application.services.strategy_dataset_metadata import StrategyDatasetMetadata


def _make_loaded_config(universe_preset: str, entry_decidability: str) -> SimpleNamespace:
    return SimpleNamespace(
        entry_params={},
        exit_params={},
        shared_config=SimpleNamespace(
            data_source="market",
            universe_preset=universe_preset,
            dataset=universe_preset,
        ),
        compiled_strategy=SimpleNamespace(),
        screening_support="supported",
        entry_decidability=entry_decidability,
    )


def test_resolve_default_screening_markets_uses_selected_strategy_union(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = MagicMock()
    loader.get_strategy_metadata.return_value = [
        SimpleNamespace(
            name="production/range_break_v15",
            category="production",
            path=Path("/tmp/production/range_break_v15.yaml"),
        ),
        SimpleNamespace(
            name="production/forward_eps_driven",
            category="production",
            path=Path("/tmp/production/forward_eps_driven.yaml"),
        ),
        SimpleNamespace(
            name="production/topix_gap_down_intraday",
            category="production",
            path=Path("/tmp/production/topix_gap_down_intraday.yaml"),
        ),
    ]

    monkeypatch.setattr(
        "src.application.services.screening_default_markets.load_strategy_screening_config",
        lambda _loader, name: {
            "production/range_break_v15": _make_loaded_config("primeMarket", "pre_open_decidable"),
            "production/forward_eps_driven": _make_loaded_config("standardMarket", "pre_open_decidable"),
            "production/topix_gap_down_intraday": _make_loaded_config(
                "growthMarket",
                "requires_same_session_observation",
            ),
        }[name],
    )

    resolved = resolve_default_screening_markets(
        entry_decidability="pre_open_decidable",
        strategies="production/forward_eps_driven,production/range_break_v15",
        config_loader=loader,
    )

    assert resolved.strategy_names == [
        "production/forward_eps_driven",
        "production/range_break_v15",
    ]
    assert resolved.markets == ["prime", "standard"]
    assert resolved.markets_param == "prime,standard"
    assert resolved.scope_label == "Standard Market + Prime Market"


def test_resolve_default_screening_markets_uses_all_eligible_when_unselected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = MagicMock()
    loader.get_strategy_metadata.return_value = [
        SimpleNamespace(
            name="production/range_break_v15",
            category="production",
            path=Path("/tmp/production/range_break_v15.yaml"),
        ),
        SimpleNamespace(
            name="production/forward_eps_driven",
            category="production",
            path=Path("/tmp/production/forward_eps_driven.yaml"),
        ),
        SimpleNamespace(
            name="production/topix_gap_down_intraday",
            category="production",
            path=Path("/tmp/production/topix_gap_down_intraday.yaml"),
        ),
    ]

    monkeypatch.setattr(
        "src.application.services.screening_default_markets.load_strategy_screening_config",
        lambda _loader, name: {
            "production/range_break_v15": _make_loaded_config("primeMarket", "pre_open_decidable"),
            "production/forward_eps_driven": _make_loaded_config("standardMarket", "pre_open_decidable"),
            "production/topix_gap_down_intraday": _make_loaded_config(
                "growthMarket",
                "requires_same_session_observation",
            ),
        }[name],
    )

    resolved = resolve_default_screening_markets(
        entry_decidability="pre_open_decidable",
        strategies=None,
        config_loader=loader,
    )

    assert resolved.strategy_names == [
        "production/forward_eps_driven",
        "production/range_break_v15",
    ]
    assert resolved.markets == ["prime", "standard"]
    assert resolved.markets_param == "prime,standard"
    assert resolved.scope_label == "Standard Market + Prime Market"


def test_resolve_default_screening_markets_raises_for_unresolvable_dataset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = MagicMock()
    loader.get_strategy_metadata.return_value = [
        SimpleNamespace(
            name="production/broken",
            category="production",
            path=Path("/tmp/production/broken.yaml"),
        ),
    ]

    monkeypatch.setattr(
        "src.application.services.screening_default_markets.load_strategy_screening_config",
        lambda _loader, _name: _make_loaded_config("unknownPreset", "pre_open_decidable"),
    )

    with pytest.raises(ValueError, match="production/broken"):
        resolve_default_screening_markets(
            entry_decidability="pre_open_decidable",
            strategies=None,
            config_loader=loader,
        )


def test_validate_selected_screening_strategy_datasets_ignores_unselected_broken_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = MagicMock()
    loader.get_strategy_metadata.return_value = [
        SimpleNamespace(
            name="production/good",
            category="production",
            path=Path("/tmp/production/good.yaml"),
        ),
        SimpleNamespace(
            name="production/broken",
            category="production",
            path=Path("/tmp/production/broken.yaml"),
        ),
    ]

    monkeypatch.setattr(
        "src.application.services.screening_default_markets.load_strategy_screening_config",
        lambda _loader, name: {
            "production/good": _make_loaded_config("primeMarket", "pre_open_decidable"),
            "production/broken": _make_loaded_config("unknownPreset", "pre_open_decidable"),
        }[name],
    )

    selected = validate_selected_screening_strategy_datasets(
        entry_decidability="pre_open_decidable",
        strategies="production/good",
        config_loader=loader,
    )

    assert selected == ["production/good"]


def test_validate_selected_screening_strategy_datasets_raises_for_selected_broken_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = MagicMock()
    loader.get_strategy_metadata.return_value = [
        SimpleNamespace(
            name="production/broken",
            category="production",
            path=Path("/tmp/production/broken.yaml"),
        ),
    ]

    monkeypatch.setattr(
        "src.application.services.screening_default_markets.load_strategy_screening_config",
        lambda _loader, _name: _make_loaded_config("unknownPreset", "pre_open_decidable"),
    )

    with pytest.raises(
        ValueError,
        match=r"Invalid universe for screening strategy production/broken: shared_config.universe_preset is required",
    ):
        validate_selected_screening_strategy_datasets(
            entry_decidability="pre_open_decidable",
            strategies="production/broken",
            config_loader=loader,
        )


def test_build_scope_label_falls_back_when_dataset_preset_is_missing() -> None:
    assert (
        _build_scope_label(
            [
                StrategyDatasetMetadata(
                    dataset_name="dataset-a",
                    dataset_preset=None,
                    screening_default_markets=["prime"],
                )
            ],
            ["prime"],
        )
        == "Prime"
    )


def test_build_scope_label_falls_back_when_preset_label_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.application.services.screening_default_markets.get_preset_label",
        lambda _preset_name: None,
    )

    assert (
        _build_scope_label(
            [
                StrategyDatasetMetadata(
                    dataset_name="dataset-a",
                    dataset_preset="unknown",
                    screening_default_markets=["prime", "standard"],
                )
            ],
            ["prime", "standard"],
        )
        == "Prime + Standard"
    )


def test_build_scope_label_deduplicates_preset_labels() -> None:
    assert (
        _build_scope_label(
            [
                StrategyDatasetMetadata(
                    dataset_name="dataset-a",
                    dataset_preset="primeMarket",
                    screening_default_markets=["prime"],
                ),
                StrategyDatasetMetadata(
                    dataset_name="dataset-b",
                    dataset_preset="primeMarket",
                    screening_default_markets=["prime"],
                ),
            ],
            ["prime"],
        )
        == "Prime Market"
    )


def test_build_scope_label_falls_back_for_empty_dataset_list() -> None:
    assert _build_scope_label([], ["prime", "standard", "growth"]) == "All Markets"


def test_resolve_default_screening_markets_raises_when_union_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = MagicMock()
    monkeypatch.setattr(
        "src.application.services.screening_default_markets._resolve_selected_strategy_context",
        lambda **_kwargs: (SimpleNamespace(runtime_payloads={}), ["production/empty"]),
    )
    monkeypatch.setattr(
        "src.application.services.screening_default_markets._resolve_selected_strategy_datasets",
        lambda **_kwargs: [],
    )

    with pytest.raises(
        ValueError,
        match="Failed to resolve default screening markets from production strategy universes",
    ):
        resolve_default_screening_markets(
            entry_decidability="pre_open_decidable",
            strategies=None,
            config_loader=loader,
        )
