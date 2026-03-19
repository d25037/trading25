from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.application.services.screening_default_markets import (
    resolve_default_screening_markets,
)
from src.application.services.strategy_dataset_metadata import StrategyDatasetMetadata


def _make_loaded_config(dataset_name: str, entry_decidability: str) -> SimpleNamespace:
    return SimpleNamespace(
        entry_params={},
        exit_params={},
        shared_config=SimpleNamespace(dataset=dataset_name),
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
            "production/range_break_v15": _make_loaded_config("dataset-prime", "pre_open_decidable"),
            "production/forward_eps_driven": _make_loaded_config("dataset-standard", "pre_open_decidable"),
            "production/topix_gap_down_intraday": _make_loaded_config(
                "dataset-growth",
                "requires_same_session_observation",
            ),
        }[name],
    )
    monkeypatch.setattr(
        "src.application.services.screening_default_markets.resolve_dataset_metadata",
        lambda dataset_name, dataset_base_path=None: {
            "dataset-prime": StrategyDatasetMetadata(
                dataset_name="dataset-prime",
                dataset_preset="preset-prime",
                screening_default_markets=["prime"],
            ),
            "dataset-standard": StrategyDatasetMetadata(
                dataset_name="dataset-standard",
                dataset_preset="preset-standard",
                screening_default_markets=["standard"],
            ),
            "dataset-growth": StrategyDatasetMetadata(
                dataset_name="dataset-growth",
                dataset_preset="preset-growth",
                screening_default_markets=["growth"],
            ),
        }[dataset_name],
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
            "production/range_break_v15": _make_loaded_config("dataset-prime", "pre_open_decidable"),
            "production/forward_eps_driven": _make_loaded_config("dataset-standard", "pre_open_decidable"),
            "production/topix_gap_down_intraday": _make_loaded_config(
                "dataset-growth",
                "requires_same_session_observation",
            ),
        }[name],
    )
    monkeypatch.setattr(
        "src.application.services.screening_default_markets.resolve_dataset_metadata",
        lambda dataset_name, dataset_base_path=None: {
            "dataset-prime": StrategyDatasetMetadata(
                dataset_name="dataset-prime",
                dataset_preset="preset-prime",
                screening_default_markets=["prime"],
            ),
            "dataset-standard": StrategyDatasetMetadata(
                dataset_name="dataset-standard",
                dataset_preset="preset-standard",
                screening_default_markets=["standard"],
            ),
            "dataset-growth": StrategyDatasetMetadata(
                dataset_name="dataset-growth",
                dataset_preset="preset-growth",
                screening_default_markets=["growth"],
            ),
        }[dataset_name],
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
        lambda _loader, _name: _make_loaded_config("dataset-broken", "pre_open_decidable"),
    )
    monkeypatch.setattr(
        "src.application.services.screening_default_markets.resolve_dataset_metadata",
        lambda dataset_name, dataset_base_path=None: StrategyDatasetMetadata(
            dataset_name=dataset_name,
            dataset_preset="preset-broken",
            screening_default_markets=None,
        ),
    )

    with pytest.raises(ValueError, match="production/broken"):
        resolve_default_screening_markets(
            entry_decidability="pre_open_decidable",
            strategies=None,
            config_loader=loader,
        )
