from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from src.application.services.screening_strategy_selection import (
    build_strategy_response_names,
    build_strategy_selection_catalog,
    resolve_selected_strategy_names,
    resolve_strategy_token,
)
from src.domains.strategy.runtime.compiler import CompiledStrategyIR
from src.domains.strategy.runtime.screening_profile import EntryDecidability
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams
from src.shared.paths.resolver import StrategyMetadata


def _metadata(name: str, *, stem: str | None = None, category: str = "production") -> StrategyMetadata:
    resolved_stem = stem or name.split("/")[-1]
    return StrategyMetadata(
        name=name,
        category=category,
        path=Path(f"/tmp/{category}/{resolved_stem}.yaml"),
        mtime=datetime(2026, 3, 17),
    )


def _loaded_config(
    *,
    screening_support: str = "supported",
    entry_decidability: EntryDecidability | None = "pre_open_decidable",
):
    return SimpleNamespace(
        entry_params=SignalParams(),
        exit_params=SignalParams(),
        shared_config=SharedConfig.model_validate(
            {"dataset": "primeExTopix500"},
            context={"resolve_stock_codes": False},
        ),
        compiled_strategy=cast(CompiledStrategyIR, object()),
        screening_support=screening_support,
        entry_decidability=entry_decidability,
    )


class TestBuildStrategySelectionCatalog:
    def test_rejects_when_no_production_metadata(self) -> None:
        metadata = [_metadata("experimental/foo", category="experimental")]

        with pytest.raises(ValueError, match="No production strategies found"):
            build_strategy_selection_catalog(
                metadata,
                load_strategy_config=lambda _: _loaded_config(),
                entry_decidability="pre_open_decidable",
            )

    def test_wraps_invalid_strategy_config_error(self) -> None:
        metadata = [_metadata("production/foo")]

        def _load(_: str):
            raise RuntimeError("bad config")

        with pytest.raises(
            ValueError,
            match=r"Invalid production strategy config for screening: production/foo \(bad config\)",
        ):
            build_strategy_selection_catalog(
                metadata,
                load_strategy_config=_load,
                entry_decidability="pre_open_decidable",
            )

    def test_collects_supported_and_eligible_names(self) -> None:
        metadata = [
            _metadata("production/pre-open"),
            _metadata("production/in-session"),
            _metadata("production/unsupported"),
        ]

        def _load(name: str):
            if name == "production/pre-open":
                return _loaded_config(entry_decidability="pre_open_decidable")
            if name == "production/in-session":
                return _loaded_config(
                    entry_decidability="requires_same_session_observation"
                )
            return _loaded_config(
                screening_support="unsupported",
                entry_decidability=None,
            )

        catalog = build_strategy_selection_catalog(
            metadata,
            load_strategy_config=_load,
            entry_decidability="pre_open_decidable",
        )

        assert sorted(catalog.metadata_by_name) == [
            "production/in-session",
            "production/pre-open",
            "production/unsupported",
        ]
        assert catalog.basename_map == {
            "pre-open": ["production/pre-open"],
            "in-session": ["production/in-session"],
            "unsupported": ["production/unsupported"],
        }
        assert catalog.supported_names == {
            "production/pre-open",
            "production/in-session",
        }
        assert catalog.eligible_names == {"production/pre-open"}

    def test_rejects_when_no_strategy_matches_entry_decidability(self) -> None:
        metadata = [_metadata("production/in-session")]

        with pytest.raises(
            ValueError,
            match="No production strategies found for pre_open_decidable screening",
        ):
            build_strategy_selection_catalog(
                metadata,
                load_strategy_config=lambda _: _loaded_config(
                    entry_decidability="requires_same_session_observation"
                ),
                entry_decidability="pre_open_decidable",
            )


class TestResolveStrategyToken:
    def test_resolves_full_name_prefixed_name_and_unique_basename(self) -> None:
        metadata_by_name = {
            "production/foo": _metadata("production/foo"),
            "production/bar": _metadata("production/bar"),
        }
        basename_map = {
            "foo": ["production/foo"],
            "bar": ["production/bar"],
        }

        assert (
            resolve_strategy_token("production/foo", metadata_by_name, basename_map)
            == "production/foo"
        )
        assert resolve_strategy_token("foo", metadata_by_name, basename_map) == "production/foo"
        assert resolve_strategy_token("bar", metadata_by_name, basename_map) == "production/bar"
        assert resolve_strategy_token("missing", metadata_by_name, basename_map) is None

    def test_returns_none_for_ambiguous_basename(self) -> None:
        metadata_by_name = {
            "production/group/foo": _metadata("production/group/foo", stem="foo"),
            "production/other/foo": _metadata("production/other/foo", stem="foo"),
        }
        basename_map = {"foo": ["production/group/foo", "production/other/foo"]}

        assert resolve_strategy_token("foo", metadata_by_name, basename_map) is None


class TestResolveSelectedStrategyNames:
    @pytest.fixture
    def catalog(self):
        metadata = [
            _metadata("production/pre-open"),
            _metadata("production/in-session"),
            _metadata("production/unsupported"),
        ]

        def _load(name: str):
            if name == "production/pre-open":
                return _loaded_config(entry_decidability="pre_open_decidable")
            if name == "production/in-session":
                return _loaded_config(
                    entry_decidability="requires_same_session_observation"
                )
            return _loaded_config(
                screening_support="unsupported",
                entry_decidability=None,
            )

        return build_strategy_selection_catalog(
            metadata,
            load_strategy_config=_load,
            entry_decidability="pre_open_decidable",
        )

    def test_returns_sorted_eligible_names_for_blank_selection(self, catalog) -> None:
        resolved = resolve_selected_strategy_names(
            strategies=" ",
            catalog=catalog,
            entry_decidability="pre_open_decidable",
        )

        assert resolved == ["production/pre-open"]

    def test_dedupes_selected_names(self, catalog) -> None:
        resolved = resolve_selected_strategy_names(
            strategies="pre-open,production/pre-open,pre-open",
            catalog=catalog,
            entry_decidability="pre_open_decidable",
        )

        assert resolved == ["production/pre-open"]

    def test_rejects_invalid_requested_strategy(self, catalog) -> None:
        with pytest.raises(
            ValueError,
            match="Invalid strategies \\(production only\\): missing",
        ):
            resolve_selected_strategy_names(
                strategies="missing",
                catalog=catalog,
                entry_decidability="pre_open_decidable",
            )

    def test_rejects_unsupported_requested_strategy(self, catalog) -> None:
        with pytest.raises(
            ValueError,
            match="Unsupported screening strategies: unsupported",
        ):
            resolve_selected_strategy_names(
                strategies="unsupported",
                catalog=catalog,
                entry_decidability="pre_open_decidable",
            )

    def test_rejects_wrong_decidability_requested_strategy(self, catalog) -> None:
        with pytest.raises(
            ValueError,
            match=(
                "Strategies do not support pre_open_decidable screening: in-session"
            ),
        ):
            resolve_selected_strategy_names(
                strategies="in-session",
                catalog=catalog,
                entry_decidability="pre_open_decidable",
            )


class TestBuildStrategyResponseNames:
    def test_uses_full_name_when_selected_basenames_collide(self) -> None:
        metadata_by_name = {
            "production/group/foo": _metadata("production/group/foo", stem="foo"),
            "production/other/foo": _metadata("production/other/foo", stem="foo"),
            "production/bar": _metadata("production/bar"),
        }

        response_names = build_strategy_response_names(
            metadata_by_name,
            ["production/group/foo", "production/other/foo", "production/bar"],
        )

        assert response_names == {
            "production/group/foo": "production/group/foo",
            "production/other/foo": "production/other/foo",
            "production/bar": "bar",
        }
