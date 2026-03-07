"""StrategyFactory の単体テスト."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.domains.strategy.core.factory import StrategyFactory
from src.shared.models.allocation import AllocationInfo
from src.shared.models.config import SharedConfig


class _FakeSignalParams:
    def __init__(self, fields_set: set[str] | None = None) -> None:
        self.model_fields_set = fields_set or set()


class _FakeStrategy:
    def __init__(self, *, all_entries=None) -> None:
        self.all_entries = all_entries

    def run_optimized_backtest(self, group_by: bool = True):
        assert group_by is True
        return "initial-pf", "kelly-pf", AllocationInfo(
            method="kelly",
            allocation=0.2,
            win_rate=0.5,
            avg_win=1.0,
            avg_loss=0.5,
            total_trades=10,
            full_kelly=0.4,
            kelly_fraction=1.0,
        )


class TestStrategyFactoryHelpers:
    def test_has_configured_exit_trigger_params_handles_supported_shapes(self) -> None:
        assert StrategyFactory._has_configured_exit_trigger_params({}, None) is False
        assert (
            StrategyFactory._has_configured_exit_trigger_params(
                {"volume": {"enabled": True}},
                None,
            )
            is True
        )
        assert (
            StrategyFactory._has_configured_exit_trigger_params(
                None,
                _FakeSignalParams({"volume"}),
            )
            is True
        )
        assert (
            StrategyFactory._has_configured_exit_trigger_params(
                None,
                _FakeSignalParams(set()),
            )
            is False
        )
        assert (
            StrategyFactory._has_configured_exit_trigger_params(
                SimpleNamespace(),
                None,
            )
            is True
        )
        assert (
            StrategyFactory._has_configured_exit_trigger_params(
                None,
                SimpleNamespace(),
            )
            is True
        )

    def test_validate_next_session_round_trip_noops_when_disabled(self) -> None:
        shared_config = SharedConfig.model_validate(
            {
                "dataset": "sample",
                "stock_codes": ["1111"],
                "next_session_round_trip": False,
            },
            context={"resolve_stock_codes": False},
        )

        StrategyFactory._validate_next_session_round_trip(
            shared_config,
            {"volume": {"enabled": True}},
            None,
        )

    def test_create_strategy_rejects_exit_trigger_for_next_session_round_trip(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "src.domains.strategy.core.yaml_configurable_strategy.YamlConfigurableStrategy",
            lambda **kwargs: SimpleNamespace(**kwargs),
        )

        shared_config = SharedConfig.model_validate(
            {
                "dataset": "sample",
                "stock_codes": ["1111"],
                "next_session_round_trip": True,
            },
            context={"resolve_stock_codes": False},
        )

        with pytest.raises(ValueError, match="exit_trigger_params must be empty"):
            StrategyFactory.create_strategy(
                shared_config=shared_config,
                entry_filter_params={"volume": {"enabled": True}},
                exit_trigger_params={"volume": {"enabled": True}},
            )

    def test_create_strategy_accepts_empty_exit_trigger_for_next_session_round_trip(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        created: dict[str, object] = {}

        def _fake_yaml_strategy(**kwargs):
            created.update(kwargs)
            return SimpleNamespace(**kwargs)

        monkeypatch.setattr(
            "src.domains.strategy.core.yaml_configurable_strategy.YamlConfigurableStrategy",
            _fake_yaml_strategy,
        )

        strategy = StrategyFactory.create_strategy(
            shared_config={
                "dataset": "sample",
                "stock_codes": ["1111"],
                "next_session_round_trip": True,
            },
            entry_filter_params={"volume": {"enabled": True}},
            exit_trigger_params={},
        )

        assert strategy is not None
        assert created["shared_config"].next_session_round_trip is True
        assert created["exit_trigger_params"] is None


class TestStrategyFactoryFilesystemHelpers:
    def test_get_available_strategies_and_is_supported_strategy(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        strategies_dir = tmp_path / "config" / "strategies"
        strategies_dir.mkdir(parents=True)
        (strategies_dir / "template.yaml").write_text("strategy_params: {}\n", encoding="utf-8")
        (strategies_dir / "alpha.yaml").write_text(
            "strategy_params:\n  description: Alpha strategy\n",
            encoding="utf-8",
        )
        (strategies_dir / "broken.yaml").write_text(":: invalid yaml", encoding="utf-8")

        monkeypatch.chdir(tmp_path)

        available = StrategyFactory.get_available_strategies()

        assert available["alpha"] == "Alpha strategy"
        assert available["broken"] == "broken"
        assert "template" not in available
        assert StrategyFactory.is_supported_strategy("alpha") is True
        assert StrategyFactory.is_supported_strategy("missing") is False


class TestStrategyFactoryExecution:
    def test_execute_strategy_with_config_returns_expected_payload(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_strategy = _FakeStrategy(all_entries="entries-df")
        monkeypatch.setattr(StrategyFactory, "create_strategy", lambda **kwargs: fake_strategy)
        printed: list[str] = []

        class _FakeConsole:
            def print(self, message: str) -> None:
                printed.append(message)

        monkeypatch.setattr("src.domains.strategy.core.factory.Console", _FakeConsole)
        monkeypatch.setattr("src.domains.strategy.core.factory.setup_logger", lambda **kwargs: None)

        result = StrategyFactory.execute_strategy_with_config(
            shared_config={
                "dataset": "sample",
                "stock_codes": ["1111", "2222"],
                "printlog": False,
            },
            entry_filter_params={"volume": {"enabled": True}},
            exit_trigger_params={},
        )

        assert result["initial_portfolio"] == "initial-pf"
        assert result["kelly_portfolio"] == "kelly-pf"
        assert result["all_entries"] == "entries-df"
        assert os.environ["LOG_LEVEL"] == "ERROR"
        assert any("統合ポートフォリオ実行" in message for message in printed)

    def test_execute_strategy_with_config_reraises_failures(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _raise_create_strategy(**kwargs):
            raise RuntimeError("boom")

        monkeypatch.setattr(StrategyFactory, "create_strategy", _raise_create_strategy)

        class _FakeConsole:
            def print(self, message: str) -> None:
                return None

        monkeypatch.setattr("src.domains.strategy.core.factory.Console", _FakeConsole)

        with pytest.raises(RuntimeError, match="boom"):
            StrategyFactory.execute_strategy_with_config(
                shared_config={
                    "dataset": "sample",
                    "stock_codes": ["1111"],
                },
                entry_filter_params={"volume": {"enabled": True}},
            )
