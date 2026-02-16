from __future__ import annotations

import sys
import types

import pytest

import src.server.services as services


def test_getattr_resolves_backtest_service_without_eager_import(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_module = types.ModuleType("src.server.services.backtest_service")

    class FakeBacktestService:
        pass

    fake_module.BacktestService = FakeBacktestService
    monkeypatch.setitem(sys.modules, "src.server.services.backtest_service", fake_module)

    resolved = services.__getattr__("BacktestService")
    assert resolved is FakeBacktestService


def test_getattr_resolves_backtest_attribution_service_without_eager_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_module = types.ModuleType("src.server.services.backtest_attribution_service")

    class FakeBacktestAttributionService:
        pass

    fake_module.BacktestAttributionService = FakeBacktestAttributionService
    monkeypatch.setitem(
        sys.modules,
        "src.server.services.backtest_attribution_service",
        fake_module,
    )

    resolved = services.__getattr__("BacktestAttributionService")
    assert resolved is FakeBacktestAttributionService


def test_getattr_raises_for_unknown_name() -> None:
    with pytest.raises(AttributeError, match="has no attribute"):
        services.__getattr__("UnknownService")
