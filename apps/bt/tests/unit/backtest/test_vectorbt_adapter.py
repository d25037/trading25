"""Tests for the VectorBT execution adapter."""

from __future__ import annotations

import pandas as pd
import pytest

from src.domains.backtest.vectorbt_adapter import (
    VECTORBT_ENGINE_ENV,
    VectorbtAdapter,
    resolve_vectorbt_engine,
)


def test_resolve_vectorbt_engine_defaults_to_auto(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(VECTORBT_ENGINE_ENV, raising=False)

    assert resolve_vectorbt_engine() == "auto"


def test_resolve_vectorbt_engine_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(VECTORBT_ENGINE_ENV, " RuSt ")

    assert resolve_vectorbt_engine() == "rust"


def test_resolve_vectorbt_engine_rejects_invalid_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(VECTORBT_ENGINE_ENV, "python")

    with pytest.raises(ValueError, match="Invalid VectorBT engine"):
        resolve_vectorbt_engine()


def test_create_signal_portfolio_passes_selected_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_from_signals(**kwargs):
        captured.update(kwargs)
        return "portfolio"

    monkeypatch.setattr(
        "vectorbt.Portfolio.from_signals",
        _fake_from_signals,
        raising=False,
    )

    index = pd.date_range("2024-01-01", periods=2)
    close = pd.DataFrame({"7203": [100.0, 101.0]}, index=index)
    entries = pd.DataFrame({"7203": [True, False]}, index=index)
    exits = pd.DataFrame({"7203": [False, True]}, index=index)

    portfolio = VectorbtAdapter(engine="rust").create_signal_portfolio(
        close=close,
        entries=entries,
        exits=exits,
        direction="longonly",
        init_cash=1_000_000,
        fees=0.001,
        slippage=0.0,
    )

    assert portfolio == "portfolio"
    assert captured["engine"] == "rust"

