"""Tests for the VectorBT execution adapter."""

from __future__ import annotations

import pandas as pd
import pytest

from src.domains.backtest.vectorbt_adapter import (
    DEFAULT_VECTORBT_ENGINE,
    PERCENT_SIZE_TYPE,
    ROUND_TRIP_DIRECTION_MAP,
    VECTORBT_ENGINE_ENV,
    VectorbtAdapter,
    resolve_vectorbt_engine,
)


def test_resolve_vectorbt_engine_defaults_to_numba(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(VECTORBT_ENGINE_ENV, raising=False)

    assert resolve_vectorbt_engine() == DEFAULT_VECTORBT_ENGINE


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


def test_create_signal_portfolio_default_engine_runs_vectorbt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(VECTORBT_ENGINE_ENV, raising=False)

    index = pd.date_range("2024-01-01", periods=2)
    close = pd.DataFrame({"7203": [100.0, 101.0]}, index=index)
    entries = pd.DataFrame({"7203": [True, False]}, index=index)
    exits = pd.DataFrame({"7203": [False, True]}, index=index)

    portfolio = VectorbtAdapter().create_signal_portfolio(
        close=close,
        entries=entries,
        exits=exits,
        direction="longonly",
        init_cash=1_000_000,
        fees=0.001,
        slippage=0.0,
    )

    assert portfolio.total_return() is not None


def test_create_signal_portfolio_rust_matches_numba_when_optional_engine_installed() -> None:
    pytest.importorskip("vectorbt_rust")

    index = pd.date_range("2024-01-01", periods=3)
    close = pd.DataFrame({"7203": [100.0, 102.0, 101.0]}, index=index)
    entries = pd.DataFrame({"7203": [True, False, False]}, index=index)
    exits = pd.DataFrame({"7203": [False, False, True]}, index=index)

    numba_portfolio = VectorbtAdapter(engine="numba").create_signal_portfolio(
        close=close,
        entries=entries,
        exits=exits,
        direction="longonly",
        init_cash=1_000_000,
        fees=0.001,
        slippage=0.0,
    )
    rust_portfolio = VectorbtAdapter(engine="rust").create_signal_portfolio(
        close=close,
        entries=entries,
        exits=exits,
        direction="longonly",
        init_cash=1_000_000,
        fees=0.001,
        slippage=0.0,
    )

    pd.testing.assert_series_equal(
        rust_portfolio.total_return(),
        numba_portfolio.total_return(),
        check_names=False,
    )


@pytest.mark.parametrize("engine", ["auto", "rust"])
def test_round_trip_portfolio_rejects_non_numba_engine(engine: str) -> None:
    index = pd.date_range("2024-01-01", periods=2)
    open_data = pd.DataFrame({"7203": [100.0, 101.0]}, index=index)
    close_data = pd.DataFrame({"7203": [101.0, 102.0]}, index=index)
    entries = pd.DataFrame({"7203": [True, False]}, index=index)

    with pytest.raises(ValueError, match="only supported for signal portfolios"):
        VectorbtAdapter(engine=engine).create_round_trip_portfolio(
            open_data=open_data,
            close_data=close_data,
            entries_data=entries,
            execution_mode="next_session_round_trip",
            entry_size=1.0,
            entry_size_type=PERCENT_SIZE_TYPE,
            direction=ROUND_TRIP_DIRECTION_MAP["longonly"],
            fees=0.001,
            slippage=0.0,
            init_cash=1_000_000,
            max_size=float("inf"),
            cash_sharing=False,
            group_by=None,
        )
