from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from src.domains.analytics.screening_evaluator import (
    StockEvaluationOutcome,
    apply_stock_outcome,
    build_strategy_signal_cache_token,
    evaluate_stock,
    evaluate_strategy,
    evaluate_strategy_input,
)
from src.domains.strategy.runtime.compiler import (
    CompiledStrategyIR,
    compile_runtime_strategy,
)
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams, Signals


@dataclass(frozen=True)
class _Stock:
    code: str


@dataclass(frozen=True)
class _Strategy:
    response_name: str
    entry_params: SignalParams
    exit_params: SignalParams
    entry_decidability: str = "pre_open_decidable"
    compiled_strategy: CompiledStrategyIR = field(
        default_factory=lambda: _compiled_strategy()
    )


@dataclass
class _DataBundle:
    multi_data: dict[str, dict[str, Any]]
    benchmark_data: pd.DataFrame | None = None
    sector_data: dict[str, pd.DataFrame] | None = None
    stock_sector_mapping: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class _StrategyInput:
    strategy: _Strategy
    data_bundle: _DataBundle
    load_warnings: list[str]


@dataclass
class _Accumulator:
    matched_rows: list[tuple[_Stock, str]] = field(default_factory=list)
    processed_codes: set[str] = field(default_factory=set)
    warnings: list[str] = field(default_factory=list)


def _signals(index: pd.DatetimeIndex) -> Signals:
    return Signals(
        entries=pd.Series([True] * len(index), index=index),
        exits=pd.Series([False] * len(index), index=index),
    )


def _compiled_strategy(
    *,
    entry_params: SignalParams | None = None,
    execution_policy: dict[str, str] | None = None,
) -> CompiledStrategyIR:
    shared_config = SharedConfig.model_validate(
        {
            "dataset": "primeExTopix500",
            "timeframe": "daily",
            "execution_policy": execution_policy or {"mode": "standard"},
        },
        context={"resolve_stock_codes": False},
    )
    return compile_runtime_strategy(
        strategy_name="screening-test",
        shared_config=shared_config,
        entry_signal_params=entry_params or SignalParams(),
        exit_signal_params=SignalParams(),
    )


def test_evaluate_stock_reuses_per_stock_signal_cache() -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    daily = pd.DataFrame({"Close": [1.0, 1.1]}, index=index)

    s1 = _Strategy("s1", SignalParams(), SignalParams())
    s2 = _Strategy("s2", SignalParams(), SignalParams())
    stock = _Stock("1001")
    bundle = _DataBundle(
        multi_data={"1001": {"daily": daily, "margin_daily": None, "statements_daily": None}}
    )
    strategy_inputs = [
        _StrategyInput(strategy=s1, data_bundle=bundle, load_warnings=[]),
        _StrategyInput(strategy=s2, data_bundle=bundle, load_warnings=[]),
    ]
    strategy_cache_tokens = {
        s1.response_name: build_strategy_signal_cache_token(s1),
        s2.response_name: build_strategy_signal_cache_token(s2),
    }

    calls = {"count": 0}

    def _generate_signals(**_kwargs: Any) -> Signals:
        calls["count"] += 1
        return _signals(index)

    outcome = evaluate_stock(
        stock=stock,
        strategy_inputs=strategy_inputs,
        recent_days=2,
        strategy_cache_tokens=strategy_cache_tokens,
        generate_signals=_generate_signals,
        find_recent_match_date=lambda _signals, _recent_days: "2026-01-02",
    )

    assert calls["count"] == 1
    assert outcome.processed_strategy_names == {"s1", "s2"}
    assert outcome.matched_dates_by_strategy == {"s1": "2026-01-02", "s2": "2026-01-02"}


def test_evaluate_stock_passes_entry_decidability_without_affecting_signal_generation() -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    daily = pd.DataFrame({"Close": [1.0, 1.1]}, index=index)
    strategy = _Strategy(
        "same_day",
        SignalParams(),
        SignalParams(),
        entry_decidability="requires_same_session_observation",
        compiled_strategy=_compiled_strategy(
            execution_policy={"mode": "current_session_round_trip"}
        ),
    )
    stock = _Stock("1001")
    bundle = _DataBundle(
        multi_data={"1001": {"daily": daily, "margin_daily": None, "statements_daily": None}}
    )

    captured: dict[str, Any] = {}

    def _generate_signals(**kwargs: Any) -> Signals:
        captured.update(kwargs)
        return _signals(index)

    outcome = evaluate_stock(
        stock=stock,
        strategy_inputs=[_StrategyInput(strategy=strategy, data_bundle=bundle, load_warnings=[])],
        recent_days=2,
        strategy_cache_tokens={},
        generate_signals=_generate_signals,
        find_recent_match_date=lambda _signals, _recent_days: "2026-01-02",
    )

    assert outcome.matched_dates_by_strategy == {"same_day": "2026-01-02"}
    assert captured["compiled_strategy"].execution_semantics == (
        "current_session_round_trip"
    )
    assert captured["stock_code"] == "1001"
    assert captured["universe_multi_data"] is bundle.multi_data
    assert captured["universe_member_codes"] == ("1001",)
    assert "current_session_round_trip" not in captured


def test_evaluate_stock_does_not_infer_round_trip_from_decidability_label_alone() -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    daily = pd.DataFrame({"Close": [1.0, 1.1]}, index=index)
    strategy = _Strategy(
        "same-day-ish",
        SignalParams(),
        SignalParams(),
        entry_decidability="requires_same_session_observation",
        compiled_strategy=_compiled_strategy(),
    )
    stock = _Stock("1001")
    bundle = _DataBundle(
        multi_data={"1001": {"daily": daily, "margin_daily": None, "statements_daily": None}}
    )

    captured: dict[str, Any] = {}

    def _generate_signals(**kwargs: Any) -> Signals:
        captured.update(kwargs)
        return _signals(index)

    evaluate_stock(
        stock=stock,
        strategy_inputs=[_StrategyInput(strategy=strategy, data_bundle=bundle, load_warnings=[])],
        recent_days=2,
        strategy_cache_tokens={},
        generate_signals=_generate_signals,
        find_recent_match_date=lambda _signals, _recent_days: "2026-01-02",
    )

    assert captured["compiled_strategy"].execution_semantics == "standard"
    assert captured["universe_multi_data"] is bundle.multi_data
    assert captured["universe_member_codes"] == ("1001",)
    assert "current_session_round_trip" not in captured


def test_evaluate_stock_converts_unexpected_matcher_error_to_warning() -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    daily = pd.DataFrame({"Close": [1.0, 1.1]}, index=index)

    s1 = _Strategy("s1", SignalParams(), SignalParams())
    s2 = _Strategy("s2", SignalParams(), SignalParams())
    stock = _Stock("1001")
    bundle = _DataBundle(multi_data={"1001": {"daily": daily}})
    strategy_inputs = [
        _StrategyInput(strategy=s1, data_bundle=bundle, load_warnings=[]),
        _StrategyInput(strategy=s2, data_bundle=bundle, load_warnings=[]),
    ]

    calls = {"count": 0}

    def _matcher(_signals: Signals, _recent_days: int) -> str | None:
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("unexpected matcher error")
        return "2026-01-02"

    outcome = evaluate_stock(
        stock=stock,
        strategy_inputs=strategy_inputs,
        recent_days=2,
        strategy_cache_tokens={},
        generate_signals=lambda **_kwargs: _signals(index),
        find_recent_match_date=_matcher,
    )

    assert outcome.processed_strategy_names == {"s1", "s2"}
    assert outcome.matched_dates_by_strategy == {"s2": "2026-01-02"}
    assert outcome.warning_by_strategy == [("s1", "1001 evaluation failed (unexpected matcher error)")]


def test_apply_stock_outcome_updates_accumulators() -> None:
    stock = _Stock("1001")
    outcome = StockEvaluationOutcome(
        stock=stock,
        matched_dates_by_strategy={"s1": "2026-01-02"},
        processed_strategy_names={"s1", "s2"},
        warning_by_strategy=[("s2", "1001 signal generation failed (boom)")],
    )
    accumulators = {"s1": _Accumulator(), "s2": _Accumulator()}

    apply_stock_outcome(outcome, accumulators)

    assert accumulators["s1"].processed_codes == {"1001"}
    assert accumulators["s2"].processed_codes == {"1001"}
    assert accumulators["s1"].matched_rows == [(stock, "2026-01-02")]
    assert accumulators["s2"].warnings == ["1001 signal generation failed (boom)"]


def test_apply_stock_outcome_ignores_unknown_strategy_names() -> None:
    outcome = StockEvaluationOutcome(
        stock=_Stock("1001"),
        matched_dates_by_strategy={"missing": "2026-01-02"},
        processed_strategy_names={"missing"},
        warning_by_strategy=[("missing", "ignored warning")],
    )
    accumulators = {"s1": _Accumulator()}

    apply_stock_outcome(outcome, accumulators)

    assert accumulators["s1"].processed_codes == set()
    assert accumulators["s1"].matched_rows == []
    assert accumulators["s1"].warnings == []


def test_evaluate_strategy_handles_signal_failures() -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    daily = pd.DataFrame({"close": [1.0, 1.1]}, index=index)
    strategy = _Strategy("s1", SignalParams(), SignalParams())
    stock_universe = [_Stock("1001"), _Stock("1002"), _Stock("1003")]
    bundle = _DataBundle(
        multi_data={
            "1001": {"daily": daily, "margin_daily": "m1", "statements_daily": "st1"},
            "1002": {},
            "1003": {"daily": daily, "margin_daily": "m3", "statements_daily": "st3"},
        }
    )

    calls = {"count": 0}

    def _generate_signals(**_kwargs: Any) -> Signals:
        calls["count"] += 1
        if calls["count"] == 1:
            return _signals(index)
        raise RuntimeError("signal failed")

    summary = evaluate_strategy(
        strategy=strategy,
        stock_universe=stock_universe,
        recent_days=2,
        data_bundle=bundle,
        generate_signals=_generate_signals,
        find_recent_match_date=lambda _signals, _recent_days: "2026-01-02",
    )

    assert summary.matched_rows == [(_Stock("1001"), "2026-01-02")]
    assert summary.processed_codes == {"1001", "1003"}
    assert summary.warnings == ["1003 signal generation failed (signal failed)"]


def test_evaluate_stock_skips_missing_or_empty_data_and_allows_no_match() -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    daily = pd.DataFrame({"close": [1.0, 1.1]}, index=index)
    empty_daily = pd.DataFrame({"close": []})

    stock = _Stock("1001")
    strategy_inputs = [
        _StrategyInput(
            strategy=_Strategy("s-missing", SignalParams(), SignalParams()),
            data_bundle=_DataBundle(multi_data={}),
            load_warnings=[],
        ),
        _StrategyInput(
            strategy=_Strategy("s-empty", SignalParams(), SignalParams()),
            data_bundle=_DataBundle(multi_data={"1001": {"daily": empty_daily}}),
            load_warnings=[],
        ),
        _StrategyInput(
            strategy=_Strategy("s-valid-none", SignalParams(), SignalParams()),
            data_bundle=_DataBundle(multi_data={"1001": {"daily": daily}}),
            load_warnings=[],
        ),
    ]

    outcome = evaluate_stock(
        stock=stock,
        strategy_inputs=strategy_inputs,
        recent_days=2,
        strategy_cache_tokens={},
        generate_signals=lambda **_kwargs: _signals(index),
        find_recent_match_date=lambda _signals, _recent_days: None,
    )

    assert outcome.processed_strategy_names == {"s-valid-none"}
    assert outcome.matched_dates_by_strategy == {}
    assert outcome.warning_by_strategy == []


def test_evaluate_strategy_skips_invalid_daily_and_none_match() -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    daily = pd.DataFrame({"close": [1.0, 1.1]}, index=index)
    stock_universe = [_Stock("1001"), _Stock("1002")]
    bundle = _DataBundle(
        multi_data={
            "1001": {"daily": ["not", "dataframe"]},
            "1002": {"daily": daily},
        }
    )

    summary = evaluate_strategy(
        strategy=_Strategy("s1", SignalParams(), SignalParams()),
        stock_universe=stock_universe,
        recent_days=2,
        data_bundle=bundle,
        generate_signals=lambda **_kwargs: _signals(index),
        find_recent_match_date=lambda _signals, _recent_days: None,
    )

    assert summary.matched_rows == []
    assert summary.processed_codes == {"1002"}
    assert summary.warnings == []


def test_evaluate_strategy_input_merges_load_warnings() -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    daily = pd.DataFrame({"close": [1.0, 1.1]}, index=index)
    strategy_input = _StrategyInput(
        strategy=_Strategy("s1", SignalParams(), SignalParams()),
        data_bundle=_DataBundle(multi_data={"1001": {"daily": daily}}),
        load_warnings=["multi data load failed (boom)"],
    )
    stock_universe = [_Stock("1001")]

    summary = evaluate_strategy_input(
        strategy_input=strategy_input,
        stock_universe=stock_universe,
        recent_days=2,
        generate_signals=lambda **_kwargs: _signals(index),
        find_recent_match_date=lambda _signals, _recent_days: "2026-01-02",
    )

    assert summary.matched_rows == [(_Stock("1001"), "2026-01-02")]
    assert summary.processed_codes == {"1001"}
    assert summary.warnings == ["multi data load failed (boom)"]
