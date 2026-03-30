from __future__ import annotations

import pandas as pd
import pytest

from src.domains.strategy.runtime.compiler import (
    CompiledAvailabilityPoint,
    CompiledExecutionSession,
    CompiledSignalAvailability,
)
from src.domains.strategy.signals.feature_registry import (
    AvailabilitySourceKind,
    FeatureObservationPoint,
    resolve_feature_requirement_spec,
    resolve_signal_availability_policy,
)
from src.domains.strategy.signals.scheduler import SignalDecisionScheduler


def test_resolve_feature_requirement_spec_for_statements_field() -> None:
    spec = resolve_feature_requirement_spec("statements:EPS")

    assert spec.data_domain == "statements"
    assert spec.loader_key == "statements_data"
    assert spec.source_kind == AvailabilitySourceKind.DISCLOSURE_TIMESTAMP
    assert spec.observation_time == FeatureObservationPoint.CURRENT_SESSION_CLOSE
    assert spec.required_columns == ("EPS",)


def test_resolve_feature_requirement_spec_for_universe_ohlcv() -> None:
    spec = resolve_feature_requirement_spec("universe_ohlcv")

    assert spec.data_domain == "market"
    assert spec.loader_key == "universe_multi_data"
    assert spec.source_kind == AvailabilitySourceKind.MARKET_EVENT
    assert spec.observation_time == FeatureObservationPoint.CURRENT_SESSION_CLOSE
    assert spec.required_columns == ("Close", "Volume")


def test_resolve_feature_requirement_spec_unknown_requirement_raises() -> None:
    with pytest.raises(KeyError, match="Unknown feature requirement"):
        resolve_feature_requirement_spec("unknown_requirement")


def test_resolve_signal_availability_policy_keeps_same_day_market_signal() -> None:
    policy = resolve_signal_availability_policy(["benchmark_open_gap"])

    assert policy.source_kind == AvailabilitySourceKind.MARKET_EVENT
    assert policy.observation_time == FeatureObservationPoint.CURRENT_SESSION_OPEN


def test_resolve_signal_availability_policy_prefers_disclosure_over_open_gap() -> None:
    policy = resolve_signal_availability_policy(
        ["benchmark_open_gap", "statements:EPS"]
    )

    assert policy.source_kind == AvailabilitySourceKind.DISCLOSURE_TIMESTAMP
    assert policy.observation_time == FeatureObservationPoint.CURRENT_SESSION_CLOSE


def test_signal_decision_scheduler_shifts_prior_close_current_session_signal() -> None:
    scheduler = SignalDecisionScheduler()
    signal = pd.Series([True, False, True], index=pd.date_range("2024-01-01", periods=3))

    availability = CompiledSignalAvailability(
        observation_time=CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE,
        available_at=CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE,
        decision_cutoff=CompiledAvailabilityPoint.CURRENT_SESSION_OPEN,
        execution_session=CompiledExecutionSession.CURRENT_SESSION,
    )

    result = scheduler.project(signal, availability=availability)

    expected = pd.Series([False, True, False], index=signal.index)
    pd.testing.assert_series_equal(result, expected)


def test_signal_decision_scheduler_keeps_next_session_signal_unchanged() -> None:
    scheduler = SignalDecisionScheduler()
    signal = pd.Series([True, False, True], index=pd.date_range("2024-01-01", periods=3))

    availability = CompiledSignalAvailability(
        observation_time=CompiledAvailabilityPoint.CURRENT_SESSION_CLOSE,
        available_at=CompiledAvailabilityPoint.CURRENT_SESSION_CLOSE,
        decision_cutoff=CompiledAvailabilityPoint.NEXT_SESSION_OPEN,
        execution_session=CompiledExecutionSession.NEXT_SESSION,
    )

    result = scheduler.project(signal, availability=availability)

    pd.testing.assert_series_equal(result, signal)
