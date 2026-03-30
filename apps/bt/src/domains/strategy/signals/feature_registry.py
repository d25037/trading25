"""Typed feature requirements and base availability metadata for signals."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal


class AvailabilitySourceKind(str, Enum):
    """Source category for availability semantics."""

    MARKET_EVENT = "market_event"
    DISCLOSURE_TIMESTAMP = "disclosure_timestamp"


class FeatureObservationPoint(str, Enum):
    """Base observation point before execution-policy projection."""

    CURRENT_SESSION_OPEN = "current_session_open"
    CURRENT_SESSION_CLOSE = "current_session_close"


@dataclass(frozen=True)
class FeatureRequirementSpec:
    """Typed feature requirement metadata."""

    key: str
    data_domain: Literal["market", "benchmark", "margin", "statements", "sector"]
    loader_key: str
    source_kind: AvailabilitySourceKind
    observation_time: FeatureObservationPoint
    required_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class SignalAvailabilityPolicy:
    """Base availability policy for a signal before runtime projection."""

    source_kind: AvailabilitySourceKind
    observation_time: FeatureObservationPoint


_FEATURE_REQUIREMENT_SPECS: dict[str, FeatureRequirementSpec] = {
    "ohlc": FeatureRequirementSpec(
        key="ohlc",
        data_domain="market",
        loader_key="ohlc_data",
        source_kind=AvailabilitySourceKind.MARKET_EVENT,
        observation_time=FeatureObservationPoint.CURRENT_SESSION_CLOSE,
        required_columns=("Open", "High", "Low", "Close"),
    ),
    "volume": FeatureRequirementSpec(
        key="volume",
        data_domain="market",
        loader_key="volume",
        source_kind=AvailabilitySourceKind.MARKET_EVENT,
        observation_time=FeatureObservationPoint.CURRENT_SESSION_CLOSE,
    ),
    "universe_ohlcv": FeatureRequirementSpec(
        key="universe_ohlcv",
        data_domain="market",
        loader_key="universe_multi_data",
        source_kind=AvailabilitySourceKind.MARKET_EVENT,
        observation_time=FeatureObservationPoint.CURRENT_SESSION_CLOSE,
        required_columns=("Close", "Volume"),
    ),
    "benchmark_close": FeatureRequirementSpec(
        key="benchmark_close",
        data_domain="benchmark",
        loader_key="benchmark_data",
        source_kind=AvailabilitySourceKind.MARKET_EVENT,
        observation_time=FeatureObservationPoint.CURRENT_SESSION_CLOSE,
        required_columns=("Close",),
    ),
    "benchmark_open_gap": FeatureRequirementSpec(
        key="benchmark_open_gap",
        data_domain="benchmark",
        loader_key="benchmark_data",
        source_kind=AvailabilitySourceKind.MARKET_EVENT,
        observation_time=FeatureObservationPoint.CURRENT_SESSION_OPEN,
        required_columns=("Open", "Close"),
    ),
    "margin": FeatureRequirementSpec(
        key="margin",
        data_domain="margin",
        loader_key="margin_data",
        source_kind=AvailabilitySourceKind.MARKET_EVENT,
        observation_time=FeatureObservationPoint.CURRENT_SESSION_CLOSE,
    ),
    "sector": FeatureRequirementSpec(
        key="sector",
        data_domain="sector",
        loader_key="sector_data",
        source_kind=AvailabilitySourceKind.MARKET_EVENT,
        observation_time=FeatureObservationPoint.CURRENT_SESSION_CLOSE,
    ),
}


def resolve_feature_requirement_spec(requirement: str) -> FeatureRequirementSpec:
    """Resolve a typed feature requirement spec."""

    if requirement.startswith("statements:"):
        field_name = requirement.split(":", 1)[1]
        return FeatureRequirementSpec(
            key=requirement,
            data_domain="statements",
            loader_key="statements_data",
            source_kind=AvailabilitySourceKind.DISCLOSURE_TIMESTAMP,
            observation_time=FeatureObservationPoint.CURRENT_SESSION_CLOSE,
            required_columns=(field_name,),
        )

    spec = _FEATURE_REQUIREMENT_SPECS.get(requirement)
    if spec is None:
        raise KeyError(f"Unknown feature requirement: {requirement}")
    return spec


def resolve_signal_availability_policy(
    feature_requirements: list[str],
) -> SignalAvailabilityPolicy:
    """Resolve base signal availability from feature requirements."""

    if not feature_requirements:
        return SignalAvailabilityPolicy(
            source_kind=AvailabilitySourceKind.MARKET_EVENT,
            observation_time=FeatureObservationPoint.CURRENT_SESSION_CLOSE,
        )

    specs = [resolve_feature_requirement_spec(requirement) for requirement in feature_requirements]
    has_disclosure_requirement = any(
        spec.source_kind == AvailabilitySourceKind.DISCLOSURE_TIMESTAMP
        for spec in specs
    )
    has_current_open_requirement = any(
        spec.observation_time == FeatureObservationPoint.CURRENT_SESSION_OPEN
        for spec in specs
    )

    if has_disclosure_requirement:
        return SignalAvailabilityPolicy(
            source_kind=AvailabilitySourceKind.DISCLOSURE_TIMESTAMP,
            observation_time=FeatureObservationPoint.CURRENT_SESSION_CLOSE,
        )

    if has_current_open_requirement:
        return SignalAvailabilityPolicy(
            source_kind=AvailabilitySourceKind.MARKET_EVENT,
            observation_time=FeatureObservationPoint.CURRENT_SESSION_OPEN,
        )

    return SignalAvailabilityPolicy(
        source_kind=AvailabilitySourceKind.MARKET_EVENT,
        observation_time=FeatureObservationPoint.CURRENT_SESSION_CLOSE,
    )
