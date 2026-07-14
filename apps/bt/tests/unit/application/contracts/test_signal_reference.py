from typing import get_args

import pytest
from pydantic import TypeAdapter, ValidationError

from src.application.contracts import signal_reference as signal_reference_contracts
from src.domains.strategy.runtime.compiler import (
    CompiledAvailabilityPoint,
    CompiledExecutionSession,
    CompiledSignalAvailability,
    CompiledSignalScope,
)


def _availability() -> CompiledSignalAvailability:
    return CompiledSignalAvailability(
        observation_time=CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE,
        available_at=CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE,
        decision_cutoff=CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE,
        execution_session=CompiledExecutionSession.CURRENT_SESSION,
    )


def test_signal_reference_literals_are_stable() -> None:
    assert get_args(signal_reference_contracts.SignalFieldTypeValue) == (
        "boolean",
        "number",
        "string",
        "select",
    )
    assert get_args(signal_reference_contracts.SignalExecutionSemantics) == (
        "standard",
        "next_session_round_trip",
        "current_session_round_trip",
        "overnight_round_trip",
    )

    for alias, values in (
        (
            signal_reference_contracts.SignalFieldTypeValue,
            ("boolean", "number", "string", "select"),
        ),
        (
            signal_reference_contracts.SignalExecutionSemantics,
            (
                "standard",
                "next_session_round_trip",
                "current_session_round_trip",
                "overnight_round_trip",
            ),
        ),
    ):
        adapter = TypeAdapter(alias)
        for value in values:
            assert adapter.validate_python(value) == value
        with pytest.raises(ValidationError):
            adapter.validate_python("legacy")


def test_signal_reference_response_complete_serialization_is_stable() -> None:
    response = signal_reference_contracts.SignalReferenceResponse(
        signals=[
            signal_reference_contracts.SignalReferenceSchema(
                key="price_above_sma",
                signal_type="price_above_sma",
                name="Price Above SMA",
                category="trend",
                description="Price is above its moving average.",
                summary="Trend confirmation",
                when_to_use=["Confirm an uptrend"],
                pitfalls=["Whipsaws in ranges"],
                examples=["close > sma_20"],
                usage_hint="Use for entry and exit confirmation.",
                fields=[
                    signal_reference_contracts.SignalFieldSchema(
                        name="period",
                        label="Period",
                        type="number",
                        description="Moving-average period",
                        default=20,
                        options=["10", "20"],
                        constraints=signal_reference_contracts.FieldConstraints(
                            gt=0,
                            ge=1,
                            lt=500,
                            le=400,
                        ),
                        unit="sessions",
                        placeholder="20",
                    )
                ],
                yaml_snippet="price_above_sma:\n  period: 20",
                exit_disabled=True,
                data_requirements=["stock_data"],
                availability_profiles=[
                    signal_reference_contracts.SignalAvailabilityProfile(
                        scope=CompiledSignalScope.ENTRY,
                        execution_semantics="current_session_round_trip",
                        availability=_availability(),
                    )
                ],
                chart=signal_reference_contracts.SignalChartCapability(
                    supported=False,
                    supported_modes=["absolute"],
                    supports_relative_mode=False,
                    requires_benchmark=True,
                    requires_sector_data=True,
                    requires_margin_data=True,
                    requires_statements_data=True,
                ),
            )
        ],
        categories=[
            signal_reference_contracts.SignalCategorySchema(
                key="trend",
                label="Trend",
            )
        ],
        total=1,
    )

    assert response.model_dump(mode="json") == {
        "signals": [
            {
                "key": "price_above_sma",
                "signal_type": "price_above_sma",
                "name": "Price Above SMA",
                "category": "trend",
                "description": "Price is above its moving average.",
                "summary": "Trend confirmation",
                "when_to_use": ["Confirm an uptrend"],
                "pitfalls": ["Whipsaws in ranges"],
                "examples": ["close > sma_20"],
                "usage_hint": "Use for entry and exit confirmation.",
                "fields": [
                    {
                        "name": "period",
                        "label": "Period",
                        "type": "number",
                        "description": "Moving-average period",
                        "default": 20,
                        "options": ["10", "20"],
                        "constraints": {"gt": 0.0, "ge": 1.0, "lt": 500.0, "le": 400.0},
                        "unit": "sessions",
                        "placeholder": "20",
                    }
                ],
                "yaml_snippet": "price_above_sma:\n  period: 20",
                "exit_disabled": True,
                "data_requirements": ["stock_data"],
                "availability_profiles": [
                    {
                        "scope": "entry",
                        "execution_semantics": "current_session_round_trip",
                        "availability": {
                            "observation_time": "prior_session_close",
                            "available_at": "prior_session_close",
                            "decision_cutoff": "prior_session_close",
                            "execution_session": "current_session",
                        },
                    }
                ],
                "chart": {
                    "supported": False,
                    "supported_modes": ["absolute"],
                    "supports_relative_mode": False,
                    "requires_benchmark": True,
                    "requires_sector_data": True,
                    "requires_margin_data": True,
                    "requires_statements_data": True,
                },
            }
        ],
        "categories": [{"key": "trend", "label": "Trend"}],
        "total": 1,
    }


def test_signal_reference_mutable_defaults_are_fresh() -> None:
    kwargs = {
        "key": "price_above_sma",
        "signal_type": "price_above_sma",
        "name": "Price Above SMA",
        "category": "trend",
        "description": "Price is above its moving average.",
        "usage_hint": "Use for trend confirmation.",
        "fields": [],
        "yaml_snippet": "price_above_sma: {}",
    }
    first = signal_reference_contracts.SignalReferenceSchema(**kwargs)
    second = signal_reference_contracts.SignalReferenceSchema(**kwargs)

    for field_name in (
        "when_to_use",
        "pitfalls",
        "examples",
        "data_requirements",
        "availability_profiles",
    ):
        assert getattr(first, field_name) is not getattr(second, field_name)
    assert first.chart is not second.chart
    assert first.chart.supported_modes is not second.chart.supported_modes


EXPECTED_SHAPES = {
    "FieldConstraints": ({"gt", "ge", "lt", "le"}, []),
    "SignalFieldSchema": (
        {
            "name",
            "label",
            "type",
            "description",
            "default",
            "options",
            "constraints",
            "unit",
            "placeholder",
        },
        ["name", "type", "description"],
    ),
    "SignalChartCapability": (
        {
            "supported",
            "supported_modes",
            "supports_relative_mode",
            "requires_benchmark",
            "requires_sector_data",
            "requires_margin_data",
            "requires_statements_data",
        },
        [],
    ),
    "SignalReferenceSchema": (
        {
            "key",
            "signal_type",
            "name",
            "category",
            "description",
            "summary",
            "when_to_use",
            "pitfalls",
            "examples",
            "usage_hint",
            "fields",
            "yaml_snippet",
            "exit_disabled",
            "data_requirements",
            "availability_profiles",
            "chart",
        },
        [
            "key",
            "signal_type",
            "name",
            "category",
            "description",
            "usage_hint",
            "fields",
            "yaml_snippet",
        ],
    ),
    "SignalAvailabilityProfile": (
        {"scope", "execution_semantics", "availability"},
        ["scope", "execution_semantics", "availability"],
    ),
    "SignalCategorySchema": ({"key", "label"}, ["key", "label"]),
    "SignalReferenceResponse": (
        {"signals", "categories", "total"},
        ["signals", "categories", "total"],
    ),
}


@pytest.mark.parametrize(("name", "expected"), EXPECTED_SHAPES.items())
def test_signal_reference_json_schema_shapes_are_stable(
    name: str,
    expected: tuple[set[str], list[str]],
) -> None:
    schema = getattr(signal_reference_contracts, name).model_json_schema()

    assert set(schema["properties"]) == expected[0]
    assert schema.get("required", []) == expected[1]


def test_signal_reference_forward_references_and_compiler_types_are_resolved() -> None:
    schema = signal_reference_contracts.SignalReferenceSchema.model_json_schema()

    assert schema["title"] == "SignalReferenceSchema"
    assert signal_reference_contracts.SignalReferenceSchema.__doc__ == "シグナル定義"
    assert (
        signal_reference_contracts.SignalAvailabilityProfile.model_fields[
            "scope"
        ].annotation
        is CompiledSignalScope
    )
    assert (
        signal_reference_contracts.SignalAvailabilityProfile.model_fields[
            "availability"
        ].annotation
        is CompiledSignalAvailability
    )
