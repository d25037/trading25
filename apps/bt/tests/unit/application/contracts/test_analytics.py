import pytest
from pydantic import ValidationError

from src.application.contracts.analytics import DataProvenance, ResponseDiagnostics


def test_response_diagnostics_complete_serialization_is_stable() -> None:
    diagnostics = ResponseDiagnostics(
        missing_required_data=["statements"],
        used_fields=["eps", "forecast_eps"],
        effective_period_type="FY",
        warnings=["partial history"],
    )
    assert diagnostics.model_dump(mode="json") == {
        "missing_required_data": ["statements"],
        "used_fields": ["eps", "forecast_eps"],
        "effective_period_type": "FY",
        "warnings": ["partial history"],
    }


def test_data_provenance_complete_serialization_is_stable() -> None:
    provenance = DataProvenance(
        source_kind="dataset",
        market_snapshot_id="market-1",
        dataset_snapshot_id="dataset-1",
        reference_date="2026-07-14",
        loaded_domains=["stock_data", "statements"],
        strategy_name="production/example",
        strategy_fingerprint="sha256:example",
        warnings=["snapshot warning"],
    )
    assert provenance.model_dump(mode="json") == {
        "source_kind": "dataset",
        "market_snapshot_id": "market-1",
        "dataset_snapshot_id": "dataset-1",
        "reference_date": "2026-07-14",
        "loaded_domains": ["stock_data", "statements"],
        "strategy_name": "production/example",
        "strategy_fingerprint": "sha256:example",
        "warnings": ["snapshot warning"],
    }


def test_analytics_contract_defaults_are_stable_and_independent() -> None:
    first_diagnostics = ResponseDiagnostics()
    second_diagnostics = ResponseDiagnostics()
    first_provenance = DataProvenance(source_kind="market")
    second_provenance = DataProvenance(source_kind="market")

    assert first_diagnostics.model_dump(mode="json") == {
        "missing_required_data": [],
        "used_fields": [],
        "effective_period_type": None,
        "warnings": [],
    }
    assert first_provenance.model_dump(mode="json") == {
        "source_kind": "market",
        "market_snapshot_id": None,
        "dataset_snapshot_id": None,
        "reference_date": None,
        "loaded_domains": [],
        "strategy_name": None,
        "strategy_fingerprint": None,
        "warnings": [],
    }
    assert first_diagnostics.missing_required_data is not second_diagnostics.missing_required_data
    assert first_diagnostics.used_fields is not second_diagnostics.used_fields
    assert first_diagnostics.warnings is not second_diagnostics.warnings
    assert first_provenance.loaded_domains is not second_provenance.loaded_domains
    assert first_provenance.warnings is not second_provenance.warnings


def test_analytics_contract_required_fields_and_literals_are_stable() -> None:
    with pytest.raises(ValidationError):
        DataProvenance.model_validate({})

    with pytest.raises(ValidationError):
        DataProvenance.model_validate({"source_kind": "legacy"})


def test_analytics_contract_json_schema_is_stable() -> None:
    diagnostics_schema = ResponseDiagnostics.model_json_schema()
    provenance_schema = DataProvenance.model_json_schema()

    assert diagnostics_schema["title"] == "ResponseDiagnostics"
    assert diagnostics_schema.get("required", []) == []
    assert set(diagnostics_schema["properties"]) == {
        "missing_required_data",
        "used_fields",
        "effective_period_type",
        "warnings",
    }
    assert provenance_schema["title"] == "DataProvenance"
    assert provenance_schema["required"] == ["source_kind"]
    assert set(provenance_schema["properties"]) == {
        "source_kind",
        "market_snapshot_id",
        "dataset_snapshot_id",
        "reference_date",
        "loaded_domains",
        "strategy_name",
        "strategy_fingerprint",
        "warnings",
    }
