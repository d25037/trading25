from typing import get_args

import pytest
from pydantic import TypeAdapter, ValidationError

from src.application.contracts.analytics import DataProvenance, ResponseDiagnostics
from src.application.contracts.screening import (
    MarketScreeningResponse,
    MatchedStrategyItem,
    ScreeningJobPayload,
    ScreeningJobRequest,
    ScreeningResultItem,
    ScreeningSummary,
)
from src.domains.analytics.screening_results import ScreeningSortBy, SortOrder
from src.domains.strategy.runtime.screening_profile import (
    EntryDecidability,
    ScreeningSupport,
)


def test_market_screening_response_complete_serialization_is_stable() -> None:
    response = MarketScreeningResponse(
        results=[
            ScreeningResultItem(
                stockCode="7203",
                companyName="Toyota Motor",
                scaleCategory="TOPIX Core30",
                sector33Name="Transportation Equipment",
                matchedDate="2026-07-14",
                bestStrategyName="range_break_v15",
                bestStrategyScore=1.25,
                matchStrategyCount=1,
                matchedStrategies=[
                    MatchedStrategyItem(
                        strategyName="range_break_v15",
                        matchedDate="2026-07-14",
                        strategyScore=1.25,
                    )
                ],
            )
        ],
        summary=ScreeningSummary(
            totalStocksScreened=100,
            matchCount=1,
            skippedCount=2,
            byStrategy={"range_break_v15": 1},
            strategiesEvaluated=["range_break_v15"],
            strategiesWithoutBacktestMetrics=[],
            warnings=["partial fundamentals"],
        ),
        entry_decidability="pre_open_decidable",
        markets=["0111"],
        scopeLabel="Prime",
        recentDays=10,
        referenceDate="2026-07-14",
        sortBy="bestStrategyScore",
        order="desc",
        lastUpdated="2026-07-14T00:00:00+00:00",
        provenance=DataProvenance(
            source_kind="market",
            market_snapshot_id="market-1",
            reference_date="2026-07-14",
            loaded_domains=["stock_data"],
            warnings=["snapshot warning"],
        ),
        diagnostics=ResponseDiagnostics(
            missing_required_data=["statements"],
            used_fields=["stock_data"],
            effective_period_type="multi",
            warnings=["partial fundamentals"],
        ),
    )

    assert response.model_dump(mode="json") == {
        "results": [
            {
                "stockCode": "7203",
                "companyName": "Toyota Motor",
                "scaleCategory": "TOPIX Core30",
                "sector33Name": "Transportation Equipment",
                "matchedDate": "2026-07-14",
                "bestStrategyName": "range_break_v15",
                "bestStrategyScore": 1.25,
                "matchStrategyCount": 1,
                "matchedStrategies": [
                    {
                        "strategyName": "range_break_v15",
                        "matchedDate": "2026-07-14",
                        "strategyScore": 1.25,
                    }
                ],
            }
        ],
        "summary": {
            "totalStocksScreened": 100,
            "matchCount": 1,
            "skippedCount": 2,
            "byStrategy": {"range_break_v15": 1},
            "strategiesEvaluated": ["range_break_v15"],
            "strategiesWithoutBacktestMetrics": [],
            "warnings": ["partial fundamentals"],
        },
        "entry_decidability": "pre_open_decidable",
        "markets": ["0111"],
        "scopeLabel": "Prime",
        "recentDays": 10,
        "referenceDate": "2026-07-14",
        "sortBy": "bestStrategyScore",
        "order": "desc",
        "lastUpdated": "2026-07-14T00:00:00+00:00",
        "provenance": {
            "source_kind": "market",
            "market_snapshot_id": "market-1",
            "dataset_snapshot_id": None,
            "reference_date": "2026-07-14",
            "loaded_domains": ["stock_data"],
            "strategy_name": None,
            "strategy_fingerprint": None,
            "warnings": ["snapshot warning"],
        },
        "diagnostics": {
            "missing_required_data": ["statements"],
            "used_fields": ["stock_data"],
            "effective_period_type": "multi",
            "warnings": ["partial fundamentals"],
        },
    }


def test_screening_mutable_defaults_are_fresh_and_stable() -> None:
    first_result = ScreeningResultItem(
        stockCode="7203",
        companyName="Toyota Motor",
        matchedDate="2026-07-14",
        bestStrategyName="range_break_v15",
        matchStrategyCount=0,
    )
    second_result = ScreeningResultItem(
        stockCode="6758",
        companyName="Sony Group",
        matchedDate="2026-07-14",
        bestStrategyName="range_break_v15",
        matchStrategyCount=0,
    )
    first_summary = ScreeningSummary(totalStocksScreened=2, matchCount=0)
    second_summary = ScreeningSummary(totalStocksScreened=2, matchCount=0)
    first_response = MarketScreeningResponse(
        results=[],
        summary=first_summary,
        markets=["0111"],
        recentDays=10,
        sortBy="matchedDate",
        order="desc",
        lastUpdated="2026-07-14T00:00:00+00:00",
        provenance=DataProvenance(source_kind="market"),
    )
    second_response = MarketScreeningResponse(
        results=[],
        summary=second_summary,
        markets=["0111"],
        recentDays=10,
        sortBy="matchedDate",
        order="desc",
        lastUpdated="2026-07-14T00:00:00+00:00",
        provenance=DataProvenance(source_kind="market"),
    )

    assert first_result.matchedStrategies == []
    assert first_result.matchedStrategies is not second_result.matchedStrategies
    assert first_summary.model_dump(mode="json") == {
        "totalStocksScreened": 2,
        "matchCount": 0,
        "skippedCount": 0,
        "byStrategy": {},
        "strategiesEvaluated": [],
        "strategiesWithoutBacktestMetrics": [],
        "warnings": [],
    }
    assert first_summary.byStrategy is not second_summary.byStrategy
    assert first_summary.strategiesEvaluated is not second_summary.strategiesEvaluated
    assert (
        first_summary.strategiesWithoutBacktestMetrics
        is not second_summary.strategiesWithoutBacktestMetrics
    )
    assert first_summary.warnings is not second_summary.warnings
    assert first_response.diagnostics.model_dump(mode="json") == {
        "missing_required_data": [],
        "used_fields": [],
        "effective_period_type": None,
        "warnings": [],
    }
    assert (
        first_response.diagnostics.missing_required_data
        is not second_response.diagnostics.missing_required_data
    )


def test_screening_job_request_defaults_and_payload_are_stable() -> None:
    request = ScreeningJobRequest()
    payload = ScreeningJobPayload(response={"results": []})

    assert request.model_dump(mode="json") == {
        "entry_decidability": "pre_open_decidable",
        "markets": None,
        "strategies": None,
        "recentDays": 10,
        "date": None,
        "sortBy": "matchedDate",
        "order": "desc",
        "limit": None,
    }
    assert payload.model_dump(mode="json") == {"response": {"results": []}}


@pytest.mark.parametrize(
    "payload",
    (
        {"unknown": True},
        {"recentDays": 0},
        {"recentDays": 91},
        {"date": "20260714"},
        {"limit": 0},
    ),
)
def test_screening_job_request_validation_is_stable(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ScreeningJobRequest.model_validate(payload)


def test_screening_alias_values_and_order_are_stable() -> None:
    assert get_args(EntryDecidability) == (
        "pre_open_decidable",
        "requires_same_session_observation",
    )
    assert get_args(ScreeningSupport) == ("supported", "unsupported")
    assert get_args(ScreeningSortBy) == (
        "bestStrategyScore",
        "matchedDate",
        "stockCode",
        "matchStrategyCount",
    )
    assert get_args(SortOrder) == ("asc", "desc")

    alias_cases = (
        (EntryDecidability, ("pre_open_decidable", "requires_same_session_observation")),
        (ScreeningSupport, ("supported", "unsupported")),
        (
            ScreeningSortBy,
            ("bestStrategyScore", "matchedDate", "stockCode", "matchStrategyCount"),
        ),
        (SortOrder, ("asc", "desc")),
    )
    for alias, accepted_values in alias_cases:
        adapter = TypeAdapter(alias)
        for value in accepted_values:
            assert adapter.validate_python(value) == value
        with pytest.raises(ValidationError):
            adapter.validate_python("legacy")


def test_screening_contract_json_schemas_are_stable() -> None:
    expected_shapes = {
        MatchedStrategyItem: (
            {"strategyName", "matchedDate", "strategyScore"},
            ["strategyName", "matchedDate"],
        ),
        ScreeningResultItem: (
            {
                "stockCode",
                "companyName",
                "scaleCategory",
                "sector33Name",
                "matchedDate",
                "bestStrategyName",
                "bestStrategyScore",
                "matchStrategyCount",
                "matchedStrategies",
            },
            [
                "stockCode",
                "companyName",
                "matchedDate",
                "bestStrategyName",
                "matchStrategyCount",
            ],
        ),
        ScreeningSummary: (
            {
                "totalStocksScreened",
                "matchCount",
                "skippedCount",
                "byStrategy",
                "strategiesEvaluated",
                "strategiesWithoutBacktestMetrics",
                "warnings",
            },
            ["totalStocksScreened", "matchCount"],
        ),
        MarketScreeningResponse: (
            {
                "results",
                "summary",
                "entry_decidability",
                "markets",
                "scopeLabel",
                "recentDays",
                "referenceDate",
                "sortBy",
                "order",
                "lastUpdated",
                "provenance",
                "diagnostics",
            },
            [
                "results",
                "summary",
                "markets",
                "recentDays",
                "sortBy",
                "order",
                "lastUpdated",
                "provenance",
            ],
        ),
        ScreeningJobRequest: (
            {
                "entry_decidability",
                "markets",
                "strategies",
                "recentDays",
                "date",
                "sortBy",
                "order",
                "limit",
            },
            [],
        ),
        ScreeningJobPayload: ({"response"}, ["response"]),
    }

    for model, (properties, required) in expected_shapes.items():
        schema = model.model_json_schema()
        assert schema["title"] == model.__name__
        assert set(schema["properties"]) == properties
        assert schema.get("required", []) == required

    request_schema = ScreeningJobRequest.model_json_schema()
    assert request_schema["additionalProperties"] is False
    assert request_schema["properties"]["recentDays"]["minimum"] == 1
    assert request_schema["properties"]["recentDays"]["maximum"] == 90
    assert request_schema["properties"]["date"]["anyOf"][0]["pattern"] == r"^\d{4}-\d{2}-\d{2}$"
    assert request_schema["properties"]["limit"]["anyOf"][0]["minimum"] == 1
