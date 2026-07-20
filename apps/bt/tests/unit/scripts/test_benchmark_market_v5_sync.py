from __future__ import annotations

import json
from pathlib import Path

from scripts.benchmark_market_v5_sync import run_benchmark_fixture


def _fixture(*, one_day_rows: int = 4, affected_codes: int = 4) -> dict[str, object]:
    return {
        "fixtureVersion": 1,
        "providerPlan": "standard",
        "scenarios": {
            "provider_noop": {
                "engine": "provider_v5_incremental",
                "newRows": 0,
                "affectedCodes": 0,
                "rowsPerAffectedCode": 0,
            },
            "provider_one_day": {
                "engine": "provider_v5_incremental",
                "newRows": one_day_rows,
                "affectedCodes": affected_codes,
                "rowsPerAffectedCode": 0,
            },
            "provider_fundamentals_only": {
                "engine": "provider_v5_incremental",
                "newRows": 0,
                "affectedCodes": 1,
                "rowsPerAffectedCode": 0,
            },
            "provider_split_drift": {
                "engine": "provider_v5_incremental",
                "newRows": 1,
                "affectedCodes": 1,
                "rowsPerAffectedCode": 5,
            },
            "legacy_all_code_local_projection": {
                "engine": "local_projection_all_code",
                "allCodes": 12,
                "rowsPerCode": 5,
            },
        },
    }


def test_fixture_benchmark_emits_measured_resource_and_work_json(
    tmp_path: Path,
) -> None:
    report = run_benchmark_fixture(
        _fixture(),
        evidence_source="fixture",
        representative_evidence_reason="local Market is schema v4, not v5",
        workspace=tmp_path,
    )

    assert report["schemaVersion"] == 2
    assert report["benchmark"] == "market_v5_incremental_sync"
    assert report["evidenceSource"] == "fixture"
    assert report["representativeEvidence"] == "unavailable"
    assert report["representativeEvidenceReason"] == (
        "local Market is schema v4, not v5"
    )
    scenarios = report["scenarios"]
    assert isinstance(scenarios, dict)
    for metrics in scenarios.values():
        assert isinstance(metrics, dict)
        assert metrics["wallSeconds"] >= 0
        assert metrics["cpuSeconds"] >= 0
        assert metrics["peakRssBytes"] > 0
        assert metrics["storageGrowthBytes"] >= 0
        assert len(metrics["checksumSha256"]) == 64
        assert metrics["requests"] <= metrics["pages"]
        assert metrics["processId"] > 0
        assert metrics["observations"]["fixtureDeclaredCounters"] is False
        assert metrics["seedBaseline"] == {
            "excludedFromMeasurements": True,
            "pendingCurrentBasisCodeCount": 0,
            "readyProviderWindowCount": 12,
            "currentBasisStateCount": 12,
        }
        assert metrics["seedProcessId"] != metrics["processId"]
        assert metrics["pendingCurrentBasisCodeCountAfterMeasurement"] == 0

    assert len({metrics["processId"] for metrics in scenarios.values()}) == len(
        scenarios
    )
    assert scenarios["provider_one_day"]["coordinator"] == ("StockDataIngestionSession")
    assert (
        scenarios["provider_split_drift"]["materializerSpy"]["implementation"]
        == "AdjustedMetricsMaterializer.rebuild_current_basis"
    )
    assert (
        scenarios["provider_one_day"]["inputFingerprint"]
        == scenarios["legacy_all_code_local_projection"]["inputFingerprint"]
    )


def test_materializer_observations_reject_hidden_pending_union(tmp_path: Path) -> None:
    report = run_benchmark_fixture(_fixture(), workspace=tmp_path)
    scenarios = report["scenarios"]

    for name, expected_codes in (
        ("provider_fundamentals_only", 1),
        ("provider_split_drift", 1),
        ("legacy_all_code_local_projection", 12),
    ):
        metrics = scenarios[name]
        assert metrics["currentBasisRecomputedCodes"] == expected_codes
        assert metrics["currentBasisCompletedCodes"] == expected_codes
        call = metrics["materializerSpy"]["calls"][-1]
        assert call["result"] == {
            "totalCodes": expected_codes,
            "completedCodes": expected_codes,
            "failedCodes": 0,
        }
        assert len(call["processedCodes"]) == expected_codes
        assert set(call["processedCodes"]) == set(call["requestedCodes"])

    assert scenarios["provider_split_drift"]["allCodeMaterializerInvocations"] == 0
    assert (
        scenarios["provider_fundamentals_only"]["allCodeMaterializerInvocations"] == 0
    )
    assert (
        scenarios["legacy_all_code_local_projection"]["allCodeMaterializerInvocations"]
        == 1
    )


def test_provider_incremental_work_scales_with_delta_not_all_codes(
    tmp_path: Path,
) -> None:
    small = run_benchmark_fixture(_fixture(), workspace=tmp_path / "small")
    large = run_benchmark_fixture(
        _fixture(one_day_rows=8, affected_codes=8),
        workspace=tmp_path / "large",
    )

    small_scenarios = small["scenarios"]
    large_scenarios = large["scenarios"]
    assert isinstance(small_scenarios, dict)
    assert isinstance(large_scenarios, dict)
    one_day = small_scenarios["provider_one_day"]
    larger_one_day = large_scenarios["provider_one_day"]
    baseline = small_scenarios["legacy_all_code_local_projection"]
    assert one_day["rowMutations"] == 4
    assert one_day["affectedCodes"] == 0
    assert one_day["publishedCodes"] == 4
    assert one_day["allCodeMaterializerInvocations"] == 0
    assert larger_one_day["rowMutations"] == 8
    assert larger_one_day["affectedCodes"] == 0
    assert larger_one_day["publishedCodes"] == 8
    assert larger_one_day["workUnits"] > one_day["workUnits"]
    assert baseline["allCodeMaterializerInvocations"] == 1
    assert baseline["rowMutations"] == 4
    assert small["assertions"] == {
        "legacyBaselineInvokesAllCodeMaterializer": True,
        "normalIncrementalUsesNoAllCodeMaterializer": True,
        "normalIncrementalWorkBelowLegacyBaseline": True,
        "splitDriftRefreshLimitedToAffectedCodes": True,
        "currentAndLegacyUseIdenticalInput": True,
        "scenariosUseIsolatedProcesses": True,
        "seedDrainExcludedInSeparateProcesses": True,
        "boundedMaterializerUsesActualProcessedCodes": True,
    }


def test_fixture_benchmark_report_is_json_serializable(tmp_path: Path) -> None:
    report = run_benchmark_fixture(_fixture(), workspace=tmp_path)

    assert json.loads(json.dumps(report, sort_keys=True)) == report
