from __future__ import annotations

import json
from pathlib import Path
import sys

import duckdb

import scripts.benchmark_market_v5_sync as benchmark_module
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


def test_fixture_benchmark_can_commit_matched_v4_v5_resource_evidence(
    tmp_path: Path,
) -> None:
    report = run_benchmark_fixture(
        _fixture(),
        evidence_source="matched_production_fixture",
        representative_fixture_comparison=True,
        workspace=tmp_path,
    )

    assert report["representativeEvidence"] == "measured"
    comparison = report["representativeComparison"]
    assert comparison["v4"]["schemaVersion"] == 4
    assert comparison["v5"]["schemaVersion"] == 5
    assert comparison["v4"]["inputFingerprint"] == comparison["v5"][
        "inputFingerprint"
    ]
    assert "legacy_all_code_adapter" in comparison["v4"]["measurementPath"]
    assert "provider_v5" in comparison["v5"]["measurementPath"]
    for version in ("v4", "v5"):
        assert comparison[version]["wallSeconds"] > 0
        assert comparison[version]["cpuSeconds"] > 0
        assert comparison[version]["peakRssBytes"] > 0


def test_fixture_report_includes_measured_representative_v4_v5_comparison(
    tmp_path: Path,
) -> None:
    comparison = {
        "v4": {
            "schemaVersion": 4,
            "stockPriceAdjustmentMode": "local_projection_v2_event_time",
            "wallSeconds": 300.0,
            "cpuSeconds": 240.0,
            "peakRssBytes": 2_000_000_000,
        },
        "v5": {
            "schemaVersion": 5,
            "stockPriceAdjustmentMode": "provider_adjusted_v1",
            "wallSeconds": 60.0,
            "cpuSeconds": 45.0,
            "peakRssBytes": 900_000_000,
        },
        "v5ToV4WallRatio": 0.2,
        "v5ToV4CpuRatio": 0.1875,
        "v5ToV4PeakRssRatio": 0.45,
    }

    report = run_benchmark_fixture(
        _fixture(),
        representative_comparison=comparison,
        representative_inspection={"eligible": True, "schemaVersion": 5},
        workspace=tmp_path,
    )

    assert report["representativeEvidence"] == "measured"
    assert report["representativeEvidenceReason"] is None
    assert report["representativeComparison"] == comparison


def test_main_measures_eligible_v5_market_against_v4_evidence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture_path = tmp_path / "fixture.json"
    fixture_path.write_text(json.dumps(_fixture()), encoding="utf-8")
    market_root = tmp_path / "representative-market"
    market_root.mkdir()
    conn = duckdb.connect(str(market_root / "market.duckdb"))
    conn.execute(
        "CREATE TABLE market_schema_version "
        "(version INTEGER, applied_at TEXT, notes TEXT)"
    )
    conn.execute("INSERT INTO market_schema_version VALUES (5, NULL, NULL)")
    conn.execute("CREATE TABLE sync_metadata (key TEXT, value TEXT, updated_at TEXT)")
    conn.execute(
        "INSERT INTO sync_metadata VALUES "
        "('stock_price_adjustment_mode', 'provider_adjusted_v1', NULL)"
    )
    conn.close()
    v4_path = tmp_path / "v4.json"
    v4_path.write_text(
        json.dumps(
            {
                "schemaVersion": 4,
                "stockPriceAdjustmentMode": "local_projection_v2_event_time",
                "wallSeconds": 300.0,
                "cpuSeconds": 240.0,
                "peakRssBytes": 2_000_000_000,
                "inputFingerprint": "a" * 64,
            }
        ),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    def fake_measure(*_args, **_kwargs):
        return {
            "schemaVersion": 5,
            "stockPriceAdjustmentMode": "provider_adjusted_v1",
            "wallSeconds": 60.0,
            "cpuSeconds": 45.0,
            "peakRssBytes": 900_000_000,
            "inputFingerprint": "a" * 64,
        }

    def fake_fixture(*_args, **kwargs):
        captured.update(kwargs)
        return {"allAssertionsPassed": True}

    monkeypatch.setattr(
        benchmark_module,
        "_run_representative_v5_benchmark",
        fake_measure,
        raising=False,
    )
    monkeypatch.setattr(benchmark_module, "run_benchmark_fixture", fake_fixture)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark_market_v5_sync.py",
            "--fixture",
            str(fixture_path),
            "--workspace",
            str(tmp_path / "work"),
            "--representative-market-root",
            str(market_root),
            "--representative-v4-evidence",
            str(v4_path),
        ],
    )

    assert benchmark_module.main() == 0
    comparison = captured["representative_comparison"]
    assert comparison["v4"]["wallSeconds"] == 300.0
    assert comparison["v5"]["wallSeconds"] == 60.0
    assert comparison["v5ToV4WallRatio"] == 0.2


def test_representative_v5_measurement_uses_copy_and_preserves_source(
    tmp_path: Path,
) -> None:
    source_scenario = tmp_path / "source"
    session = benchmark_module._open_scenario(source_scenario)
    benchmark_module._close_scenario(session)
    market_root = source_scenario / "data/market-timeseries"
    source_before = benchmark_module._tree_checksum(market_root)

    metrics = benchmark_module._run_representative_v5_benchmark(
        _fixture(),
        market_root=market_root,
        workspace=tmp_path / "measurement",
    )

    assert benchmark_module._tree_checksum(market_root) == source_before
    assert metrics["schemaVersion"] == 5
    assert metrics["stockPriceAdjustmentMode"] == "provider_adjusted_v1"
    assert metrics["measurementPath"] == (
        "representative_copy_production_sync_coordinator_duckdb_parquet"
    )
    assert metrics["wallSeconds"] > 0
    assert metrics["cpuSeconds"] > 0
    assert metrics["peakRssBytes"] > 0
