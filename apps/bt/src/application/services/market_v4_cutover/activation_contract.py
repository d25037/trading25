"""Pure validation of full-rebuild rehearsal evidence."""

from __future__ import annotations

from urllib.parse import quote

from .contracts import SmokeConfig
from .smoke import RuntimeSmokeService


def full_rebuild_report_contract_valid(
    report: dict[str, object],
    *,
    config: SmokeConfig,
) -> bool:
    api_checks = report.get("apiChecks")
    required_api_checks = {
        "/api/db/stats",
        "/api/db/validate",
        f"/api/analytics/fundamentals/{quote(config.symbol, safe='')}",
        "/api/fundamentals/compute",
        "/api/analytics/screening/jobs",
        "/api/analytics/fundamental-ranking",
        "/api/dataset",
        "/api/db/sync",
    }
    if (
        not isinstance(api_checks, list)
        or not all(isinstance(path, str) for path in api_checks)
        or not required_api_checks.issubset(set(api_checks))
        or not any(
            path.startswith("/api/analytics/screening/jobs/")
            for path in api_checks
        )
        or not any("/api/analytics/screening/result/" in path for path in api_checks)
        or not any(path.startswith("/api/dataset/jobs/") for path in api_checks)
        or not any(path.endswith("/info") for path in api_checks)
        or not any("/sample?count=1" in path for path in api_checks)
        or not any(path.startswith("/api/db/sync/jobs/") for path in api_checks)
        or any(
            forbidden in path
            for path in api_checks
            for forbidden in ("materialize", "stocks/refresh", "intraday/sync")
        )
    ):
        return False

    counter_keys = {
        "sourceStatementKeyCount",
        "expectedAdjustedStatementRows",
        "invalidProviderWindowCount",
        "invalidAdjustmentEventCount",
        "providerAdjustedMismatchCount",
        "invalidCurrentBasisStateCount",
        "pendingCurrentBasisCodeCount",
        "missingAdjustedStatementRows",
        "extraAdjustedStatementRows",
        "staleAdjustedStatementRows",
        "wrongBasisAdjustedStatementRows",
        "orphanAdjustedStatementRows",
        "currentBasisStatementCount",
        "currentBasisStateCount",
        "providerWindowCount",
        "readyProviderWindowCount",
    }
    positive_keys = {
        "sourceStatementKeyCount",
        "expectedAdjustedStatementRows",
        "currentBasisStatementCount",
        "currentBasisStateCount",
        "providerWindowCount",
        "readyProviderWindowCount",
    }
    coverage = report.get("schemaCoverage")
    if not isinstance(coverage, dict) or set(coverage) != {
        "schemaVersion",
        "stockPriceAdjustmentMode",
        "providerVintage",
    }:
        return False
    provider_vintage = coverage.get("providerVintage")
    if (
        coverage.get("schemaVersion") != 5
        or coverage.get("stockPriceAdjustmentMode") != "provider_adjusted_v1"
        or not isinstance(provider_vintage, dict)
        or not RuntimeSmokeService._is_ready_provider_vintage(provider_vintage)
        or any(type(provider_vintage.get(key)) is not int for key in counter_keys)
        or any(provider_vintage[key] <= 0 for key in positive_keys)
        or any(
            provider_vintage[key] != 0 for key in counter_keys - positive_keys
        )
    ):
        return False

    phases = report.get("phases")
    required_phases = {
        "initial_sync_and_provider_vintage",
        "semantic_smoke",
    }
    return (
        isinstance(phases, list)
        and len(phases) == len(required_phases)
        and all(
            isinstance(phase, dict)
            and phase.get("status") == "passed"
            and isinstance(phase.get("durationSeconds"), (int, float))
            and not isinstance(phase.get("durationSeconds"), bool)
            and float(phase["durationSeconds"]) >= 0
            for phase in phases
        )
        and required_phases
        == {
            str(phase.get("name"))
            for phase in phases
            if isinstance(phase, dict)
        }
    )
