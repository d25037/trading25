"""Pure validation of full-rebuild rehearsal evidence."""

from __future__ import annotations

from collections.abc import Mapping
import json
from urllib.parse import quote

from .contracts import ActivationAttempt, MarketTreeIdentity, SmokeConfig
from .smoke import RuntimeSmokeService


def _mutable_evidence(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _mutable_evidence(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_mutable_evidence(child) for child in value]
    return value


def market_tree_identity_evidence(identity: MarketTreeIdentity) -> dict[str, object]:
    return {
        "path": identity.path,
        "directory": dict(identity.directory),
        "payload": _mutable_evidence(identity.payload),
    }


def _canonical_evidence(value: object) -> bytes | None:
    try:
        return json.dumps(
            _mutable_evidence(value),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError):
        return None


def _exact_evidence(actual: object, expected: object) -> bool:
    actual_bytes = _canonical_evidence(actual)
    expected_bytes = _canonical_evidence(expected)
    return (
        actual_bytes is not None
        and expected_bytes is not None
        and actual_bytes == expected_bytes
    )


def activation_report_contract_valid(
    report: dict[str, object],
    *,
    attempt: ActivationAttempt,
    quarantine: MarketTreeIdentity,
    evidence: dict[str, object],
) -> bool:
    """Bind success publication to the immutable journal attempt."""

    frozen_evidence = attempt.source.payload.get("schemaCoverage")
    if not isinstance(frozen_evidence, Mapping):
        return False
    provider_vintage = frozen_evidence.get("providerVintage")
    active_backup_sha256 = attempt.active_before.payload.get("marketTreeSha256")
    if (
        not isinstance(provider_vintage, Mapping)
        or not isinstance(active_backup_sha256, str)
        or not _exact_evidence(
            attempt.backup.payload.get("marketTreeSha256"),
            active_backup_sha256,
        )
    ):
        return False
    expected_fields: dict[str, object] = {
        "reportId": attempt.report_id,
        "rehearsalReportId": attempt.rehearsal_report_id,
        "backupId": attempt.backup_id,
        "codeVersion": attempt.code_version,
        "phase": "cutover",
        "status": "passed",
        "activationMode": "journaled_atomic_exchange",
        "smokeConfig": {
            "symbol": attempt.config.symbol,
            "strategy": attempt.config.strategy,
            "datasetPreset": attempt.config.dataset_preset,
        },
        "schemaCoverage": frozen_evidence,
        "sourceMarketIdentity": market_tree_identity_evidence(attempt.source),
        "stagedMarketIdentity": market_tree_identity_evidence(attempt.staged),
        "activeMarketIdentityBefore": market_tree_identity_evidence(
            attempt.active_before
        ),
        "backupMarketIdentity": market_tree_identity_evidence(attempt.backup),
        "activatedMarketIdentity": market_tree_identity_evidence(
            attempt.expected_active
        ),
        "quarantineMarketIdentity": market_tree_identity_evidence(quarantine),
        "activeBackupTreeSha256": active_backup_sha256,
        "stagedProviderVintage": provider_vintage,
        "activeProviderVintage": provider_vintage,
    }
    return _exact_evidence(evidence, frozen_evidence) and all(
        _exact_evidence(report.get(field), expected)
        for field, expected in expected_fields.items()
    )


def bind_activation_report(
    report: dict[str, object],
    *,
    attempt: ActivationAttempt,
    quarantine: MarketTreeIdentity,
    evidence: dict[str, object],
) -> None:
    report.update(
        {
            "activationMode": "journaled_atomic_exchange",
            "sourceMarketIdentity": market_tree_identity_evidence(attempt.source),
            "stagedMarketIdentity": market_tree_identity_evidence(attempt.staged),
            "activeMarketIdentityBefore": market_tree_identity_evidence(
                attempt.active_before
            ),
            "backupMarketIdentity": market_tree_identity_evidence(attempt.backup),
            "activatedMarketIdentity": market_tree_identity_evidence(
                attempt.expected_active
            ),
            "quarantineMarketIdentity": market_tree_identity_evidence(quarantine),
        }
    )
    if not activation_report_contract_valid(
        report,
        attempt=attempt,
        quarantine=quarantine,
        evidence=evidence,
    ):
        raise ValueError("Cutover report is not bound to its activation attempt")


def activated_lineage_valid(
    staged_evidence: dict[str, object],
    active_lineage: dict[str, object],
) -> bool:
    staged = staged_evidence.get("providerVintage")
    return isinstance(staged, dict) and _exact_evidence(active_lineage, staged)


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
