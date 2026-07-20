"""Focused Market v5 cutover responsibility module."""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import cast

from src.infrastructure.db.market import managed_root as _managed_root

from .contracts import (
    PromotionIdentityEvidence,
    PromotionState,
)

SCHEMA_VERSION = 1
RECORD_NAME = re.compile(r"[0-9]{8}\.json")
RECORD_KEYS = {
    "schema_version",
    "operation_id",
    "sequence",
    "state",
    "created_at",
    "identities",
    "previous_record_sha256",
}
CONTROL_NAME = re.compile(r"[0-9]{8}\.(?:intent|resolution)\.json")
CONTROL_COMMON_KEYS = {
    "schema_version",
    "control_sequence",
    "kind",
    "operation_id",
    "attempt_id",
    "created_at",
    "previous_control_sha256",
}
INTENT_KEYS = CONTROL_COMMON_KEYS | {
    "target_sequence",
    "target_name",
    "payload_sha256",
    "previous_record_sha256",
    "state",
    "identities",
}
RESOLUTION_KEYS = CONTROL_COMMON_KEYS | {
    "target_sequence",
    "target_name",
    "payload_sha256",
    "outcome",
}


class JournalValidator:
    """Stateless schema and state-transition validation."""

    _IDENTITY_KEYS = {
        "active_before_directory",
        "active_before_payload",
        "retained_v4_directory",
        "retained_v4_payload",
        "backup_manifest_sha256",
        "backup_file_set_sha256",
        "active_current",
        "retained_current",
        "quarantine_current",
        "holding_current",
        "detached_runtime_names",
        "detached_artifacts",
        "rollback_mode",
        "promotion_report_sha256",
    }
    _TRANSITIONS: dict[PromotionState | None, frozenset[PromotionState]] = {
        None: frozenset({PromotionState.VALIDATED}),
        PromotionState.VALIDATED: frozenset(
            {
                PromotionState.RUNTIMES_DETACHED,
                PromotionState.ROLLED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.RUNTIMES_DETACHED: frozenset(
            {
                PromotionState.PREPARED,
                PromotionState.ROLLED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.PREPARED: frozenset(
            {
                PromotionState.EXCHANGED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
                PromotionState.ROLLED_BACK,
            }
        ),
        PromotionState.EXCHANGED: frozenset(
            {
                PromotionState.QUARANTINED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.QUARANTINED: frozenset(
            {
                PromotionState.ACTIVE_SMOKE_PASSED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.ACTIVE_SMOKE_PASSED: frozenset(
            {
                PromotionState.CLEANUP_STAGED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.CLEANUP_STAGED: frozenset(
            {
                PromotionState.REPORT_PERSISTED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.REPORT_PERSISTED: frozenset(
            {
                PromotionState.COMMITTED,
                PromotionState.EXCHANGED_BACK,
                PromotionState.ROLLBACK_DEFERRED,
            }
        ),
        PromotionState.COMMITTED: frozenset(),
        PromotionState.EXCHANGED_BACK: frozenset({PromotionState.ROLLED_BACK}),
        PromotionState.ROLLED_BACK: frozenset(),
        PromotionState.ROLLBACK_DEFERRED: frozenset({PromotionState.EXCHANGED_BACK}),
    }
    _LOCATION_REQUIREMENTS: dict[
        PromotionState, tuple[bool, bool | None, bool | None, bool | None]
    ] = {
        PromotionState.VALIDATED: (True, True, False, False),
        PromotionState.RUNTIMES_DETACHED: (True, True, False, True),
        PromotionState.PREPARED: (True, True, False, True),
        PromotionState.EXCHANGED: (True, True, False, True),
        PromotionState.QUARANTINED: (True, False, True, True),
        PromotionState.ACTIVE_SMOKE_PASSED: (True, False, True, True),
        PromotionState.CLEANUP_STAGED: (True, False, True, True),
        PromotionState.REPORT_PERSISTED: (True, False, True, True),
        PromotionState.COMMITTED: (True, False, True, True),
        PromotionState.EXCHANGED_BACK: (True, True, None, None),
        PromotionState.ROLLED_BACK: (True, True, None, False),
        PromotionState.ROLLBACK_DEFERRED: (True, None, None, None),
    }

    @staticmethod
    def _canonical_json(value: object) -> bytes:
        try:
            return (
                json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode()
        except (TypeError, ValueError) as exc:
            raise _managed_root.CutoverSafetyError(
                "Promotion journal record is not JSON-safe"
            ) from exc

    @staticmethod
    def _sha256_valid(value: object) -> bool:
        return (
            isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None
        )

    @staticmethod
    def _directory_valid(value: object) -> bool:
        return (
            isinstance(value, dict)
            and set(value) == {"device", "inode"}
            and all(type(value[key]) is int and value[key] >= 0 for key in value)
        )

    @classmethod
    def _file_valid(cls, candidate: object) -> bool:
        return (
            isinstance(candidate, dict)
            and set(candidate) == {"device", "inode", "size", "sha256"}
            and all(
                type(candidate[key]) is int and candidate[key] >= 0
                for key in ("device", "inode", "size")
            )
            and cls._sha256_valid(candidate["sha256"])
        )

    @classmethod
    def _payload_valid(cls, value: object) -> bool:
        if not isinstance(value, dict) or set(value) != {
            "marketDuckdb",
            "parquetSha256",
        }:
            return False

        parquet = value["parquetSha256"]
        return (
            cls._file_valid(value["marketDuckdb"])
            and isinstance(parquet, dict)
            and bool(parquet)
            and all(
                isinstance(path, str)
                and bool(path)
                and not Path(path).is_absolute()
                and ".." not in Path(path).parts
                and Path(path).as_posix() == path
                and all(part not in {"", "."} for part in path.split("/"))
                and cls._file_valid(identity)
                for path, identity in parquet.items()
            )
        )

    @classmethod
    def _location_valid(cls, value: object) -> bool:
        return (
            isinstance(value, dict)
            and set(value) == {"directory", "payload"}
            and cls._directory_valid(value["directory"])
            and cls._payload_valid(value["payload"])
        )

    @classmethod
    def _identity_mapping_valid(
        cls,
        value: object,
        state: PromotionState,
    ) -> bool:
        if not isinstance(value, dict) or set(value) != cls._IDENTITY_KEYS:
            return False
        if not (
            cls._directory_valid(value["active_before_directory"])
            and cls._payload_valid(value["active_before_payload"])
            and cls._directory_valid(value["retained_v4_directory"])
            and cls._payload_valid(value["retained_v4_payload"])
            and cls._sha256_valid(value["backup_manifest_sha256"])
            and cls._sha256_valid(value["backup_file_set_sha256"])
        ):
            return False
        detached = value["detached_runtime_names"]
        if not (
            isinstance(detached, list)
            and all(
                isinstance(name, str)
                and bool(name)
                and "/" not in name
                and name not in {".", ".."}
                for name in detached
            )
            and len(set(detached)) == len(detached)
        ):
            return False
        artifacts = value["detached_artifacts"]
        if not isinstance(artifacts, list):
            return False
        artifact_names: list[str] = []
        for artifact in artifacts:
            if not isinstance(artifact, dict) or set(artifact) != {
                "name",
                "kind",
                "identity",
                "directories",
                "files",
            }:
                return False
            name = artifact["name"]
            kind = artifact["kind"]
            directories = artifact["directories"]
            files = artifact["files"]
            if not (
                isinstance(name, str)
                and name
                in {
                    *detached,
                    "duckdb-tmp",
                    "market.duckdb.wal",
                    "maintenance.v1.json",
                }
                and isinstance(kind, str)
                and kind in {"directory", "regular"}
                and isinstance(directories, dict)
                and isinstance(files, dict)
                and all(
                    isinstance(path, str) and cls._directory_valid(identity)
                    for path, identity in directories.items()
                )
                and all(
                    isinstance(path, str) and cls._file_valid(identity)
                    for path, identity in files.items()
                )
                and (
                    cls._directory_valid(artifact["identity"])
                    if kind == "directory"
                    else cls._file_valid(artifact["identity"])
                )
            ):
                return False
            artifact_names.append(name)
        if len(set(artifact_names)) != len(artifact_names):
            return False
        if value["rollback_mode"] not in {
            None,
            "atomic_exchange",
            "backup_restore",
        }:
            return False
        report_sha256 = value["promotion_report_sha256"]
        if state is PromotionState.COMMITTED:
            if not cls._sha256_valid(report_sha256):
                return False
        elif report_sha256 is not None:
            return False
        active, retained, quarantine, holding = cls._LOCATION_REQUIREMENTS[state]
        locations = (
            ("active_current", active),
            ("retained_current", retained),
            ("quarantine_current", quarantine),
            ("holding_current", holding),
        )
        for key, required in locations:
            if required is True and not cls._location_valid(value[key]):
                return False
            if required is False and value[key] is not None:
                return False
        if state is PromotionState.ROLLBACK_DEFERRED:
            retained = value["retained_current"]
            quarantine = value["quarantine_current"]
            if not (
                (cls._location_valid(retained) and quarantine is None)
                or (retained is None and cls._location_valid(quarantine))
            ):
                return False
        if state in {PromotionState.EXCHANGED_BACK, PromotionState.ROLLBACK_DEFERRED}:
            holding = value["holding_current"]
            if holding is not None and not cls._location_valid(holding):
                return False
        if state in {PromotionState.EXCHANGED_BACK, PromotionState.ROLLED_BACK}:
            rollback_mode = value["rollback_mode"]
            quarantine = value["quarantine_current"]
            if rollback_mode == "atomic_exchange" and quarantine is not None:
                return False
            if rollback_mode == "backup_restore" and not cls._location_valid(
                quarantine
            ):
                return False
        return True

    @classmethod
    def _identity_to_mapping(
        cls, identities: PromotionIdentityEvidence
    ) -> dict[str, object]:
        return {
            "active_before_directory": identities.active_before_directory,
            "active_before_payload": identities.active_before_payload,
            "retained_v4_directory": identities.retained_v4_directory,
            "retained_v4_payload": identities.retained_v4_payload,
            "backup_manifest_sha256": identities.backup_manifest_sha256,
            "backup_file_set_sha256": identities.backup_file_set_sha256,
            "active_current": identities.active_current,
            "retained_current": identities.retained_current,
            "quarantine_current": identities.quarantine_current,
            "holding_current": identities.holding_current,
            "detached_runtime_names": list(identities.detached_runtime_names),
            "detached_artifacts": list(identities.detached_artifacts),
            "rollback_mode": identities.rollback_mode,
            "promotion_report_sha256": identities.promotion_report_sha256,
        }

    @classmethod
    def _identity_from_mapping(
        cls,
        value: dict[str, object],
        state: PromotionState,
    ) -> PromotionIdentityEvidence:
        if not cls._identity_mapping_valid(value, state):
            raise _managed_root.CutoverSafetyError(
                "Promotion journal identity schema is invalid"
            )
        return PromotionIdentityEvidence(
            active_before_directory=cast(
                dict[str, int], value["active_before_directory"]
            ),
            active_before_payload=cast(
                dict[str, object], value["active_before_payload"]
            ),
            retained_v4_directory=cast(dict[str, int], value["retained_v4_directory"]),
            retained_v4_payload=cast(dict[str, object], value["retained_v4_payload"]),
            backup_manifest_sha256=cast(str, value["backup_manifest_sha256"]),
            backup_file_set_sha256=cast(str, value["backup_file_set_sha256"]),
            active_current=cast(dict[str, object] | None, value["active_current"]),
            retained_current=cast(dict[str, object] | None, value["retained_current"]),
            quarantine_current=cast(
                dict[str, object] | None, value["quarantine_current"]
            ),
            holding_current=cast(dict[str, object] | None, value["holding_current"]),
            detached_runtime_names=tuple(
                cast(list[str], value["detached_runtime_names"])
            ),
            detached_artifacts=tuple(
                cast(list[dict[str, object]], value["detached_artifacts"])
            ),
            rollback_mode=cast(str | None, value["rollback_mode"]),
            promotion_report_sha256=cast(str | None, value["promotion_report_sha256"]),
        )

    @staticmethod
    def _immutable_identity(
        identities: PromotionIdentityEvidence,
    ) -> tuple[object, ...]:
        return (
            identities.active_before_directory,
            identities.active_before_payload,
            identities.retained_v4_directory,
            identities.retained_v4_payload,
            identities.backup_manifest_sha256,
            identities.backup_file_set_sha256,
        )

    @classmethod
    def _validate_transition(
        cls,
        previous: PromotionState | None,
        current: PromotionState,
    ) -> None:
        if current not in cls._TRANSITIONS[previous]:
            raise _managed_root.CutoverSafetyError(
                f"Invalid promotion journal state transition: {previous!s} -> {current}"
            )
