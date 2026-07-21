"""Durability and fail-closed tests for the Market v5 activation journal."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import FrozenInstanceError, replace
import importlib
import json
import os
from pathlib import Path
import stat
from types import ModuleType
from typing import Any

import pytest


def _journal_module() -> ModuleType:
    try:
        return importlib.import_module(
            "src.application.services.market_v4_cutover.activation_journal"
        )
    except ModuleNotFoundError as exc:
        pytest.fail(f"durable activation journal primitive is missing: {exc}")


def _contracts_module() -> ModuleType:
    return importlib.import_module(
        "src.application.services.market_v4_cutover.contracts"
    )


def _types() -> tuple[Any, Any, Any, Any, Any]:
    journal = _journal_module()
    contracts = _contracts_module()
    try:
        return (
            journal.ActivationJournalRepository,
            contracts.ActivationState,
            contracts.MarketTreeIdentity,
            contracts.ActivationAttempt,
            contracts.SmokeConfig,
        )
    except AttributeError as exc:
        pytest.fail(f"activation journal contract is incomplete: {exc}")


def _identity(identity_type: Any, path: str, seed: int) -> Any:
    return identity_type(
        path=path,
        directory={"device": seed, "inode": seed + 100},
        payload={
            "marketTreeSha256": f"tree-{seed}",
            "schemaVersion": 5,
            "stockPriceAdjustmentMode": "provider_adjusted_v1",
        },
    )


def _attempt(*, report_id: str = "cutover-20260721") -> Any:
    _, _, identity_type, attempt_type, smoke_config_type = _types()
    return attempt_type(
        report_id=report_id,
        rehearsal_report_id="rehearsal-20260721",
        backup_id="backup-20260721",
        code_version="fafa3fb451afbf608d07e890e37202920f33746c",
        config=smoke_config_type(
            symbol="7203",
            strategy="production/range_break_v5",
            dataset_preset="prime-all",
        ),
        source=_identity(
            identity_type,
            "operations/market-v5-cutover/staging/cutover-20260721/source",
            1,
        ),
        staged=_identity(
            identity_type,
            "operations/market-v5-cutover/staging/cutover-20260721/market-timeseries",
            2,
        ),
        active_before=_identity(identity_type, "market-timeseries", 3),
        backup=_identity(
            identity_type,
            "operations/market-v5-cutover/backups/backup-20260721/payload",
            4,
        ),
        expected_active=_identity(identity_type, "market-timeseries", 2),
    )


def _operations_root(tmp_path: Path) -> Path:
    root = tmp_path / "operations" / "market-v5-cutover"
    root.mkdir(parents=True)
    return root


def _journal_dir(operations_root: Path, report_id: str) -> Path:
    return operations_root / "activation-journals" / report_id


def _mutable_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _mutable_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_mutable_json(item) for item in value]
    return value


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            _mutable_json(value),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("ascii")


def _record_mapping(sequence: int, state: str, attempt: Any) -> dict[str, object]:
    def identity(value: Any) -> dict[str, object]:
        return {
            "directory": value.directory,
            "path": value.path,
            "payload": value.payload,
        }

    return {
        "attempt": {
            "activeBefore": identity(attempt.active_before),
            "backup": identity(attempt.backup),
            "backupId": attempt.backup_id,
            "codeVersion": attempt.code_version,
            "config": {
                "datasetPreset": attempt.config.dataset_preset,
                "strategy": attempt.config.strategy,
                "symbol": attempt.config.symbol,
            },
            "expectedActive": identity(attempt.expected_active),
            "rehearsalReportId": attempt.rehearsal_report_id,
            "reportId": attempt.report_id,
            "source": identity(attempt.source),
            "staged": identity(attempt.staged),
        },
        "sequence": sequence,
        "state": state,
    }


def _write_record(
    journal_dir: Path,
    *,
    sequence: int,
    name_state: str,
    content_state: str,
    attempt: Any,
    mapping_mutator: Any | None = None,
    raw: bytes | None = None,
) -> Path:
    journal_dir.mkdir(parents=True, exist_ok=True)
    mapping = _record_mapping(sequence, content_state, attempt)
    if mapping_mutator is not None:
        mapping_mutator(mapping)
    path = journal_dir / f"{sequence:08d}-{name_state}.json"
    path.write_bytes(_canonical(mapping) if raw is None else raw)
    return path


def test_attempt_and_identity_contracts_are_frozen() -> None:
    attempt = _attempt()

    with pytest.raises(FrozenInstanceError):
        attempt.report_id = "different"
    with pytest.raises(FrozenInstanceError):
        attempt.source = attempt.staged


def test_market_tree_identity_copies_nested_caller_input() -> None:
    _, _, identity_type, _, _ = _types()
    directory_input = {"device": 7, "inode": 107}
    payload_input = {
        "marketTreeSha256": "tree-7",
        "lineage": {"windows": [{"symbol": "7203", "rows": [1, 2]}]},
    }

    identity = identity_type(
        path="market-timeseries",
        directory=directory_input,
        payload=payload_input,
    )
    directory_input["inode"] = 999
    payload_input["lineage"]["windows"][0]["symbol"] = "6758"
    payload_input["lineage"]["windows"][0]["rows"].append(3)

    assert identity.directory == {"device": 7, "inode": 107}
    assert identity.payload["lineage"]["windows"][0]["symbol"] == "7203"
    assert identity.payload["lineage"]["windows"][0]["rows"] == (1, 2)


def test_market_tree_identity_exposes_recursive_immutable_evidence() -> None:
    _, _, identity_type, _, _ = _types()
    identity = identity_type(
        path="market-timeseries",
        directory={"device": 7, "inode": 107},
        payload={
            "marketTreeSha256": "tree-7",
            "lineage": {"windows": [{"symbol": "7203", "rows": [1, 2]}]},
        },
    )
    lineage = identity.payload["lineage"]
    windows = lineage["windows"]
    first_window = windows[0]

    with pytest.raises(TypeError):
        identity.directory["inode"] = 999
    with pytest.raises(TypeError):
        identity.payload["marketTreeSha256"] = "drifted"
    with pytest.raises(TypeError):
        lineage["extra"] = True
    with pytest.raises(TypeError):
        windows[0] = {"symbol": "6758"}
    with pytest.raises(TypeError):
        first_window["symbol"] = "6758"


def test_append_cannot_drift_with_caller_input_during_encoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_type, state_type, identity_type, _, _ = _types()
    journal = _journal_module()
    payload_input = {
        "marketTreeSha256": "tree-1",
        "lineage": {"windows": [{"symbol": "7203"}]},
    }
    source = identity_type(
        path="operations/market-v5-cutover/staging/cutover-20260721/source",
        directory={"device": 1, "inode": 101},
        payload=payload_input,
    )
    attempt = replace(_attempt(), source=source)
    real_canonical_json = journal._canonical_json
    drifted = False

    def drift_then_encode(value: object) -> bytes:
        nonlocal drifted
        if not drifted:
            drifted = True
            payload_input["lineage"]["windows"][0]["symbol"] = "6758"
        return real_canonical_json(value)

    monkeypatch.setattr(journal, "_canonical_json", drift_then_encode)
    operations_root = _operations_root(tmp_path)
    repository = repository_type(operations_root)

    record = repository.append(attempt, state_type.PREPARED)

    assert record.attempt.source.payload["lineage"]["windows"][0]["symbol"] == "7203"
    record_path = (
        _journal_dir(operations_root, attempt.report_id)
        / "00000001-prepared.json"
    )
    assert record_path.read_bytes() == _canonical(
        _record_mapping(1, "prepared", attempt)
    )
    loaded = repository.load(attempt)
    assert loaded == (record,)
    with pytest.raises(TypeError):
        record.attempt.source.payload["lineage"]["windows"][0]["symbol"] = "6758"
    with pytest.raises(TypeError):
        loaded[0].attempt.source.payload["lineage"]["windows"][0]["symbol"] = "6758"


def test_appends_exact_canonical_sequence_and_loads_it(tmp_path: Path) -> None:
    repository_type, state_type, _, _, _ = _types()
    operations_root = _operations_root(tmp_path)
    repository = repository_type(operations_root)
    attempt = _attempt()
    states = (
        state_type.PREPARED,
        state_type.EXCHANGE_STARTED,
        state_type.ACTIVATED,
        state_type.REPORTED,
    )

    records = tuple(repository.append(attempt, state) for state in states)

    assert tuple(record.state for record in records) == states
    assert tuple(record.sequence for record in records) == (1, 2, 3, 4)
    assert repository.load(attempt) == records
    journal_dir = _journal_dir(operations_root, attempt.report_id)
    assert [path.name for path in sorted(journal_dir.iterdir())] == [
        "00000001-prepared.json",
        "00000002-exchange_started.json",
        "00000003-activated.json",
        "00000004-reported.json",
    ]
    for record, path in zip(records, sorted(journal_dir.iterdir()), strict=True):
        assert path.read_bytes() == _canonical(
            _record_mapping(record.sequence, record.state.value, attempt)
        )


@pytest.mark.parametrize(
    ("existing_states", "requested_state"),
    [
        (("prepared",), "prepared"),
        ((), "exchange_started"),
        (("prepared", "exchange_started"), "prepared"),
        (("prepared", "exchange_started", "activated", "reported"), "reported"),
    ],
    ids=("duplicate", "skipped", "regressed", "after-terminal"),
)
def test_append_rejects_any_nonsequential_transition(
    tmp_path: Path,
    existing_states: tuple[str, ...],
    requested_state: str,
) -> None:
    repository_type, state_type, _, _, _ = _types()
    repository = repository_type(_operations_root(tmp_path))
    attempt = _attempt()
    for state in existing_states:
        repository.append(attempt, state_type(state))

    with pytest.raises(Exception, match="activation journal|transition|state"):
        repository.append(attempt, state_type(requested_state))


@pytest.mark.parametrize(
    ("case", "sequence", "name_state", "content_state", "raw", "mutator"),
    [
        ("skipped-sequence", 2, "exchange_started", "exchange_started", None, None),
        ("unknown-name-state", 1, "unknown", "prepared", None, None),
        ("unknown-content-state", 1, "prepared", "unknown", None, None),
        ("name-content-mismatch", 1, "prepared", "activated", None, None),
        ("torn", 1, "prepared", "prepared", b'{"attempt":', None),
        (
            "extra-field",
            1,
            "prepared",
            "prepared",
            None,
            lambda mapping: mapping.__setitem__("unexpected", True),
        ),
        (
            "wrong-sequence-field",
            1,
            "prepared",
            "prepared",
            None,
            lambda mapping: mapping.__setitem__("sequence", 7),
        ),
        (
            "missing-field",
            1,
            "prepared",
            "prepared",
            None,
            lambda mapping: mapping.pop("attempt"),
        ),
    ],
)
def test_load_rejects_noncanonical_or_torn_records(
    tmp_path: Path,
    case: str,
    sequence: int,
    name_state: str,
    content_state: str,
    raw: bytes | None,
    mutator: Any | None,
) -> None:
    repository_type, _, _, _, _ = _types()
    operations_root = _operations_root(tmp_path)
    attempt = _attempt()
    _write_record(
        _journal_dir(operations_root, attempt.report_id),
        sequence=sequence,
        name_state=name_state,
        content_state=content_state,
        attempt=attempt,
        mapping_mutator=mutator,
        raw=raw,
    )

    with pytest.raises(Exception, match="activation journal|record|canonical|sequence"):
        repository_type(operations_root).load(attempt)


def test_load_rejects_noncanonical_json_encoding(tmp_path: Path) -> None:
    repository_type, _, _, _, _ = _types()
    operations_root = _operations_root(tmp_path)
    attempt = _attempt()
    mapping = _record_mapping(1, "prepared", attempt)
    raw = (
        json.dumps(_mutable_json(mapping), indent=2, sort_keys=False) + "\n"
    ).encode()
    _write_record(
        _journal_dir(operations_root, attempt.report_id),
        sequence=1,
        name_state="prepared",
        content_state="prepared",
        attempt=attempt,
        raw=raw,
    )

    with pytest.raises(Exception, match="canonical"):
        repository_type(operations_root).load(attempt)


@pytest.mark.parametrize(
    "changed",
    (
        "report_id",
        "rehearsal_report_id",
        "backup_id",
        "code_version",
        "config",
        "source",
        "staged",
        "active_before",
        "backup",
        "expected_active",
    ),
)
def test_load_rejects_attempt_or_identity_mismatch(
    tmp_path: Path,
    changed: str,
) -> None:
    repository_type, state_type, identity_type, _, smoke_config_type = _types()
    operations_root = _operations_root(tmp_path)
    original = _attempt()
    repository_type(operations_root).append(original, state_type.PREPARED)
    replacements: dict[str, object] = {
        "report_id": "different-report",
        "rehearsal_report_id": "different-rehearsal",
        "backup_id": "different-backup",
        "code_version": "a" * 40,
        "config": smoke_config_type("6758", "production/other", "topix500"),
        "source": _identity(identity_type, original.source.path, 11),
        "staged": _identity(identity_type, original.staged.path, 12),
        "active_before": _identity(identity_type, original.active_before.path, 13),
        "backup": _identity(identity_type, original.backup.path, 14),
        "expected_active": _identity(identity_type, original.expected_active.path, 15),
    }
    expected = replace(original, **{changed: replacements[changed]})
    if changed == "report_id":
        original_dir = _journal_dir(operations_root, original.report_id)
        original_dir.rename(_journal_dir(operations_root, expected.report_id))

    with pytest.raises(Exception, match="activation journal|attempt|identity|mismatch"):
        repository_type(operations_root).load(expected)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("report_id", "../escape"),
        ("rehearsal_report_id", ""),
        ("backup_id", "backup/escape"),
        ("code_version", ""),
    ),
)
def test_append_rejects_invalid_attempt_identifiers(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    repository_type, state_type, _, _, _ = _types()
    attempt = replace(_attempt(), **{field: value})

    with pytest.raises(Exception, match="invalid|activation journal|ID|identifier"):
        repository_type(_operations_root(tmp_path)).append(
            attempt,
            state_type.PREPARED,
        )


def test_append_rejects_unsafe_or_malformed_tree_identity(tmp_path: Path) -> None:
    repository_type, state_type, identity_type, _, _ = _types()
    unsafe = identity_type(
        path="/absolute/market-timeseries",
        directory={"device": True, "inode": 1},
        payload={"marketTreeSha256": "tree"},
    )

    with pytest.raises(Exception, match="identity|path|activation journal"):
        repository_type(_operations_root(tmp_path)).append(
            replace(_attempt(), source=unsafe),
            state_type.PREPARED,
        )


def test_load_rejects_noncanonical_record_name(tmp_path: Path) -> None:
    repository_type, _, _, _, _ = _types()
    operations_root = _operations_root(tmp_path)
    attempt = _attempt()
    journal_dir = _journal_dir(operations_root, attempt.report_id)
    path = _write_record(
        journal_dir,
        sequence=1,
        name_state="prepared",
        content_state="prepared",
        attempt=attempt,
    )
    path.rename(journal_dir / "1-prepared.json")

    with pytest.raises(Exception, match="canonical|name|activation journal"):
        repository_type(operations_root).load(attempt)


def test_load_rejects_hardlinked_record(tmp_path: Path) -> None:
    repository_type, _, _, _, _ = _types()
    operations_root = _operations_root(tmp_path)
    attempt = _attempt()
    path = _write_record(
        _journal_dir(operations_root, attempt.report_id),
        sequence=1,
        name_state="prepared",
        content_state="prepared",
        attempt=attempt,
    )
    os.link(path, tmp_path / "record-alias.json")

    with pytest.raises(Exception, match="hardlink|link|record|activation journal"):
        repository_type(operations_root).load(attempt)


@pytest.mark.parametrize("symlink_target", ("journal-root", "record"))
def test_load_rejects_symlinked_journal_components(
    tmp_path: Path,
    symlink_target: str,
) -> None:
    repository_type, _, _, _, _ = _types()
    operations_root = _operations_root(tmp_path)
    attempt = _attempt()
    external = tmp_path / "external"
    external.mkdir()
    if symlink_target == "journal-root":
        (operations_root / "activation-journals").symlink_to(
            external,
            target_is_directory=True,
        )
    else:
        journal_dir = _journal_dir(operations_root, attempt.report_id)
        journal_dir.mkdir(parents=True)
        external_record = _write_record(
            external,
            sequence=1,
            name_state="prepared",
            content_state="prepared",
            attempt=attempt,
        )
        (journal_dir / "00000001-prepared.json").symlink_to(external_record)

    with pytest.raises(Exception, match="symlink|journal|directory|record"):
        repository_type(operations_root).load(attempt)


def test_append_uses_exclusive_nofollow_creation_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_type, state_type, _, _, _ = _types()
    journal = _journal_module()
    observed_flags: list[int] = []
    real_open = journal.os.open

    def recording_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if str(path).endswith("prepared.json"):
            observed_flags.append(flags)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(journal.os, "open", recording_open)
    repository_type(_operations_root(tmp_path)).append(
        _attempt(), state_type.PREPARED
    )

    assert len(observed_flags) == 1
    assert observed_flags[0] & os.O_CREAT
    assert observed_flags[0] & os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        assert observed_flags[0] & os.O_NOFOLLOW


def test_append_fsyncs_complete_record_then_parent_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_type, state_type, _, _, _ = _types()
    journal = _journal_module()
    attempt = _attempt()
    operations_root = _operations_root(tmp_path)
    record_path = (
        _journal_dir(operations_root, attempt.report_id)
        / "00000001-prepared.json"
    )
    observed: list[str] = []
    real_fsync = journal.os.fsync

    def recording_fsync(fd: int) -> None:
        mode = os.fstat(fd).st_mode
        if stat.S_ISREG(mode):
            assert record_path.read_bytes() == _canonical(
                _record_mapping(1, "prepared", attempt)
            )
            observed.append("file")
        elif stat.S_ISDIR(mode):
            observed.append("directory")
        else:
            raise AssertionError("journal fsync target is neither file nor directory")
        real_fsync(fd)

    monkeypatch.setattr(journal.os, "fsync", recording_fsync)
    repository_type(operations_root).append(
        attempt,
        state_type.PREPARED,
    )

    assert observed == ["file", "directory"]


@pytest.mark.parametrize("failure_point", ("file", "directory"))
def test_fsync_failure_leaves_present_record_but_blocks_later_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    repository_type, state_type, _, _, _ = _types()
    journal = _journal_module()
    operations_root = _operations_root(tmp_path)
    repository = repository_type(operations_root)
    attempt = _attempt()
    real_fsync = journal.os.fsync
    calls = 0

    def failing_fsync(fd: int) -> None:
        nonlocal calls
        calls += 1
        if calls == (1 if failure_point == "file" else 2):
            raise OSError(f"injected {failure_point} fsync failure")
        real_fsync(fd)

    monkeypatch.setattr(journal.os, "fsync", failing_fsync)
    with pytest.raises(OSError, match=f"{failure_point} fsync failure"):
        repository.append(attempt, state_type.PREPARED)

    record = (
        _journal_dir(operations_root, attempt.report_id)
        / "00000001-prepared.json"
    )
    assert record.is_file()
    monkeypatch.setattr(journal.os, "fsync", real_fsync)
    with pytest.raises(Exception, match="durability|indeterminate|activation journal"):
        repository.append(attempt, state_type.EXCHANGE_STARTED)
    with pytest.raises(Exception, match="durability|indeterminate|activation journal"):
        repository.load(attempt)
    reloaded = repository_type(operations_root)
    loaded = reloaded.load(attempt)
    assert len(loaded) == 1
    assert loaded[0].state is state_type.PREPARED
    reloaded.append(attempt, state_type.EXCHANGE_STARTED)

    record.write_bytes(record.read_bytes() + b" ")
    with pytest.raises(Exception, match="canonical|record|activation journal"):
        repository_type(operations_root).load(attempt)
