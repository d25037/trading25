from __future__ import annotations

from pathlib import Path
import shutil

import duckdb
import pytest

import src.infrastructure.db.market.market_compaction as compaction_module
from src.infrastructure.db.market.market_compaction import (
    HARD_FREE_BYTES,
    SOFT_FREE_BYTES,
    SOFT_FREE_RATIO,
    CompactionTrigger,
    DuckDbSizeSnapshot,
    evaluate_compaction_trigger,
    MarketCompactionError,
    MarketCompactor,
    required_compaction_capacity,
)
from src.infrastructure.db.market.atomic_exchange import PlatformAtomicExchange
from src.infrastructure.db.market.managed_root import ManagedRootError, ManagedRootFd
from src.infrastructure.db.market.market_writer_resources import (
    MarketWriterResourceFactory,
)


def _snapshot(
    *, block_size: int, total_blocks: int, free_blocks: int
) -> DuckDbSizeSnapshot:
    return DuckDbSizeSnapshot(
        block_size=block_size,
        total_blocks=total_blocks,
        used_blocks=total_blocks - free_blocks,
        free_blocks=free_blocks,
        wal_bytes=0,
    )


@pytest.mark.parametrize(
    ("free_bytes", "ratio", "expected"),
    [
        (SOFT_FREE_BYTES - 1, 0.50, CompactionTrigger.NONE),
        (SOFT_FREE_BYTES, SOFT_FREE_RATIO - 0.001, CompactionTrigger.NONE),
        (SOFT_FREE_BYTES, SOFT_FREE_RATIO, CompactionTrigger.SOFT),
        (HARD_FREE_BYTES, 0.01, CompactionTrigger.HARD),
    ],
)
def test_compaction_policy_has_fixed_soft_and_hard_thresholds(
    free_bytes: int,
    ratio: float,
    expected: CompactionTrigger,
) -> None:
    block_size = 1024
    total_bytes = max(int(free_bytes / ratio) if ratio else free_bytes, free_bytes)
    total_blocks = max(1, total_bytes // block_size)
    free_blocks = free_bytes // block_size
    snapshot = _snapshot(
        block_size=block_size,
        total_blocks=total_blocks,
        free_blocks=free_blocks,
    )

    assert evaluate_compaction_trigger(snapshot) is expected


def test_free_ratio_uses_total_blocks_times_block_size() -> None:
    snapshot = _snapshot(block_size=1024, total_blocks=100, free_blocks=10)

    assert snapshot.free_ratio == pytest.approx(0.10)
    assert snapshot.free_bytes == 10 * 1024
    assert snapshot.total_bytes == 100 * 1024


@pytest.mark.parametrize(
    ("source_bytes", "expected_reserve"),
    [
        (100 * 1024 * 1024, 512 * 1024 * 1024),
        (10 * 1024 * 1024 * 1024, 1024 * 1024 * 1024),
        (10 * 1024 * 1024 * 1024 + 1, 1024 * 1024 * 1024 + 1),
    ],
)
def test_required_capacity_is_source_plus_larger_fixed_or_ten_percent_reserve(
    source_bytes: int,
    expected_reserve: int,
) -> None:
    assert required_compaction_capacity(source_bytes) == source_bytes + expected_reserve


def test_compaction_module_no_longer_exposes_unsafe_copy_or_in_place_entrypoints() -> (
    None
):
    from src.infrastructure.db.market import market_compaction

    assert not hasattr(market_compaction, "compact_market_duckdb")
    assert not hasattr(market_compaction, "compact_market_duckdb_in_place_if_needed")


def test_legacy_market_compact_cli_is_removed() -> None:
    cli_source = (
        Path(__file__).parents[4] / "src" / "entrypoints" / "cli" / "__init__.py"
    ).read_text(encoding="utf-8")

    assert 'name="market-compact"' not in cli_source
    assert 'name="market-maintain"' in cli_source


def _closed_v4_session(tmp_path: Path):
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    session = MarketWriterResourceFactory(
        data_root=data_root, market_root=market_root
    ).reset_and_open_v4()
    session.handles.market_db._execute(
        """INSERT INTO stocks(
        code, company_name, market_code, market_name, sector_17_code,
        sector_17_name, sector_33_code, sector_33_name, listed_date
        ) VALUES ('7203', 'Toyota', '0111', 'Prime', '1', 'Mfg', '1', 'Auto', '1949-05-16')"""
    )
    token = session.close_writable_handles()
    return session, token, session.authorize_maintenance(token)


def _hard_snapshot() -> DuckDbSizeSnapshot:
    return DuckDbSizeSnapshot(
        block_size=256 * 1024,
        total_blocks=5000,
        used_blocks=900,
        free_blocks=4100,
        wal_bytes=0,
    )


def _compact_snapshot() -> DuckDbSizeSnapshot:
    return DuckDbSizeSnapshot(
        block_size=256 * 1024,
        total_blocks=100,
        used_blocks=100,
        free_blocks=0,
        wal_bytes=0,
    )


def test_verified_compaction_exchanges_candidate_and_returns_structured_evidence(
    tmp_path: Path,
) -> None:
    session, token, authority = _closed_v4_session(tmp_path)
    source = authority.identity.path
    original_inode = source.stat().st_ino
    snapshots = iter((_hard_snapshot(), _compact_snapshot()))
    compactor = MarketCompactor(
        size_reader=lambda _path: next(snapshots),
        available_bytes=lambda _path: required_compaction_capacity(
            source.stat().st_size
        ),
    )

    evidence = compactor.maintain(authority)

    assert evidence.compacted is True
    assert evidence.trigger is CompactionTrigger.HARD
    assert evidence.validation == "passed"
    assert evidence.before.free_bytes >= HARD_FREE_BYTES
    assert evidence.after.free_bytes < HARD_FREE_BYTES
    assert evidence.table_counts
    assert evidence.schema_fingerprint
    assert evidence.semantic_digests
    assert source.stat().st_ino != original_inode
    assert not list(source.parent.glob(".market-maintenance-*"))
    assert not (source.parent / ".market-maintenance.v1.jsonl").exists()
    conn = duckdb.connect(str(source), read_only=True)
    try:
        assert conn.execute("SELECT code, company_name FROM stocks").fetchall() == [
            ("7203", "Toyota")
        ]
    finally:
        conn.close()
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


def test_compaction_rejects_insufficient_capacity_without_changing_source(
    tmp_path: Path,
) -> None:
    session, token, authority = _closed_v4_session(tmp_path)
    source = authority.identity.path
    original = (source.stat().st_ino, source.read_bytes())
    compactor = MarketCompactor(
        size_reader=lambda _path: _hard_snapshot(),
        available_bytes=lambda _path: (
            required_compaction_capacity(source.stat().st_size) - 1
        ),
    )

    with pytest.raises(MarketCompactionError, match="capacity"):
        compactor.maintain(authority)

    assert (source.stat().st_ino, source.read_bytes()) == original
    assert not list(source.parent.glob(".market-maintenance-*"))
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


def test_candidate_verification_failure_preserves_exact_original(
    tmp_path: Path,
) -> None:
    session, token, authority = _closed_v4_session(tmp_path)
    source = authority.identity.path
    original = (source.stat().st_ino, source.read_bytes())

    def corrupt_copy(
        source_path: Path, candidate_path: Path, _authority: object
    ) -> None:
        shutil.copyfile(source_path, candidate_path)
        conn = duckdb.connect(str(candidate_path))
        try:
            conn.execute("DELETE FROM stocks")
            conn.execute("CHECKPOINT")
        finally:
            conn.close()

    compactor = MarketCompactor(
        size_reader=lambda _path: _hard_snapshot(),
        available_bytes=lambda _path: required_compaction_capacity(
            source.stat().st_size
        ),
        copy_builder=corrupt_copy,
    )

    with pytest.raises(MarketCompactionError, match="verification"):
        compactor.maintain(authority)

    assert (source.stat().st_ino, source.read_bytes()) == original
    assert not list(source.parent.glob(".market-maintenance-*"))
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


def test_candidate_verifier_counts_all_schema_qualified_persistent_tables(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    session = MarketWriterResourceFactory(
        data_root=data_root, market_root=market_root
    ).reset_and_open_v4()
    session.handles.market_db._execute("CREATE SCHEMA extra")
    session.handles.market_db._execute("CREATE TABLE extra.audit(id INTEGER)")
    session.handles.market_db._execute("INSERT INTO extra.audit VALUES (1), (2)")
    token = session.close_writable_handles()
    authority = session.authorize_maintenance(token)
    source = authority.identity.path

    def lose_extra_schema_rows(
        source_path: Path, candidate: Path, authority_arg
    ) -> None:
        MarketCompactor._build_copy(source_path, candidate, authority_arg)
        conn = duckdb.connect(str(candidate))
        try:
            conn.execute("DELETE FROM extra.audit")
            conn.execute("CHECKPOINT")
        finally:
            conn.close()

    compactor = MarketCompactor(
        size_reader=lambda _path: _hard_snapshot(),
        available_bytes=lambda _path: required_compaction_capacity(
            source.stat().st_size
        ),
        copy_builder=lose_extra_schema_rows,
    )

    with pytest.raises(MarketCompactionError, match="verification"):
        compactor.maintain(authority)

    conn = duckdb.connect(str(source), read_only=True)
    try:
        assert conn.execute("SELECT * FROM extra.audit ORDER BY id").fetchall() == [
            (1,),
            (2,),
        ]
    finally:
        conn.close()
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


def test_candidate_verifier_fingerprints_persisted_macro_and_user_type(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    session = MarketWriterResourceFactory(
        data_root=data_root, market_root=market_root
    ).reset_and_open_v4()
    session.handles.market_db._execute("CREATE SCHEMA extra")
    session.handles.market_db._execute("CREATE TYPE extra.side AS ENUM ('buy', 'sell')")
    session.handles.market_db._execute("CREATE MACRO extra.bump(x) AS x + 1")
    token = session.close_writable_handles()
    authority = session.authorize_maintenance(token)
    source = authority.identity.path

    def lose_catalog_objects(source_path: Path, candidate: Path, authority_arg) -> None:
        MarketCompactor._build_copy(source_path, candidate, authority_arg)
        conn = duckdb.connect(str(candidate))
        try:
            conn.execute("DROP MACRO extra.bump")
            conn.execute("DROP TYPE extra.side")
            conn.execute("CHECKPOINT")
        finally:
            conn.close()

    compactor = MarketCompactor(
        size_reader=lambda _path: _hard_snapshot(),
        available_bytes=lambda _path: required_compaction_capacity(
            source.stat().st_size
        ),
        copy_builder=lose_catalog_objects,
    )

    with pytest.raises(MarketCompactionError, match="verification"):
        compactor.maintain(authority)

    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


def test_candidate_table_counts_are_injective_for_dotted_identifiers(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    session = MarketWriterResourceFactory(
        data_root=data_root, market_root=market_root
    ).reset_and_open_v4()
    session.handles.market_db._execute('CREATE SCHEMA "a"')
    session.handles.market_db._execute('CREATE SCHEMA "a.b"')
    session.handles.market_db._execute('CREATE TABLE "a"."b.c"(id INTEGER)')
    session.handles.market_db._execute('CREATE TABLE "a.b"."c"(id INTEGER)')
    session.handles.market_db._execute('INSERT INTO "a"."b.c" VALUES (1), (2)')
    session.handles.market_db._execute('INSERT INTO "a.b"."c" VALUES (3)')
    token = session.close_writable_handles()
    authority = session.authorize_maintenance(token)
    source = authority.identity.path
    baseline = compaction_module._validation_snapshot(source)
    assert baseline.table_counts['["a","b.c"]'] == 2
    assert baseline.table_counts['["a.b","c"]'] == 1

    def lose_colliding_table_rows(
        source_path: Path, candidate: Path, authority_arg
    ) -> None:
        MarketCompactor._build_copy(source_path, candidate, authority_arg)
        conn = duckdb.connect(str(candidate))
        try:
            conn.execute('DELETE FROM "a"."b.c"')
            conn.execute("CHECKPOINT")
        finally:
            conn.close()

    compactor = MarketCompactor(
        size_reader=lambda _path: _hard_snapshot(),
        available_bytes=lambda _path: required_compaction_capacity(
            source.stat().st_size
        ),
        copy_builder=lose_colliding_table_rows,
    )

    with pytest.raises(MarketCompactionError, match="verification"):
        compactor.maintain(authority)

    conn = duckdb.connect(str(source), read_only=True)
    try:
        assert conn.execute('SELECT * FROM "a"."b.c" ORDER BY id').fetchall() == [
            (1,),
            (2,),
        ]
        assert conn.execute('SELECT * FROM "a.b"."c"').fetchall() == [(3,)]
    finally:
        conn.close()
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


@pytest.mark.parametrize("preexisting_kind", ["directory", "symlink"])
def test_candidate_staging_is_create_only_and_rejects_preexisting_paths(
    tmp_path: Path, preexisting_kind: str
) -> None:
    session, token, authority = _closed_v4_session(tmp_path)
    staging = authority.market_root / ".market-maintenance-test"
    if preexisting_kind == "directory":
        staging.mkdir(mode=0o700)
    else:
        target = authority.market_root / "unrelated"
        target.mkdir()
        staging.symlink_to(target, target_is_directory=True)

    with pytest.raises((FileExistsError, OSError, ManagedRootError)):
        compaction_module._create_candidate_staging(authority, "test")

    if staging.is_symlink():
        staging.unlink()
    else:
        staging.rmdir()
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


def test_candidate_parent_replacement_race_preserves_source_and_fences_session(
    tmp_path: Path,
) -> None:
    session, _token, authority = _closed_v4_session(tmp_path)
    source = authority.identity.path
    original_inode = source.stat().st_ino
    snapshots = iter((_hard_snapshot(), _compact_snapshot()))

    def replace_parent(source_path: Path, candidate: Path, _authority: object) -> None:
        shutil.copyfile(source_path, candidate)
        detached = candidate.parent.with_name(f"{candidate.parent.name}-detached")
        candidate.parent.rename(detached)
        candidate.parent.mkdir(mode=0o700)
        shutil.copyfile(source_path, candidate)

    compactor = MarketCompactor(
        size_reader=lambda _path: next(snapshots),
        available_bytes=lambda _path: required_compaction_capacity(
            source.stat().st_size
        ),
        copy_builder=replace_parent,
    )

    with pytest.raises(MarketCompactionError, match="fenced"):
        compactor.maintain(authority)

    assert source.stat().st_ino == original_inode
    assert session.fenced
    for path in source.parent.glob(".market-maintenance-*"):
        shutil.rmtree(path)
    session.lease.release()
    if session._process_lock is not None:
        session._process_lock.release()
        session._process_lock = None


def test_exchange_failure_after_swap_rolls_back_exact_original(tmp_path: Path) -> None:
    session, token, authority = _closed_v4_session(tmp_path)
    source = authority.identity.path
    original = (source.stat().st_ino, source.read_bytes())

    class SwapThenFailOnce:
        def __init__(self) -> None:
            self.real = PlatformAtomicExchange()
            self.calls = 0

        def exchange_regular_files(
            self,
            root,
            left: Path,
            right: Path,
            *,
            expected_right_parent_identity=None,
        ) -> None:
            self.calls += 1
            self.real.exchange_regular_files(
                root,
                left,
                right,
                expected_right_parent_identity=expected_right_parent_identity,
            )
            if self.calls == 1:
                raise OSError("injected fsync failure")

    compactor = MarketCompactor(
        size_reader=lambda _path: _hard_snapshot(),
        available_bytes=lambda _path: required_compaction_capacity(
            source.stat().st_size
        ),
        exchange=SwapThenFailOnce(),
    )

    with pytest.raises(MarketCompactionError, match="exchange"):
        compactor.maintain(authority)

    assert (source.stat().st_ino, source.read_bytes()) == original
    assert not session.fenced
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


def test_post_commit_cleanup_failure_rolls_forward_but_is_not_suppressed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, token, authority = _closed_v4_session(tmp_path)
    source = authority.identity.path
    original_inode = source.stat().st_ino
    snapshots = iter((_hard_snapshot(), _compact_snapshot()))
    real_remove = compaction_module._remove_candidate_staging
    failed = False

    def remove_then_fail_once(authority_arg, path: Path) -> None:
        nonlocal failed
        real_remove(authority_arg, path)
        if not failed:
            failed = True
            raise OSError("injected cleanup fsync failure")

    monkeypatch.setattr(
        compaction_module, "_remove_candidate_staging", remove_then_fail_once
    )
    compactor = MarketCompactor(
        size_reader=lambda _path: next(snapshots),
        available_bytes=lambda _path: required_compaction_capacity(
            source.stat().st_size
        ),
    )

    with pytest.raises(MarketCompactionError, match="post-commit cleanup"):
        compactor.maintain(authority)

    assert source.stat().st_ino != original_inode
    assert not list(source.parent.glob(".market-maintenance-*"))
    assert not (source.parent / ".market-maintenance.v1.jsonl").exists()
    assert not session.fenced
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


def test_source_validation_reuses_exact_adjusted_metric_diagnostics(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    session = MarketWriterResourceFactory(
        data_root=data_root, market_root=market_root
    ).reset_and_open_v4()
    session.handles.market_db._execute(
        "INSERT INTO statements(code, disclosed_date, earnings_per_share, type_of_current_period) "
        "VALUES ('7203', '2026-01-10', 100.0, 'FY')"
    )
    session.handles.market_db._execute(
        """INSERT INTO stock_adjustment_bases(
        code, basis_id, valid_from, valid_to_exclusive, adjustment_through_date,
        source_fingerprint, materialized_through_date, status
        ) VALUES (
        '7203', 'event-pit-v1:7203:2026-01-01', '2026-01-01', NULL,
        '2026-01-31', 'fixture', '2026-01-31', 'ready'
        )"""
    )
    token = session.close_writable_handles()
    authority = session.authorize_maintenance(token)
    compactor = MarketCompactor(size_reader=lambda _path: _compact_snapshot())

    with pytest.raises(MarketCompactionError, match="PIT lineage"):
        compactor.maintain(authority)

    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


def test_source_validation_reuses_catalog_overlap_and_status_snapshot(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    session = MarketWriterResourceFactory(
        data_root=data_root, market_root=market_root
    ).reset_and_open_v4()
    session.handles.market_db._execute(
        """INSERT INTO stock_adjustment_bases(
        code, basis_id, valid_from, valid_to_exclusive, adjustment_through_date,
        source_fingerprint, materialized_through_date, status
        ) VALUES (
        '7203', 'event-pit-v1:7203:2026-01-01', '2026-01-01', NULL,
        '2026-01-31', 'fixture', '2026-01-31', 'building'
        )"""
    )
    token = session.close_writable_handles()
    authority = session.authorize_maintenance(token)

    with pytest.raises(MarketCompactionError, match="PIT lineage"):
        MarketCompactor(size_reader=lambda _path: _compact_snapshot()).maintain(
            authority
        )

    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


@pytest.mark.parametrize(
    ("last_state", "swapped", "cleaned", "keeps_original"),
    [
        ("VALIDATED", False, False, True),
        ("EXCHANGE_INTENT", False, False, True),
        ("EXCHANGE_INTENT", True, False, True),
        ("EXCHANGED", True, False, True),
        ("ACTIVE_VALIDATED", True, False, True),
        ("COMMITTED", True, False, False),
        ("CLEANED", True, True, False),
    ],
)
def test_recovery_classifies_every_durable_state_prefix(
    tmp_path: Path,
    last_state: str,
    swapped: bool,
    cleaned: bool,
    keeps_original: bool,
) -> None:
    session, token, authority = _closed_v4_session(tmp_path)
    source = authority.identity.path
    staging = compaction_module._create_candidate_staging(authority, "recovery")
    candidate = staging.candidate_path
    journal = source.parent / ".market-maintenance.v1.jsonl"
    original_inode = source.stat().st_ino
    shutil.copyfile(source, candidate)
    compact_inode = candidate.stat().st_ino
    validation = compaction_module._validation_snapshot(source)
    payload = {
        "source": MarketCompactor._identity_payload(source),
        "candidate": MarketCompactor._identity_payload(candidate),
        "trigger": "hard",
        "candidateRelative": staging.candidate_relative.as_posix(),
        "candidateParent": {
            "device": staging.parent_identity[0],
            "inode": staging.parent_identity[1],
        },
        "schemaFingerprint": validation.schema_fingerprint,
        "tableCounts": validation.table_counts,
        "semanticDigests": validation.semantic_digests,
    }
    for state in compaction_module._JOURNAL_STATES:
        compaction_module._append_journal(journal, state, payload)
        if state == last_state:
            break
    if swapped:
        with ManagedRootFd.open(authority.data_root) as root:
            PlatformAtomicExchange().exchange_regular_files(
                root,
                source.relative_to(authority.data_root),
                candidate.relative_to(authority.data_root),
                expected_right_parent_identity=staging.parent_identity,
            )
    if cleaned:
        compaction_module._remove_candidate_staging(authority, candidate)

    MarketCompactor().recover(authority)

    assert source.stat().st_ino == (original_inode if keeps_original else compact_inode)
    assert not candidate.exists()
    assert not journal.exists()
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


@pytest.mark.parametrize(
    "last_state",
    ["EXCHANGE_INTENT", "EXCHANGED", "ACTIVE_VALIDATED"],
)
def test_fresh_session_recovers_exchanged_precommit_state_and_can_retry(
    tmp_path: Path,
    last_state: str,
) -> None:
    session, token, authority = _closed_v4_session(tmp_path)
    factory = session.factory
    source = authority.identity.path
    original_inode = source.stat().st_ino
    staging = compaction_module._create_candidate_staging(authority, "fresh-recovery")
    candidate = staging.candidate_path
    journal = source.parent / ".market-maintenance.v1.jsonl"
    shutil.copyfile(source, candidate)
    compact_inode = candidate.stat().st_ino
    validation = compaction_module._validation_snapshot(source)
    payload = {
        "source": MarketCompactor._identity_payload(source),
        "candidate": MarketCompactor._identity_payload(candidate),
        "trigger": "hard_cap",
        "candidateRelative": staging.candidate_relative.as_posix(),
        "candidateParent": {
            "device": staging.parent_identity[0],
            "inode": staging.parent_identity[1],
        },
        "schemaFingerprint": validation.schema_fingerprint,
        "tableCounts": validation.table_counts,
        "semanticDigests": validation.semantic_digests,
    }
    for state in compaction_module._JOURNAL_STATES:
        compaction_module._append_journal(journal, state, payload)
        if state == last_state:
            break
    with ManagedRootFd.open(authority.data_root) as root:
        PlatformAtomicExchange().exchange_regular_files(
            root,
            source.relative_to(authority.data_root),
            candidate.relative_to(authority.data_root),
            expected_right_parent_identity=staging.parent_identity,
        )
    assert source.stat().st_ino == compact_inode
    old_read_only = session.reopen_read_only_and_release(token)
    old_read_only.close()

    fresh = factory.open_existing()
    fresh_token = fresh.close_writable_handles()
    fresh_authority = fresh.authorize_maintenance(fresh_token)
    assert fresh_authority.identity.inode == compact_inode

    MarketCompactor().recover(fresh_authority)

    assert source.stat().st_ino == original_inode
    assert fresh_authority.identity.inode == original_inode
    assert not fresh.fenced
    assert not journal.exists()
    assert not staging.candidate_path.exists()
    recovered_read_only = fresh.reopen_read_only_and_release(fresh_token)
    recovered_read_only.close()

    retry = factory.open_existing()
    retry_token = retry.close_writable_handles()
    retry_read_only = retry.reopen_read_only_and_release(retry_token)
    retry_read_only.close()


@pytest.mark.parametrize(
    "corruption", ["torn", "illegal_transition", "identity_mismatch"]
)
def test_recovery_rejects_invalid_journal_without_mutation(
    tmp_path: Path, corruption: str
) -> None:
    session, token, authority = _closed_v4_session(tmp_path)
    source = authority.identity.path
    staging = compaction_module._create_candidate_staging(authority, "invalid")
    candidate = staging.candidate_path
    journal = source.parent / ".market-maintenance.v1.jsonl"
    shutil.copyfile(source, candidate)
    before = (source.stat().st_ino, candidate.stat().st_ino)
    validation = compaction_module._validation_snapshot(source)
    payload = {
        "source": MarketCompactor._identity_payload(source),
        "candidate": MarketCompactor._identity_payload(candidate),
        "trigger": "hard",
        "candidateRelative": staging.candidate_relative.as_posix(),
        "candidateParent": {
            "device": staging.parent_identity[0],
            "inode": staging.parent_identity[1],
        },
        "schemaFingerprint": validation.schema_fingerprint,
        "tableCounts": validation.table_counts,
        "semanticDigests": validation.semantic_digests,
    }
    if corruption == "torn":
        journal.write_bytes(b'{"schemaVersion":1,"state":"VALIDATED"')
    elif corruption == "illegal_transition":
        compaction_module._append_journal(journal, "VALIDATED", payload)
        compaction_module._append_journal(journal, "EXCHANGED", payload)
    else:
        payload["candidate"] = {
            **payload["candidate"],
            "inode": candidate.stat().st_ino + 1,
        }
        compaction_module._append_journal(journal, "VALIDATED", payload)

    with pytest.raises(MarketCompactionError):
        MarketCompactor().recover(authority)

    assert (source.stat().st_ino, candidate.stat().st_ino) == before
    journal.unlink()
    compaction_module._remove_candidate_staging(authority, candidate)
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


@pytest.mark.parametrize(
    ("last_state", "swapped", "cleaned"),
    [
        ("VALIDATED", False, False),
        ("EXCHANGE_INTENT", False, False),
        ("EXCHANGE_INTENT", True, False),
        ("EXCHANGED", True, False),
        ("ACTIVE_VALIDATED", True, False),
        ("COMMITTED", True, False),
        ("CLEANED", True, True),
    ],
)
def test_recovery_fences_full_identity_drift_without_mutating_files(
    tmp_path: Path,
    last_state: str,
    swapped: bool,
    cleaned: bool,
) -> None:
    session, _token, authority = _closed_v4_session(tmp_path)
    source = authority.identity.path
    staging = compaction_module._create_candidate_staging(authority, "identity-drift")
    candidate = staging.candidate_path
    journal = source.parent / ".market-maintenance.v1.jsonl"
    shutil.copyfile(source, candidate)
    validation = compaction_module._validation_snapshot(source)
    payload = {
        "source": MarketCompactor._identity_payload(source),
        "candidate": MarketCompactor._identity_payload(candidate),
        "trigger": "hard_cap",
        "candidateRelative": staging.candidate_relative.as_posix(),
        "candidateParent": {
            "device": staging.parent_identity[0],
            "inode": staging.parent_identity[1],
        },
        "schemaFingerprint": validation.schema_fingerprint,
        "tableCounts": validation.table_counts,
        "semanticDigests": validation.semantic_digests,
    }
    payload["candidate"] = {
        **payload["candidate"],
        "device": payload["candidate"]["device"] + 1,
    }
    for state in compaction_module._JOURNAL_STATES:
        compaction_module._append_journal(journal, state, payload)
        if state == last_state:
            break
    if swapped:
        with ManagedRootFd.open(authority.data_root) as root:
            PlatformAtomicExchange().exchange_regular_files(
                root,
                source.relative_to(authority.data_root),
                candidate.relative_to(authority.data_root),
                expected_right_parent_identity=staging.parent_identity,
            )
    if cleaned:
        compaction_module._remove_candidate_staging(authority, candidate)
    before_source = (source.stat().st_dev, source.stat().st_ino, source.stat().st_size)
    before_candidate = (
        (candidate.stat().st_dev, candidate.stat().st_ino, candidate.stat().st_size)
        if candidate.exists()
        else None
    )
    before_journal = journal.read_bytes()

    try:
        with pytest.raises(MarketCompactionError, match="identity"):
            MarketCompactor().recover(authority)
        assert (
            source.stat().st_dev,
            source.stat().st_ino,
            source.stat().st_size,
        ) == before_source
        assert (
            (candidate.stat().st_dev, candidate.stat().st_ino, candidate.stat().st_size)
            if candidate.exists()
            else None
        ) == before_candidate
        assert journal.read_bytes() == before_journal
        assert session.fenced
    finally:
        journal.unlink(missing_ok=True)
        if candidate.parent.exists():
            shutil.rmtree(candidate.parent)
        session.lease.release()
        if session._process_lock is not None:
            session._process_lock.release()
            session._process_lock = None


def test_committed_recovery_requires_full_recorded_semantic_snapshot(
    tmp_path: Path,
) -> None:
    session, _token, authority = _closed_v4_session(tmp_path)
    source = authority.identity.path
    writer = duckdb.connect(str(source))
    try:
        writer.execute("INSERT INTO margin_data VALUES ('7203', '2026-01-05', 100, 50)")
        writer.execute("CHECKPOINT")
    finally:
        writer.close()
    baseline = compaction_module._validation_snapshot(source)
    staging = compaction_module._create_candidate_staging(authority, "semantic-drift")
    candidate = staging.candidate_path
    shutil.copyfile(source, candidate)
    with ManagedRootFd.open(authority.data_root) as root:
        PlatformAtomicExchange().exchange_regular_files(
            root,
            source.relative_to(authority.data_root),
            candidate.relative_to(authority.data_root),
            expected_right_parent_identity=staging.parent_identity,
        )
    tamper = duckdb.connect(str(source))
    try:
        tamper.execute("UPDATE margin_data SET long_margin_volume = 101")
        tamper.execute("CHECKPOINT")
    finally:
        tamper.close()
    journal = source.parent / ".market-maintenance.v1.jsonl"
    payload = {
        "source": MarketCompactor._identity_payload(candidate),
        "candidate": MarketCompactor._identity_payload(source),
        "trigger": "hard_cap",
        "candidateRelative": staging.candidate_relative.as_posix(),
        "candidateParent": {
            "device": staging.parent_identity[0],
            "inode": staging.parent_identity[1],
        },
        "schemaFingerprint": baseline.schema_fingerprint,
        "tableCounts": baseline.table_counts,
        "semanticDigests": baseline.semantic_digests,
    }
    for state in compaction_module._JOURNAL_STATES:
        compaction_module._append_journal(journal, state, payload)
        if state == "COMMITTED":
            break
    before = (source.read_bytes(), candidate.read_bytes(), journal.read_bytes())

    try:
        with pytest.raises(MarketCompactionError, match="validation identity"):
            MarketCompactor().recover(authority)
        assert (
            source.read_bytes(),
            candidate.read_bytes(),
            journal.read_bytes(),
        ) == before
        assert session.fenced
    finally:
        journal.unlink(missing_ok=True)
        if candidate.parent.exists():
            shutil.rmtree(candidate.parent)
        session.lease.release()
        if session._process_lock is not None:
            session._process_lock.release()
            session._process_lock = None
