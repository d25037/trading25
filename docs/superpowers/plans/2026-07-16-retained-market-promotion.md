# Retained Market Atomic Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL:
> `superpowers:subagent-driven-development`. Implement one task at a time with a
> fresh implementer, then separate specification and quality reviewers.

**Goal:** Promote the exact r10 Market v4 payload proven by retained report r13
to the active XDG path without sync or J-Quants, with an internally created v3
backup, atomic active-path exchange, durable crash recovery, full current-code
active smoke, and one-shot provenance evidence.

**Architecture:** Keep orchestration in the existing service until operational
cutover evidence is complete, but isolate atomic-syscall and journal behavior
behind injectable typed helpers. Acquire active then retained exclusive leases,
complete all read-only eligibility checks, create/verify backup, detach proven
runtimes, atomically exchange directories, quarantine v3, smoke active v4, and
commit the report. Rollback uses atomic exchange first and backup only as the
last fallback.

**Tech stack:** Python 3.12, Darwin `renameatx_np`, descriptor-relative managed
filesystem helpers, `flock`, Typer, owned FastAPI subprocess, pytest, Ruff,
Pyright.

## Fixed contracts

```python
class AtomicExchange(Protocol):
    def exchange(
        self,
        *,
        source_parent_fd: int,
        source_name: str,
        target_parent_fd: int,
        target_name: str,
    ) -> None: ...

class PromotionState(StrEnum):
    VALIDATED = "validated"
    RUNTIMES_DETACHED = "runtimes_detached"
    PREPARED = "prepared"
    EXCHANGED = "exchanged"
    QUARANTINED = "quarantined"
    ACTIVE_SMOKE_PASSED = "active_smoke_passed"
    REPORT_PERSISTED = "report_persisted"
    COMMITTED = "committed"
    EXCHANGED_BACK = "exchanged_back"
    ROLLED_BACK = "rolled_back"
    ROLLBACK_DEFERRED = "rollback_deferred_with_lease_held"

@dataclass(frozen=True)
class PromotionJournalRecord:
    sequence: int
    state: PromotionState
    operation_id: str
    identities: PromotionIdentityEvidence
    created_at: str

@dataclass(frozen=True)
class PromotionIdentityEvidence:
    active_before_directory: dict[str, int]
    active_before_payload: dict[str, object]
    retained_v4_directory: dict[str, int]
    retained_v4_payload: dict[str, object]
    backup_manifest_sha256: str
    backup_file_set_sha256: str
    active_current: dict[str, object] | None
    retained_current: dict[str, object] | None
    quarantine_current: dict[str, object] | None
    holding_current: dict[str, object] | None
    detached_runtime_names: tuple[str, ...]

class PromotionAppendStatus(StrEnum):
    COMMITTED = "committed"
    NOT_COMMITTED = "not_committed"
    INDETERMINATE = "indeterminate"

@dataclass(frozen=True)
class PromotionAppendResult:
    status: PromotionAppendStatus
    record: PromotionJournalRecord | None
    attempt_id: str

class PromotionJournal:
    def append(
        self,
        state: PromotionState,
        *,
        identities: PromotionIdentityEvidence,
    ) -> PromotionAppendResult: ...

    def read_validated(self) -> tuple[PromotionJournalRecord, ...]: ...

def MarketV4CutoverService.promote_retained(
    self,
    report_id: str,
    *,
    retained_report_id: str,
    backup_id: str,
    config: SmokeConfig,
    inherited_environment: dict[str, str] | None = None,
) -> OperationResult: ...
```

`inherited_environment` defaults to an empty mapping and is allowlisted. The
promotion runtime API is extended to inherit both active and retained lease
FDs; J-Quants key/token/plan variables are always removed.

Journal construction is pure and does not touch the filesystem. Every durable
record serializes exactly the fields in `PromotionIdentityEvidence`; absent
locations are explicit `null`, never omitted. `read_validated()` enforces the
exact state-specific null/non-null locations and nested types. For example,
`PREPARED` requires active/retained/holding and no quarantine,
`QUARANTINED` requires active/quarantine/holding and no retained Market, and
`COMMITTED` requires active/quarantine with no holding. Unknown or extra keys
fail closed rather than forming a compatibility contract.

Append publication uses a separate append-only control directory, not the
numbered-record allowlist directory. Before a candidate is published, a durable
intent binds operation/attempt ID, target sequence/name, canonical payload SHA,
previous-record SHA, expected state, and identities. Append/read/recovery use a
cross-process descriptor-confined lock. Exact outcomes are:

- `committed`: candidate and journal parent fsync succeeded and a durable
  accepted resolution exists;
- `not_committed`: publication provably did not occur, or removal plus parent
  fsync succeeded;
- `indeterminate`: any post-publication durability/cleanup boundary cannot be
  proven. Ordinary reading rejects, both leases remain fenced, and only
  dedicated same-ID recovery may resolve it.

Intent/resolution records are canonical, create-only, sequenced, hash-chained,
and exact-schema validated. Recovery adopts only an exact candidate after
candidate and parent fsync plus a durable accepted resolution. Missing is
`not_committed` only after directory fsync. Suspicious candidates remain
fail-stop and their numbered paths are never reused.

---

### Task 1: Atomic exchange primitive

**Files:**

- Modify: `apps/bt/src/application/services/market_v4_cutover.py`
- Test: `apps/bt/tests/unit/server/services/test_market_v4_cutover.py`

**Produces:** `AtomicExchange`, `DarwinAtomicExchange`, and an injected
`MarketV4CutoverService.atomic_exchange` dependency.

**Consumes:** `ManagedRootFd.open_parent`, retained parent descriptors,
`_DIR_OPEN_FLAGS`, and existing identity guards.

- [ ] Add these RED tests:

  - `test_atomic_exchange_swaps_real_directories_without_changing_inodes`
  - `test_atomic_exchange_rejects_cross_device_before_syscall`
  - `test_atomic_exchange_rejects_unavailable_platform_without_fallback`
  - `test_atomic_exchange_rejects_symlink_leaf_and_parent_replacement`
  - `test_atomic_exchange_fsyncs_both_parents_after_swap`

  The first test creates two real directories and asserts each file/directory
  inode appears at the opposite pathname. Failure tests assert neither pathname
  changed and no copy/no-replace hook ran.

- [ ] Run RED:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover.py \
  -k atomic_exchange -q
```

Expected: collection or assertion failure because the adapter is absent.

- [ ] Implement `DarwinAtomicExchange.exchange` with `ctypes` binding to
  `renameatx_np`, `RENAME_SWAP=0x2`, descriptor-relative names, pre-syscall
  `fstat`/`stat(..., follow_symlinks=False)` identity and same-device checks,
  post-syscall parent identity checks, and both parent fsyncs. Translate
  unsupported/`EXDEV` to `CutoverSafetyError`; never fall back to copy or two
  renames.

- [ ] Run GREEN plus static checks:

```bash
uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover.py -k atomic_exchange -q
uv run --directory apps/bt ruff check src/application/services/market_v4_cutover.py tests/unit/server/services/test_market_v4_cutover.py
uv run --directory apps/bt pyright src/application/services/market_v4_cutover.py
```

Expected: all pass.

- [ ] Commit only Task 1 files:

```bash
git add apps/bt/src/application/services/market_v4_cutover.py \
  apps/bt/tests/unit/server/services/test_market_v4_cutover.py
git commit -m "feat(bt): add atomic Market exchange"
```

---

### Task 2: Append-only promotion journal

**Files:** same service/test files.

**Produces:** `PromotionState`, `PromotionJournalRecord`, `PromotionJournal`,
strict transition table, and create-only record paths under
`operations/market-v4-cutover/journals/<operation-id>/`.

**Consumes:** managed descriptor file creation, `_write_all`, exact ID
validation, `now`, and JSON canonicalization.

- [ ] Add RED tests:

  - `test_promotion_journal_appends_create_only_fsynced_records`
  - `test_promotion_journal_rejects_skipped_duplicate_or_regressed_state`
  - `test_promotion_journal_rejects_torn_or_unknown_record`
  - `test_promotion_journal_rejects_operation_and_identity_mismatch`
  - `test_promotion_journal_reload_reconstructs_exact_state`
  - `test_promotion_journal_requires_exact_state_identity_schema`
  - `test_promotion_journal_never_publishes_to_ordinary_reader_during_append`
  - `test_promotion_journal_returns_indeterminate_when_cleanup_is_unprovable`
  - `test_promotion_journal_recovery_adopts_only_exact_durable_candidate`
  - `test_promotion_journal_recovery_keeps_mismatch_fail_stopped`
  - `test_promotion_journal_serializes_append_read_and_recovery_cross_process`

  Inject every ancestor/intent/stage/publication/parent-fsync/cleanup/resolution
  boundary. Concurrent ordinary reads block or reject until accepted resolution.
  File fsync before publication is `not_committed`; publication plus unprovable
  cleanup is `indeterminate`, never success. Reject booleans for every integer.

- [ ] Run RED with `-k promotion_journal`; expected failure due missing types.
- [ ] Implement numbered immutable JSON records with schema version, operation
  ID, sequence, state, timestamp, exact identity map, and previous-record SHA.
  `read_validated()` checks file allowlist, contiguous sequence, hash chain,
  transition table, canonical JSON, and path confinement.
- [ ] Durably fsync every newly created ancestor. Implement the separate
  intent/resolution control ledger, cross-process lock, three append outcomes,
  and dedicated exact-candidate recovery. An indeterminate result must be
  propagated so later orchestration retains/inherits both leases and stops.
- [ ] Run GREEN, Ruff, and Pyright using Task 1 command shapes.
- [ ] Commit:

```bash
git add apps/bt/src/application/services/market_v4_cutover.py \
  apps/bt/tests/unit/server/services/test_market_v4_cutover.py
git commit -m "feat(bt): journal Market promotion states"
```

---

### Task 3: Read-only promotion eligibility

**Files:** same service/test files.

**Produces:**

```python
@dataclass(frozen=True)
class RetainedPromotionEligibility:
    retained_report_id: str
    retained_report_sha256: str
    source_report_id: str
    source_report_sha256: str
    retained_root: Path
    source_market_identity: dict[str, object]
    active_market_identity: dict[str, object]
    target_root_fingerprint: str
    configuration_fingerprint: str

def _validate_retained_promotion_eligibility_under_leases(
    self,
    *,
    report_id: str,
    retained_report_id: str,
    backup_id: str,
    config: SmokeConfig,
    code_version: str,
    retained_lease: MarketOperationLease,
) -> RetainedPromotionEligibility: ...
```

**Consumes:** exact retained report validator, source-report resolver, payload
identity hashing, DuckDB inspector, root/config fingerprinting, path/descriptor
guards, and active/retained exclusive leases acquired in fixed order.

- [ ] Add a parameterized RED test
  `test_promote_retained_rejects_ineligible_source_before_any_mutation` covering:
  report/source SHA/provenance/config/root/code drift; v3/wrong mode/inexact
  lineage; DB or any Parquet identity drift; source/ancestor/leaf replacement;
  live retained lease; non-empty WAL; unexpected artifact; cross-device;
  unavailable exchange; existing report/journal/holding/quarantine/backup ID;
  and mismatched smoke config.

  Assert backup copy, journal create, holding create, runtime start, rename,
  exchange, and active mutation hooks were all untouched.

- [ ] Add RED tests for fixed active-then-retained lock ordering and lease
  retention through the returned operation scope.
- [ ] Run RED with `-k 'promote_retained and (ineligible or lock_order)'`.
- [ ] Implement the eligibility object and validator. Re-read/hash both reports
  under retained lease, bind r13 to r10, re-hash DB plus exact Parquet set,
  inspect schema/lineage, require canonical allowable runtime names from report
  provenance, and complete all syscall/device/destination checks without
  creating a file or directory.
- [ ] Run GREEN, Ruff, Pyright; expected all pass.
- [ ] Commit:

```bash
git add apps/bt/src/application/services/market_v4_cutover.py \
  apps/bt/tests/unit/server/services/test_market_v4_cutover.py
git commit -m "feat(bt): validate retained promotion source"
```

---

### Task 4: In-operation backup and proven runtime detachment

**Files:** same service/test files.

**Produces:**

```python
@dataclass(frozen=True)
class RetainedPromotionPreparation:
    eligibility: RetainedPromotionEligibility
    backup_manifest_sha256: str
    holding_root: Path
    detached_runtime_names: tuple[str, ...]

def _prepare_retained_promotion_under_leases(
    self,
    eligibility: RetainedPromotionEligibility,
    *,
    backup_id: str,
    journal: PromotionJournal,
) -> RetainedPromotionPreparation: ...
```

**Consumes:** existing snapshot-copy/manifest verifier internals but not
`_preflight_under_lease`; managed no-replace rename and payload identity guard.

- [ ] Add RED tests:

  - `test_promotion_creates_and_verifies_backup_inside_active_lease`
  - `test_promotion_backup_requires_payload_bytes_plus_reserve_not_rebuild_space`
  - `test_promotion_rejects_backup_identity_mismatch_before_detach`
  - `test_promotion_detaches_only_report_proven_runtimes`
  - `test_promotion_rejects_prefix_matched_unproven_runtime`
  - `test_promotion_requires_canonical_payload_after_detach`

- [ ] Run RED with `-k 'promotion and (backup or detach)'`.
- [ ] Create the backup under the held active lease with create-only paths;
  fsync payload/manifest, verify all files, re-check active identity. Then create
  the journal and append `validated` with the verified backup identity. Create
  the holding path, no-replace move exact r10/r12/r13 runtimes and empty temp
  artifacts, validate final `market.duckdb`/`parquet` allowlist, and re-hash
  payload. Append `runtimes_detached` then `prepared` only after durability.

  The immutable order is: complete read-only eligibility -> backup create/fsync/
  verify and active recheck (first mutation) -> create-only journal directory ->
  `VALIDATED` -> holding creation/detach -> `RUNTIMES_DETACHED` -> `PREPARED`.
- [ ] Run GREEN, Ruff, Pyright; commit as
  `feat(bt): prepare retained Market promotion`.

---

### Task 5: Atomic happy path and active smoke

**Files:** same service/test files.

**Produces:** `_promote_retained_under_leases(...) -> OperationResult`, dedicated
promotion report builder/validator, and runtime-start support for both active
and retained lease FDs.

**Consumes:** Tasks 1-4, existing full `smoke()`, owned runtime, quarantine and
report writers.

- [ ] Add RED test
  `test_promote_retained_atomically_activates_exact_payload_without_sync`.
  Assert exact active inode/hash equals r13, v3 identity is in quarantine,
  `_run_rebuild` is not called, forbidden mutation API paths are absent, all
  `JQUANTS_*`/key/token/plan variables are absent, logs lack `jquants_fetch`,
  workers join, active payload is unchanged, and canonical allowlist holds.
- [ ] Add RED report-contract tests for activation method, current/r13/r10 report
  IDs/code/SHA, all identities, same-device evidence, journal/quarantine/backup,
  runtime cleanup, `noSync`, `noJQuants`, join verdicts, source consumption, and
  final report validator.
- [ ] Run RED with `-k 'promote_retained and (atomically or report_contract)'`.
- [ ] Implement exchange -> identity validation -> v3 quarantine -> fresh active
  runtime -> full smoke -> joined shutdown -> runtime removal -> active identity
  validation -> provenance revalidation and held-runtime deletion -> canonical
  allowlist -> report fsync -> consumed marker -> committed journal.
- [ ] Extend runtime start/inheritance so the owned child receives both lease
  FDs. Do not pass ambient environment; construct an allowlist and explicitly
  reject credential keys.
- [ ] Run GREEN, the whole service test file, Ruff, and Pyright.
- [ ] Commit as `feat(bt): promote retained Market atomically`.

---

### Task 6: Rollback and restart recovery

**Files:** same service/test files.

**Produces:**

```python
def _rollback_retained_promotion(
    self,
    context: RetainedPromotionContext,
    *,
    processes_joined: bool,
) -> None: ...

def _recover_retained_promotion(
    self,
    report_id: str,
    *,
    retained_report_id: str,
    backup_id: str,
) -> OperationResult | None: ...
```

**Consumes:** journal, exact active/source/quarantine identities, atomic
exchange, verified backup restore, and dual lease inheritance.

- [ ] Parameterize RED test
  `test_promotion_failure_at_durable_boundary_restores_exact_v3` across every
  journal/exchange/quarantine/smoke/cleanup/report fsync boundary.
- [ ] Add RED tests for atomic exchange-back, backup-only fallback, fallback
  failure, unjoined child deferred state/blocked competing lease, detached
  runtime restoration, ambiguous identity fail-closed, and source one-shot.
- [ ] Add restart tests: matching incomplete journal recovers; valid committed
  report returns/rejects replay without mutation; mismatched operation/report
  rejects; success report missing after exchange rolls back.
- [ ] Add
  `test_recovery_detects_swap_after_prepared_before_exchanged_record`: inject a
  crash after the exchange syscall and parent fsync but before the `EXCHANGED`
  append. Recovery must inspect the exact active/retained directory and payload
  identities, recognize the swapped `PREPARED` layout, and exchange back. The
  journal state alone is never trusted as proof that its next filesystem action
  did not already occur.
- [ ] Run RED with `-k 'promotion and (rollback or recovery or durable_boundary)'`.
- [ ] Implement rollback/recovery. Never exchange/restore with an unjoined child.
  For joined failures, exchange active with current v3 location, verify exact v3,
  re-home v4 to retained source, and restore held runtime evidence if required.
  Backup restore is last fallback only.
- [ ] Run GREEN, full service suite, Ruff, Pyright; commit as
  `fix(bt): recover interrupted Market promotion`.

---

### Task 7: CLI, runbook, full focused gates

**Files:**

- Modify: `apps/bt/src/entrypoints/cli/market_cutover.py`
- Modify: `apps/bt/tests/unit/cli_bt/test_market_cutover_cli.py`
- Modify: `docs/runbooks/market-v4-cutover.md`
- Modify: `AGENTS.md`

**Produces:** canonical `bt market-cutover promote-retained` command.

**Consumes:** public service signature from Fixed contracts.

- [ ] Add RED CLI tests for help, exact option mapping, empty inherited
  environment, no rebuild credential lookup, and no legacy/source-path/force
  options.
- [ ] Implement the command and update runbook/AGENTS with exact evidence,
  recovery, backup/quarantine retention, and no-sync rule.
- [ ] Run:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover.py \
  tests/unit/cli_bt/test_market_cutover_cli.py -q
uv run --directory apps/bt ruff check src tests/unit/server/services/test_market_v4_cutover.py tests/unit/cli_bt/test_market_cutover_cli.py
uv run --directory apps/bt pyright src
```

Expected: all pass.

- [ ] Commit as `docs(bt): document retained Market promotion` including the CLI
  and tests.

---

### Task 8: Operational promotion and independent acceptance

**Files:** external XDG operation artifacts only; no source edit.

**Produces:** fresh immutable backup, active promotion report, quarantined v3,
and current-code active smoke evidence.

- [ ] Confirm `git status --short` has only the known user-owned
  `.codex/config.toml`, HEAD is clean/immutable, focused gates are current, and
  active/r10/r13 identities match expected preconditions.
- [ ] Select create-only IDs (planned:
  `market-v3-pre-v4-20260716` and `market-v4-active-20260716`; increment rather
  than overwrite if either exists).
- [ ] Run exactly one `promote-retained` command against
  `market-v4-retained-20260715-r13`. Do not run backup separately and do not run
  sync.
- [ ] Independently inspect report and logs; query active stats/validate,
  Fundamentals GET/POST, Screening job/result, Ranking, Dataset create/job/info/
  sample. Verify active DB/Parquet identity equals r13 before/after and logs have
  no mutation route/J-Quants fetch.
- [ ] Verify backup and quarantine identities and that only operation-owned
  runtimes were removed. Keep both rollback artifacts.
- [ ] Update the roadmap task state and commit only tracked evidence/runbook
  changes if any; never commit local absolute paths or credentials.

## Review and handoff

After every code task, the main agent writes a task report, obtains separate
spec and quality reviews, applies findings through the same implementer, reruns
verification, and only then accepts the commit. Task 8 is executed by the main
agent and independently audited by a read-only subagent.
