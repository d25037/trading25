# Market v5 Hardening Task 8 Report

## Identity and scope

- Clean base: `d1d03f1270899ce95dbbd8cfc6a66a38f522a68c`.
- Worktree: repository-relative root `.` (Market v5 cutover worktree).
- Production scope is limited to full-rebuild activation, workspace exchange,
  backup/tree identity, report publication, and activation report contracts.
- Test scope is the activation/atomic-exchange/caught-failure files named by Task 8.
- Task 9 fresh-process or same-ID recovery is intentionally absent.

## Contract delivered

- [x] Activation quarantine is exactly
      `operations/market-v5-cutover/quarantine/pre-cutover-<REPORT_ID>`.
- [x] No random suffix is used for activation ownership.
- [x] After staged smoke and the final backup/active equality recheck, activation
      captures immutable source, staged, active-before, backup-payload, and
      expected-active directory/payload identities together with exact report,
      rehearsal, backup, code-version, config, and schema evidence.
- [x] The journal appends and fsyncs `prepared`, then `exchange_started`; the
      exchange test observes exactly those two records at the filesystem exchange.
- [x] After exchange and deterministic quarantine, active and quarantine directory
      and payload identities are verified, and the staged path must be consumed,
      before `activated` is appended.
- [x] Success publication embeds every journaled identity, publishes create-only,
      reads the report back, and compares canonical exact report evidence before
      `reported` is appended.
- [x] A pre-existing exact report is adopted idempotently. A mismatched report is
      neither accepted nor overwritten.
- [x] Existing caught-exception server/worker stop fencing and backup restore paths
      remain in force. A post-exchange identity mismatch restores the exact backup
      and leaves the journal before `activated`.
- [x] No P0-P2 finding remains. No P3-or-lower issue was discovered in Task 8 scope.

## Strict RED evidence

No production file was edited before this run; only the Task 8 activation regression
tests had changed.

```text
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_atomic_exchange.py \
  tests/unit/server/services/test_market_v4_cutover_cutover_activation.py -q

collected 24 items
4 failed, 20 passed, 1 warning
```

The four failures were the intended missing behaviors:

1. The exact deterministic quarantine path did not exist because the old path had a
   timestamp/token suffix.
2. No activation journal existed when the atomic exchange hook ran.
3. Drift injected into the displaced tree before quarantine was not detected.
4. An exact pre-existing success report was rejected instead of adopted.

The mismatched-report no-overwrite regression already failed closed on the base and
remained a preservation requirement during GREEN.

## GREEN and verification

Focused activation GREEN after the minimal implementation:

```text
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_cutover_activation.py -q

17 passed, 1 warning
```

Required final three suites:

```text
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_cutover_activation.py \
  tests/unit/server/services/test_market_v4_cutover_cutover_failure_recovery.py \
  tests/unit/server/services/test_market_v4_cutover_atomic_exchange.py -q

38 passed, 1 warning
```

Scoped lint:

```text
uv run --directory apps/bt ruff check \
  src/application/services/market_v4_cutover/activation.py \
  src/application/services/market_v4_cutover/activation_contract.py \
  src/application/services/market_v4_cutover/backup.py \
  src/application/services/market_v4_cutover/reports.py \
  src/application/services/market_v4_cutover/workspace.py \
  tests/unit/server/services/test_market_v4_cutover_cutover_activation.py

All checks passed!
```

Scoped type check:

```text
uv run --directory apps/bt pyright \
  src/application/services/market_v4_cutover/activation.py \
  src/application/services/market_v4_cutover/activation_contract.py \
  src/application/services/market_v4_cutover/backup.py \
  src/application/services/market_v4_cutover/reports.py \
  src/application/services/market_v4_cutover/workspace.py

0 errors, 0 warnings, 0 informations
```

`git diff --check` exited 0. Per the Task 8 brief, no repository-wide suite was run.

## State ordering and exact identity notes

The durable order is `prepared -> exchange_started -> activated -> reported`.
`prepared` contains the complete frozen attempt. `exchange_started` is the last
durable mutation before the exchange call. `activated` is unreachable until the
new active tree, consumed staging path, and deterministic old-active quarantine all
match the journaled attempt. The runtime template is moved into active only after
`activated`, and removed after active smoke; active and quarantine are reverified at
the report boundary. Report publication also receives this identity revalidation as
its final validator.

Tree payload identity is the canonical file-set/content digest plus staged schema
coverage where applicable; directory device/inode identity is retained and rechecked
around capture. Backup payload identity must match the final pre-exchange active-tree
digest recorded by the immutable backup manifest.

Existing exact report adoption is limited to the create-only destination-collision
case. Other safety validation failures are not converted into adoption. Both newly
published and adopted reports are read back and compared as canonical JSON before
the terminal journal append.

## Explicitly absent behavior

- No fresh-service journal recovery, same-ID recovery dispatch, crash-state decision,
  or interrupted-attempt completion is implemented; that is Task 9.
- No journal overwrite, state repair, state skip, random ownership inference, backup
  deletion, quarantine deletion, compatibility path, fallback, sync, refresh, or
  rebuild behavior was added.
- No API/OpenAPI, schema, CLI, frontend, or data-plane contract changed.

Intended commit subject: `refactor(bt): bind activation to journaled identities`.

## Review remediation: commit-aware activation and type-exact reports

- Review base: `6676348c98db387361e30e39a414558314bfc067`.
- Scope is restricted to the P1 activated-boundary rollback flaw and P2 report
  contract binding. The P3 warning remains deferred to GitHub issue #495 and was
  not changed.

### Strict RED

No production file was edited before the remediation regressions were run:

```text
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_cutover_activation.py \
  tests/unit/server/services/test_market_v4_cutover_cutover_failure_recovery.py -q

collected 50 items
17 failed, 33 passed, 1 warning
```

The P1 failures proved that activated append failures, including a record made
durable before the injected exception, and failures in report build, publish,
readback, and `reported` append all entered the old restore handler. The injected
restore guard therefore produced `Active cutover failed and explicit restore also
failed`. A runtime-template rename failure also left a durable `activated` record
before rollback. The pre-exchange `prepared`/`exchange_started` failure tests already
passed and proved active remained untouched.

The P2 failures proved Python mapping equality accepted nested `1` versus `1.0`, `0`
versus `false`, and `-0.0` versus `0.0`. It also accepted caller-only schema evidence
that differed from the frozen attempt and did not bind activation mode, active backup
digest, staged/active provider vintage, phase, or status.

### Remediation behavior

- An explicit commit disposition has two states: rollback remains allowed until all
  runtime-template work, active smoke, runtime cleanup, root/code checks, and exact
  active/quarantine identity checks complete; immediately before the `activated`
  append it switches to preserve-for-recovery.
- The preserve state is set before attempting `activated`, so both an absent record
  and an append that became durable before raising preserve active v5, deterministic
  old-active quarantine, immutable backup, journal, and any existing exact report.
- The commit-aware handler raises a dedicated preserved-commit safety exception and
  performs neither restore nor failure-report publication. Task 9 exact same-ID
  recovery remains the only continuation.
- Runtime-template failure occurs before the preserved boundary and therefore retains
  the existing caught rollback behavior with only `prepared` and `exchange_started`
  durable.
- The activation report contract canonicalizes JSON with `allow_nan=False` and compares
  exact bytes. Schema coverage is bound to the frozen `attempt.source.payload` evidence,
  and every ID, config, identity, backup digest, provider vintage, activation mode,
  phase, and status is checked type-sensitively.
- Candidate/existing/read-back report comparison remains canonical and exact; a passed
  report survives readback or terminal journal failure unchanged.

### GREEN and final verification

Focused remediation GREEN:

```text
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_cutover_activation.py \
  tests/unit/server/services/test_market_v4_cutover_cutover_failure_recovery.py -q

50 passed, 1 warning
```

Final required activation, contracts, caught-failure, atomic exchange, Task 7 journal,
and structure suites:

```text
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_cutover_activation.py \
  tests/unit/server/services/test_market_v4_cutover_cutover_contracts.py \
  tests/unit/server/services/test_market_v4_cutover_cutover_failure_recovery.py \
  tests/unit/server/services/test_market_v4_cutover_atomic_exchange.py \
  tests/unit/server/services/test_market_v4_cutover_activation_journal.py \
  tests/unit/server/services/test_market_v4_cutover_structure.py -q

136 passed, 1 warning
```

Scoped Ruff: `All checks passed!`.

Scoped production Pyright: `0 errors, 0 warnings, 0 informations`.

`git diff --check` exited 0. The bounded-structure suite also verifies
`activation.py` and every split cutover test module remain within their established
line/method limits. No repository-wide suite was run, no Task 9 recovery was added,
and no push is authorized.

Fix-only commit subject: `fix(bt): preserve committed activation for recovery`.
