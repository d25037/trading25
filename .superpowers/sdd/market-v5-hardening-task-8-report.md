# Market v5 Hardening Task 8 Report

## Identity and scope

- Clean base: `d1d03f1270899ce95dbbd8cfc6a66a38f522a68c`.
- Worktree: `/Users/mirage/dev/trading25/.worktrees/market-v5-cutover`.
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
