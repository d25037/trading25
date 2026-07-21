# Market v5 Hardening Task 9 Report

## Identity and scope

- Clean base: `cf351627688abab33ec5ccc8fede3402c0c8ac92`.
- Worktree: `/Users/mirage/dev/trading25/.worktrees/market-v5-cutover`.
- Production changes are limited to exact same-ID activation recovery, secure
  report-ID-anchored journal discovery, and facade dispatch before new preparation.
- No API, OpenAPI, CLI, schema, frontend, sync, refresh, rebuild, compatibility,
  backup deletion, or quarantine deletion behavior changed.

The plan's four-file list did not include the journal discovery API required for a
fresh process or the existing Task 8 report-publication regression that still expected
automatic restore after durable `activated`. Parent approval allowed the minimal
`activation_journal.py` and focused journal tests already anticipated by the brief,
plus only that two-case existing regression update. The latter now proves the approved
Task 8 preserve boundary followed by Task 9 recovery; production behavior was not
changed to satisfy the obsolete assertion.

## Contract delivered

- [x] Same-ID recovery is dispatched under the exclusive operation lease before any
  new report directory, staging tree, journal, exchange, or other preparation.
- [x] Fresh discovery validates the exact report ID and opens the exact journal path
  descriptor-relatively with no-follow checks. It does not scan global journal names
  or infer ownership from filenames.
- [x] Exact caller report/rehearsal/backup IDs, config, clean code identity, current
  target fingerprint, staged configuration fingerprint, canonical frozen evidence,
  and journaled source/staged/active/backup identities are validated before mutation.
- [x] `prepared` resumes only with exact old-active/new-staged layout and first appends
  `exchange_started` durably.
- [x] `exchange_started` accepts only the three exact layouts: not exchanged,
  exchanged with old active at staged, or new active plus exact deterministic old
  active quarantine. Ambiguous or duplicate ownership fails closed without mutation.
- [x] `activated` requires exact active/quarantine/backup identities and no unjournaled
  runtime, runs a fresh active smoke, then publishes or adopts the exact report and
  appends `reported`.
- [x] `reported` revalidates exact filesystem and report contracts and returns the
  existing create-only report without API work or byte changes.
- [x] Final success retains v5 active, exact v4 quarantine, immutable backup, terminal
  `reported`, and no staged or duplicate/unowned market tree.
- [x] Task 8 caught-exception preserve-for-recovery and create-only report behavior
  remain intact.
- [x] No P0-P2 finding remains. No P3-or-lower issue was discovered in Task 9 scope.

## Strict RED evidence

Before production edits, the four required real-child crash boundaries all failed:

```text
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_activation_crash_recovery.py -q

4 failed
```

Each child called real `os._exit(75)` before exchange, after exchange, after durable
`activated` before report, or after report publish/readback before `reported`. A fresh
service using the same IDs/arguments could not resume: the pre-exchange case collided
with the existing report directory, while post-exchange cases rejected the changed
active identity.

The expanded RED matrix added crashes after `prepared` and deterministic quarantine,
REPORTED idempotency, mismatch/no-mutation cases, and report no-overwrite coverage:

```text
10 collected, 9 failed, 1 passed
```

Secure fresh discovery was separately RED with two `AttributeError` failures before
`load_existing(report_id)` existed. During the final fail-closed audit, a staged-config
tamper regression was also RED because recovery silently accepted the changed snapshot:

```text
1 failed: DID NOT RAISE CutoverSafetyError
```

## Recovery state and ownership behavior

Recovery first validates the complete canonical journal sequence loaded from the exact
anchored `00000001-prepared.json`. The deterministic old-active quarantine remains
`operations/market-v5-cutover/quarantine/pre-cutover-<REPORT_ID>`.

The three filesystem layouts are identified only by descriptor-derived directory
device/inode identity plus canonical payload identity. Recovery either performs the
original atomic exchange, completes quarantine after an already committed exchange,
or adopts the already quarantined exact layout. Any absent, extra, ambiguous, changed,
or mismatched identity is rejected. Backup and quarantine evidence are never removed.

An `activated` retry reconstructs only the isolated smoke runtime from the exact
staging configuration snapshot after rechecking it against the current target
configuration. It uses a unique recovery dataset operation ID, joins owned processes,
removes the runtime, rechecks root/code/active/quarantine identities, and only then
publishes/adopts success. An existing report is accepted only when its type-exact
activation contract, IDs, config, evidence, target fingerprint, backup manifest, and
tree identities match the journaled attempt.

## GREEN and verification

Final real-child recovery matrix:

```text
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_activation_crash_recovery.py -q

11 passed, 12 warnings
```

This includes six real `os._exit(75)` boundaries: after `prepared`, before exchange,
after exchange, after deterministic quarantine, after durable `activated`, and after
report publication/readback. It also covers terminal idempotency with no API calls,
caller/current-target/staged-config mismatches, ambiguous quarantine, and mismatched
published-report no-overwrite.

Required full cutover glob plus CLI:

```text
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover*.py \
  tests/unit/cli_bt/test_market_cutover_cli.py -q

265 passed, 12 warnings
```

The first full run exposed only the two obsolete Task 8 restore assertions
(`262 passed, 2 failed`). After the approved expectation update, both cases prove
preserved active/quarantine/backup/journal evidence, absence of a passed report, and
fresh exact same-ID completion to `reported`.

Final scoped lint and type checks:

```text
uv run --directory apps/bt ruff check \
  src/application/services/market_v4_cutover \
  tests/unit/server/services/test_market_v4_cutover_activation_crash_recovery.py \
  tests/unit/server/services/test_market_v4_cutover_activation_journal.py \
  tests/unit/server/services/test_market_v4_cutover_rehearsal_failures.py

All checks passed!

uv run --directory apps/bt pyright \
  src/application/services/market_v4_cutover

0 errors, 0 warnings, 0 informations
```

The package structure guard passes, including import-cycle, file-size, and method-size
bounds. `git diff --check` exits 0. Per the Task 9 brief, no repository-wide suite was
run.

Intended commit subject: `fix(bt): recover interrupted cutover by exact same id`.

## P1 remediation: recover an exact journal-owned residual runtime

- Review base: `ad8cbcf2b75dbfd4bfb863a6eafe974110397d8b`.
- Scope is limited to the P1 residual-runtime ownership and joined cleanup-failure
  paths. The general crash-boundary matrix remains deferred to GitHub issue #496.
- `activation_runtime.py` is an approved plan file-list omission. It isolates the
  runtime ownership manifest and active-core comparison from the 612-line state
  orchestrator, preserves package structure caps, and contains no journal, API, sync,
  rebuild, or Task 10 behavior.

### Strict RED

No production file changed before the focused regressions were run:

```text
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_market_v4_cutover_activation_crash_recovery.py \
  -k 'after_runtime_rename or joined_smoke or tampered_runtime' -q

3 selected, 3 failed
```

The real child exited with `os._exit(75)` immediately after the exact runtime-template
to active `.cutover-runtime-<REPORT_ID>` rename. Fresh recovery rejected the otherwise
legal quarantined layout as ambiguous because the residual runtime changed the active
tree digest. A joined recovery smoke failure followed by an injected runtime-removal
`OSError` exposed the original smoke error because cleanup failure was swallowed. A
tampered runtime reached the same generic layout rejection instead of a dedicated
ownership gate.

### Remediation behavior

- Before active identity may exclude a runtime or cleanup may remove it, recovery
  validates the report-derived managed path with descriptor-relative no-follow
  traversal and requires every regular file to have one link.
- The runtime has an exact allowed top-level entry/type set. Its default config digest
  equals the exact staging snapshot; its complete strategy directory/file map equals
  the staging map after applying the canonical repository smoke overlay. Recovery
  artifacts may exist only under the managed datasets/backtest/runtime subtrees.
- Only after this gate passes does recovery compare the active core while excluding
  that one runtime. Other active files and sibling paths remain part of the exact
  journaled active identity.
- A legal quarantined `exchange_started` or `activated` layout may therefore retain
  one exact owned runtime. Recovery validates and removes it, creates a fresh isolated
  runtime, and reruns active smoke before advancing the journal.
- Joined smoke cleanup failure raises `CutoverSafetyError` with the exact cleanup
  exception as its cause and leaves the runtime intact. A fresh same-ID retry validates
  that residual runtime, cleans/recreates it, and reaches `reported`. A tampered runtime
  is rejected without mutation or deletion.

### GREEN and final verification

```text
Focused P1 regressions: 3 passed, 11 deselected
Complete crash recovery file: 14 passed, 15 warnings
Package structure guard: 8 passed, 1 warning
Full cutover glob plus CLI: 268 passed, 15 warnings
Scoped Ruff: All checks passed!
Scoped Pyright: 0 errors, 0 warnings, 0 informations
```

No repository-wide suite was run. Intended fix commit subject:
`fix(bt): recover journal-owned cutover runtime`.
