# Market v5 Hardening Task 7 Report

## Identity and scope

- Base/head before work: `fafa3fb451afbf608d07e890e37202920f33746c`.
- Preflight: exact HEAD, empty index, empty worktree, and no untracked files.
- Source/test scope is exactly the four Task 7 files:
  - `apps/bt/src/application/services/market_v4_cutover/activation_journal.py`
  - `apps/bt/src/application/services/market_v4_cutover/contracts.py`
  - `apps/bt/tests/unit/server/services/test_market_v4_cutover_activation_journal.py`
  - `apps/bt/tests/unit/server/services/test_market_v4_cutover_structure.py`
- This report is the only additional Task 7 artifact.
- No Task 8 activation binding or Task 9 recovery orchestration was added.

## Requirements checklist

- [x] Exact state order is `prepared -> exchange_started -> activated -> reported`.
- [x] Frozen dataclasses bind report/rehearsal/backup IDs, code version, smoke config,
      paths, and source/staged/active-before/backup/expected-active identities.
- [x] Records use exact eight-digit sequential canonical names and canonical JSON.
- [x] Record creation includes `O_CREAT | O_EXCL | O_NOFOLLOW` with the documented
      no-follow portability check.
- [x] Append writes the complete record, fsyncs the file, then fsyncs the containing
      directory before returning the state to its caller.
- [x] Load and append reject duplicate, skipped, regressed, terminal-follow-on,
      unknown, torn, noncanonical, mismatched, symlinked, hardlinked, and unsafe
      records/identities.
- [x] File-fsync and directory-fsync failures surface and make the same repository
      instance indeterminate, blocking any later load or append in that process.
- [x] A fresh repository accepts a physically present record only after exact canonical
      name/bytes/sequence/state/attempt/tree-identity validation, and rejects tampering.
- [x] Structure ownership includes `activation_journal.py` and continues to forbid
      the removed retained-promotion `journal*.py` module family.
- [x] No overwrite, gap healing, fallback parser, current/latest/global fallback,
      implicit recovery, compatibility path, or state skipping was introduced.

## RED evidence

1. Binding journal RED:

   `uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover_activation_journal.py -q`

   Collected/selected 37; 37 failed; no collection errors. Every failure reported the
   concrete missing module `src.application.services.market_v4_cutover.activation_journal`.

2. Structure RED:

   `uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover_structure.py -k focused_package -q`

   Collected 8; selected 1; deselected 7; failed 1 because
   `activation_journal.py` was absent from the required responsibility set.

3. Same-instance/new-instance semantics RED after the clarified binding contract:

   `uv run --directory apps/bt pytest tests/unit/server/services/test_market_v4_cutover_activation_journal.py -k fsync_failure -q`

   Collected 38; selected 2; deselected 36; failed 2 because the removed permission
   sentinel rejected a canonical physically present record in the fresh repository.
   Removing that extra metadata restored canonical revalidation semantics while the
   original repository remains indeterminate.

No production file was edited before the initial binding and structure RED runs.

## GREEN and debug evidence

- Initial journal GREEN: 37 collected, 37 passed.
- One intermediate run was 36 passed / 1 failed because the mismatch fixture used
  an invalid non-hex code version. The fixture was corrected to a different valid
  40-hex identity; production was not changed for that fixture failure.
- Clarified fsync/reload focused GREEN: 2 selected, 2 passed, 36 deselected.
- Final two-file Task 7 test run: 46 collected, 46 passed, 0 failed.
- Scoped Ruff: `All checks passed!`.
- Scoped Pyright production files: `0 errors, 0 warnings, 0 informations`.

## Security and durability invariants

- All journal traversal below the supplied operations root is descriptor-relative.
- Directory and record opens compare no-follow pre-stat identity with the opened
  descriptor; records must be regular, single-link files with canonical mode `0600`.
- IDs and relative paths are validated before directory creation.
- Every record repeats the full attempt and exact identities; load compares it to the
  caller's expected attempt and accepts no partial or additional fields.
- A file-fsync or parent-directory fsync exception is recorded as process-local
  indeterminate state before it is re-raised. That repository instance cannot load or
  append another state afterward, even if the final filename is physically visible.
- A fresh process has no knowledge of another process's fsync return value. It may load
  the present record only by revalidating the exact canonical filename, bytes, sequence,
  state, attempt IDs, config, paths, and every tree identity. This is conventional
  process-death recovery, not a power-loss outcome oracle.

## Explicitly absent behavior and residual risk

- There is no call from activation, cutover, report publication, rollback, CLI, or
  service code to these primitives. Tasks 8 and 9 must provide that wiring and exact
  same-ID recovery behavior.
- This task does not perform activation, exchange, rollback, report adoption, journal
  gap repair, or operator recovery.
- An fsync error has an inherently indeterminate persistence outcome across process or
  power loss. Per the binding Task 7 contract, only the process that observed the error
  is permanently blocked; a fresh process revalidates physical canonical evidence.
- Full-repository tests were intentionally not run per the binding Task 7 brief.
- The only expected test-run warning is the existing pytest environment warning; the
  Task 7 assertions have no failures.

## Commit

Proposed subject: `feat(bt): add durable v5 activation journal`

## Review remediation: recursive identity immutability

- Review base: `72e64211121eeda79d0fa2830868face8599286d`.
- Added strict regressions proving that construction copies caller-owned directory and
  nested payload structures, returned record/attempt evidence rejects mutation at every
  mapping/sequence depth, and caller drift injected during canonical encoding cannot
  change returned evidence or disk bytes.
- RED command selected 3 of 41 journal tests; all 3 failed on the mutable implementation:
  stored inode drifted from `107` to `999`, returned evidence allowed assignment, and
  encoding-time source symbol drifted from `7203` to `6758`.
- `MarketTreeIdentity` now recursively copies mappings into read-only mapping proxies
  and JSON arrays into tuples. Journal serialization converts that immutable value tree
  to ordinary JSON objects/arrays only at the canonical encoding boundary; load freezes
  parsed evidence again, preserving equality and round-trip bytes.
- Focused remediation GREEN: 3 selected, 3 passed, 38 deselected.
- Final journal plus structure verification: 49 collected, 49 passed, 0 failed.
- The P3 journal file-mode observation is intentionally deferred to GitHub issue #494;
  this remediation changes no file-mode behavior or test expectation.
- Fix-only commit subject: `fix(bt): freeze activation journal identities`.

## Second review remediation: type-exact nested identity matching

- Review base: `2b1a48634ea0fa6713375563480bfc95f80dfa59`.
- Added fresh-load regressions for stored/expected nested identity scalars `1` versus
  `1.0`, `0` versus `false`, and `-0.0` versus `0.0`. Each test first proves the
  canonical stored and expected record bytes differ, then requires attempt mismatch.
- RED command collected 45 journal tests, selected 3, deselected 42, and failed all 3
  because Python mapping/dataclass equality coerced the distinct scalar values equal.
- Load now compares exact canonical attempt-mapping bytes using the existing journal
  encoder with `allow_nan=False`, rather than Python's type-coercing nested equality.
- An explicit JSON array regression preserves the intentional input-list to immutable
  tuple representation and fresh-load equality because both encode to the same JSON
  array.
- Focused GREEN: 4 selected, 4 passed, 41 deselected (three scalar mismatch cases plus
  the array round-trip case).
- Final journal plus structure verification: 53 collected, 53 passed, 0 failed.
- Scoped Ruff passed; scoped production Pyright reported zero errors and warnings.
- P3 journal file mode remains unchanged and deferred to GitHub issue #494.
- Fix-only commit subject: `fix(bt): compare journal identities canonically`.
