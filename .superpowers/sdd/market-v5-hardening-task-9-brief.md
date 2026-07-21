# Market v5 Hardening Task 9: recover interrupted activation by exact same ID

Implement exactly Task 9 in `docs/superpowers/plans/2026-07-21-market-v5-review-hardening.md` after independently approved Task 8 head `cf351627688abab33ec5ccc8fede3402c0c8ac92`.

## Scope

- create `apps/bt/src/application/services/market_v4_cutover/activation_recovery.py`
- create `apps/bt/tests/unit/server/services/test_market_v4_cutover_activation_crash_recovery.py`
- modify `apps/bt/src/application/services/market_v4_cutover/activation.py`
- modify `apps/bt/src/application/services/market_v4_cutover/service.py`
- minimally modify `apps/bt/src/application/services/market_v4_cutover/activation_journal.py` and its focused tests when required to securely discover and load the stored attempt by an exact validated report ID in a fresh process
- only unavoidable existing cutover test helpers needed to run true child-process crash cases
- `.superpowers/sdd/market-v5-hardening-task-9-report.md`

### Approved plan file-list omissions

The Task 9 plan's four-file list omits two changes required by its own fresh-process
contract: an anchored descriptor-relative `load_existing(report_id)` entry point in
`activation_journal.py` with focused journal tests, and the two existing Task 8 report
publication failure cases whose obsolete restore expectation contradicts Task 8's
approved preserve-for-recovery boundary. The task brief and parent approval therefore
allow those minimal focused changes; they do not broaden production recovery scope.

## Contract

- Recovery runs at same-ID entry before any new preparation or mutation and requires exact report/rehearsal/backup IDs, config, code version, target/source/staged/active/backup/quarantine identities and canonical evidence.
- Fresh discovery must use an anchored validated report ID and descriptor-relative/no-follow canonical loading; it must not scan filenames or infer ownership from path names.
- Fresh child-process crash tests use `os._exit(75)` before exchange, after exchange, after durable `activated` before report, and after report publication before `reported`.
- `reported`: validate exact report and filesystem identities, then return it.
- `activated`: validate exact active/quarantine/backup identities, run active smoke, publish/adopt exact report, append `reported`.
- `exchange_started`: recognize only the three explicitly legal exact-identity layouts and resume deterministically.
- `prepared`: proceed only while source/staged identities remain exact.
- Mismatched attempt arguments, target fingerprint, report, or ambiguous/duplicate ownership must never be adopted. Restore immutable backup only when ownership is exactly provable; otherwise fail closed without mutation.
- Final success keeps expected v5 active, exact source v4 quarantine, immutable backup, latest state `reported`, and no duplicate/unowned tree.
- Preserve Task 8 caught-exception preserve-for-recovery boundaries and create-only report behavior.
- P0-P2 findings block. P3-or-lower findings are GitHub Issues, not immediate fixes.

## Process

Follow Task 9 RED/GREEN with real fresh child processes. Run the full cutover glob plus CLI test, scoped Ruff, scoped Pyright. Do not run repository-wide tests. Write the report, commit only Task 9, and do not push.
