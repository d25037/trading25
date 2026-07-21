# Market v5 Hardening Task 10 Report — Phase A documentation

## Identity and boundary

- Clean base: `a2b98a2d86d592f7812616dec74cac8a9dc5b106`.
- Worktree: `/Users/mirage/dev/trading25/.worktrees/market-v5-cutover`.
- This commit is Task 10 Phase A only: active documentation, two financial-analysis
  skills, current experiment rerun clauses, and their guard tests.
- No production code, API/OpenAPI contract, generated TypeScript artifact, data,
  benchmark result, or historical experiment result changed.
- Final `main` fetch/merge, contract regeneration, whole-suite verification, final
  branch review, push, and PR transition are intentionally deferred until this
  documentation commit is independently reviewed.

## Guidance delivered

- Normative guidance now requires Market schema v5 and
  `stock_price_adjustment_mode=provider_adjusted_v1`.
- `resetBeforeSync` is documented only as maintenance for an already-compatible
  Market v5 root. Market v4/older, malformed, and adjustment-mode-incompatible roots
  name only `bt market-cutover cutover` as the isolated full-rebuild recovery path.
- The cutover runbook and invariants document durable `prepared`,
  `exchange_started`, `activated`, and `reported` states and exact same-ID recovery
  before new preparation.
- Operators are instructed not to manually mutate the journal, operation lock,
  staging, backup, active, retained-runtime, or quarantine paths.
- The two financial-analysis skills now describe provider-window/current-basis reads
  and prohibit frontend/service-local fallback from replacing the Market v5 contract.
- Twenty-one experiment READMEs changed only current rerun/adoption clauses to Market
  v5 provider-adjusted lineage. Recorded schema-v3/v4 results, benchmark evidence,
  bundle paths, measurements, and clearly historical/superseded descriptions remain
  unchanged.

## Strict RED

Only the three guard tests were edited before documentation. The focused run failed
for the intended missing guidance:

```text
3 failed

- SoT matrix still advertised reset-backed initial migration.
- Financial-analysis skills still required Market v4/local projection.
- 15 detected active rerun clauses still required Market v4/local projection.
```

Command:

```bash
uv run pytest \
  tests/unit/server/services/test_market_v4_cutover_cutover_contracts.py::test_active_market_v5_guidance_requires_cutover_and_same_id_recovery \
  tests/unit/scripts/test_audit_skills.py::test_financial_analysis_skills_use_current_market_v5_contract \
  tests/unit/scripts/test_check_research_guardrails.py::test_current_experiment_rerun_guidance_uses_market_v5_provider_contract -q
```

The experiment guard deliberately scopes itself to current rerun/adoption clauses;
it does not ban historical schema or benchmark provenance.

## GREEN and Phase A verification

The focused guard is green. The complete plan-selected guard files then passed:

```text
184 passed, 1 warning
```

Additional Phase A checks:

```text
research guardrails: OK
skill reference refresh --check: exit 0, no drift
strict legacy skill audit: passed
git diff --check: exit 0
```

The host `/usr/bin/python3` is Python 3.9.6 and cannot import stdlib `tomllib`, so the
literal `python3 scripts/check-research-guardrails.py` command fails at import time.
The same repository script passed with the project Python 3.12 runtime:

```bash
uv run --directory apps/bt python ../../scripts/check-research-guardrails.py
```

No final full suite was run in Phase A, as required. No P0-P2 or new P3-or-lower
finding was identified in this documentation scope. Deferred issues #494, #495,
#496, and #497 were not implemented.

Intended commit subject: `docs: align Market v5 safety and recovery guidance`.

## P2 remediation: exact failure disposition boundary

Review of the Phase A commit found that the runbook's blanket post-activation
automatic-restore sentence and the SoT matrix's blanket `joined failure is exact
rollback` sentence contradicted the durable activation boundary.

Strict docs RED added one focused guard before changing either document. It failed on
the missing rollback/preserve/fencing distinction:

```text
1 failed

assert 'joined failure before durable `activated` restores the exact immutable backup'
```

The corrected guidance now states exactly:

- rollback-allowed joined failure before durable `activated` restores the exact
  immutable backup;
- durable `activated` or preserve-for-recovery joined failure preserves active v5,
  exact quarantine, immutable backup, and retained recovery evidence, and requires
  the exact same-ID retry; and
- an unjoined child keeps both active and staging leases fenced, defers recovery, and
  does not authorize manual artifact edits.

The focused guard passed after only the runbook and SoT matrix were corrected. The
planned 184 documentation guards, research guardrails, skill reference drift check,
strict skill audit, focused Ruff, and `git diff --check` were rerun before the fix-only
commit. The two P3 findings remain tracked in #498 and were not changed.
