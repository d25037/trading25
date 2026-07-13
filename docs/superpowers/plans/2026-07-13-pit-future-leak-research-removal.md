# PIT Future-Leak Research Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Delete every confirmed future-leaking research surface, including retrospective wrappers around future-derived parameters, and make research CI fail closed.

**Architecture:** Remove archive exemptions first. Delete contaminated domains, runners, tests, publications, catalog entries, UI fixtures, and downstream consumers as complete dependency closures. Preserve only shared infrastructure and generic primitives with independent active consumers. Finish with referential guards and repository-wide negative scans.

**Tech Stack:** Python 3.12, pandas, pytest, Ruff, Pyright, GitHub Actions, `tomllib`, TypeScript/React, Bun.

## Global Constraints

- No compatibility aliases, archived executable paths, retrospective wrappers, or family-prefix CI exemptions remain.
- Fixed parameters selected from future returns remain contaminated even if their later calculation is future-extension stable.
- Delete helpers whose reverse dependency set becomes empty after contaminated consumers are removed.
- Preserve shared research infrastructure and independently active downside-risk and SMA primitives.
- New guard behavior follows RED-GREEN-REFACTOR; pure deletion is verified by surviving-family tests and negative scans.
- Each task commits independently and leaves focused verification green.

## Task 1: Fail-Closed Research Test Routing

**Files:**

- `scripts/ci/research-test-targets.py`
- `apps/bt/tests/unit/scripts/test_research_test_targets.py`
- `.github/workflows/ci.yml`

1. Add failing tests proving uncovered research runners and domains fall back to their complete test directories.
2. Delete `ARCHIVED_RESEARCH_PREFIXES`, `_is_archived_research_module()`, and the skip branch.
3. Make CI run the complete research guard scan.
4. Verify:

```bash
./scripts/bt-pytest.sh tests/unit/scripts/test_research_test_targets.py tests/unit/scripts/test_check_research_guardrails.py
uv run --project apps/bt ruff check scripts/ci/research-test-targets.py apps/bt/tests/unit/scripts/test_research_test_targets.py
uv run --project apps/bt python scripts/check-research-guardrails.py
```

## Task 2: Delete the Complete Fixed-3/53 Closure

**Files:**

- Delete fixed-3/53 domains, runners, domain tests, and runner tests.
- Delete Top1 derivative studies and family-local LightGBM helpers.
- Delete the three future-derived upstream streak experiment surfaces.
- Delete `topix100_streak_353_transfer`, its runner/tests, and `topix_streak_state.py`.

1. Add a failing active-surface test that detects the transfer and neutral-helper identifiers across backend, runner, publication, and web roots.
2. Delete all signal-score, next-session, open-to-close, open-to-open, Top1, and upstream streak experiment files.
3. Delete the transfer study instead of preserving it as retrospective evidence. Its fixed `3/53` pair came from future-return selection, so later PIT-stable state calculation does not make the research valid.
4. Delete `topix_streak_state.py`; it has no independent active consumer after transfer removal.
5. Remove the obsolete transfer-specific test-routing case.
6. Verify surviving generic streak primitives and the active-surface guard:

```bash
./scripts/bt-pytest.sh tests/unit/domains/analytics/test_topix_close_return_streaks.py tests/unit/domains/analytics/test_topix_extreme_close_to_close_mode.py tests/unit/scripts/test_research_test_targets.py tests/unit/scripts/test_check_research_guardrails.py
uv run --project apps/bt ruff check apps/bt/src apps/bt/tests scripts
uv run --project apps/bt pyright apps/bt/src
```

## Task 3: Delete Invalid Breadth and SMA Families

Delete matching domain, runner, domain-test, runner-test, and dedicated report files for:

```text
topix_downside_return_standard_deviation_trend_breadth_overlay
topix_downside_return_standard_deviation_shock_confirmation_vote_overlay
topix_downside_return_standard_deviation_shock_confirmation_committee_overlay
topix100_sma_ratio_rank_future_close_lightgbm
topix100_price_vs_sma_q10_bounce_regime_conditioning
topix100_sma50_raw_vs_atr_q10_bounce
```

Preserve downside-risk baselines and generic SMA modules only where an independent active consumer remains. Verify those surviving consumers before and after deletion.

## Task 4: Remove Publications and Add Referential Guarding

**Files:**

- `scripts/check-research-guardrails.py`
- `apps/bt/tests/unit/scripts/test_check_research_guardrails.py`
- `apps/bt/docs/experiments/README.md`
- `apps/bt/docs/experiments/research-catalog-metadata.toml`
- `docs/research-pit-invalidation-register.md`
- `docs/streak-point-in-time-audit-2026-04-10.md`
- Research page/detail tests under `apps/ts/packages/web/src/pages`

1. Add failing tests for missing catalog readouts and dangling related experiment IDs.
2. Implement full-scan publication integrity with invalid TOML converted to a finding.
3. Delete all contaminated readouts, including the fixed-3/53 transfer publication if present.
4. Remove complete catalog sections, index links, related IDs, and TS fixtures for deleted experiments.
5. Record only the contamination classes and deletion date; do not preserve performance headlines or rerun promises.
6. Update the streak audit to state that no fixed-3/53 retrospective or compatibility surface remains.
7. Verify:

```bash
./scripts/bt-pytest.sh tests/unit/scripts/test_check_research_guardrails.py
uv run --project apps/bt python scripts/check-research-guardrails.py
bun test apps/ts/packages/web/src/pages/ResearchPage.test.tsx apps/ts/packages/web/src/pages/ResearchDetailPage.test.tsx
```

## Task 5: Whole-Repository Verification

Run a negative scan across executable, publication, and UI roots. It must find no fixed-3/53 transfer, neutral-helper, deleted streak, breadth-overlay, or invalid SMA identifiers.

Then run:

```bash
./scripts/bt-pytest.sh tests/
uv run --project apps/bt ruff check apps/bt/src apps/bt/tests scripts
uv run --project apps/bt pyright apps/bt/src
./scripts/check-contract-sync.sh
./scripts/check-dep-direction.sh
HOME=/tmp/trading25-home bun --cwd apps/ts run workspace:test
bun --cwd apps/ts run quality:typecheck
uv run --project apps/bt python scripts/check-research-guardrails.py
git diff --check
git status --short
```

## Acceptance Criteria

- No contaminated research runner or domain entry point remains importable.
- No fixed-3/53 transfer or neutral-helper compatibility surface remains.
- No active catalog, publication, or web fixture exposes a deleted experiment.
- No legacy path accepts future-conditioned event rows as daily signals.
- Research CI has no family-prefix archive exemption and fails closed for untested research files.
- Dangling publication references fail CI.
- Repository-wide verification passes without weakening unrelated tests or guards.
