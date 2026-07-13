# PIT Future-Leak Research Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete every confirmed future-leaking research surface while preserving the independently PIT-safe retrospective transfer study and making research CI fail closed.

**Architecture:** First remove the broad CI archive exemption. Then extract the neutral streak-state primitives required by the retained transfer study and delete the contaminated fixed-3/53 family. Delete the independent breadth and SMA contamination families next. Finally remove publication/catalog/UI references atomically with a cross-surface integrity guard.

**Tech Stack:** Python 3.12, pandas, pytest, Ruff, Pyright, GitHub Actions, `tomllib`, TypeScript/React, Bun.

## Global Constraints

- Delete contaminated implementations, runners, tests, readouts, catalog entries, and downstream bundle consumers; retain no compatibility aliases or archived executable paths.
- Preserve `topix100_streak_353_transfer` only as a retrospective event study and preserve its future-extension PIT stability tests.
- Remove legacy `segment_end_date` event-row coercion from every retained daily-signal path.
- Preserve shared research infrastructure and independently active downside-risk and SMA primitives.
- Do not rewrite dated maintainability snapshots or completed issue history.
- New behavior follows RED-GREEN-REFACTOR; pure deletion is verified by retained-family tests and negative scans.
- Each task commits independently and leaves its focused verification green.

---

### Task 1: Fail-Closed Research Test Routing

**Files:**
- Modify: `scripts/ci/research-test-targets.py`
- Modify: `apps/bt/tests/unit/scripts/test_research_test_targets.py`
- Modify: `.github/workflows/ci.yml`
- Test: `apps/bt/tests/unit/scripts/test_check_research_guardrails.py`

**Interfaces:**
- Consumes: `pytest_targets_for_research_changes(paths: list[str]) -> tuple[str, ...]`
- Produces: normal transfer test routing and conservative directory fallback for uncovered research files

- [ ] **Step 1: Add failing routing tests**

Replace the archived-prefix test with:

```python
def test_retained_streak_transfer_module_maps_to_matching_domain_test() -> None:
    module = _load_module()
    targets = module.pytest_targets_for_research_changes(
        ["apps/bt/src/domains/analytics/topix100_streak_353_transfer.py"]
    )
    assert targets == (
        "tests/unit/domains/analytics/test_topix100_streak_353_transfer.py",
    )


def test_runner_without_matching_test_falls_back_to_script_tests() -> None:
    module = _load_module()
    targets = module.pytest_targets_for_research_changes(
        ["apps/bt/scripts/research/run_uncovered_research.py"]
    )
    assert targets == ("tests/unit/scripts",)


def test_domain_without_matching_test_falls_back_to_analytics_tests() -> None:
    module = _load_module()
    targets = module.pytest_targets_for_research_changes(
        ["apps/bt/src/domains/analytics/uncovered_research.py"]
    )
    assert targets == ("tests/unit/domains/analytics",)
```

- [ ] **Step 2: Verify RED**

```bash
./scripts/bt-pytest.sh tests/unit/scripts/test_research_test_targets.py
```

Expected: all three new assertions fail because the prefix exemption or empty-target behavior remains.

- [ ] **Step 3: Implement conservative routing**

Delete `ARCHIVED_RESEARCH_PREFIXES`, `_is_archived_research_module()`, and its skip branch. Use:

```python
target = f"tests/unit/scripts/test_{module_name}.py"
targets.append(target if _exists(target) else "tests/unit/scripts")
```

and:

```python
target = f"tests/unit/domains/analytics/test_{module_name}.py"
targets.append(target if _exists(target) else "tests/unit/domains/analytics")
```

- [ ] **Step 4: Change CI to a full guard scan**

```yaml
- name: Check research guardrails
  run: python scripts/check-research-guardrails.py
```

- [ ] **Step 5: Verify GREEN and commit**

```bash
./scripts/bt-pytest.sh tests/unit/scripts/test_research_test_targets.py tests/unit/scripts/test_check_research_guardrails.py
uv run --project apps/bt ruff check scripts/ci/research-test-targets.py apps/bt/tests/unit/scripts/test_research_test_targets.py
uv run --project apps/bt python scripts/check-research-guardrails.py
git add .github/workflows/ci.yml scripts/ci/research-test-targets.py apps/bt/tests/unit/scripts/test_research_test_targets.py
git commit -m "test(bt): fail closed on research test routing"
```

Expected: tests and Ruff pass; guard prints `[research-guardrails] OK`.

### Task 2: Preserve Neutral Streak State and Delete Fixed-3/53

**Files:**
- Create: `apps/bt/src/domains/analytics/topix_streak_state.py`
- Modify: `apps/bt/src/domains/analytics/topix100_streak_353_transfer.py`
- Modify: `apps/bt/tests/unit/domains/analytics/test_topix100_streak_353_transfer.py`
- Delete: fixed-3/53, Top1 derivative, and upstream streak files listed below

**Interfaces:**
- Produces: `MODE_ORDER`, `MULTI_TIMEFRAME_STATE_ORDER`, `prepare_streak_candle_frame`, `build_sample_split_labels`, `build_mode_assignments_df`, `build_multi_timeframe_state_streak_df`
- Consumes: generic streak-table/classification utilities and existing formatting helpers

- [ ] **Step 1: Add a failing neutral-state PIT test**

Import the wished-for API:

```python
from src.domains.analytics.topix_streak_state import (
    build_mode_assignments_df,
    build_multi_timeframe_state_streak_df,
)
```

Use the existing base/future-extended streak fixtures and `assert_pit_stable_frame` to require identical date/state/segment rows through the original cutoff.

- [ ] **Step 2: Verify RED**

```bash
./scripts/bt-pytest.sh tests/unit/domains/analytics/test_topix100_streak_353_transfer.py
```

Expected: collection fails with `ModuleNotFoundError: src.domains.analytics.topix_streak_state`.

- [ ] **Step 3: Extract only neutral state construction**

Move and rename exactly:

```text
topix_streak_extreme_mode.py:
  _prepare_streak_candle_frame           -> prepare_streak_candle_frame
  _build_sample_split_labels             -> build_sample_split_labels
  _build_mode_assignments_df             -> build_mode_assignments_df
topix_streak_multi_timeframe_mode.py:
  MULTI_TIMEFRAME_STATE_ORDER            -> MULTI_TIMEFRAME_STATE_ORDER
  _build_multi_timeframe_state_streak_df -> build_multi_timeframe_state_streak_df
```

Define:

```python
MODE_ORDER: tuple[str, ...] = ("bullish", "bearish")
MULTI_TIMEFRAME_STATE_ORDER: tuple[str, ...] = (
    "long_bullish__short_bullish",
    "long_bullish__short_bearish",
    "long_bearish__short_bullish",
    "long_bearish__short_bearish",
)
```

Update transfer imports to the public neutral names. Do not move `_select_best_window_streaks` or other forward-return selection code.

- [ ] **Step 4: Verify extraction GREEN**

```bash
./scripts/bt-pytest.sh tests/unit/domains/analytics/test_topix100_streak_353_transfer.py tests/unit/scripts/test_run_topix100_streak_353_transfer.py
```

- [ ] **Step 5: Delete the contaminated closure**

Delete domain modules with these stems, their `run_*.py` runners, and matching domain/runner tests:

```text
topix100_streak_353_signal_score_lightgbm
topix100_streak_353_next_session_intraday_lightgbm
topix100_streak_353_next_session_intraday_lightgbm_walkforward
topix100_streak_353_next_session_open_to_close_5d_lightgbm
topix100_streak_353_next_session_open_to_close_5d_lightgbm_walkforward
topix100_streak_353_next_session_open_to_close_5d_excess_vs_topix_lightgbm_walkforward
topix100_streak_353_next_session_open_to_close_10d_lightgbm
topix100_streak_353_next_session_open_to_close_10d_lightgbm_walkforward
topix100_streak_353_next_session_open_to_open_5d_lightgbm
topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward
topix100_top1_open_to_open_5d_fixed_committee_overlay
topix100_top1_open_to_open_5d_duplicate_policy_analysis
```

Also delete `topix100_streak_lightgbm_feature_panel.py` and `topix100_streak_lightgbm_validation_support.py`.

Delete domain/runner/tests for `topix_streak_extreme_mode`, `topix_extreme_mode_mean_reversion_comparison`, and `topix_streak_multi_timeframe_mode`, plus `topix_streak_extreme_mode_helpers.py`. Preserve transfer and generic close-return modules.

- [ ] **Step 6: Verify and commit**

```bash
./scripts/bt-pytest.sh tests/unit/domains/analytics/test_topix100_streak_353_transfer.py tests/unit/scripts/test_run_topix100_streak_353_transfer.py tests/unit/domains/analytics/test_topix_close_return_streaks.py tests/unit/domains/analytics/test_topix_extreme_close_to_close_mode.py
uv run --project apps/bt ruff check apps/bt/src/domains/analytics/topix_streak_state.py apps/bt/src/domains/analytics/topix100_streak_353_transfer.py
uv run --project apps/bt pyright apps/bt/src/domains/analytics/topix_streak_state.py apps/bt/src/domains/analytics/topix100_streak_353_transfer.py
rg -n "topix100_streak_353_(signal_score|next_session)|topix100_top1_open_to_open_5d|topix_streak_lightgbm_(feature_panel|validation_support)" apps/bt/src apps/bt/scripts apps/bt/tests
git add -A apps/bt/src/domains/analytics apps/bt/scripts/research apps/bt/tests/unit/domains/analytics apps/bt/tests/unit/scripts
git commit -m "refactor(bt): remove future-derived streak research"
```

Expected: checks pass and `rg` has no matches.

### Task 3: Delete Invalid Breadth and SMA Families

**Files:**
- Delete: three breadth domains/runners/test pairs
- Delete: three SMA domains/runners/test pairs and the family-local LightGBM report
- Preserve: independent downside-risk baselines and generic SMA primitives

**Interfaces:**
- Consumes: Task 2 removal of Top1 consumers
- Produces: no current-membership breadth or invalid SMA executable surface

- [ ] **Step 1: Establish the retained baseline**

```bash
./scripts/bt-pytest.sh tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_exposure_timing.py tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_family_committee_walkforward.py tests/unit/domains/analytics/test_topix100_price_vs_sma_q10_bounce.py tests/unit/domains/analytics/test_topix100_price_vs_sma_rank_future_close.py tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close.py tests/unit/domains/analytics/test_topix100_sma_ratio_regime_conditioning.py
```

- [ ] **Step 2: Delete breadth files**

Delete matching domain, runner, domain-test, and runner-test files for:

```text
topix_downside_return_standard_deviation_trend_breadth_overlay
topix_downside_return_standard_deviation_shock_confirmation_vote_overlay
topix_downside_return_standard_deviation_shock_confirmation_committee_overlay
```

Preserve `topix_downside_return_standard_deviation_exposure_timing.py` and `topix_downside_return_standard_deviation_family_committee_walkforward.py`.

- [ ] **Step 3: Delete SMA files**

Delete matching domain, runner, domain-test, and runner-test files for:

```text
topix100_sma_ratio_rank_future_close_lightgbm
topix100_price_vs_sma_q10_bounce_regime_conditioning
topix100_sma50_raw_vs_atr_q10_bounce
```

Also delete `topix100_sma_ratio_rank_future_close_lightgbm_report.py`. Preserve generic SMA core, selection, support, and regime-conditioning modules with independent consumers.

- [ ] **Step 4: Verify and commit**

Repeat Step 1, then run:

```bash
rg -n "topix_downside_return_standard_deviation_(trend_breadth|shock_confirmation_(vote|committee))_overlay|topix100_sma_ratio_rank_future_close_lightgbm|topix100_price_vs_sma_q10_bounce_regime_conditioning|topix100_sma50_raw_vs_atr_q10_bounce" apps/bt/src apps/bt/scripts apps/bt/tests
git add -A apps/bt/src/domains/analytics apps/bt/scripts/research apps/bt/tests/unit/domains/analytics apps/bt/tests/unit/scripts
git commit -m "refactor(bt): remove future-leaking universe research"
```

Expected: retained tests pass and `rg` has no matches.

### Task 4: Remove Publications and Add Referential Guarding

**Files:**
- Modify: `scripts/check-research-guardrails.py`
- Modify: `apps/bt/tests/unit/scripts/test_check_research_guardrails.py`
- Modify: `apps/bt/docs/experiments/README.md`
- Modify: `apps/bt/docs/experiments/research-catalog-metadata.toml`
- Modify: `docs/research-pit-invalidation-register.md`
- Modify: `docs/streak-point-in-time-audit-2026-04-10.md`
- Modify: `apps/ts/packages/web/src/pages/ResearchPage.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/ResearchDetailPage.test.tsx`
- Delete: the 13 contaminated experiment directories

**Interfaces:**
- Produces: `scan_research_publication_integrity(repo_root: Path) -> list[Finding]`
- Consumes: existing `Finding`, experiment README layout, and catalog TOML

- [ ] **Step 1: Add failing integrity tests**

Create temp repositories and assert these exact rule names:

```python
assert {item.rule_name for item in findings} == {"catalog-readout-missing"}
assert {item.rule_name for item in findings} == {"dangling-related-experiment"}
assert {item.rule_name for item in findings} == {"domain-readout-missing"}
```

The three fixtures respectively contain: a catalog key without a README; an existing catalog/README with a missing `relatedExperiments` ID; and a literal `*_RESEARCH_EXPERIMENT_ID` domain constant without a README.

- [ ] **Step 2: Verify RED**

```bash
./scripts/bt-pytest.sh tests/unit/scripts/test_check_research_guardrails.py
```

Expected: failures because `scan_research_publication_integrity` is absent.

- [ ] **Step 3: Implement the scanner**

Use `tomllib` and all experiment `README.md` parent paths. Exclude the root experiment README. Emit:

```text
catalog key missing from README IDs             -> catalog-readout-missing
relatedExperiments item missing from README IDs -> dangling-related-experiment
literal *_RESEARCH_EXPERIMENT_ID in a domain
  missing from README IDs                       -> domain-readout-missing
```

Call this only during the full/default scan so explicit changed-file local scans remain compatible. Convert invalid TOML into a `Finding`, not an uncaught exception.

- [ ] **Step 4: Delete publications and clean references**

Delete experiment directories for these IDs:

```text
topix-streak-extreme-mode
topix-extreme-mode-mean-reversion-comparison
topix-streak-multi-timeframe-mode
topix100-streak-3-53-next-session-intraday-lightgbm-walkforward
topix100-streak-3-53-next-session-open-to-close-5d-lightgbm-walkforward
topix100-streak-3-53-next-session-open-to-close-10d-lightgbm-walkforward
topix100-streak-3-53-next-session-open-to-close-5d-excess-vs-topix-lightgbm-walkforward
topix-downside-return-standard-deviation-trend-breadth-overlay
topix-downside-return-standard-deviation-shock-confirmation-vote-overlay
topix-downside-return-standard-deviation-shock-confirmation-committee-overlay
topix100-sma-ratio-lightgbm
topix100-price-vs-sma-q10-bounce-regime-conditioning
topix100-sma50-raw-vs-atr-q10-bounce
```

Remove their index links, complete catalog sections, and all dangling `relatedExperiments`. Replace register rows with one 2026-07-13 deletion record naming only the three contamination classes. Update the streak audit to point at `topix_streak_state.py`, mark transfer retrospective-only, and remove deleted links/current guidance.

- [ ] **Step 5: Remove stale TS fixtures**

Remove deleted experiment fixtures/assertions from both Research page tests. Preserve the `market-behavior/topix100-streak-3-53-transfer` fixture.

- [ ] **Step 6: Verify and commit**

```bash
./scripts/bt-pytest.sh tests/unit/scripts/test_check_research_guardrails.py
uv run --project apps/bt python scripts/check-research-guardrails.py
bun test apps/ts/packages/web/src/pages/ResearchPage.test.tsx apps/ts/packages/web/src/pages/ResearchDetailPage.test.tsx
git add -A scripts/check-research-guardrails.py apps/bt/tests/unit/scripts/test_check_research_guardrails.py apps/bt/docs/experiments docs/research-pit-invalidation-register.md docs/streak-point-in-time-audit-2026-04-10.md apps/ts/packages/web/src/pages/ResearchPage.test.tsx apps/ts/packages/web/src/pages/ResearchDetailPage.test.tsx
git commit -m "docs(bt): remove future-leaking research publications"
```

Expected: tests pass and full guard prints `OK`.

### Task 5: Whole-Repository Verification

**Files:**
- Modify only files implicated by a concrete failing check
- Do not modify dated snapshots or completed issue history

**Interfaces:**
- Consumes: Tasks 1-4
- Produces: clean, verified repository

- [ ] **Step 1: Negative scan**

```bash
rg -n "topix100_streak_353_(signal_score|next_session)|topix100_top1_open_to_open_5d|topix_streak_lightgbm_(feature_panel|validation_support)|topix_downside_return_standard_deviation_(trend_breadth|shock_confirmation_(vote|committee))_overlay|topix100_sma_ratio_rank_future_close_lightgbm|topix100_price_vs_sma_q10_bounce_regime_conditioning|topix100_sma50_raw_vs_atr_q10_bounce" apps/bt/src apps/bt/scripts apps/bt/tests apps/ts/packages/web/src
```

Expected: no matches.

- [ ] **Step 2: Focused retained-family tests**

```bash
./scripts/bt-pytest.sh tests/unit/domains/analytics/test_topix100_streak_353_transfer.py tests/unit/scripts/test_run_topix100_streak_353_transfer.py tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_exposure_timing.py tests/unit/domains/analytics/test_topix_downside_return_standard_deviation_family_committee_walkforward.py tests/unit/domains/analytics/test_topix100_price_vs_sma_q10_bounce.py tests/unit/domains/analytics/test_topix100_price_vs_sma_rank_future_close.py tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close.py tests/unit/domains/analytics/test_topix100_sma_ratio_regime_conditioning.py tests/unit/scripts/test_research_test_targets.py tests/unit/scripts/test_check_research_guardrails.py
```

- [ ] **Step 3: Full verification**

```bash
uv run --project apps/bt pytest apps/bt/tests
uv run --project apps/bt ruff check apps/bt/src apps/bt/tests scripts
uv run --project apps/bt pyright apps/bt/src
./scripts/check-contract-sync.sh
./scripts/check-dep-direction.sh
bun --cwd apps/ts run workspace:test
bun --cwd apps/ts run quality:typecheck
uv run --project apps/bt python scripts/check-research-guardrails.py
git diff --check
git status --short
```

Expected: all checks pass; only intentional commits remain.
