# Maintainability Phase 6 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce research-runner hotspot concentration without changing research semantics, PIT/as-of behavior, model training, bundle contracts, or published readout meaning.

**Architecture:** Keep the existing research runner modules as public entrypoints. Move low-risk output-surface responsibilities, especially portfolio summary/report construction and published-summary payload construction, into focused helper modules. Do not split core event-ledger, feature-value, or walk-forward model logic in this phase.

**Tech Stack:** Python 3.12, pandas research runners, bt research bundle helpers, pytest, ruff, pyright, research guardrail script, maintainability snapshot tooling.

---

## Phase 6 Scope

Phase 6 is a research-runner maintainability slice. Phase 5 finished the safe sync-orchestration target, so continuing sync now has lower return than reducing the current top research hotspots.

Primary targets:

- `apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py`
- `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py`

Create focused modules if the implementation stays cleaner:

- `apps/bt/src/domains/analytics/annual_first_open_last_close_portfolio.py`
- `apps/bt/src/domains/analytics/annual_first_open_last_close_report.py`
- `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm_report.py`

Out of scope:

- Changing `AnnualFirstOpenLastCloseFundamentalPanelResult` or LightGBM result dataclass fields.
- Changing `_build_event_ledger`, `_build_feature_values`, or PIT/as-of feature construction.
- Changing `_run_walkforward_research`, model training, split policy, gates, feature selection, or candidate ranking.
- Changing bundle directory names, experiment IDs, table names, or public runner script arguments.
- Adding a new `_build_published_summary()` fallback path without a documented fallback reason accepted by the research guardrail.
- Continuing sync refactor just because remaining `execute` methods are still large.

## Numeric Targets

Starting point is `docs/maintainability-snapshot-latest.md` after Phase 5:

| metric | phase 5 actual | phase 6 target |
| --- | ---: | ---: |
| repo top hotspot file score | 6,938 | <= 6,500 |
| `annual_first_open_last_close_fundamental_panel.py` hotspot score | 6,938 | <= 6,350 |
| `topix100_sma_ratio_rank_future_close_lightgbm.py` hotspot score | 6,677 | <= 6,500 |
| `annual_first_open_last_close_fundamental_panel.py` code lines | 1,990 | <= 1,750 |
| `topix100_sma_ratio_rank_future_close_lightgbm.py` code lines | 2,140 | <= 2,000 |
| functions/blocks branch score >= 50 | 2 | <= 2 |
| top max function/block code lines | 407 | unchanged or lower; do not force risky walk-forward split |

These targets deliberately prioritize file-level hotspot reduction and branch concentration. Phase 6 should not chase the `407`-line walk-forward function yet; that requires a separate characterization-heavy model workflow phase.

## Actual Results

Completed on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | phase 5 actual | phase 6 actual | phase 6 target |
| --- | ---: | ---: | ---: |
| repo top hotspot file score | 6,938 | 6,430 | <= 6,500 |
| `annual_first_open_last_close_fundamental_panel.py` hotspot score | 6,938 | 5,727 | <= 6,350 |
| `topix100_sma_ratio_rank_future_close_lightgbm.py` hotspot score | 6,677 | 5,926 | <= 6,500 |
| `annual_first_open_last_close_fundamental_panel.py` code lines | 1,990 | 1,686 | <= 1,750 |
| `topix100_sma_ratio_rank_future_close_lightgbm.py` code lines | 2,140 | 1,857 | <= 2,000 |
| functions/blocks branch score >= 50 | 2 | 2 | <= 2 |
| top max function/block code lines | 407 | 407 | unchanged or lower |

The target was met without splitting `_run_walkforward_research`. The current repo top hotspot is `market_db.py` at 6,430.

## Tasks

### Task 1: Characterize Research Baseline

**Files:**

- Test: `apps/bt/tests/unit/domains/analytics/test_annual_first_open_last_close_fundamental_panel.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py`
- Test: `apps/bt/tests/unit/scripts/test_run_topix100_sma_ratio_rank_future_close_lightgbm.py`

- [x] **Step 1: Run focused baseline tests**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_annual_first_open_last_close_fundamental_panel.py \
  apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py \
  apps/bt/tests/unit/scripts/test_run_topix100_sma_ratio_rank_future_close_lightgbm.py -q
```

Expected: tests pass before edits.

- [x] **Step 2: Capture current hotspot values**

```bash
python3 scripts/maintainability_snapshot.py \
  --json-out /tmp/trading25-maintainability-phase6-before.json \
  --md-out /tmp/trading25-maintainability-phase6-before.md
```

Expected: the top hotspots match the Phase 5 snapshot closely enough to use the targets above.

### Task 2: Extract Annual Portfolio and Report Builders

**Files:**

- Create: `apps/bt/src/domains/analytics/annual_first_open_last_close_portfolio.py`
- Create: `apps/bt/src/domains/analytics/annual_first_open_last_close_report.py`
- Modify: `apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_annual_first_open_last_close_fundamental_panel.py`

- [x] **Step 1: Move portfolio summary construction**

Move these downstream helpers out of the runner module without changing their behavior:

- `_annualized_volatility_pct`
- `_annualized_sharpe`
- `_annualized_sortino`
- `_build_annual_portfolio_daily_df`
- `_build_annual_portfolio_summary_df`

Keep the original runner call graph readable: the runner should still build the event ledger and feature summaries, then delegate annual portfolio table construction.

- [x] **Step 2: Move annual report payload construction**

Move `_fmt_num`, `_build_summary_markdown`, and `_build_published_summary` into a report helper module. Keep `write_annual_first_open_last_close_fundamental_panel_bundle()` as the public bundle writer in the original module.

- [x] **Step 3: Preserve bundle outputs**

The result tables, summary markdown content, and `published_summary.json` keys must remain equivalent. If tests only cover structure, add focused assertions for the moved report payloads rather than broad snapshot strings.

- [x] **Step 4: Verify annual runner tests**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_annual_first_open_last_close_fundamental_panel.py -q
```

Expected: tests pass.

### Task 3: Extract LightGBM Report Payload Builders

**Files:**

- Create: `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm_report.py`
- Modify: `apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py`
- Test: `apps/bt/tests/unit/scripts/test_run_topix100_sma_ratio_rank_future_close_lightgbm.py`

- [x] **Step 1: Move report-only helpers**

Move `_build_research_bundle_summary_markdown` and `_build_published_summary_payload` into the report helper module. Leave `_run_walkforward_research` and all model/data-preparation helpers in place.

- [x] **Step 2: Preserve published summary semantics**

Keep payload keys, value normalization, warning text, headline construction, and promoted-surface metadata equivalent. Do not add a new fallback summary builder.

- [x] **Step 3: Verify LightGBM tests**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py \
  apps/bt/tests/unit/scripts/test_run_topix100_sma_ratio_rank_future_close_lightgbm.py -q
```

Expected: tests pass.

### Task 4: Run Research Guardrails and Static Checks

**Files:**

- Modify only the touched research modules and tests if needed.

- [x] **Step 1: Run research guardrails**

```bash
python3 scripts/check-research-guardrails.py
```

Expected: no new research workflow violations.

- [x] **Step 2: Run lint and type checks on touched modules**

```bash
uv run --project apps/bt ruff check \
  apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py \
  apps/bt/src/domains/analytics/annual_first_open_last_close_portfolio.py \
  apps/bt/src/domains/analytics/annual_first_open_last_close_report.py \
  apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py \
  apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm_report.py
uv run --project apps/bt pyright \
  apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py \
  apps/bt/src/domains/analytics/annual_first_open_last_close_portfolio.py \
  apps/bt/src/domains/analytics/annual_first_open_last_close_report.py \
  apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py \
  apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm_report.py
```

Expected: both commands pass.

### Task 5: Re-measure and Update Maintainability Docs

**Files:**

- Modify: `docs/maintainability-snapshot-latest.json`
- Modify: `docs/maintainability-snapshot-latest.md`
- Modify: `docs/superpowers/plans/2026-05-27-maintainability-refactor-targets.md`

- [x] **Step 1: Regenerate snapshot**

```bash
python3 scripts/maintainability_snapshot.py \
  --json-out docs/maintainability-snapshot-latest.json \
  --md-out docs/maintainability-snapshot-latest.md
```

Expected: Phase 6 numeric targets are met, or any miss is documented as a deliberate avoidance of core research logic risk.

- [x] **Step 2: Record Phase 6 results**

Update `docs/superpowers/plans/2026-05-27-maintainability-refactor-targets.md` with a Phase 6 completion table after validation. Do not rewrite older phase results except to append the new phase status.

### Task 6: Final Validation Before Commit

**Files:**

- All touched files.

- [x] **Step 1: Run the full focused gate**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_annual_first_open_last_close_fundamental_panel.py \
  apps/bt/tests/unit/domains/analytics/test_topix100_sma_ratio_rank_future_close_lightgbm.py \
  apps/bt/tests/unit/scripts/test_run_topix100_sma_ratio_rank_future_close_lightgbm.py -q
python3 scripts/check-research-guardrails.py
uv run --project apps/bt ruff check \
  apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py \
  apps/bt/src/domains/analytics/annual_first_open_last_close_portfolio.py \
  apps/bt/src/domains/analytics/annual_first_open_last_close_report.py \
  apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py \
  apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm_report.py
uv run --project apps/bt pyright \
  apps/bt/src/domains/analytics/annual_first_open_last_close_fundamental_panel.py \
  apps/bt/src/domains/analytics/annual_first_open_last_close_portfolio.py \
  apps/bt/src/domains/analytics/annual_first_open_last_close_report.py \
  apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm.py \
  apps/bt/src/domains/analytics/topix100_sma_ratio_rank_future_close_lightgbm_report.py
```

Expected: all commands pass.

## Phase 6 Stop Rule

Stop Phase 6 after the report/portfolio extraction and re-measurement. If the repo top hotspot remains above target because `_run_walkforward_research` is still dominant, document it and defer that work to a later model-workflow phase with stronger characterization tests. Do not split walk-forward training in this phase just to hit a number.
