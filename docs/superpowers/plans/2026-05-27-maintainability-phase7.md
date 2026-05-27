# Maintainability Phase 7 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the next research-runner hotspot cluster without changing research outputs, PIT/as-of behavior, bundle contracts, or runner CLI behavior.

**Architecture:** Keep the existing research modules as public entrypoints and compatibility surfaces. Move data-driven bucket definitions, report builders, and markdown formatting helpers into focused modules, while re-exporting private helper names that existing tests import.

**Tech Stack:** Python 3.12, pandas research runners, bt research bundle helpers, pytest, ruff, pyright, research guardrail script, maintainability snapshot tooling.

---

## Phase 7 Decision

Phase 7 is research-runner related.

The absolute top hotspot after Phase 6 is `apps/bt/src/infrastructure/db/market/market_db.py` at `6,430`, but the next two hotspots are research runners with existing focused tests:

- `apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py` at `6,323`
- `apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py` at `6,322`

This phase intentionally targets that research cluster. `market_db.py` remains a separate Data Plane phase because its score comes from many small persistence responsibilities rather than one oversized runner section.

## Phase 7 Scope

Primary targets:

- `apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py`
- `apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py`

Create focused modules:

- `apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_buckets.py`
- `apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_report.py`
- `apps/bt/src/domains/analytics/earnings_holdthrough_expectancy_report.py`

Out of scope:

- Changing event selection, PIT/as-of filtering, ranking logic, or winner labels.
- Changing bundle experiment IDs, result dataclasses, table names, runner script args, or output directory layout.
- Splitting `topix100_sma_ratio_rank_future_close_lightgbm._run_walkforward_research`; that remains a later model-workflow phase.
- Touching `market_db.py`; that belongs to a Data Plane phase, not this research-runner slice.
- Adding new `_build_published_summary()` fallback paths without accepted guardrail comments.

## Numeric Targets

Starting point is `docs/maintainability-snapshot-latest.md` after Phase 6:

| metric | phase 6 actual | phase 7 target |
| --- | ---: | ---: |
| repo top hotspot file score | 6,430 | unchanged or lower; `market_db.py` is out of scope |
| research-runner top hotspot score | 6,323 | <= 6,200 |
| `standard_negative_eps_speculative_winner_feature_combos.py` hotspot score | 6,323 | <= 5,700 |
| `earnings_holdthrough_expectancy.py` hotspot score | 6,322 | <= 6,100 |
| `standard_negative_eps_speculative_winner_feature_combos.py` max block code lines | 235 | <= 180 |
| `standard_negative_eps_speculative_winner_feature_combos.py` code lines | 1,744 | <= 1,500 |
| `earnings_holdthrough_expectancy.py` code lines | 1,705 | <= 1,580 |
| functions/blocks branch score >= 50 | 2 | <= 2 |

If the repo top remains `market_db.py`, that is expected. Phase 7 succeeds if the research-runner cluster falls below target while tests and guardrails pass.

## Actual Results

Completed on 2026-05-27 in `docs/maintainability-snapshot-latest.md`.

| metric | phase 6 actual | phase 7 actual | phase 7 target |
| --- | ---: | ---: | ---: |
| repo top hotspot file score | 6,430 | 6,430 | unchanged or lower; `market_db.py` out of scope |
| research-runner top hotspot score | 6,323 | 5,926 | <= 6,200 |
| `standard_negative_eps_speculative_winner_feature_combos.py` hotspot score | 6,323 | 4,418 | <= 5,700 |
| `earnings_holdthrough_expectancy.py` hotspot score | 6,322 | 5,758 | <= 6,100 |
| `standard_negative_eps_speculative_winner_feature_combos.py` max block code lines | 235 | 180 | <= 180 |
| `standard_negative_eps_speculative_winner_feature_combos.py` code lines | 1,744 | 1,192 | <= 1,500 |
| `earnings_holdthrough_expectancy.py` code lines | 1,705 | 1,573 | <= 1,580 |
| functions/blocks branch score >= 50 | 2 | 2 | <= 2 |

The research-runner cluster target was met. The repo top remains `market_db.py`, which is intentionally deferred to a Data Plane phase.

## Tasks

### Task 1: Characterize Research Baseline

**Files:**

- Test: `apps/bt/tests/unit/domains/analytics/test_standard_negative_eps_speculative_winner_feature_combos.py`
- Test: `apps/bt/tests/unit/scripts/test_run_standard_negative_eps_speculative_winner_feature_combos.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_earnings_holdthrough_expectancy.py`
- Test: `apps/bt/tests/unit/scripts/test_run_earnings_holdthrough_expectancy.py`

- [x] **Step 1: Run focused baseline tests**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_standard_negative_eps_speculative_winner_feature_combos.py \
  apps/bt/tests/unit/scripts/test_run_standard_negative_eps_speculative_winner_feature_combos.py \
  apps/bt/tests/unit/domains/analytics/test_earnings_holdthrough_expectancy.py \
  apps/bt/tests/unit/scripts/test_run_earnings_holdthrough_expectancy.py -q
```

Expected: tests pass before edits.

- [x] **Step 2: Capture current hotspot values**

```bash
python3 scripts/maintainability_snapshot.py \
  --json-out /tmp/trading25-maintainability-phase7-before.json \
  --md-out /tmp/trading25-maintainability-phase7-before.md
```

Expected: `standard_negative_eps_speculative_winner_feature_combos.py` and `earnings_holdthrough_expectancy.py` remain the top research-runner hotspots.

### Task 2: Extract Standard Negative EPS Bucket Specs

**Files:**

- Create: `apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_buckets.py`
- Modify: `apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_standard_negative_eps_speculative_winner_feature_combos.py`

- [x] **Step 1: Move bucket classifiers and feature definitions**

Move these helpers into `standard_negative_eps_speculative_winner_feature_buckets.py`:

- `_bucket_entry_market_cap`
- `_bucket_entry_adv`
- `_bucket_entry_open`
- `_bucket_prior_252d_return`
- `_bucket_prior_20d_return`
- `_bucket_prior_63d_return`
- `_bucket_volume_ratio_20d`
- `_bucket_pre_entry_volatility_20d`
- `_bucket_equity_ratio`
- `_bucket_profit_margin`
- `_bucket_cfo_margin`
- `_bucket_sector_name`
- `_build_feature_bucket_def_df`

Keep the original helper names importable from `standard_negative_eps_speculative_winner_feature_combos.py` by importing/re-exporting them.

- [x] **Step 2: Reduce the moved max block**

In the new module, replace the single long `_build_feature_bucket_def_df` body with a small data-driven spec table:

```python
@dataclass(frozen=True)
class FeatureBucketSpec:
    feature_name: str
    feature_label: str
    bucket_column: str
    bucket_order: tuple[str, ...]


def build_feature_bucket_def_df(
    *,
    sparse_sector_min_event_count: int,
) -> pd.DataFrame:
    records = [
        _spec_record(spec, sparse_sector_min_event_count=sparse_sector_min_event_count)
        for spec in FEATURE_BUCKET_SPECS
    ]
    return pd.DataFrame(records, columns=_FEATURE_BUCKET_DEF_COLUMNS)
```

Expected: no function in the new bucket module is near the old 235-line block.

- [x] **Step 3: Verify standard negative EPS tests**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_standard_negative_eps_speculative_winner_feature_combos.py -q
```

Expected: tests pass, including direct imports of moved private helpers through the original module.

### Task 3: Extract Standard Negative EPS Report Builders

**Files:**

- Create: `apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_report.py`
- Modify: `apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_standard_negative_eps_speculative_winner_feature_combos.py`

- [x] **Step 1: Move report-only helpers**

Move these helpers into the report module:

- `_fmt_num`
- `_top_summary_rows`
- `_build_summary_markdown`
- `_build_published_summary`

Expose public names in the report module as:

```python
def build_summary_markdown(result: Any) -> str: ...
def build_published_summary(result: Any) -> dict[str, Any]: ...
```

Keep compatibility aliases in the original module:

```python
_build_summary_markdown = build_summary_markdown
_build_published_summary = build_published_summary
```

- [x] **Step 2: Preserve bundle output**

Change `write_standard_negative_eps_speculative_winner_feature_combos_bundle()` to call `build_summary_markdown(result)` and `build_published_summary(result)`. Do not change summary text, JSON keys, table names, or params.

- [x] **Step 3: Verify standard negative EPS runner and script tests**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_standard_negative_eps_speculative_winner_feature_combos.py \
  apps/bt/tests/unit/scripts/test_run_standard_negative_eps_speculative_winner_feature_combos.py -q
```

Expected: tests pass.

### Task 4: Extract Earnings Hold-Through Report Builders

**Files:**

- Create: `apps/bt/src/domains/analytics/earnings_holdthrough_expectancy_report.py`
- Modify: `apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py`
- Test: `apps/bt/tests/unit/domains/analytics/test_earnings_holdthrough_expectancy.py`
- Test: `apps/bt/tests/unit/scripts/test_run_earnings_holdthrough_expectancy.py`

- [x] **Step 1: Move markdown/report helpers**

Move these helpers into `earnings_holdthrough_expectancy_report.py`:

- `build_summary_markdown`
- `_top_rows_for_markdown`
- `_frame_to_markdown`
- `_format_markdown_cell`

Keep `build_summary_markdown` importable from the original module for existing tests.

- [x] **Step 2: Preserve summary markdown**

Update `write_earnings_holdthrough_expectancy_bundle()` to call the imported `build_summary_markdown(result)`. Do not change section order, headings, table formatting, or bundle table outputs.

- [x] **Step 3: Verify earnings tests**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_earnings_holdthrough_expectancy.py \
  apps/bt/tests/unit/scripts/test_run_earnings_holdthrough_expectancy.py -q
```

Expected: tests pass.

### Task 5: Run Guardrails and Static Checks

**Files:**

- All touched research modules.

- [x] **Step 1: Run research guardrails**

```bash
python3 scripts/check-research-guardrails.py
```

Expected: no new research workflow violations.

- [x] **Step 2: Run lint and type checks on touched modules**

```bash
uv run --project apps/bt ruff check \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_buckets.py \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_report.py \
  apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py \
  apps/bt/src/domains/analytics/earnings_holdthrough_expectancy_report.py
uv run --project apps/bt pyright \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_buckets.py \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_report.py \
  apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py \
  apps/bt/src/domains/analytics/earnings_holdthrough_expectancy_report.py
```

Expected: both commands pass.

### Task 6: Re-measure and Update Maintainability Docs

**Files:**

- Modify: `docs/maintainability-snapshot-latest.json`
- Modify: `docs/maintainability-snapshot-latest.md`
- Modify: `docs/superpowers/plans/2026-05-27-maintainability-refactor-targets.md`
- Modify: `docs/superpowers/plans/2026-05-27-maintainability-phase7.md`

- [x] **Step 1: Regenerate snapshot**

```bash
python3 scripts/maintainability_snapshot.py \
  --json-out docs/maintainability-snapshot-latest.json \
  --md-out docs/maintainability-snapshot-latest.md
```

Expected: Phase 7 research-runner targets are met, or a miss is documented as an intentional avoidance of research behavior changes.

- [x] **Step 2: Record Phase 7 results**

Append a Phase 7 completion table to `docs/superpowers/plans/2026-05-27-maintainability-refactor-targets.md` and add an `Actual Results` section to this plan after validation.

### Task 7: Final Validation Before Commit

**Files:**

- All touched files.

- [x] **Step 1: Run the full focused gate**

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_standard_negative_eps_speculative_winner_feature_combos.py \
  apps/bt/tests/unit/scripts/test_run_standard_negative_eps_speculative_winner_feature_combos.py \
  apps/bt/tests/unit/domains/analytics/test_earnings_holdthrough_expectancy.py \
  apps/bt/tests/unit/scripts/test_run_earnings_holdthrough_expectancy.py -q
python3 scripts/check-research-guardrails.py
uv run --project apps/bt ruff check \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_buckets.py \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_report.py \
  apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py \
  apps/bt/src/domains/analytics/earnings_holdthrough_expectancy_report.py
uv run --project apps/bt pyright \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_combos.py \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_buckets.py \
  apps/bt/src/domains/analytics/standard_negative_eps_speculative_winner_feature_report.py \
  apps/bt/src/domains/analytics/earnings_holdthrough_expectancy.py \
  apps/bt/src/domains/analytics/earnings_holdthrough_expectancy_report.py
```

Expected: all commands pass.

## Phase 7 Stop Rule

Stop after bucket/report extraction and re-measurement. If `standard_negative_eps_speculative_winner_feature_combos.py` or `earnings_holdthrough_expectancy.py` remains above target because deeper event construction would need semantic changes, document the miss and defer. Do not split LightGBM walk-forward or `market_db.py` in this phase.
