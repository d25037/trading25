# Post-Earnings Next-Day Entry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a PIT-safe research runner that answers "what happens if we buy after the earnings announcement?" while accounting for next-session execution feasibility, especially stop-limit no-fill cases.

**Architecture:** Add a new runner-first research module beside the hold-through study instead of overloading it. The event table will use only information known after the disclosure and before the next session entry decision, classify the next session's executable state, and publish separate return tables for attempted entries, executable entries, no-fill limit-up, and no-fill limit-down cases.

**Tech Stack:** Python 3.12, DuckDB, pandas, existing `research_bundle.py`, `readonly_duckdb_support.py`, JPX stop-limit helper or a small local stop-limit classifier, pytest, ruff, pyright.

---

## File Structure

- Create: `apps/bt/src/domains/analytics/post_earnings_next_day_entry.py`
  - Owns event construction, next-session execution classification, forward return calculation, summary tables, and bundle writing.
- Create: `apps/bt/scripts/research/run_post_earnings_next_day_entry.py`
  - CLI entrypoint that runs the research and writes `manifest.json + results.duckdb + summary.md`.
- Create: `apps/bt/tests/unit/domains/analytics/test_post_earnings_next_day_entry.py`
  - Unit tests for PIT timing, stop-limit no-fill classification, executable-entry returns, and outcome grouping.
- Create: `apps/bt/tests/unit/scripts/test_run_post_earnings_next_day_entry.py`
  - Runner argument and bundle payload tests.
- Create: `apps/bt/docs/experiments/market-behavior/post-earnings-next-day-entry/README.md`
  - Canonical Japanese readout with `Decision`, `Main Findings`, `Interpretation`, `Production Implication`, `Caveats`, and `Source Artifacts`.
- Modify: `apps/bt/docs/experiments/README.md`
  - Add index entry for this experiment.

## Research Design

The unit of analysis is one disclosure event from `statements`. The entry decision is made after the disclosed row is known, and the candidate entry session is the next trading session after `disclosed_date`.

Primary event timing:

- `disclosed_date`: statement disclosure date.
- `pre_event_date`: last stock trading session strictly before `disclosed_date`.
- `entry_date`: first stock trading session strictly after `disclosed_date`.
- `entry_reference_close`: `pre_event_date` close.
- `entry_open`: `entry_date` open.
- `entry_high`, `entry_low`, `entry_close`: entry session OHLC.
- `entry_executable`: true only when the research can plausibly buy on the entry session.
- `entry_price`: use `entry_open` for the first version, but exclude no-fill limit cases from executable returns.

Execution feasibility labels:

- `executable_open`: entry session opened and was not a no-fill stop-limit session.
- `limit_up_no_fill`: entry session appears pinned at the upper stop limit from open through close, so a marketable buy at open is not assumed executable.
- `limit_down_no_fill`: entry session appears pinned at the lower stop limit from open through close. This is not a buy no-fill in the same favorable direction, but must be separated because entry at open may be an artifact and risk is extreme.
- `gap_extreme_executable`: large gap, not pinned, executable but should be diagnosed separately.
- `missing_entry_session`: no usable entry OHLC.

Stop-limit handling:

- Prefer an existing repo helper if one exists for JPX stop-limit classification.
- If no reusable helper exists, implement a small local `_jpx_daily_limit_width(reference_close)` based on the standard JPX daily price-limit table and document it as a research approximation.
- A no-fill upper stop is `entry_open == entry_high == entry_low == entry_close` and `entry_close >= reference_close + limit_width * tolerance`.
- A no-fill lower stop is `entry_open == entry_high == entry_low == entry_close` and `entry_close <= reference_close - limit_width * tolerance`.
- Use a small tolerance such as `0.995` to avoid false misses from adjusted price rounding.

Primary outcome tables:

- `event_feature_df`: event-level features, disclosure outcome, entry feasibility, and forward returns.
- `execution_diagnostics_df`: counts by market, FY, event strength, and execution label.
- `post_entry_expectancy_df`: executable-entry forward returns by market, FY, event strength, pre 20d/60d bucket, and ADV/FF bucket.
- `attempted_entry_outcome_df`: same grouping but includes no-fill cases in counts and reports no-fill rates separately.
- `limit_no_fill_df`: profile and later returns of stop-limit no-fill events, but not mixed into executable-entry expectancy.

Primary questions:

- After seeing a positive event, is next-session buy better than hold-through?
- Does FY remain weak even when waiting for the disclosure?
- Are `20d/60d runup + ADV/FF >= 1% or 2%` still fragile after waiting one session?
- How often do the best-looking positive events become `limit_up_no_fill` and therefore unavailable?
- Are `limit_down_no_fill` events large enough to explain why negative events must be avoided rather than shorted naively?

## Task 1: Build Failing Domain Tests

**Files:**
- Create: `apps/bt/tests/unit/domains/analytics/test_post_earnings_next_day_entry.py`

- [ ] **Step 1: Write the test database fixture**

Create a DuckDB fixture with:

- one positive FY event with normal next-session open,
- one positive FY event with next-session no-fill stop high,
- one negative FY event with next-session no-fill stop low,
- one non-FY positive event with normal next-session open,
- `stock_data`, `topix_data`, `statements`, and `stocks`.

- [ ] **Step 2: Write PIT timing assertions**

Assert that:

```python
event["pre_event_date"] == "2024-01-09"
event["entry_date"] == "2024-01-11"
event["entry_price"] == event["entry_open"]
event["forward_return_1d_pct"] == pytest.approx((entry_close / entry_open - 1.0) * 100.0)
```

- [ ] **Step 3: Write stop-limit no-fill assertions**

Assert that:

```python
assert stop_high_event["execution_label"] == "limit_up_no_fill"
assert stop_high_event["entry_executable"] is False
assert stop_low_event["execution_label"] == "limit_down_no_fill"
assert stop_low_event["entry_executable"] is False
```

- [ ] **Step 4: Write summary table assertions**

Assert that:

```python
assert "execution_label" in result.execution_diagnostics_df.columns
assert "limit_up_no_fill_rate_pct" in result.attempted_entry_outcome_df.columns
assert "median_forward_excess_return_pct" in result.post_entry_expectancy_df.columns
assert set(result.post_entry_expectancy_df["execution_scope"]) == {"executable"}
```

- [ ] **Step 5: Run the failing tests**

Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_post_earnings_next_day_entry.py -q
```

Expected: fail because the module does not exist.

## Task 2: Implement Domain Module

**Files:**
- Create: `apps/bt/src/domains/analytics/post_earnings_next_day_entry.py`

- [ ] **Step 1: Define constants and result dataclass**

Use:

```python
POST_EARNINGS_NEXT_DAY_ENTRY_EXPERIMENT_ID = "market-behavior/post-earnings-next-day-entry"
DEFAULT_PRE_WINDOWS = (20, 60)
DEFAULT_HORIZONS = (1, 5, 20)
DEFAULT_LIQUIDITY_WINDOW = 60
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
```

Dataclass fields:

```python
event_feature_df: pd.DataFrame
execution_diagnostics_df: pd.DataFrame
post_entry_expectancy_df: pd.DataFrame
attempted_entry_outcome_df: pd.DataFrame
limit_no_fill_df: pd.DataFrame
coverage_diagnostics_df: pd.DataFrame
```

- [ ] **Step 2: Reuse the hold-through query pattern**

Use the same read-only DuckDB access, code normalization, PIT market source, price query, TOPIX query, and share-adjustment logic as `earnings_holdthrough_expectancy.py`.

- [ ] **Step 3: Implement entry-return semantics**

For each horizon:

```python
forward_return_horizon = exit_close / entry_price - 1.0
topix_return_horizon = topix_exit_close / topix_entry_date_close_or_open_proxy - 1.0
forward_excess_return_horizon = forward_return_horizon - topix_return_horizon
```

Use the first version's benchmark convention consistently and document it. Prefer `entry_date` close-to-exit close TOPIX if no TOPIX open exists.

- [ ] **Step 4: Implement execution classification**

Implement:

```python
def _classify_entry_execution(pre_close: float, entry_open: float, entry_high: float, entry_low: float, entry_close: float) -> str:
    ...
```

Return one of:

```python
"executable_open"
"limit_up_no_fill"
"limit_down_no_fill"
"gap_extreme_executable"
"missing_entry_session"
```

- [ ] **Step 5: Build summary tables**

Build:

- `execution_diagnostics_df`: market / FY / event_strength / execution_label counts.
- `post_entry_expectancy_df`: only `entry_executable == True`.
- `attempted_entry_outcome_df`: all attempted events, with no-fill rates.
- `limit_no_fill_df`: only `execution_label in ("limit_up_no_fill", "limit_down_no_fill")`.

- [ ] **Step 6: Run domain tests**

Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/domains/analytics/test_post_earnings_next_day_entry.py -q
```

Expected: pass.

## Task 3: Add Runner and Runner Tests

**Files:**
- Create: `apps/bt/scripts/research/run_post_earnings_next_day_entry.py`
- Create: `apps/bt/tests/unit/scripts/test_run_post_earnings_next_day_entry.py`

- [ ] **Step 1: Create runner**

Mirror the CLI shape of `run_earnings_holdthrough_expectancy.py` with:

```bash
--db-path
--start-date
--end-date
--pre-windows
--horizons
--liquidity-window
--severe-loss-threshold-pct
--output-root
--run-id
--notes
```

- [ ] **Step 2: Create runner tests**

Test argument parsing and mocked `main()` bundle payload emission.

- [ ] **Step 3: Run runner tests**

Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/scripts/test_run_post_earnings_next_day_entry.py -q
```

Expected: pass.

- [ ] **Step 4: Smoke help**

Run:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_post_earnings_next_day_entry.py --help
```

Expected: argparse help prints successfully.

## Task 4: Run Full Research and Prime Focus Queries

**Files:**
- No source edits unless the run exposes a bug.

- [ ] **Step 1: Run full research**

Run:

```bash
uv run --project apps/bt python apps/bt/scripts/research/run_post_earnings_next_day_entry.py \
  --start-date 2016-04-01 \
  --end-date 2026-05-14 \
  --pre-windows 20,60 \
  --horizons 1,5,20 \
  --liquidity-window 60 \
  --output-root /private/tmp/trading25-research \
  --run-id 20260515_post_earnings_next_day_initial
```

- [ ] **Step 2: Query core result slices**

Query Prime:

- positive event next-day executable,
- FY vs non-FY,
- `20d/60d runup or strong_runup`,
- `ADV/FF >= 1.0%`,
- `ADV/FF >= 2.0%`,
- no-fill rates for the same buckets.

- [ ] **Step 3: Compare against hold-through**

Use the existing hold-through bundle:

```text
/private/tmp/trading25-research/market-behavior/earnings-holdthrough-expectancy/20260515_earnings_holdthrough_exante_v2/results.duckdb
```

Compare:

- hold-through attempted before disclosure,
- post-earnings executable next-day entry,
- no-fill excluded and no-fill included attempted view.

## Task 5: Publish Canonical Readout

**Files:**
- Create: `apps/bt/docs/experiments/market-behavior/post-earnings-next-day-entry/README.md`
- Modify: `apps/bt/docs/experiments/README.md`

- [ ] **Step 1: Write canonical README**

Required sections:

- `## Published Readout`
- `### Decision`
- `### Main Findings`
- `### Interpretation`
- `### Production Implication`
- `### Caveats`
- `### Source Artifacts`

- [ ] **Step 2: State execution caveat clearly**

Include:

```text
limit_up_no_fill は executable return から除外し、attempted view では no-fill rate として別掲する。
```

- [ ] **Step 3: Update experiment index**

Add the experiment to `apps/bt/docs/experiments/README.md`.

## Task 6: Final Verification and Commit

**Files:**
- All created / modified files from Tasks 1-5.

- [ ] **Step 1: Run focused tests**

Run:

```bash
uv run --project apps/bt pytest \
  apps/bt/tests/unit/domains/analytics/test_post_earnings_next_day_entry.py \
  apps/bt/tests/unit/scripts/test_run_post_earnings_next_day_entry.py
```

- [ ] **Step 2: Run lint and typecheck**

Run:

```bash
uv run --project apps/bt ruff check \
  apps/bt/src/domains/analytics/post_earnings_next_day_entry.py \
  apps/bt/scripts/research/run_post_earnings_next_day_entry.py \
  apps/bt/tests/unit/domains/analytics/test_post_earnings_next_day_entry.py \
  apps/bt/tests/unit/scripts/test_run_post_earnings_next_day_entry.py
uv run --project apps/bt pyright \
  apps/bt/src/domains/analytics/post_earnings_next_day_entry.py \
  apps/bt/scripts/research/run_post_earnings_next_day_entry.py
```

- [ ] **Step 3: Run research guardrails**

Run:

```bash
python3 scripts/check-research-guardrails.py
python3 scripts/skills/audit_skills.py --strict-legacy
git diff --check
```

- [ ] **Step 4: Commit**

Run:

```bash
git add \
  apps/bt/src/domains/analytics/post_earnings_next_day_entry.py \
  apps/bt/scripts/research/run_post_earnings_next_day_entry.py \
  apps/bt/tests/unit/domains/analytics/test_post_earnings_next_day_entry.py \
  apps/bt/tests/unit/scripts/test_run_post_earnings_next_day_entry.py \
  apps/bt/docs/experiments/market-behavior/post-earnings-next-day-entry/README.md \
  apps/bt/docs/experiments/README.md
git commit -m "feat(bt): add post-earnings next-day entry research"
```

## Self-Review

Spec coverage:

- Post-earnings next-day buy is covered by event timing and entry-return semantics.
- Stop-limit no-fill is covered by execution labels and separate no-fill tables.
- Prime FY vs non-FY and runup / ADV buckets are covered by Task 4 queries and canonical readout.
- PIT safety is covered by pre-event date, entry date, and event outcome timing tests.

Placeholder scan:

- No `TBD`, `TODO`, or unspecified test commands remain.

Type consistency:

- The result dataclass table names match runner, tests, and docs.
- Execution labels are consistent across design, tests, and table descriptions.
