# Task 5 Report: Event-Time Analytics Readers and Basis Observability

## Status

Implemented Task 5: ranking, ranking liquidity, value composite, and screening
analytics now resolve the exact ready adjustment basis containing their effective
market date before selecting adjusted rows. Missing, non-ready, ambiguous, or
under-covered lineage fails closed; raw/current-basis valuation fallbacks were
removed from these analytics paths.

## TDD Evidence

### RED

The new tests were run before production implementation and exposed the expected
gaps:

- the ranking suite could not import `resolve_ready_adjustment_bases`;
- a two-basis ranking fixture selected the wrong/global basis;
- screening returned rows from both basis versions;
- building, invalid, missing, and under-covered bases did not consistently fail
  through the `adjusted_metrics_pit` unavailable boundary;
- ranking valuation recomputed PER from raw statements/current adjustment events;
- value-composite ranking fell back to raw/current statement valuation;
- DB stats lacked retained/ready/invalid, coverage, overlap, and orphan fields;
- validation still treated retained versions as a prune condition.

### GREEN

The brief-specified focused command passed after implementation:

```text
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_ranking_service.py \
  tests/unit/server/services/test_screening_market_loader.py \
  tests/unit/server/services/test_db_validation_service.py \
  tests/unit/server/db/test_market_adjusted_metrics.py -q
175 passed, 1 warning
```

The warning is the repository's existing pytest configuration warning.

## Implementation

- Added a batch `resolve_ready_adjustment_bases` boundary that normalizes stock
  aliases, requires exactly one half-open containing basis per code, and requires
  `ready` status plus materialization coverage through the effective date.
- Ranking valuation and adjusted statement queries join an explicit
  `(normalized code, basis_id)` values relation before window/latest selection.
- Ranking universe resolution uses the exact `stock_master_daily` target date.
- Ranking valuation and value-composite readers no longer recompute missing values
  from raw statements or current adjustment events.
- Prime liquidity now projects `stock_data_raw` through the resolved basis segments
  and reads exact-basis `daily_valuation.free_float_market_cap`; it no longer uses
  current `stock_data` or raw statement share adjustment.
- Screening derives a strict reference date and joins adjusted statement rows to
  the resolved basis map. Missing adjusted storage and lineage errors are no longer
  downgraded to empty results.
- DB observability now reports retained, ready, and invalid basis counts; active
  coverage frontier; under-covered active bases; overlapping bases; and orphan
  adjusted-statement and valuation rows.
- Validation treats incomplete coverage/orphans as warnings and invalid/overlapping
  lineage as errors. Recovery points to `adjusted_metrics_pit`; retained historical
  bases are healthy and no prune recommendation remains.
- Updated the HTTP schema, OpenAPI snapshot, and generated TypeScript API types.

## Verification

- Brief-focused analytics/validation/DB suite with review regressions: `175 passed, 1 warning`.
- Basis/materializer/stats regression suite: `67 passed, 1 warning`.
- Ranking service suite after the liquidity migration: `93 passed, 1 warning`.
- Ruff across all changed Python source and test files: passed.
- Pyright across all changed Python production files: `0 errors, 0 warnings, 0 informations`.
- `bun run --filter @trading25/contracts bt:sync`: snapshot regenerated and up to date.
- `bun run quality:typecheck`: contracts, root, API clients, web, extension, and
  dependency audit passed.
- `git diff --check`: passed.

## Self-Review

- Confirmed exact basis joins occur before any row-number/latest selection.
- Confirmed 4/5-digit aliases are normalized at resolver and join boundaries.
- Confirmed empty analytics universes return empty without constructing invalid SQL.
- Confirmed a null or lagging materialization frontier fails closed.
- Confirmed retained closed bases do not trigger warnings or pruning guidance.
- Confirmed analytics readers remain read-only and do not request-time repair or
  materialize adjusted metrics.
- Confirmed no dataset snapshot or later PIT bundle/API work was introduced.

## Deferred Boundary

The existing FundamentalsService/client implicit reader boundary is intentionally
unchanged under the parent Task 10/11 split. All Task 5 ranking and screening
compatibility/latest paths are removed; the deferred boundary is not used by these
analytics readers.

## Reviewer Follow-up

The independent final review found four Important gaps and no Critical findings.
All four were fixed before commit:

- deleted the raw-statements fallback from fundamental ranking;
- made screening adjusted metrics mandatory rather than optional overrides and
  added a missing-disclosure fail-closed regression;
- counted missing, building, null-frontier, and lagging active bases as incomplete
  coverage, with a missing-active-basis regression;
- retained `period_end` and `period_type` in same-day adjusted statement
  canonicalization.

The adjacent value-composite unavailable-reason classifier was also moved from raw
statement/current-event recomputation to exact adjusted valuation/statement rows.

## Controller Review Follow-up

Implemented in a separate follow-up commit without amending the Task 5 commit.

### RED evidence

Added controller regressions for all three findings and ran the five focused cases.
The first run produced five failures:

- poisoning current `stock_data` changed exact-target ADV60 by 10,000x despite a
  post-target split regime;
- the exact valuation fallback regression did not obtain the intended fixture row
  until its target code was corrected to an existing materialized symbol;
- screening query rows did not expose `period_end`;
- a missing same-day Q1 sibling was accepted when the FY row existed;
- two distinct bases with the same `valid_from` reported zero overlaps.

After correcting the two test harness issues (existing valuation symbol and
missing-table count callback), the behavioral RED failures reproduced the direct
current-price path, date-only screening pairing, and strict-later overlap join.

### GREEN implementation

- Added one reusable exact-basis OHLCV projection CTE over `stock_data_raw` plus
  `stock_adjustment_basis_segments`.
- Moved value-composite target/symbol date lookup, target stock rows, adjusted
  valuation volume, adjusted statement universe gate, technical features, and
  profile features off current `stock_data`.
- Removed `COALESCE(v.close, stock_data.close)`; target price and volume now both
  come from the resolved raw+segment projection and cannot be repaired by current
  projection data.
- Screening query/group/coverage/override now carries and joins the complete
  `(disclosed_date, period_end, period_type)` key. Positional writes preserve two
  same-day rows independently, and a missing sibling fails closed.
- Overlap diagnostics now pair distinct basis identities once and apply general
  half-open interval intersection, including equal starts.
- Added explicit validation coverage proving an equal-start overlap yields
  `invalid_lineage` and overall `error`.

### Follow-up verification

- Task 5 focused plus basis/materializer/stats regressions: `249 passed, 1 warning`.
- Controller-specific focused cases: `5 passed, 1 warning`.
- Same-day pairing plus equal-start validation cases: `2 passed, 1 warning`.
- Ruff on all follow-up Python source/tests: passed.
- Pyright on all follow-up Python production files: `0 errors, 0 warnings`.
- `bun run --filter @trading25/contracts bt:check`: passed; schema unchanged.
- Search confirmed no `FROM/JOIN stock_data`, valuation close `COALESCE`, or
  `stock_data_dedup` remains in the value-composite target/feature query files.
- `git diff --check`: passed.

The follow-up read-only review found one remaining Important issue: target price
still came from materialized valuation close rather than the shared raw+segment
projection. It was changed to `basis_price.close`, and the regression now asserts
the projected raw close survives both a null valuation close and poisoned current
`stock_data`.
