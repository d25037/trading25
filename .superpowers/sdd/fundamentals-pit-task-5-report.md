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
