# Task 11 Report: FundamentalsService PIT Snapshot Cutover

## Status

Implemented Task 11 on the shared `main` workspace with the requested commit
subject `refactor(bt): consume fundamentals PIT snapshots`.

## TDD evidence

The new service isolation suite was written before production changes. Its
first run collected four tests and failed all four for the expected missing
behavior:

- `FundamentalsService` did not accept the snapshot-only client.
- request dates remained strings rather than `date | None`.
- the `asOfDate`, single-snapshot isolation, and display-only `from` tests were
  blocked by the old constructor/getter path.

After the cutover, the dedicated test file passes all four tests.

## Implemented behavior

- `FundamentalsComputeRequest.from_date` and `.to_date` are `date | None`, with
  `from_date <= to_date` validation.
- `FundamentalsComputeResponse.asOfDate` is required.
- `FundamentalsService` performs exactly one
  `get_fundamentals_pit_snapshot(symbol, to_date)` read.
- Statements, adjusted statement metrics, daily valuation, OHLCV, exact-date
  stock metadata, and the Prime liquidity panel all come from that snapshot.
- Independent/current getters, adjustment-event loading, local share
  reprojection, current-basis resolution, and latest-price fallback were
  removed from the service.
- Quarterly revised EPS is taken from snapshot adjusted metrics; raw sales and
  operating-profit revisions remain non-basis calculations.
- `to` is only the snapshot knowledge cutoff. It is not passed to the fiscal
  period filter, so forecasts disclosed before the cutoff can retain a future
  period end.
- `from` is applied only after latest/revision/rolling calculations, and only
  crops returned `data` and `dailyValuation`.
- `asOfDate`, `priceBasisDate`, `valuationBasisVersion`, and provenance now use
  the snapshot's effective date, adjustment frontier, basis ID, and knowledge
  cutoff respectively.
- The OpenAPI snapshot and generated TypeScript API types were regenerated.

## Verification

- Focused service and route suites: 158 passed.
- Ruff on changed Python source/tests: passed.
- Pyright on the service and schema: 0 errors, 0 warnings.
- `bun run --filter @trading25/contracts bt:sync`: passed and updated the
  OpenAPI/TypeScript generated contracts.
- `bun run --filter @trading25/contracts typecheck`: passed.
- `python3 scripts/skills/refresh_skill_references.py --check`: passed.

## Scope and concerns

- Task 12 route/error mapping and GET/POST empty-result parity were deliberately
  left unchanged.
- `DailyValuationRequiredError` remains as the existing defensive service
  outcome if a supplied snapshot has statements but no valuation rows; the PIT
  reader normally rejects inconsistent bundles before the service boundary.
- The unrelated untracked `.codex/config.toml` was preserved and excluded.
