# Screening PIT + Factor Regression Final Fix Report

- Date: 2026-07-14 (Asia/Tokyo)
- Base HEAD: `ba7a112773972fa551d0338db6c9c15a5757fc49`
- Scope: all four findings in `screening-pit-factor-regression-final-fix-brief.md`
- Contract/TypeScript artifacts: unchanged

## Implementation

1. Canonical Screening presets now resolve from `dataset_presets.PRESETS`.
   Legacy `prime` / `standard` / `growth` inputs remain aliases, while all
   canonical presets use their configured markets, scale inclusion/exclusion,
   and deterministic `max_stocks` behavior against the exact
   `stock_master_daily` date. `primeExTopix500` continues to subtract exact-date
   `index_membership_daily` codes. Unsupported presets raise `ValueError`.
2. Screening sector-relative inputs now derive code-to-sector mapping from the
   exact `StockUniverseItem` list already loaded for the request. The Screening
   runtime no longer calls the current `stocks_latest` sector loader.
3. OpenAPI component renames run a full collision preflight before any schema
   key or `$ref` mutation. Occupied unequal targets and conflicting sources fail
   transactionally.
4. Failure of the exact-date universe SELECT is converted to the same
   date-specific Market DB sync recovery `ValueError`, chained from the storage
   exception.

## TDD RED evidence

All tests below were added and observed failing before the corresponding
production changes.

### Finding 1: canonical presets

```bash
cd apps/bt
uv run pytest tests/unit/server/services/test_screening_service.py -q \
  -k 'canonical_screening_presets or auto_screening_collects or unsupported_name'
```

- Result: `9 failed, 22 deselected`.
- Expected reason: every canonical resolver/Auto case returned an empty set,
  and the unsupported name did not raise.

### Finding 2: historical sectors

```bash
cd apps/bt
uv run pytest tests/unit/server/services/test_screening_service_helpers.py -q \
  -k 'historical_sector or runtime_source_does_not_reference'
```

- Result: `2 failed, 33 deselected`.
- Expected reason: strategy input preparation invoked the forbidden current
  sector loader, and the Screening service source still referenced
  `load_market_stock_sector_mapping`.
- The GREEN test was strengthened to use real DuckDB rows where the same code
  has `Historical Sector` in the exact-date snapshot and `Current Sector` in
  `stocks_latest`.

### Finding 3: transactional OpenAPI rename

The initial test arrangement collided before the equal rename and therefore
passed existing behavior. It was corrected so the longer, equal portfolio
rename occurs first and the unequal factor collision occurs later, then rerun:

```bash
cd apps/bt
uv run pytest tests/unit/server/test_openapi.py -q \
  -k 'preflights_all_renames'
```

- Result: `1 failed, 21 deselected`.
- Expected reason: after the exception, both component keys and the response
  `$ref` differed from the deep-copied original.

### Finding 4: actionable query error

```bash
cd apps/bt
uv run pytest tests/unit/server/services/test_screening_service.py -q \
  -k 'wraps_exact_date_query_failure'
```

- Result: `1 failed, 30 deselected`.
- Expected reason: the raw `RuntimeError("duckdb catalog failure")` escaped
  instead of a chained actionable `ValueError`.

## Focused GREEN evidence

```bash
cd apps/bt
uv run pytest \
  tests/unit/server/services/test_screening_service.py \
  tests/unit/server/services/test_screening_service_helpers.py \
  tests/unit/server/services/test_screening_market_loader.py \
  tests/unit/server/test_openapi.py -q
```

- Result: `127 passed, 41 warnings`.

The individual post-fix groups also passed: canonical/query `10/10`,
historical-sector/runtime `3/3`, and OpenAPI collision `2/2`.

## Required integrated verification

### Prior scoped backend suite

The exact prior 453-test command from Task 6 was rerun. The 13 new regression
cases increase its collection count:

- Result: `466 passed, 41 warnings in 7.75s`.
- Failures: 0.

Adding the relevant Screening market-loader file produced:

- Result: `505 passed, 41 warnings in 7.62s`.
- Failures: 0.

### Static and contract gates

| Gate | Result |
|---|---|
| `uv run ruff check src tests/unit/application/contracts tests/unit/architecture` plus changed focused tests | `All checks passed!` |
| `uv run pyright src` | `0 errors, 0 warnings, 0 informations` |
| `./scripts/check-contract-sync.sh` | `[contract] PASS` |
| normalized committed OpenAPI diff | no diff |
| generated TypeScript diff | no diff |
| all `apps/ts` diff | no diff |
| `git diff --check` | clean |

`check-contract-sync.sh` exported and normalized FastAPI OpenAPI, regenerated
types in its check path, and matched the committed snapshots. Therefore the
public normalized OpenAPI is byte-identical and no contracts/api-clients/web
test or workspace typecheck was required by the conditional brief clause.

### Forbidden-source checks

Inverted searches across the Screening runtime path
(`screening_service.py`, `screening_input_preparation.py`, and
`screening_universe.py`) found no:

- `stocks_latest`;
- current `FROM stocks` membership query;
- `load_market_stock_sector_mapping` call/import;
- latest/current membership fallback.

The generic `load_market_stock_sector_mapping` definition remains in
`screening_market_loader.py` because non-Screening consumers (notably Signal
service) still use it; it is unreachable from the Screening runtime.

## Concerns

None. The existing pytest warnings are the same dependency/Pandas migration
warnings observed in Task 6. No OpenAPI, generated TypeScript, or frontend file
changed.
