# Market v5 Hardening Task 5 Report

## Base and scope

- Base: `bfba1532cfe8b95147e7e27ca9f1f76fa0d1549f`
- Production: `time_series_store.py`, `valuation_queries.py`, `market_compaction.py`
- Tests: `test_time_series_store.py`, `test_market_adjusted_metrics.py`, `test_market_compaction.py`
- This report is the only seventh task artifact.

## Requirement checklist

- [x] Identical authoritative raw replay repairs a deleted or semantically corrupt `stock_data` projection.
- [x] Projection-only repair preserves serialized `stock_data_raw` row bytes and raw `created_at`.
- [x] Raw and projection semantic deltas are classified independently, excluding `code`, `date`, and `created_at`.
- [x] The no-op gate includes projection delta; projection-only work commits and dirties only `stock_data`.
- [x] Raw-only work mutates/dirties only `stock_data_raw`; changed projection work mutates/dirties only `stock_data`.
- [x] Projection comparison and provider diagnostics include `adjustment_factor`.
- [x] Compaction rejects provider projection mismatch before candidate staging and preserves the active DB inode and bytes.
- [x] Existing transaction, provider-window lineage/frontier, and no-trade validation paths are preserved.
- [x] No compatibility fallback, latest/current fallback, implicit rebuild, or unrelated refactor was added.

## TDD evidence

No production file was edited before the following true RED runs.

Plan-exact replay RED:

```text
uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py -k repairs_missing_or_corrupt_consumer_projection -q
collected 97; selected 2; deselected 95; 2 failed
```

- DELETE replay remained `[]` instead of restoring `("7203", "2026-02-10", 1.0, 2.0, 1.0, 2.0, 100, 0.5, "2026-02-10T00:00:00+00:00")`.
- Corrupt replay retained close `1.5` and factor `0.25` instead of close `2.0` and factor `0.5`.

Cross-file focused RED:

```text
uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py tests/unit/server/db/test_market_adjusted_metrics.py tests/unit/server/db/test_market_compaction.py -k 'repairs_missing_or_corrupt_consumer_projection or factor_only_projection_drift or rejects_projection_factor_drift_before_staging' -q
collected 163; selected 4; deselected 159; 4 failed
```

- The two replay mismatches above remained.
- `providerAdjustedMismatchCount` was `0`, expected `1` for factor-only drift.
- Compaction reached `_create_candidate_staging` and triggered the injected `AssertionError`, instead of failing validation before staging.

The compaction test helper initially used the obsolete pre-Task-3 `provider_plan=` test call. It was changed within the allowed test file to pass the required explicit `ProviderStockStage`, after which the true feature RED above was reproduced. This was fixture repair, not production behavior.

Focused GREEN:

```text
uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py tests/unit/server/db/test_market_adjusted_metrics.py tests/unit/server/db/test_market_compaction.py -k 'repairs_missing_or_corrupt_consumer_projection or factor_only_projection_drift or rejects_projection_factor_drift_before_staging' -q
collected 163; selected 4; deselected 159; 4 passed

uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py -k repairs_missing_or_corrupt_consumer_projection -q
collected 97; selected 2; deselected 95; 2 passed
```

Related GREEN subsets:

```text
test_time_series_store.py -k 'replace_stock_provider_window': 10 passed, 87 deselected
test_market_adjusted_metrics.py -k 'source_diagnostics or provider_vintage': 11 passed, 7 deselected
test_market_compaction.py -k 'source_validation': 3 passed, 45 deselected
```

## Final verification

```text
uv run --directory apps/bt pytest tests/unit/server/db/test_time_series_store.py tests/unit/server/db/test_market_adjusted_metrics.py tests/unit/server/db/test_market_compaction.py -q
163 passed, 1 warning

uv run --directory apps/bt ruff check src/infrastructure/db/market/time_series_store.py src/infrastructure/db/market/valuation_queries.py src/infrastructure/db/market/market_compaction.py tests/unit/server/db/test_time_series_store.py tests/unit/server/db/test_market_adjusted_metrics.py tests/unit/server/db/test_market_compaction.py
All checks passed!

uv run --directory apps/bt pyright src/infrastructure/db/market/time_series_store.py src/infrastructure/db/market/valuation_queries.py src/infrastructure/db/market/market_compaction.py
0 errors, 0 warnings, 0 informations

git diff --check
exit 0
```

The final commands are rerun immediately before commit; their final counts are recorded in the handoff if they differ from this evidence.

## Invariants and residual risk

Projection repair uses the existing DuckDB transaction. Relation registration precedes the transaction, but all raw/projection/event/ledger/metadata mutations remain inside the single transaction, and dirty bookkeeping happens only after commit. A projection-only replay does not delete or insert raw rows. Compaction validates the active source before creating private maintenance staging, and the regression proves both inode and full file bytes remain unchanged on rejection.

No repository-wide suite was run, as required. Residual risk is limited to behavior outside the three scoped test files; the full relevant files, relation subsets, Ruff, and scoped Pyright cover the changed surfaces.

Intended commit subject: `fix(bt): repair and validate provider stock projection`
