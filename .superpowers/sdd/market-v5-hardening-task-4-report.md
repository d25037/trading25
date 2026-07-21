# Market v5 Hardening Task 4 Report

## Base and scope

- Canonical implementer: `/root/v5_merge_cross_project_audit/market_v5_task4_impl`
- Verified base: `2022c121ef0b1c8388e674dea14cc2c9bb7944b5`
- Initial worktree state: clean (`git status --short` produced no output)
- Binding brief: `.superpowers/sdd/market-v5-hardening-task-4-brief.md`
- Plan: Task 4 of `docs/superpowers/plans/2026-07-21-market-v5-review-hardening.md`
- The brief and plan agreed on exactly three implementation/test files. No fourth source or test file was required.

## Requirements checklist

- [x] An all-null provider daily row with explicit non-unit factor `0.5` reaches the existing reject path in bulk conversion.
- [x] The same row reaches the existing reject path during full-window stock refresh, before atomic replacement.
- [x] Explicit zero, negative, `NaN`, and infinite factors also reach the existing reject path in both flows.
- [x] All-null rows with an absent factor or a finite unit factor remain ordinary no-trade rows in both flows.
- [x] Raw/provider-adjusted projection semantics, provider lineage, refresh authority, and existing error/rejection contracts remain unchanged.
- [x] No compatibility read, inferred factor, current/latest fallback, or adjacent refactor was added.

## TDD evidence

No production file was edited before the focused RED run. The only pre-RED changes were the regressions in the two allowed test files.

### RED

Exact command:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_stock_data_row_builder.py \
  tests/unit/server/services/test_stock_refresh_service.py \
  -k 'no_trade and factor' -q
```

Result: exit 1; 54 collected, 40 deselected, 14 selected; **10 failed, 4 passed**.

- The five bulk conversion cases (`0.5`, zero, negative, `NaN`, infinity) each failed with `Failed: DID NOT RAISE ValueError`; the rows were silently logged as skipped.
- The five full-window refresh cases each returned `successCount == 1` instead of the required rejection (`successCount == 0`).
- The four absent/unit positive cases passed, proving the regressions isolated invalid/non-unit factor classification.

### Minimal production change

`is_provider_no_trade_row()` now requires both:

1. every raw and adjusted provider price/volume field in the existing no-trade set is null; and
2. `AdjFactor` is absent/null or coerces to exactly finite `1.0`.

All explicit non-unit or invalid factor values return false from this predicate, so the pre-existing bulk `ValueError` and refresh incomplete-row failure paths execute unchanged.

### GREEN

The exact RED command was rerun without modification.

Result: exit 0; 54 collected, 40 deselected, 14 selected; **14 passed** (one warning reported by the existing test environment).

## Final verification

Full affected test files:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_stock_data_row_builder.py \
  tests/unit/server/services/test_stock_refresh_service.py -q
```

Result: exit 0; **54 passed** (one warning) in 0.23s.

Scoped Ruff on every changed Python path:

```bash
uv run --directory apps/bt ruff check \
  src/application/services/stock_data_row_builder.py \
  tests/unit/server/services/test_stock_data_row_builder.py \
  tests/unit/server/services/test_stock_refresh_service.py
```

Result: exit 0, `All checks passed!`.

Whitespace/error-marker validation:

```bash
git diff --check
```

Result: exit 0 with no output.

Pyright was intentionally not run: the production change is confined to the existing predicate body and changes no signature, protocol, return type, or typed boundary, matching the brief's proportional-verification rule. The repository-wide suite was intentionally not run.

## Files changed

- `apps/bt/src/application/services/stock_data_row_builder.py`
- `apps/bt/tests/unit/server/services/test_stock_data_row_builder.py`
- `apps/bt/tests/unit/server/services/test_stock_refresh_service.py`
- `.superpowers/sdd/market-v5-hardening-task-4-report.md`

## Self-review and residual risk

- Existing assertions were not removed or weakened.
- The existing reject messages, retry behavior, atomic replacement boundary, provider-window metadata, and provider-adjusted consumer projection were not modified.
- The accepted absent-factor behavior includes a missing key and a null value because the existing provider boundary represents both through `dict.get(...) is None`.
- Residual risk is limited to provider encodings not represented in the regressions (for example, novel custom numeric objects). Strings and ordinary numeric provider values continue through the established `_coerce_float` normalization.

## Commit

Planned subject: `fix(bt): reject adjustment events on no-trade rows`
