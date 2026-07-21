# Market v5 Hardening Task 2 Report

## Status

DONE

## Scope implemented

- Added `MarketSchemaStats.resetBeforeSyncEligible: bool = False`.
- Populated the field in both Market DB stats and validation responses by calling the Task 1 `is_reset_before_sync_eligible()` predicate.
- Replaced pre-v5, legacy, malformed/wrong-mode migration guidance with the exact `bt market-cutover cutover` command; incompatible roots no longer receive Initial-reset migration guidance.
- Made the Settings reset control fail closed unless both stats and validation report eligibility. Compatible Market v5 roots retain the existing typed `RESET` confirmation flow.
- Regenerated the committed OpenAPI snapshot and TypeScript client types exclusively with `bun run --filter @trading25/contracts bt:sync`.

## RED evidence

### Backend

Command:

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_db_stats_service.py \
  tests/unit/server/services/test_db_validation_service.py -q
```

Result before production changes: **5 failed, 50 passed**. Every failure was the intended missing-contract behavior: `MarketSchemaStats` had no `resetBeforeSyncEligible` field in stats, legacy, pre-v5, and wrong-adjustment-mode cases.

### Settings

Command:

```bash
cd apps/ts
bun run --filter @trading25/web test -- src/pages/SettingsPage.test.tsx
```

Result before production changes: **1 failed, 32 passed**. The incompatible-root reset switch was still enabled.

## GREEN verification

All commands below were run after the final source/test formatting and contract generation changes.

```bash
uv run --directory apps/bt pytest \
  tests/unit/server/services/test_db_stats_service.py \
  tests/unit/server/services/test_db_validation_service.py -q
```

Result: **55 passed** (one existing pytest warning).

```bash
cd apps/ts
bun run --filter @trading25/web test -- src/pages/SettingsPage.test.tsx
```

Result: **33 passed**. This includes both the compatible Market v5 typed-confirmation path and the incompatible cutover/disabled-reset path.

```bash
cd apps/ts
bun run --filter @trading25/contracts bt:check
```

Result: **PASS**. Source OpenAPI export, generated TypeScript comparison, and handwritten wire DTO duplicate detection all passed.

```bash
cd apps/bt
uv run ruff check \
  src/application/contracts/market_data_plane.py \
  src/application/services/db_stats_service.py \
  src/application/services/db_validation_service.py \
  tests/unit/server/services/test_db_stats_service.py \
  tests/unit/server/services/test_db_validation_service.py
```

Result: **All checks passed**.

```bash
cd apps/bt
uv run pyright \
  src/application/contracts/market_data_plane.py \
  src/application/services/db_stats_service.py \
  src/application/services/db_validation_service.py
```

Result: **0 errors, 0 warnings, 0 informations**.

```bash
cd apps/ts
bun run --filter @trading25/web typecheck
bunx biome check \
  packages/web/src/pages/SettingsPage.tsx \
  packages/web/src/pages/SettingsPage.test.tsx
```

Result: both commands exited 0; Biome checked both files with no fixes required.

```bash
python3 scripts/skills/refresh_skill_references.py --check
git diff --check
```

Result: both commands exited 0.

## Files changed

- `apps/bt/src/application/contracts/market_data_plane.py`
- `apps/bt/src/application/services/db_stats_service.py`
- `apps/bt/src/application/services/db_validation_service.py`
- `apps/bt/tests/unit/server/services/test_db_stats_service.py`
- `apps/bt/tests/unit/server/services/test_db_validation_service.py`
- `apps/ts/packages/contracts/openapi/bt-openapi.json` (generated)
- `apps/ts/packages/contracts/src/clients/backtest/generated/bt-api-types.ts` (generated)
- `apps/ts/packages/web/src/pages/SettingsPage.tsx`
- `apps/ts/packages/web/src/pages/SettingsPage.test.tsx`

## Residual risks

- Verification was intentionally focused per the approved policy; no repository-wide suite was run for this isolated contract/UI change.
- Settings requires agreement from both stats and validation responses and therefore disables reset while either response is missing or stale. This is deliberate fail-closed behavior.

## Reviewer remediation

The follow-up review found two remaining fail-open signals, both fixed in the
follow-up commit:

- `Warning Recovery` no longer recommends an Initial sync with live reset for
  legacy adjustment drift. It now shows the exact
  `bt market-cutover cutover` command, matching the incompatible validation
  recommendation. The Settings regression uses the real validation wording,
  asserts both command callouts, rejects the complete legacy migration phrase,
  and retains the compatible typed-confirmation test.
- Validation health now treats `resetBeforeSyncEligible=false` as a core daily
  error even when the numeric schema version is 5. Wrong adjustment mode and a
  schema-current but malformed/ineligible root therefore cannot report
  `healthy`; the existing early cutover recommendation return remains intact.

### Follow-up RED evidence

```bash
cd apps/ts
bun run --filter @trading25/web test -- src/pages/SettingsPage.test.tsx
```

Result before the UI fix: **1 failed, 32 passed**. Only one of the two required
cutover command callouts was rendered.

```bash
cd apps/bt
uv run pytest \
  tests/unit/server/services/test_db_stats_service.py \
  tests/unit/server/services/test_db_validation_service.py -q
```

Result before the health fix: **2 failed, 54 passed**. Wrong-mode and
schema-current malformed roots both returned `healthy` instead of `error`.

### Follow-up GREEN evidence

```bash
cd apps/ts
bun run --filter @trading25/web test -- src/pages/SettingsPage.test.tsx
bun run --filter @trading25/web typecheck
bunx biome check \
  packages/web/src/pages/SettingsPage.tsx \
  packages/web/src/pages/SettingsPage.test.tsx
bun run --filter @trading25/contracts bt:check
```

Results: **33 passed**; web typecheck exited 0; Biome checked both files with no
fixes; contract check passed.

```bash
cd apps/bt
uv run pytest \
  tests/unit/server/services/test_db_stats_service.py \
  tests/unit/server/services/test_db_validation_service.py -q
uv run ruff check \
  src/application/services/db_validation_service.py \
  tests/unit/server/services/test_db_validation_service.py
uv run pyright src/application/services/db_validation_service.py
```

Results: **56 passed**; Ruff passed; Pyright reported **0 errors, 0 warnings,
0 informations**. `git diff --check` also passed.
