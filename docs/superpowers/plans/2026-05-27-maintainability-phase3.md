# Maintainability Phase 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Phase 2 後に残った上位 hotspot を、既存テストで守れる範囲に限定してさらに下げる。

**Architecture:** Phase 3 は「sync 系を広げない」「巨大永続化層 `market_db.py` には入らない」方針で、validation service、Strategy Editor orchestration、Settings page display sections の3つだけを分割する。各sliceは既存の public API/UI契約を変えず、責務別 helper/hook/component を抽出して元ファイルの巨大関数・branch集中を下げる。

**Tech Stack:** Python 3.12, FastAPI/Pydantic schemas, pytest, ruff, pyright, React 19, Vite/Vitest, Biome, TypeScript.

---

## Phase 3 Numeric Targets

Current values are from `docs/maintainability-snapshot-latest.md` after Phase 2.

| metric | phase 2 actual | phase 3 target |
| --- | ---: | ---: |
| files >= 1000 lines | 85 | <= 83 |
| functions/blocks >= 180 effective code lines | 48 | <= 46 |
| functions/blocks >= 120 effective code lines | 169 | <= 160 |
| functions/blocks branch score >= 50 | 5 | <= 3 |
| top max function/block code lines | 443 | <= 407 |
| top hotspot file score | 7,231 | <= 7,115 |

## Why This Scope

Phase 3 should not chase every visible large file. The efficient path is:

- `db_validation_service.validate_market_db`: current top function, 443 effective lines, branch score 57, direct pytest coverage exists.
- `StrategyEditor.tsx`: current second top function, 420 logic-bearing lines, branch score 82, direct Vitest coverage exists.
- `SettingsPage.tsx`: current top file hotspot, 1,067 logic-bearing lines, direct Vitest coverage exists.

Defer:

- `market_db.py`: high file score but max block is only 59; splitting persistence code is higher risk and should be a separate DB-focused phase.
- further `sync_strategies.py`: Phase 2 reached a safer pause point; only reopen if tests fail or a sync behavior change is needed.
- low-reference analytics files: do not delete or move them without runtime/workflow reachability proof.

## Files

- Modify: `apps/bt/src/application/services/db_validation_service.py`
- Create: `apps/bt/src/application/services/db_validation_payloads.py`
- Test: `apps/bt/tests/unit/server/services/test_db_validation_service.py`
- Modify: `apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx`
- Create: `apps/ts/packages/web/src/components/Backtest/useStrategyEditorDraft.ts`
- Test: `apps/ts/packages/web/src/components/Backtest/StrategyEditor.test.tsx`
- Modify: `apps/ts/packages/web/src/pages/SettingsPage.tsx`
- Create: `apps/ts/packages/web/src/pages/SettingsMarketDbPanels.tsx`
- Test: `apps/ts/packages/web/src/pages/SettingsPage.test.tsx`
- Modify: `docs/maintainability-snapshot-latest.json`
- Modify: `docs/maintainability-snapshot-latest.md`
- Modify: `docs/superpowers/plans/2026-05-27-maintainability-refactor-targets.md`

## Task 1: Split Market DB Validation Payload Assembly

**Intent:** Reduce `validate_market_db` from 443 effective lines below 300 and remove its branch score from the `>=50` bucket by moving response sub-payload construction into pure helpers.

**Files:**

- Modify: `apps/bt/src/application/services/db_validation_service.py`
- Create: `apps/bt/src/application/services/db_validation_payloads.py`
- Test: `apps/bt/tests/unit/server/services/test_db_validation_service.py`

- [ ] **Step 1: Characterize existing validation behavior**

Run:

```bash
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_db_validation_service.py -q
```

Expected: all tests pass before edits.

- [ ] **Step 2: Create payload helper module**

Create `apps/bt/src/application/services/db_validation_payloads.py` with helpers that only assemble schema objects and sample windows. Move no inspection, recommendation, or status decision logic in this step.

Required helper signatures:

```python
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from src.entrypoints.http.schemas.db import (
    AdjustmentEvent,
    DateRange,
    FundamentalsValidation,
    MarginValidation,
    Options225Validation,
    StockDataValidation,
    StockMinuteDataValidation,
    StockMasterCoverageStats,
    StockStats,
    TopixStats,
    ValidationSampleWindow,
    ValidationSampleWindows,
)
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection


def build_sample_window(*, total_count: int, returned_count: int, limit: int) -> ValidationSampleWindow:
    ...


def build_topix_stats(inspection: TimeSeriesInspection) -> TopixStats:
    ...


def build_stock_stats(*, total: int, by_market: dict[str, int]) -> StockStats:
    ...


def build_stock_data_validation(*, inspection: TimeSeriesInspection, missing_dates: list[str], missing_dates_count: int, sample_limit: int) -> StockDataValidation:
    ...


def build_stock_minute_data_validation(inspection: TimeSeriesInspection) -> StockMinuteDataValidation:
    ...


def build_options_225_validation(
    *,
    inspection: TimeSeriesInspection,
    coverage_status: str,
    allowed_topix_lag_dates: int,
    missing_topix_coverage_dates_count: int,
    missing_topix_coverage_dates: list[str],
    missing_underlying_dates_count: int,
    missing_underlying_dates: list[str],
    conflicting_underlying_dates_count: int,
    conflicting_underlying_dates: list[str],
) -> Options225Validation:
    ...


def build_margin_validation(
    *,
    inspection: TimeSeriesInspection,
    empty_skipped_count: int,
    empty_skipped_codes: list[str],
    sample_limit: int,
) -> MarginValidation:
    ...


def build_fundamentals_validation(
    *,
    inspection: TimeSeriesInspection,
    statement_codes: set[str],
    latest_disclosed: str | None,
    missing_count: int,
    missing_codes: list[str],
    alias_covered_count: int,
    empty_skipped_count: int,
    empty_skipped_codes: list[str],
    empty_skipped_sample_limit: int,
    failed_dates_count: int,
    failed_codes_count: int,
) -> FundamentalsValidation:
    ...


def build_stock_master_coverage_stats(master_coverage: dict[str, Any], *, missing_dates_count: int, missing_dates: list[str]) -> StockMasterCoverageStats:
    ...


def build_adjustment_events(events: Sequence[dict[str, Any]]) -> list[AdjustmentEvent]:
    ...


def build_validation_sample_windows(...) -> ValidationSampleWindows:
    ...
```

- [ ] **Step 3: Replace inline schema construction**

In `validate_market_db`, replace the inline construction of `TopixStats`, `StockStats`, `StockDataValidation`, `StockMinuteDataValidation`, `Options225Validation`, `MarginValidation`, `FundamentalsValidation`, `StockMasterCoverageStats`, `AdjustmentEvent`, and `ValidationSampleWindows` with calls to the new helpers. Keep recommendation/status logic in `db_validation_service.py`.

- [ ] **Step 4: Verify behavior and metric**

Run:

```bash
uv run --project apps/bt ruff check src/application/services/db_validation_service.py src/application/services/db_validation_payloads.py
uv run --project apps/bt pyright src/application/services/db_validation_service.py src/application/services/db_validation_payloads.py
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_db_validation_service.py -q
python3 scripts/maintainability_snapshot.py --json-out /tmp/phase3-task1.json --md-out /tmp/phase3-task1.md
```

Expected:

- tests pass
- `validate_market_db` effective code lines drop below 300
- `functions/blocks branch score >= 50` drops from 5 to 4

- [ ] **Step 5: Commit Task 1**

```bash
git add apps/bt/src/application/services/db_validation_service.py apps/bt/src/application/services/db_validation_payloads.py
git commit -m "refactor(bt): split market db validation payload builders"
```

## Task 2: Extract Strategy Editor Draft Orchestration Hook

**Intent:** Reduce `StrategyEditor` from 420 logic-bearing lines and branch score 82 by moving draft/YAML/validation/save orchestration into a hook. Backend validation remains the SoT.

**Files:**

- Modify: `apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx`
- Create: `apps/ts/packages/web/src/components/Backtest/useStrategyEditorDraft.ts`
- Test: `apps/ts/packages/web/src/components/Backtest/StrategyEditor.test.tsx`

- [ ] **Step 1: Characterize existing editor behavior**

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test -- StrategyEditor.test.tsx
```

Expected: existing StrategyEditor tests pass before edits.

- [ ] **Step 2: Extract hook state and handlers**

Create `useStrategyEditorDraft.ts` and move these responsibilities out of `StrategyEditor.tsx`:

- `activeTab`, `activeVisualSection`, `draftConfig`, `yamlContent`, `parseError`, `validationResult`, `previewDirty`
- `applyDraftConfig`
- context-load reset effect
- `updateDraftAtPath`, `removeDraftPath`
- `resolveCurrentConfig`
- `runBackendValidation`
- `handleSave`
- `handleOpenChange`
- `handleYamlChange`
- `handleTabChange`
- `handleCopySnippet`

The hook must accept `open`, `strategyName`, `strategyContext`, `validateStrategy`, `updateStrategy`, `onOpenChange`, and `onSuccess`, and return the state/handlers currently passed into `StrategyEditorDialogBody`.

- [ ] **Step 3: Keep UI-only derivations in the component**

Leave these in `StrategyEditor.tsx` because they are view composition, not draft lifecycle:

- signal definition grouping
- reference option lists
- `renderSharedField`
- `renderSignalSection`
- `visualSections`
- dataset info loading

- [ ] **Step 4: Verify behavior and metric**

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test -- StrategyEditor.test.tsx
bun run --filter @trading25/web typecheck
bunx biome check packages/web/src/components/Backtest/StrategyEditor.tsx packages/web/src/components/Backtest/useStrategyEditorDraft.ts
cd ../..
python3 scripts/maintainability_snapshot.py --json-out /tmp/phase3-task2.json --md-out /tmp/phase3-task2.md
```

Expected:

- tests, typecheck, and Biome pass
- `StrategyEditor.tsx` component block drops below 260 logic-bearing lines
- `functions/blocks branch score >= 50` drops from 4 to 3
- `StrategyEditor.tsx` code lines drop below 1000 if at least 50 logic-bearing lines move to the hook

- [ ] **Step 5: Commit Task 2**

```bash
git add apps/ts/packages/web/src/components/Backtest/StrategyEditor.tsx apps/ts/packages/web/src/components/Backtest/useStrategyEditorDraft.ts
git commit -m "refactor(web): extract strategy editor draft orchestration"
```

## Task 3: Split Settings Page Market DB Panels

**Intent:** Reduce the current top file hotspot by moving Market DB display panels out of `SettingsPage.tsx`, without changing sync behavior or API calls.

**Files:**

- Modify: `apps/ts/packages/web/src/pages/SettingsPage.tsx`
- Create: `apps/ts/packages/web/src/pages/SettingsMarketDbPanels.tsx`
- Test: `apps/ts/packages/web/src/pages/SettingsPage.test.tsx`

- [ ] **Step 1: Characterize existing settings behavior**

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test -- SettingsPage.test.tsx
```

Expected: existing SettingsPage tests pass before edits.

- [ ] **Step 2: Extract pure display panels**

Create `SettingsMarketDbPanels.tsx` and move display-only sections that receive already-fetched `stats`, `validation`, `activeSyncJob`, `syncJob`, `fetchDetails`, and `adjustedMetrics` data through props. Do not move mutation calls, localStorage active-job restoration, SSE subscription, reset confirmation state, or sync start/cancel handlers in this task.

Good first extraction targets:

- DuckDB snapshot metrics panel
- validation health domains panel
- options 225 diagnostics panel
- adjusted metrics status panel
- fetch strategy/details panel

- [ ] **Step 3: Keep orchestration in SettingsPage**

`SettingsPage.tsx` must continue owning:

- `useDbStats`
- `useDbValidation`
- `useStartSync`
- `useCancelSync`
- `useSyncSSE`
- active job localStorage restoration
- reset confirmation dialog state
- stock refresh input state

- [ ] **Step 4: Verify behavior and metric**

Run:

```bash
cd apps/ts
bun run --filter @trading25/web test -- SettingsPage.test.tsx
bun run --filter @trading25/web typecheck
bunx biome check packages/web/src/pages/SettingsPage.tsx packages/web/src/pages/SettingsMarketDbPanels.tsx
cd ../..
python3 scripts/maintainability_snapshot.py --json-out /tmp/phase3-task3.json --md-out /tmp/phase3-task3.md
```

Expected:

- tests, typecheck, and Biome pass
- `SettingsPage.tsx` code lines drop below 1000
- top hotspot file score drops to `market_db.py` at about 7,115 or lower
- `files >= 1000 lines` drops by at least 1 from the Task 2 result

- [ ] **Step 5: Commit Task 3**

```bash
git add apps/ts/packages/web/src/pages/SettingsPage.tsx apps/ts/packages/web/src/pages/SettingsMarketDbPanels.tsx
git commit -m "refactor(web): split settings market db panels"
```

## Task 4: Refresh Metrics and Document Phase 3

**Intent:** Make the quantitative result reproducible and update the refactor target tracker.

**Files:**

- Modify: `docs/maintainability-snapshot-latest.json`
- Modify: `docs/maintainability-snapshot-latest.md`
- Modify: `docs/superpowers/plans/2026-05-27-maintainability-refactor-targets.md`

- [ ] **Step 1: Regenerate latest snapshot**

Run:

```bash
python3 scripts/maintainability_snapshot.py \
  --json-out docs/maintainability-snapshot-latest.json \
  --md-out docs/maintainability-snapshot-latest.md
```

Expected: command exits 0 and latest snapshot shows Phase 3 target movement.

- [ ] **Step 2: Update the target tracker**

Append a Phase 3 achieved table to `docs/superpowers/plans/2026-05-27-maintainability-refactor-targets.md` using the same format as Phase 1 and Phase 2. Generate the exact measured rows with:

```bash
python3 - <<'PY'
import json
from pathlib import Path

snap = json.loads(Path("docs/maintainability-snapshot-latest.json").read_text())
rows = [
    ("files >= 1000 lines", 85, snap["file_buckets"]["critical_1000_plus"], "<= 83"),
    ("functions/blocks >= 180 effective code lines", 48, snap["function_buckets"]["critical_180_plus"], "<= 46"),
    ("functions/blocks >= 120 effective code lines", 169, snap["function_buckets"]["high_120_plus"], "<= 160"),
    ("functions/blocks branch score >= 50", 5, snap["function_buckets"]["branch_50_plus"], "<= 3"),
    ("top hotspot max function/block code lines", 443, snap["top_functions"][0]["lines"], "<= 407"),
    ("top hotspot file score", 7231, snap["top_files"][0]["hotspot_score"], "<= 7,115"),
]
print("| metric | phase 2 actual | phase 3 actual | phase 3 target |")
print("| --- | ---: | ---: | ---: |")
for metric, phase2, actual, target in rows:
    print(f"| {metric} | {phase2:,} | {actual:,} | {target} |")
PY
```

- [ ] **Step 3: Run final validation**

Run:

```bash
uv run --project apps/bt ruff check src/application/services/db_validation_service.py src/application/services/db_validation_payloads.py
uv run --project apps/bt pyright src/application/services/db_validation_service.py src/application/services/db_validation_payloads.py
uv run --project apps/bt pytest apps/bt/tests/unit/server/services/test_db_validation_service.py -q
cd apps/ts
bun run --filter @trading25/web test -- StrategyEditor.test.tsx SettingsPage.test.tsx
bun run --filter @trading25/web typecheck
bunx biome check packages/web/src/components/Backtest/StrategyEditor.tsx packages/web/src/components/Backtest/useStrategyEditorDraft.ts packages/web/src/pages/SettingsPage.tsx packages/web/src/pages/SettingsMarketDbPanels.tsx
cd ../..
python3 -m py_compile scripts/maintainability_snapshot.py
```

Expected: all commands pass.

- [ ] **Step 4: Commit Task 4**

```bash
git add docs/maintainability-snapshot-latest.json docs/maintainability-snapshot-latest.md docs/superpowers/plans/2026-05-27-maintainability-refactor-targets.md
git commit -m "docs: record maintainability phase three results"
```

## Stop Conditions

Stop and report instead of broadening scope if any of these occur:

- extracted UI hook requires changing backend validation semantics
- SettingsPage extraction requires moving sync mutations or active job recovery
- `db_validation_service.py` refactor changes any `/api/db/validate` response payload asserted by tests
- Phase 3 targets are missed after Task 3 by only one metric; do not add unrelated files just to force a green table

## Recommended Execution

Use inline execution unless a frontend/browser issue appears. The three slices touch different files, but they are small enough to review sequentially in one thread. If execution time becomes a concern, Task 1 and Task 3 can be delegated independently after Task 2 is kept in the main thread.
